#include "capi_tester.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb.h"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/storage/table/commit_version_manager.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

#include <atomic>
#include <fstream>
#include <thread>

using namespace duckdb;
using namespace std;

static CommitHLC ReadCommitHLC(duckdb_connection connection) {
	CommitHLC result;
	REQUIRE(duckdb_get_commit_hlc(connection, &result.timestamp, &result.sequence) == DuckDBSuccess);
	return result;
}

static void RequireCommitHLC(duckdb_connection connection, CommitHLC expected) {
	auto actual = ReadCommitHLC(connection);
	REQUIRE(actual.timestamp == expected.timestamp);
	REQUIRE(actual.sequence == expected.sequence);
}

// Start Anybase changes
namespace {
struct CommitHLCCDCEvent {
	cdc_event_type type;
	string transaction_id;
};

thread_local duckdb::vector<CommitHLCCDCEvent> commit_hlc_cdc_events;

void CaptureCommitHLCCDC(cdc_event_type type, const char *transaction_id, idx_t, idx_t, idx_t *, const char *,
                        const char **, idx_t *, duckdb_data_chunk values, duckdb_data_chunk previous_values) {
	commit_hlc_cdc_events.push_back({type, transaction_id ? transaction_id : ""});
	if (values) {
		duckdb_destroy_data_chunk(&values);
	}
	if (previous_values) {
		duckdb_destroy_data_chunk(&previous_values);
	}
}
} // namespace

TEST_CASE("CDC events carry the transaction commit HLC", "[capi][commit_hlc][anybase_cdc]") {
	bool persistent = false;
	SECTION("In-memory database") {
	}
	SECTION("Persistent database and WAL recovery") {
		persistent = true;
	}
	auto path = TestCreatePath("cdc_transaction_commit_hlc.db");
	if (persistent) {
		DeleteDatabase(path);
	}
	CommitHLC committed;
	commit_hlc_cdc_events.clear();
	{
		CAPITester tester;
		REQUIRE(tester.OpenDatabase(persistent ? path.c_str() : nullptr));
		REQUIRE_NO_FAIL(tester.Query("PRAGMA disable_checkpoint_on_shutdown"));
		REQUIRE_NO_FAIL(tester.Query("SET debug_skip_checkpoint_on_commit = true"));
		REQUIRE_NO_FAIL(tester.Query("CREATE TABLE cdc_commit_hlc(i INTEGER, v INTEGER)"));
		REQUIRE_NO_FAIL(tester.Query("CREATE TABLE cdc_commit_hlc_other(i INTEGER)"));
		REQUIRE_NO_FAIL(tester.Query("INSERT INTO cdc_commit_hlc VALUES (1, 10), (2, 20)"));
		auto before = ReadCommitHLC(tester.connection);
		duckdb_set_cdc_callback(tester.database, CaptureCommitHLCCDC);
		REQUIRE(duckdb_begin_transaction(tester.connection, 12345, 99, nullptr) == DuckDBSuccess);
		REQUIRE_NO_FAIL(tester.Query("INSERT INTO cdc_commit_hlc VALUES (3, 30)"));
		REQUIRE_NO_FAIL(tester.Query("UPDATE cdc_commit_hlc SET v = 11 WHERE i = 1"));
		REQUIRE_NO_FAIL(tester.Query("DELETE FROM cdc_commit_hlc WHERE i = 2"));
		REQUIRE_NO_FAIL(tester.Query("INSERT INTO cdc_commit_hlc_other VALUES (1)"));
		REQUIRE(commit_hlc_cdc_events.empty());
		REQUIRE_NO_FAIL(tester.Query("COMMIT"));
		committed = ReadCommitHLC(tester.connection);
		REQUIRE(committed.sequence == before.sequence + 1);
		auto expected_id = to_string(committed.timestamp) + ":" + to_string(committed.sequence);
		REQUIRE(commit_hlc_cdc_events.size() >= 5);
		REQUIRE(commit_hlc_cdc_events.front().type == DUCKDB_CDC_EVENT_BEGIN_TRANSACTION);
		REQUIRE(commit_hlc_cdc_events.back().type == DUCKDB_CDC_EVENT_END_TRANSACTION);
		bool saw_insert = false;
		bool saw_update = false;
		bool saw_delete = false;
		for (auto &event : commit_hlc_cdc_events) {
			REQUIRE(event.transaction_id == expected_id);
			saw_insert |= event.type == DUCKDB_CDC_EVENT_INSERT;
			saw_update |= event.type == DUCKDB_CDC_EVENT_UPDATE;
			saw_delete |= event.type == DUCKDB_CDC_EVENT_DELETE;
		}
		REQUIRE(saw_insert);
		REQUIRE(saw_update);
		REQUIRE(saw_delete);
		commit_hlc_cdc_events.clear();
		REQUIRE_NO_FAIL(tester.Query("BEGIN"));
		REQUIRE_NO_FAIL(tester.Query("INSERT INTO cdc_commit_hlc VALUES (4, 40)"));
		REQUIRE_NO_FAIL(tester.Query("ROLLBACK"));
		REQUIRE(commit_hlc_cdc_events.empty());
		RequireCommitHLC(tester.connection, committed);
		REQUIRE_NO_FAIL(tester.Query("INSERT INTO cdc_commit_hlc VALUES (5, 50)"));
		auto next = ReadCommitHLC(tester.connection);
		REQUIRE(next.sequence == committed.sequence + 1);
		REQUIRE(next.timestamp >= committed.timestamp);
		expected_id = to_string(next.timestamp) + ":" + to_string(next.sequence);
		REQUIRE(commit_hlc_cdc_events.size() == 3);
		for (auto &event : commit_hlc_cdc_events) {
			REQUIRE(event.transaction_id == expected_id);
		}
		committed = next;
		duckdb_set_cdc_callback(tester.database, nullptr);
	}
	if (persistent) {
		CAPITester tester;
		REQUIRE(tester.OpenDatabase(path.c_str()));
		RequireCommitHLC(tester.connection, committed);
		tester.Cleanup();
		DeleteDatabase(path);
	}
	commit_hlc_cdc_events.clear();
}
// End Anybase changes

TEST_CASE("Commit HLC clamps wall time and never resets its sequence", "[commit_hlc]") {
	CommitVersionManager manager;
	REQUIRE(manager.GetCommitHLC().timestamp == 0);
	REQUIRE(manager.GetCommitHLC().sequence == 0);
	auto first = manager.NextCommitHLC(1000);
	REQUIRE(first.timestamp == 1000);
	REQUIRE(first.sequence == 1);
	REQUIRE(manager.GetCommitHLC().sequence == 0);
	manager.DidCommitTransaction(2, first);
	manager.DidCommitTransaction(2, manager.NextCommitHLC(2000));
	REQUIRE(manager.GetCommitHLC().sequence == 1);
	REQUIRE(manager.GetVersion() == 1);
	auto backwards = manager.NextCommitHLC(500);
	REQUIRE(backwards.timestamp == 1000);
	REQUIRE(backwards.sequence == 2);
	manager.DidCommitTransaction(3, backwards);
	auto forwards = manager.NextCommitHLC(3000);
	REQUIRE(forwards.timestamp == 3000);
	REQUIRE(forwards.sequence == 3);
	manager.RestoreCommitHLC(forwards);
	manager.RestoreCommitHLC(first);
	REQUIRE(manager.GetCommitHLC().sequence == 3);
	CommitHLC exhausted;
	exhausted.timestamp = 3000;
	exhausted.sequence = NumericLimits<uint64_t>::Maximum();
	manager.RestoreCommitHLC(exhausted);
	REQUIRE_THROWS(manager.NextCommitHLC(4000));
	REQUIRE(manager.GetCommitHLC().sequence == exhausted.sequence);
}

TEST_CASE("C API commit HLC advances once per successful write transaction", "[capi][commit_hlc]") {
	CAPITester tester;
	REQUIRE(tester.OpenDatabase(nullptr));
	RequireCommitHLC(tester.connection, CommitHLC());
	int64_t timestamp = -1;
	uint64_t sequence = 42;
	REQUIRE(duckdb_get_commit_hlc(nullptr, &timestamp, &sequence) == DuckDBError);
	REQUIRE(duckdb_get_commit_hlc(tester.connection, nullptr, &sequence) == DuckDBError);
	REQUIRE(duckdb_get_commit_hlc(tester.connection, &timestamp, nullptr) == DuckDBError);
	REQUIRE(timestamp == -1);
	REQUIRE(sequence == 42);

	auto before_us = Timestamp::GetEpochMicroSeconds(Timestamp::GetCurrentTimestamp());
	REQUIRE_NO_FAIL(tester.Query("CREATE TABLE hlc_test(i INTEGER PRIMARY KEY)"));
	auto created = ReadCommitHLC(tester.connection);
	REQUIRE(created.sequence == 1);
	REQUIRE(created.timestamp >= before_us);
	REQUIRE(created.timestamp <= Timestamp::GetEpochMicroSeconds(Timestamp::GetCurrentTimestamp()));
	REQUIRE_NO_FAIL(tester.Query("BEGIN"));
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO hlc_test VALUES (1), (2)"));
	REQUIRE_NO_FAIL(tester.Query("CREATE TABLE hlc_other(i INTEGER)"));
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO hlc_other VALUES (1)"));
	RequireCommitHLC(tester.connection, created);
	REQUIRE_NO_FAIL(tester.Query("COMMIT"));
	auto committed = ReadCommitHLC(tester.connection);
	REQUIRE(committed.sequence == created.sequence + 1);
	REQUIRE(committed.timestamp >= created.timestamp);

	REQUIRE_NO_FAIL(tester.Query("BEGIN"));
	REQUIRE_NO_FAIL(tester.Query("UPDATE hlc_test SET i = i + 10"));
	REQUIRE_NO_FAIL(tester.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(tester.Query("BEGIN"));
	REQUIRE_NO_FAIL(tester.Query("SELECT * FROM hlc_test"));
	REQUIRE_NO_FAIL(tester.Query("COMMIT"));
	REQUIRE_NO_FAIL(tester.Query("BEGIN"));
	REQUIRE_NO_FAIL(tester.Query("COMMIT"));
	REQUIRE(tester.Query("INSERT INTO hlc_test VALUES (1)")->HasError());
	RequireCommitHLC(tester.connection, committed);
	REQUIRE_NO_FAIL(tester.Query("UPDATE hlc_test SET i = i + 10"));
	REQUIRE(ReadCommitHLC(tester.connection).sequence == committed.sequence + 1);
	REQUIRE_NO_FAIL(tester.Query("DELETE FROM hlc_test"));
	REQUIRE(ReadCommitHLC(tester.connection).sequence == committed.sequence + 2);
	REQUIRE_NO_FAIL(tester.Query("DROP TABLE hlc_test"));
	REQUIRE(ReadCommitHLC(tester.connection).sequence == committed.sequence + 3);
}

TEST_CASE("C API commit HLC follows the selected database", "[capi][commit_hlc]") {
	CAPITester tester;
	REQUIRE(tester.OpenDatabase(nullptr));
	REQUIRE_NO_FAIL(tester.Query("CREATE TABLE main_table(i INTEGER)"));
	auto main_hlc = ReadCommitHLC(tester.connection);
	REQUIRE_NO_FAIL(tester.Query("ATTACH ':memory:' AS other_hlc"));
	REQUIRE_NO_FAIL(tester.Query("USE other_hlc"));
	RequireCommitHLC(tester.connection, CommitHLC());
	REQUIRE_NO_FAIL(tester.Query("CREATE TABLE other_table(i INTEGER)"));
	REQUIRE(ReadCommitHLC(tester.connection).sequence == 1);
	REQUIRE_NO_FAIL(tester.Query("USE memory"));
	RequireCommitHLC(tester.connection, main_hlc);
}

TEST_CASE("C API commit HLC survives WAL recovery and checkpoints", "[capi][commit_hlc]") {
	auto path = TestCreatePath("commit_hlc_recovery.db");
	DeleteDatabase(path);
	CommitHLC expected;
	{
		CAPITester tester;
		REQUIRE(tester.OpenDatabase(path.c_str()));
		REQUIRE_NO_FAIL(tester.Query("PRAGMA disable_checkpoint_on_shutdown"));
		SECTION("Recover from WAL only") {
			REQUIRE_NO_FAIL(tester.Query("SET debug_skip_checkpoint_on_commit = true"));
		}
		SECTION("Recover from checkpoint and later WAL") {
			REQUIRE_NO_FAIL(tester.Query("CREATE TABLE checkpoint_table(i INTEGER)"));
			REQUIRE_NO_FAIL(tester.Query("CHECKPOINT"));
		}
		SECTION("Commit using an automatic checkpoint with WAL") {
			REQUIRE_NO_FAIL(tester.Query("SET checkpoint_threshold = '0B'"));
			REQUIRE_NO_FAIL(tester.Query("SET auto_checkpoint_skip_wal_threshold = 18446744073709551615"));
		}
		SECTION("Commit using an automatic checkpoint without WAL") {
			REQUIRE_NO_FAIL(tester.Query("SET checkpoint_threshold = '0B'"));
			REQUIRE_NO_FAIL(tester.Query("SET debug_skip_checkpoint_on_commit = false"));
			REQUIRE_NO_FAIL(tester.Query("SET auto_checkpoint_skip_wal_threshold = 0"));
		}
		REQUIRE_NO_FAIL(tester.Query("CREATE TABLE recovered_hlc(i INTEGER)"));
		REQUIRE_NO_FAIL(tester.Query("INSERT INTO recovered_hlc VALUES (42)"));
		expected = ReadCommitHLC(tester.connection);
		REQUIRE(expected.sequence >= 2);
	}
	{
		CAPITester tester;
		REQUIRE(tester.OpenDatabase(path.c_str()));
		REQUIRE_NO_FAIL(tester.Query("PRAGMA disable_checkpoint_on_shutdown"));
		RequireCommitHLC(tester.connection, expected);
		REQUIRE_NO_FAIL(tester.Query("SELECT * FROM recovered_hlc"));
		RequireCommitHLC(tester.connection, expected);
	}
	{
		CAPITester tester;
		REQUIRE(tester.OpenDatabase(path.c_str()));
		RequireCommitHLC(tester.connection, expected);
		REQUIRE_NO_FAIL(tester.Query("INSERT INTO recovered_hlc VALUES (43)"));
		auto next = ReadCommitHLC(tester.connection);
		REQUIRE(next.sequence == expected.sequence + 1);
		REQUIRE(next.timestamp >= expected.timestamp);
		expected = next;
		REQUIRE_NO_FAIL(tester.Query("CHECKPOINT"));
		RequireCommitHLC(tester.connection, expected);
	}
	{
		CAPITester tester;
		REQUIRE(tester.OpenDatabase(path.c_str()));
		RequireCommitHLC(tester.connection, expected);
	}
	{
		CAPITester tester;
		duckdb_config config;
		REQUIRE(duckdb_create_config(&config) == DuckDBSuccess);
		REQUIRE(duckdb_set_config(config, "access_mode", "READ_ONLY") == DuckDBSuccess);
		auto state = duckdb_open_ext(path.c_str(), &tester.database, config, nullptr);
		duckdb_destroy_config(&config);
		REQUIRE(state == DuckDBSuccess);
		REQUIRE(duckdb_connect(tester.database, &tester.connection) == DuckDBSuccess);
		RequireCommitHLC(tester.connection, expected);
	}
	DeleteDatabase(path);
}

TEST_CASE("HLC exhaustion rolls back staged WAL data", "[commit_hlc]") {
	auto path = TestCreatePath("commit_hlc_exhausted.db");
	DeleteDatabase(path);
	CommitHLC durable;
	{
		DuckDB db(path);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("PRAGMA disable_checkpoint_on_shutdown"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE exhausted_hlc(i INTEGER)"));
		auto attached =
		    db.instance->GetDatabaseManager().GetDatabase(DatabaseManager::GetDefaultDatabase(*con.context));
		REQUIRE(attached);
		auto &manager = DuckTransactionManager::Get(*attached);
		durable = manager.GetCommitHLC();
		auto exhausted = durable;
		exhausted.sequence = NumericLimits<uint64_t>::Maximum();
		manager.RestoreCommitHLC(exhausted);
		auto result = con.Query("INSERT INTO exhausted_hlc VALUES (1)");
		REQUIRE(result->HasError());
		REQUIRE(result->GetError().find("commit HLC sequence exhausted") != string::npos);
		REQUIRE(manager.GetCommitHLC().sequence == exhausted.sequence);
		result = con.Query("SELECT count(*) FROM exhausted_hlc");
		REQUIRE_NO_FAIL(*result);
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
	}
	{
		CAPITester tester;
		REQUIRE(tester.OpenDatabase(path.c_str()));
		RequireCommitHLC(tester.connection, durable);
		auto result = tester.Query("SELECT count(*) FROM exhausted_hlc");
		REQUIRE_NO_FAIL(*result);
		REQUIRE(result->Fetch<int64_t>(0, 0) == 0);
	}
	DeleteDatabase(path);
}

TEST_CASE("A concurrent checkpoint stores the HLC of its data snapshot", "[commit_hlc]") {
	auto path = TestCreatePath("commit_hlc_concurrent_checkpoint.db");
	auto snapshot_path = TestCreatePath("commit_hlc_checkpoint_snapshot.db");
	DeleteDatabase(path);
	DeleteDatabase(snapshot_path);
	CommitHLC checkpoint_hlc;
	CommitHLC latest_hlc;
	{
		DuckDB db(path);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("PRAGMA disable_checkpoint_on_shutdown"));
		REQUIRE_NO_FAIL(con.Query("SET checkpoint_threshold = '1GB'"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE checkpoint_hlc(i INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO checkpoint_hlc VALUES (1)"));
		REQUIRE_NO_FAIL(con.Query("SET debug_checkpoint_sleep_ms = 200"));
		auto attached =
		    db.instance->GetDatabaseManager().GetDatabase(DatabaseManager::GetDefaultDatabase(*con.context));
		REQUIRE(attached);
		auto &manager = DuckTransactionManager::Get(*attached);
		checkpoint_hlc = manager.GetCommitHLC();
		std::atomic<bool> done {false};
		bool checkpoint_failed = false;
		std::thread checkpoint([&]() {
			Connection checkpoint_con(db);
			checkpoint_failed = checkpoint_con.Query("CHECKPOINT")->HasError();
			done = true;
		});
		while (manager.GetActiveCheckpoint() == MAX_TRANSACTION_ID && !done) {
			std::this_thread::yield();
		}
		bool overlapped = !done;
		auto insert = con.Query("INSERT INTO checkpoint_hlc VALUES (2)");
		checkpoint.join();
		REQUIRE(overlapped);
		REQUIRE_FALSE(checkpoint_failed);
		REQUIRE_NO_FAIL(*insert);
		latest_hlc = manager.GetCommitHLC();
		REQUIRE(latest_hlc.sequence == checkpoint_hlc.sequence + 1);
	}
	{
		std::ifstream source(path, std::ios::binary);
		std::ofstream target(snapshot_path, std::ios::binary);
		target << source.rdbuf();
		REQUIRE(source.good());
		REQUIRE(target.good());
	}
	{
		CAPITester tester;
		REQUIRE(tester.OpenDatabase(snapshot_path.c_str()));
		RequireCommitHLC(tester.connection, checkpoint_hlc);
		auto result = tester.Query("SELECT count(*) FROM checkpoint_hlc");
		REQUIRE_NO_FAIL(*result);
		REQUIRE(result->Fetch<int64_t>(0, 0) == 1);
	}
	{
		CAPITester tester;
		REQUIRE(tester.OpenDatabase(path.c_str()));
		RequireCommitHLC(tester.connection, latest_hlc);
		auto result = tester.Query("SELECT count(*) FROM checkpoint_hlc");
		REQUIRE_NO_FAIL(*result);
		REQUIRE(result->Fetch<int64_t>(0, 0) == 2);
	}
	DeleteDatabase(path);
	DeleteDatabase(snapshot_path);
}

TEST_CASE("An incomplete WAL commit does not advance recovered HLC", "[capi][commit_hlc]") {
	auto path = TestCreatePath("commit_hlc_torn_wal.db");
	DeleteDatabase(path);
	CommitHLC expected;
	{
		CAPITester tester;
		REQUIRE(tester.OpenDatabase(path.c_str()));
		REQUIRE_NO_FAIL(tester.Query("PRAGMA disable_checkpoint_on_shutdown"));
		REQUIRE_NO_FAIL(tester.Query("SET debug_skip_checkpoint_on_commit = true"));
		REQUIRE_NO_FAIL(tester.Query("CREATE TABLE torn_hlc(i INTEGER)"));
		REQUIRE_NO_FAIL(tester.Query("INSERT INTO torn_hlc VALUES (1)"));
		expected = ReadCommitHLC(tester.connection);
		REQUIRE_NO_FAIL(tester.Query("INSERT INTO torn_hlc VALUES (2)"));
		REQUIRE(ReadCommitHLC(tester.connection).sequence == expected.sequence + 1);
	}
	{
		auto fs = FileSystem::CreateLocal();
		auto wal = fs->OpenFile(path + ".wal", FileFlags::FILE_FLAGS_WRITE);
		fs->Truncate(*wal, fs->GetFileSize(*wal) - 1);
	}
	{
		CAPITester tester;
		REQUIRE(tester.OpenDatabase(path.c_str()));
		RequireCommitHLC(tester.connection, expected);
		auto result = tester.Query("SELECT count(*) FROM torn_hlc");
		REQUIRE_NO_FAIL(*result);
		REQUIRE(result->Fetch<int64_t>(0, 0) == 1);
	}
	DeleteDatabase(path);
}

TEST_CASE("Concurrent commits share a coherent database HLC", "[capi][commit_hlc]") {
	CAPITester tester;
	REQUIRE(tester.OpenDatabase(nullptr));
	REQUIRE_NO_FAIL(tester.Query("CREATE TABLE concurrent_hlc(i INTEGER)"));
	auto initial = ReadCommitHLC(tester.connection);
	std::atomic<bool> failed {false};
	std::atomic<idx_t> finished {0};
	duckdb::vector<std::thread> writers;
	for (idx_t t = 0; t < 4; t++) {
		writers.emplace_back([&]() {
			duckdb_connection connection = nullptr;
			if (duckdb_connect(tester.database, &connection) != DuckDBSuccess) {
				failed = true;
			} else {
				for (idx_t i = 0; i < 25; i++) {
					if (duckdb_query(connection, "INSERT INTO concurrent_hlc VALUES (1)", nullptr) != DuckDBSuccess) {
						failed = true;
						break;
					}
				}
				duckdb_disconnect(&connection);
			}
			finished++;
		});
	}
	auto previous = initial;
	while (finished < 4) {
		CommitHLC current;
		if (duckdb_get_commit_hlc(tester.connection, &current.timestamp, &current.sequence) != DuckDBSuccess ||
		    current.timestamp < previous.timestamp || current.sequence < previous.sequence ||
		    (current.sequence == previous.sequence && current.timestamp != previous.timestamp)) {
			failed = true;
		}
		previous = current;
		std::this_thread::yield();
	}
	for (auto &writer : writers) {
		writer.join();
	}
	REQUIRE_FALSE(failed.load());
	REQUIRE(ReadCommitHLC(tester.connection).sequence == initial.sequence + 100);
}
// End Anybase changes


TEST_CASE("Batch column versions preserve ordering and scalar behavior", "[capi][cAnybaseApi]") {
	CAPITester tester;
	REQUIRE(tester.OpenDatabase(nullptr));
	REQUIRE_NO_FAIL(tester.Query("CREATE SCHEMA versions_schema"));
	REQUIRE_NO_FAIL(tester.Query(
	    "CREATE TABLE versions_schema.versions(i INTEGER, v INTEGER, \"caf\xC3\xA9\" INTEGER)"));
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO versions_schema.versions VALUES (1, 2, 3)"));
	REQUIRE_NO_FAIL(tester.Query("UPDATE versions_schema.versions SET v = 4"));

	const char *columns[] = {"v", "I", "caf\xC3\xA9", "missing", "v"};
	idx_t versions[5] = {};
	idx_t table_version = 0;
	char *error = nullptr;
	REQUIRE(duckdb_get_table_and_column_versions(tester.connection, "versions_schema", "versions", columns, 5, versions,
	                                  &table_version, &error) == DuckDBSuccess);
	REQUIRE(error == nullptr);
	REQUIRE(table_version == duckdb_get_table_version(tester.connection, "versions_schema", "versions", &error));
	REQUIRE(error == nullptr);
	REQUIRE(table_version > 0);
	for (idx_t i = 0; i < 5; i++) {
		REQUIRE(versions[i] ==
		        duckdb_get_column_version(tester.connection, "versions_schema", "versions", columns[i], &error));
		REQUIRE(error == nullptr);
	}
	REQUIRE(versions[0] > versions[1]);
	REQUIRE(versions[0] == versions[4]);
	REQUIRE(versions[3] == 0);

	duckdb::vector<const char *> many_columns(4096, "v");
	duckdb::vector<idx_t> many_versions(many_columns.size());
	REQUIRE(duckdb_get_table_and_column_versions(tester.connection, "versions_schema", "versions", many_columns.data(),
	                                  many_columns.size(), many_versions.data(), &table_version, &error) ==
	        DuckDBSuccess);
	REQUIRE(error == nullptr);
	for (auto version : many_versions) {
		REQUIRE(version == versions[0]);
	}
}

TEST_CASE("Batch column versions use one transaction and reuse active transactions", "[capi][cAnybaseApi]") {
	CAPITester tester;
	REQUIRE(tester.OpenDatabase(nullptr));
	REQUIRE_NO_FAIL(tester.Query("CREATE TABLE versions(i INTEGER, v INTEGER)"));
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO versions VALUES (1, 2)"));
	auto &context = *reinterpret_cast<Connection *>(tester.connection)->context;
	auto transaction_id = [&]() {
		transaction_t id = 0;
		context.RunFunctionInTransaction([&]() {
			auto &catalog = Catalog::GetCatalog(context, INVALID_CATALOG);
			id = DuckTransaction::Get(context, catalog).transaction_id;
		});
		return id;
	};
	const char *columns[] = {"v", "i", "v", "missing"};
	idx_t versions[4];
	idx_t table_version = 0;
	auto before = transaction_id();
	REQUIRE(duckdb_get_table_and_column_versions(tester.connection, nullptr, "versions", columns, 4, versions, &table_version,
	                                  nullptr) == DuckDBSuccess);
	REQUIRE_FALSE(context.transaction.HasActiveTransaction());
	REQUIRE(transaction_id() == before + 2);
	REQUIRE(table_version == 1);
	before = transaction_id();
	table_version = 0;
	REQUIRE(duckdb_get_table_and_column_versions(tester.connection, nullptr, "versions", nullptr, 0, nullptr, &table_version,
	                                  nullptr) == DuckDBSuccess);
	REQUIRE(transaction_id() == before + 2);
	REQUIRE_FALSE(context.transaction.HasActiveTransaction());
	REQUIRE(table_version == 1);

	REQUIRE_NO_FAIL(tester.Query("BEGIN"));
	auto active_id = transaction_id();
	REQUIRE_NO_FAIL(tester.Query("UPDATE versions SET v = 99"));
	REQUIRE(duckdb_get_table_and_column_versions(tester.connection, "", "versions", columns, 4, versions, &table_version,
	                                  nullptr) == DuckDBSuccess);
	REQUIRE(context.transaction.HasActiveTransaction());
	REQUIRE_FALSE(context.transaction.IsAutoCommit());
	REQUIRE(transaction_id() == active_id);
	REQUIRE(versions[0] == 1);
	REQUIRE(table_version == 1);
	REQUIRE_NO_FAIL(tester.Query("ROLLBACK"));
	auto result = tester.Query("SELECT v FROM versions");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->Fetch<int32_t>(0, 0) == 2);

	REQUIRE_NO_FAIL(tester.Query("UPDATE versions SET v = 100"));
	REQUIRE(duckdb_get_table_and_column_versions(tester.connection, nullptr, "versions", columns, 4, versions, &table_version,
	                                  nullptr) == DuckDBSuccess);
	REQUIRE(table_version == 2);
	REQUIRE(versions[0] == 2);
	REQUIRE(versions[1] == 1);
}

TEST_CASE("Batch column version errors leave output unchanged", "[capi][cAnybaseApi]") {
	CAPITester tester;
	REQUIRE(tester.OpenDatabase(nullptr));
	REQUIRE_NO_FAIL(tester.Query("CREATE TABLE versions(i INTEGER, g INTEGER GENERATED ALWAYS AS (i + 1))"));
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO versions(i) VALUES (1)"));
	const char *columns[] = {"i", "g"};
	const char *null_column[] = {"i", nullptr};
	idx_t versions[] = {123, 456};
	idx_t table_version = 789;
	auto expect_error = [&](duckdb_connection connection, const char *table, const char *const *names,
	                        idx_t *output) {
		char *error = nullptr;
		auto state = duckdb_get_table_and_column_versions(connection, nullptr, table, names, 2, output, &table_version, &error);
		string message = error ? error : "";
		duckdb_free(error);
		INFO(message);
		REQUIRE(state == DuckDBError);
		REQUIRE_FALSE(message.empty());
		REQUIRE(versions[0] == 123);
		REQUIRE(versions[1] == 456);
		REQUIRE(table_version == 789);
	};
	expect_error(nullptr, "versions", columns, versions);
	expect_error(tester.connection, nullptr, columns, versions);
	expect_error(tester.connection, "versions", nullptr, versions);
	expect_error(tester.connection, "versions", columns, nullptr);
	expect_error(tester.connection, "versions", null_column, versions);
	expect_error(tester.connection, "missing_table", columns, versions);
	expect_error(tester.connection, "versions", columns, versions);
	REQUIRE(duckdb_get_table_and_column_versions(tester.connection, nullptr, "missing_table", columns, 2, versions, &table_version,
	                                  nullptr) == DuckDBError);
	char *empty_error = nullptr;
	REQUIRE(duckdb_get_table_and_column_versions(tester.connection, nullptr, "missing_table", nullptr, 0, nullptr, &table_version,
	                                  &empty_error) == DuckDBError);
	REQUIRE(empty_error != nullptr);
	duckdb_free(empty_error);
	REQUIRE(table_version == 789);
	char *null_output_error = nullptr;
	REQUIRE(duckdb_get_table_and_column_versions(tester.connection, nullptr, "versions", columns, 2, versions, nullptr,
	                                  &null_output_error) == DuckDBError);
	REQUIRE(null_output_error != nullptr);
	duckdb_free(null_output_error);
	REQUIRE(versions[0] == 123);
	REQUIRE(versions[1] == 456);
	auto &context = *reinterpret_cast<Connection *>(tester.connection)->context;
	REQUIRE_FALSE(context.transaction.HasActiveTransaction());

	char previous_message[] = "previous message";
	char *error = previous_message;
	const char *physical_column[] = {"i"};
	REQUIRE(duckdb_get_table_and_column_versions(tester.connection, nullptr, "versions", physical_column, 1, versions,
	                                  &table_version, &error) == DuckDBSuccess);
	REQUIRE(error == nullptr);
	REQUIRE(versions[0] == 1);
	REQUIRE(versions[1] == 456);
	REQUIRE(table_version == 1);
}

TEST_CASE("Test Insert version isolation", "[capi]") {

	duckdb_database db;
	duckdb_connection con;
	duckdb_connection con2;
	duckdb_result result;

	idx_t tableVersion = 0;
	idx_t vColumnVersion = 0;
	idx_t iColumnVersion = 0;
	idx_t xColumnVersion = 0;

	REQUIRE(duckdb_open(nullptr, &db) != DuckDBError);
	REQUIRE(duckdb_connect(db, &con) != DuckDBError);
	REQUIRE(duckdb_connect(db, &con2) != DuckDBError);

	// REQUIRE(duckdb_query(con, "CREATE SCHEMA FOO;", NULL) != DuckDBError);
	REQUIRE(duckdb_query(con, "CREATE TABLE FOO(i INTEGER unique, v INTEGER DEFAULT 5, x INTEGER default 22);", NULL) != DuckDBError);
	REQUIRE(duckdb_query(con, "Insert INTO FOO VALUES (1, 5, 22), (2, 5, 22);", NULL) != DuckDBError);

	REQUIRE(duckdb_query(con, "Begin Transaction", NULL) != DuckDBError);
	REQUIRE(duckdb_query(con, "Insert INTO FOO VALUES (3, 5, 22), (4, 5, 22);", NULL) != DuckDBError);
	REQUIRE(duckdb_query(con, "select * from FOO", NULL) != DuckDBError);
	REQUIRE(duckdb_query(con, "INSERT INTO FOO values (1, 5, 22) on conflict do update set v = 10;", NULL) != DuckDBError);
	REQUIRE(duckdb_query(con, "INSERT INTO FOO values (2, 5, 22) on conflict do update set x = 15;", NULL) != DuckDBError);

	tableVersion = duckdb_get_table_version(con, "", "FOO", nullptr);
	vColumnVersion = duckdb_get_column_version(con, "", "FOO", "v", nullptr);
	iColumnVersion = duckdb_get_column_version(con, "", "FOO", "i", nullptr);
	xColumnVersion = duckdb_get_column_version(con, "", "FOO", "x", nullptr);
	REQUIRE(tableVersion == 1);
	REQUIRE(iColumnVersion == 1);
	REQUIRE(vColumnVersion == 1);
	REQUIRE(xColumnVersion == 1);


	REQUIRE(duckdb_query(con, "commit", NULL) != DuckDBError);
	REQUIRE(duckdb_query(con, "select * from FOO", &result) != DuckDBError);


	tableVersion = duckdb_get_table_version(con, "", "FOO", nullptr);
	vColumnVersion = duckdb_get_column_version(con, "", "FOO", "v", nullptr);
	iColumnVersion = duckdb_get_column_version(con, "", "FOO", "i", nullptr);
	xColumnVersion = duckdb_get_column_version(con, "", "FOO", "x", nullptr);
	REQUIRE(tableVersion == 2);
	REQUIRE(iColumnVersion == 2);
	REQUIRE(vColumnVersion == 2);
	REQUIRE(xColumnVersion == 2);

	tableVersion = duckdb_get_table_version(con, "", "FOO", nullptr);
	vColumnVersion = duckdb_get_column_version(con2, "", "FOO", "v", nullptr);
	iColumnVersion = duckdb_get_column_version(con, "", "FOO", "i", nullptr);
	xColumnVersion = duckdb_get_column_version(con, "", "FOO", "x", nullptr);
	REQUIRE(tableVersion == 2);
	REQUIRE(iColumnVersion == 2);
	REQUIRE(vColumnVersion == 2);
	REQUIRE(xColumnVersion == 2);

	duckdb_destroy_result(&result);
	duckdb_disconnect(&con);
	duckdb_disconnect(&con2);
	duckdb_close(&db);
}

TEST_CASE("Get table row count", "[capi]") {
	duckdb_database db;
	duckdb_connection con;

	idx_t row_count = 0;

	REQUIRE(duckdb_open(nullptr, &db) != DuckDBError);
	REQUIRE(duckdb_connect(db, &con) != DuckDBError);

	REQUIRE(duckdb_query(con, "CREATE TABLE FOO(i INTEGER unique, v INTEGER DEFAULT 5, x INTEGER default 22);", NULL) != DuckDBError);
	REQUIRE(duckdb_query(con, "Insert INTO FOO VALUES (1, 5, 22), (2, 5, 22);", NULL) != DuckDBError);

	row_count = duckdb_estimated_row_count(con,nullptr, nullptr, "FOO", nullptr);
	REQUIRE(row_count == 2);

	REQUIRE(duckdb_query(con, "Insert INTO FOO VALUES (3, 5, 22), (4, 5, 22);", NULL) != DuckDBError);

	row_count = duckdb_estimated_row_count(con,nullptr, nullptr, "FOO", nullptr);
	REQUIRE(row_count == 4);

	duckdb_disconnect(&con);
	duckdb_close(&db);
}

TEST_CASE("Estimated table row count excludes committed deletes", "[capi][cAnybaseApi]") {
	CAPITester tester;
	REQUIRE(tester.OpenDatabase(nullptr));
	REQUIRE_NO_FAIL(tester.Query("CREATE TABLE live_rows(i INTEGER)"));
	auto estimate = [&]() {
		char *error = nullptr;
		auto count = duckdb_estimated_row_count(tester.connection, nullptr, nullptr, "live_rows", &error);
		string message = error ? error : "";
		duckdb_free(error);
		INFO(message);
		REQUIRE(message.empty());
		return count;
	};
	REQUIRE(estimate() == 0);
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO live_rows SELECT range FROM range(1000)"));
	REQUIRE(estimate() == 1000);
	REQUIRE_NO_FAIL(tester.Query("DELETE FROM live_rows WHERE i < 400"));
	REQUIRE(estimate() == 600);

	// Local inserts and uncommitted deletes do not affect the committed live-row estimate.
	REQUIRE_NO_FAIL(tester.Query("BEGIN"));
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO live_rows VALUES (1000), (1001)"));
	REQUIRE_NO_FAIL(tester.Query("DELETE FROM live_rows WHERE i < 500 OR i = 1001"));
	REQUIRE(estimate() == 600);
	REQUIRE_NO_FAIL(tester.Query("ROLLBACK"));
	REQUIRE(estimate() == 600);

	// The estimate uses current committed deletes, not an older reader's snapshot.
	CAPITester reader;
	REQUIRE(duckdb_connect(tester.database, &reader.connection) == DuckDBSuccess);
	REQUIRE_NO_FAIL(reader.Query("BEGIN"));
	auto snapshot = reader.Query("SELECT count(*) FROM live_rows");
	REQUIRE_NO_FAIL(*snapshot);
	REQUIRE(snapshot->Fetch<int64_t>(0, 0) == 600);
	REQUIRE_NO_FAIL(tester.Query("DELETE FROM live_rows WHERE i < 500"));
	REQUIRE(estimate() == 500);
	REQUIRE(duckdb_estimated_row_count(reader.connection, nullptr, nullptr, "live_rows", nullptr) == 500);
	snapshot = reader.Query("SELECT count(*) FROM live_rows");
	REQUIRE_NO_FAIL(*snapshot);
	REQUIRE(snapshot->Fetch<int64_t>(0, 0) == 600);
	REQUIRE_NO_FAIL(reader.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(tester.Query("DELETE FROM live_rows"));
	REQUIRE(estimate() == 0);
}

TEST_CASE("Estimated table row count loads persisted deletes", "[capi][cAnybaseApi]") {
	auto path = TestCreatePath("estimated_live_rows.db");
	DeleteDatabase(path);
	{
		CAPITester tester;
		REQUIRE(tester.OpenDatabase(path.c_str()));
		// Keep an index so checkpointing does not simply vacuum away all deleted slots.
		REQUIRE_NO_FAIL(tester.Query("CREATE TABLE live_rows(i BIGINT PRIMARY KEY)"));
		REQUIRE_NO_FAIL(tester.Query("INSERT INTO live_rows SELECT range FROM range(300000)"));
		REQUIRE_NO_FAIL(tester.Query("DELETE FROM live_rows WHERE i % 3 = 0"));
		REQUIRE(duckdb_estimated_row_count(tester.connection, nullptr, nullptr, "live_rows", nullptr) == 200000);
		REQUIRE_NO_FAIL(tester.Query("CHECKPOINT"));
	}
	{
		CAPITester tester;
		REQUIRE(tester.OpenDatabase(path.c_str()));
		// Count before running SELECT so the API itself must load persisted deletion metadata.
		REQUIRE(duckdb_estimated_row_count(tester.connection, nullptr, nullptr, "live_rows", nullptr) == 200000);
		auto actual = tester.Query("SELECT count(*) FROM live_rows");
		REQUIRE_NO_FAIL(*actual);
		REQUIRE(actual->Fetch<int64_t>(0, 0) == 200000);
		REQUIRE_NO_FAIL(tester.Query("INSERT INTO live_rows VALUES (300001)"));
		REQUIRE(duckdb_estimated_row_count(tester.connection, nullptr, nullptr, "live_rows", nullptr) == 200001);
	}
	DeleteDatabase(path);
}

TEST_CASE("Ensure begin transaction succeeds", "[cAnybaseApi]") {
	duckdb_database db;
	duckdb_connection con;

	REQUIRE(duckdb_open(nullptr, &db) != DuckDBError);
	REQUIRE(duckdb_connect(db, &con) != DuckDBError);

	REQUIRE(duckdb_begin_transaction(con, 1234, 1, nullptr) != DuckDBError);
	REQUIRE(duckdb_query(con, "commit;", NULL) != DuckDBError);

	duckdb_disconnect(&con);
	duckdb_close(&db);
}

TEST_CASE("Ensure calling begin twice causes an exception", "[cAnybaseApi]") {
	duckdb_database db;
	duckdb_connection con;

	REQUIRE(duckdb_open(nullptr, &db) != DuckDBError);
	REQUIRE(duckdb_connect(db, &con) != DuckDBError);

	REQUIRE(duckdb_begin_transaction(con, 1234, 1, nullptr) != DuckDBError);
	REQUIRE(duckdb_begin_transaction(con, 5432, 2, nullptr) == DuckDBError);

	duckdb_disconnect(&con);
	duckdb_close(&db);
}

TEST_CASE("Ensure calling begin called twice by two different interfaces causes an exception", "[cAnybaseApi]") {
	duckdb_database db;
	duckdb_connection con;

	REQUIRE(duckdb_open(nullptr, &db) != DuckDBError);
	REQUIRE(duckdb_connect(db, &con) != DuckDBError);

	REQUIRE(duckdb_begin_transaction(con, 1234, 1, nullptr) != DuckDBError);
	REQUIRE(duckdb_query(con, "begin;", NULL) == DuckDBError);

	duckdb_disconnect(&con);
	duckdb_close(&db);
}

TEST_CASE("Append C-Arrow Table", "[cAnybaseApi]") {
	duckdb_database db;
	duckdb_connection con;
	duckdb_result result;
	duckdb_result append_result;
	auto *arrow_array = new ArrowArray();
	ArrowSchema arrow_schema;

	REQUIRE(duckdb_open(nullptr, &db) != DuckDBError);
	REQUIRE(duckdb_connect(db, &con) != DuckDBError);

	REQUIRE(duckdb_query(con, "CREATE TABLE FOO(i INTEGER PRIMARY KEY, v INTEGER, x INTEGER);", nullptr) != DuckDBError);
	REQUIRE(duckdb_query(con, "CREATE TABLE BAR(i INTEGER PRIMARY KEY, v INTEGER, x INTEGER);", nullptr) != DuckDBError);
	REQUIRE(duckdb_query(con, "Insert INTO FOO VALUES (1, 5, 22), (2, 5, 22);", nullptr) != DuckDBError);

	REQUIRE((duckdb_query(con, "BEGIN TRANSACTION", nullptr) != DuckDBError));
	REQUIRE((duckdb_query(con, "SELECT * FROM FOO;", &result) != DuckDBError));

	auto count = duckdb_result_chunk_count(result);
	auto chunks = new duckdb_data_chunk[count];

	for (auto i = 0UL; i < count; i++) {
		chunks[i] = duckdb_result_get_chunk(result, i);
	}

	auto column_count = duckdb_data_chunk_get_column_count(chunks[0]);
	auto *types = new duckdb_logical_type[column_count];

	for (auto i = 0UL; i < column_count; i++) {
		types[i] = duckdb_vector_get_column_type(duckdb_data_chunk_get_vector(chunks[0], i));
	}

	duckdb_arrow_options arrow_options;
	duckdb_connection_get_arrow_options(con, &arrow_options);
	const char *column_names[3];
	column_names[0] = "i";
	column_names[1] = "v";
	column_names[2] = "x";

	duckdb_appender appender;
	duckdb_merger_create(con, nullptr, "BAR", &appender);
	duckdb_appender_add_column(appender, "i");
	duckdb_appender_add_column(appender, "v");
	duckdb_appender_add_column(appender, "x");
	REQUIRE(duckdb_data_chunks_to_arrow_array(result, chunks, count, (duckdb_arrow_array *)&arrow_array) == DuckDBSuccess);
	REQUIRE(duckdb_to_arrow_schema(arrow_options, types, column_names, column_count, &arrow_schema) == nullptr);
	REQUIRE(duckdb_append_arrow(con, appender, arrow_array, &arrow_schema) == nullptr);
	REQUIRE(duckdb_appender_close(appender) == DuckDBSuccess);

	REQUIRE((duckdb_query(con, "COMMIT", nullptr) != DuckDBError));
	REQUIRE((duckdb_query(con, "FORCE CHECKPOINT", nullptr) != DuckDBError));

	REQUIRE((duckdb_query(con, "SELECT * FROM BAR;", &append_result) != DuckDBError));

	auto chunk = duckdb_result_get_chunk(append_result, 0);
	REQUIRE(chunk != nullptr);
	auto data_chunk = reinterpret_cast<duckdb::DataChunk *>(chunk);
	REQUIRE(data_chunk->GetValue(0, 0) == 1);
	REQUIRE(data_chunk->GetValue(1, 0) == 5);
	REQUIRE(data_chunk->GetValue(2, 0) == 22);

	REQUIRE(data_chunk->GetValue(0, 1) == 2);
	REQUIRE(data_chunk->GetValue(1, 1) == 5);
	REQUIRE(data_chunk->GetValue(2, 1) == 22);

	REQUIRE(duckdb_appender_destroy(&appender) == DuckDBSuccess);
	duckdb_destroy_arrow_options(&arrow_options);
	duckdb_destroy_data_chunk(&chunk);
	if (arrow_schema.release) {
		arrow_schema.release(&arrow_schema);
	}
	if (arrow_array->release) {
		arrow_array->release(arrow_array);
	}
	for (auto i = 0UL; i < column_count; i++) {
		duckdb_destroy_logical_type(&types[i]);
	}
	delete[] types;
	delete arrow_array;
	for (auto i = 0UL; i < count; i++) {
		duckdb_destroy_data_chunk(&chunks[i]);
	}
	delete [] chunks;
	duckdb_destroy_result(&result);
	duckdb_destroy_result(&append_result);
	duckdb_disconnect(&con);
	duckdb_close(&db);
}

// TEST_CASE("Test Update version isolation", "[cAnybaseApi]") {
//
// 	duckdb_database db;
// 	duckdb_connection con;
// 	duckdb_connection con2;
// 	duckdb_result result;
//
// 	idx_t tableVersion = 0;
// 	idx_t vColumnVersion = 0;
// 	idx_t iColumnVersion = 0;
// 	idx_t xColumnVersion = 0;
//
// 	REQUIRE(duckdb_open(nullptr, &db) != DuckDBError);
// 	REQUIRE(duckdb_connect(db, &con) != DuckDBError);
// 	REQUIRE(duckdb_connect(db, &con2) != DuckDBError);
//
// 	// REQUIRE(duckdb_query(con, "CREATE SCHEMA FOO;", NULL) != DuckDBError);
// 	REQUIRE(duckdb_query(con, "CREATE TABLE FOO(i INTEGER unique, v INTEGER DEFAULT 5, x INTEGER default 22);", NULL) != DuckDBError);
// 	REQUIRE(duckdb_query(con, "Insert INTO FOO VALUES (1, 5, 22), (2, 5, 22);", NULL) != DuckDBError);
//
// 	REQUIRE(duckdb_query(con, "Begin Transaction", NULL) != DuckDBError);
// 	REQUIRE(duckdb_query(con, "update FOO set v = 22 where i = 1;", NULL) != DuckDBError);
//
// 	tableVersion = duckdb_get_table_version(con, "", "FOO", nullptr);
// 	vColumnVersion = duckdb_get_column_version(con, "", "FOO", "v", nullptr);
// 	iColumnVersion = duckdb_get_column_version(con, "", "FOO", "i", nullptr);
// 	xColumnVersion = duckdb_get_column_version(con, "", "FOO", "x", nullptr);
// 	REQUIRE(tableVersion == 1);
// 	REQUIRE(iColumnVersion == 1);
// 	REQUIRE(vColumnVersion == 1);
// 	REQUIRE(xColumnVersion == 1);
// 	REQUIRE(duckdb_query(con, "commit", NULL) != DuckDBError);
// 	REQUIRE(duckdb_query(con, "select * from FOO", &result) != DuckDBError);
//
//
// 	tableVersion = duckdb_get_table_version(con, "", "FOO", nullptr);
// 	vColumnVersion = duckdb_get_column_version(con, "", "FOO", "v", nullptr);
// 	iColumnVersion = duckdb_get_column_version(con, "", "FOO", "i", nullptr);
// 	xColumnVersion = duckdb_get_column_version(con, "", "FOO", "x", nullptr);
// 	REQUIRE(tableVersion == 2);
// 	REQUIRE(iColumnVersion == 1);
// 	REQUIRE(vColumnVersion == 2);
// 	REQUIRE(xColumnVersion == 1);
//
// 	// duckdb_destroy_result(&result);
// 	// duckdb_disconnect(&con);
// 	// duckdb_disconnect(&con2);
// 	// duckdb_close(&db);
// }
//
// TEST_CASE("Test rollback", "[cAnybaseApi]") {
// 	duckdb_database db;
// 	duckdb_connection con;
// 	duckdb_connection con2;
// 	duckdb_result result;
//
// 	idx_t tableVersion = 0;
// 	idx_t vColumnVersion = 0;
// 	idx_t iColumnVersion = 0;
// 	idx_t xColumnVersion = 0;
//
// 	REQUIRE(duckdb_open(nullptr, &db) != DuckDBError);
// 	REQUIRE(duckdb_connect(db, &con) != DuckDBError);
// 	REQUIRE(duckdb_connect(db, &con2) != DuckDBError);
//
// 	REQUIRE(duckdb_query(con, "CREATE SCHEMA FOO;", NULL) != DuckDBError);
// 	REQUIRE(duckdb_query(con, "CREATE TABLE FOO.FOO(i INTEGER unique, v INTEGER DEFAULT 5, x INTEGER default 22);", NULL) != DuckDBError);
// 	REQUIRE(duckdb_query(con, "Insert INTO FOO.FOO VALUES (1, 5, 22), (2, 5, 22);", NULL) != DuckDBError);
//
// 	REQUIRE(duckdb_query(con, "Begin Transaction", NULL) != DuckDBError);
// 	REQUIRE(duckdb_query(con, "Insert INTO FOO.FOO VALUES (3, 5, 22), (4, 5, 22);", NULL) != DuckDBError);
// 	REQUIRE(duckdb_query(con, "select * from FOO.FOO", NULL) != DuckDBError);
// 	REQUIRE(duckdb_query(con, "INSERT INTO FOO.FOO values (1, 5, 22) on conflict do update set v = 10;", NULL) != DuckDBError);
// 	REQUIRE(duckdb_query(con, "INSERT INTO FOO.FOO values (2, 5, 22) on conflict do update set x = 15;", NULL) != DuckDBError);
//
// 	tableVersion = duckdb_get_table_version(con, "FOO", "FOO", nullptr);
// 	vColumnVersion = duckdb_get_column_version(con, "FOO", "FOO", "v", nullptr);
// 	iColumnVersion = duckdb_get_column_version(con, "FOO", "FOO", "i", nullptr);
// 	xColumnVersion = duckdb_get_column_version(con, "FOO", "FOO", "x", nullptr);
// 	REQUIRE(tableVersion == 1);
// 	REQUIRE(iColumnVersion == 1);
// 	REQUIRE(vColumnVersion == 1);
// 	REQUIRE(xColumnVersion == 1);
//
//
// 	REQUIRE(duckdb_query(con, "rollback", NULL) != DuckDBError);
// 	REQUIRE(duckdb_query(con, "select * from FOO.FOO", &result) != DuckDBError);
//
// 	tableVersion = duckdb_get_table_version(con, "FOO", "FOO", nullptr);
// 	vColumnVersion = duckdb_get_column_version(con, "FOO", "FOO", "v", nullptr);
// 	iColumnVersion = duckdb_get_column_version(con, "FOO", "FOO", "i", nullptr);
// 	xColumnVersion = duckdb_get_column_version(con, "FOO", "FOO", "x", nullptr);
// 	REQUIRE(tableVersion == 1);
// 	REQUIRE(iColumnVersion == 1);
// 	REQUIRE(vColumnVersion == 1);
// 	REQUIRE(xColumnVersion == 1);
//
// 	tableVersion = duckdb_get_table_version(con, "FOO", "FOO", nullptr);
// 	vColumnVersion = duckdb_get_column_version(con2, "FOO", "FOO", "v", nullptr);
// 	iColumnVersion = duckdb_get_column_version(con, "FOO", "FOO", "i", nullptr);
// 	xColumnVersion = duckdb_get_column_version(con, "FOO", "FOO", "x", nullptr);
// 	REQUIRE(tableVersion == 1);
// 	REQUIRE(iColumnVersion == 1);
// 	REQUIRE(vColumnVersion == 1);
// 	REQUIRE(xColumnVersion == 1);
// 	REQUIRE(duckdb_query(con, "checkpoint", NULL) != DuckDBError);
//
// 	// duckdb_destroy_result(&result);
// 	// duckdb_disconnect(&con);
// 	// duckdb_disconnect(&con2);
// 	// duckdb_close(&db);
// }

// TEST_CASE("Reload Version Info", "[capi]") {
//
// 	duckdb_database db;
// 	duckdb_connection con;
//
// 	idx_t tableVersion = 0;
// 	idx_t vColumnVersion = 0;
// 	idx_t iColumnVersion = 0;
// 	idx_t xColumnVersion = 0;
//
// 	REQUIRE(duckdb_open("/Users/jeremyosterhoudt/Development/apps/GE/ampere/data/ods-runtime-normal/storage/Foo/0/Foo_0.db", &db) != DuckDBError);
// 	REQUIRE(duckdb_connect(db, &con) != DuckDBError);
// 	REQUIRE(duckdb_query(con, "checkpoint", NULL) != DuckDBError);
//
// 	tableVersion = duckdb_get_table_version(con, nullptr, "FOO", nullptr);
// 	iColumnVersion = duckdb_get_column_version(con, nullptr, "FOO", "i", nullptr);
// 	vColumnVersion = duckdb_get_column_version(con, nullptr, "FOO", "v", nullptr);
// 	xColumnVersion = duckdb_get_column_version(con, nullptr, "FOO", "x", nullptr);
// 	REQUIRE(tableVersion == 2);
// 	REQUIRE(iColumnVersion == 2);
// 	REQUIRE(vColumnVersion == 2);
// 	REQUIRE(xColumnVersion == 2);
// 	REQUIRE(duckdb_query(con, "checkpoint", NULL) != DuckDBError);
// }

TEST_CASE("Convert DuckDBResult to Arrow Array in C API", "[cAnybaseApi]") {
	duckdb_database db;
	duckdb_connection con;
	duckdb_result result;
	auto *arrow_array = new ArrowArray();

	REQUIRE(duckdb_open(NULL, &db) != DuckDBError);
	REQUIRE(duckdb_connect(db, &con) != DuckDBError);

	REQUIRE(duckdb_query(con, "CREATE TABLE test(i INTEGER);", NULL) != DuckDBError);
	REQUIRE(duckdb_query(con, "Insert INTO test VALUES (1), (2);", NULL) != DuckDBError);
	REQUIRE((duckdb_query(con, "SELECT * FROM test;", &result) != DuckDBError));

	REQUIRE(duckdb_result_to_arrow(&result, (duckdb_arrow_array *)&arrow_array) == DuckDBSuccess);
	REQUIRE(arrow_array->length == 2);

	arrow_array->release(arrow_array);
	delete arrow_array;
	duckdb_destroy_result(&result); // segmentation failure happens here
	duckdb_disconnect(&con);
	duckdb_close(&db);
}

// This test requires physical storage which is hard to do cross-platform with C++ 11.  Keeping commented out for test reference
// TEST_CASE("Snapshot header tests", "[cAnybaseApi]") {
// 	duckdb_database db;
// 	duckdb_connection con;
//
// 	std::remove("<path>/header-test.db");
// 	auto exists = access("<path>/baseHeaderTest.db", 0) != -1;
//
// 	REQUIRE(duckdb_open("<path>/baseHeaderTest.db", &db) != DuckDBError);
// 	REQUIRE(duckdb_connect(db, &con) != DuckDBError);
//
// 	auto id = "0:0";
// 	if (!exists) {
// 		duckdb_begin_transaction(con, 1234, 1, nullptr);
// 		REQUIRE(duckdb_query(con, "CREATE TABLE test(i INTEGER);", nullptr) != DuckDBError);
// 		REQUIRE(duckdb_query(con, "Insert INTO test VALUES (1);", nullptr) != DuckDBError);
// 		REQUIRE(duckdb_query(con, "Insert INTO test VALUES (2);", nullptr) != DuckDBError);
// 		REQUIRE(duckdb_query(con, "Insert INTO test VALUES (4);", nullptr) != DuckDBError);
// 		REQUIRE(duckdb_query(con, "COMMIT;", nullptr) != DuckDBError);
// 		id = duckdb_checkpoint_and_get_snapshot_id(con);
// 		REQUIRE(strcmp(id, "1234:1") == 0);
// 		delete[] id;
// 	} else {
// 		id = duckdb_get_snapshot_id(con);
// 		REQUIRE(strcmp(id, "1234:1") == 0);
// 		delete[] id;
// 	}
// 	REQUIRE((duckdb_query(con, "ATTACH '<path>/header-test.db' as aClone", nullptr) != DuckDBError));
// 	REQUIRE((duckdb_query(con, "COPY FROM DATABASE baseHeaderTest TO aClone", nullptr) != DuckDBError));
// 	duckdb_set_snapshot_id(con, "aClone", 1234, 1);
// 	REQUIRE((duckdb_query(con, "DETACH aClone", nullptr) != DuckDBError));
//
// 	duckdb_disconnect(&con);
// 	duckdb_close(&db);
//
// 	REQUIRE(duckdb_open("<path>/header-test.db", &db) != DuckDBError);
// 	REQUIRE(duckdb_connect(db, &con) != DuckDBError);
// 	id = duckdb_get_snapshot_id(con);
// 	REQUIRE(strcmp(id, "1234:1") == 0);
// 	delete[] id;
//
//
// 	std::remove("<path>/header-test.db");
// 	// std::remove("<path>/baseHeaderTest.db");
//
// 	duckdb_disconnect(&con);
// 	duckdb_close(&db);
// }

TEST_CASE("Convert DuckDB Chunks to Arrow Array in C API", "[cAnybaseApi]") {
	duckdb_database db;
	duckdb_connection con;
	duckdb_result result;
	auto *arrow_array = new ArrowArray();

	REQUIRE(duckdb_open(NULL, &db) != DuckDBError);
	REQUIRE(duckdb_connect(db, &con) != DuckDBError);

	REQUIRE(duckdb_query(con, "CREATE TABLE test(i INTEGER);", NULL) != DuckDBError);
	REQUIRE(duckdb_query(con, "Insert INTO test VALUES (1), (2);", NULL) != DuckDBError);
	REQUIRE((duckdb_query(con, "SELECT * FROM test;", &result) != DuckDBError));

	auto count = duckdb_result_chunk_count(result);
	auto chunks = new duckdb_data_chunk[count];

	for (auto i = 0UL; i < count; i++) {
		chunks[i] = duckdb_result_get_chunk(result, i);
	}

	REQUIRE(duckdb_data_chunks_to_arrow_array(result, chunks, count, (duckdb_arrow_array *)&arrow_array) == DuckDBSuccess);
	REQUIRE(arrow_array->length == 2);

	arrow_array->release(arrow_array);
	delete arrow_array;
	for (auto i = 0UL; i < count; i++) {
		duckdb_destroy_data_chunk(&chunks[i]);
	}
	delete [] chunks;
	duckdb_destroy_result(&result); // segmentation failure happens here
	duckdb_disconnect(&con);
	duckdb_close(&db);
}

TEST_CASE("Convert DuckDB Chunk column to Arrow Array in C API", "[cAnybaseApi]") {
	duckdb_database db;
	duckdb_connection con;
	duckdb_result result;
	auto *i_arrow_array = new ArrowArray();
	auto *s_arrow_array = new ArrowArray();

	REQUIRE(duckdb_open(NULL, &db) != DuckDBError);
	REQUIRE(duckdb_connect(db, &con) != DuckDBError);

	REQUIRE(duckdb_query(con, "CREATE TABLE test(i INTEGER, s VARCHAR);", NULL) != DuckDBError);
	REQUIRE(duckdb_query(con, "Insert INTO test VALUES (1, 'a'), (2, 'b');", NULL) != DuckDBError);
	REQUIRE((duckdb_query(con, "SELECT * FROM test;", &result) != DuckDBError));

	auto count = duckdb_result_chunk_count(result);
	auto chunks = new duckdb_data_chunk[count];

	for (auto i = 0UL; i < count; i++) {
		chunks[i] = duckdb_result_get_chunk(result, i);
	}

	// Check column 0
	REQUIRE(duckdb_data_chunk_column_to_arrow_array(con, chunks, count, 0, (duckdb_arrow_array *)&i_arrow_array) == DuckDBSuccess);
	REQUIRE(i_arrow_array->length == 2);
	REQUIRE(i_arrow_array->n_buffers == 1);
	REQUIRE(i_arrow_array->n_children == 1);

	// Check column 1
	REQUIRE(duckdb_data_chunk_column_to_arrow_array(con, chunks, count, 1, (duckdb_arrow_array *)&s_arrow_array) == DuckDBSuccess);
	REQUIRE(s_arrow_array->length == 2);
	REQUIRE(s_arrow_array->n_buffers == 1);
	REQUIRE(s_arrow_array->n_children == 1);

	i_arrow_array->release(i_arrow_array);
	s_arrow_array->release(s_arrow_array);
	delete i_arrow_array;
	delete s_arrow_array;
	duckdb_destroy_result(&result); // segmentation failure happens here
	duckdb_disconnect(&con);
	duckdb_close(&db);
	for (auto i = 0UL; i < count; i++) {
		duckdb_destroy_data_chunk(&chunks[i]);
	}
	delete[] chunks;
}

TEST_CASE("Convert DuckDB UUID Chunk column to Arrow Array is 16 Byte Binary in C API", "[cAnybaseApi]") {
	duckdb_database db;
	duckdb_connection con;
	duckdb_result result;
	auto *i_arrow_array = new ArrowArray();
	auto *s_arrow_array = new ArrowArray();

	REQUIRE(duckdb_open(nullptr, &db) != DuckDBError);
	REQUIRE(duckdb_connect(db, &con) != DuckDBError);

	REQUIRE(duckdb_query(con, "CREATE TABLE test(i INTEGER, s UUID);", nullptr) != DuckDBError);
	REQUIRE(duckdb_query(con, "Insert INTO test VALUES (1, '00000000-0000-0000-0000-000000000000'), (2, '00010203-0405-0607-0809-0a0b0c0d0e0f');", nullptr) != DuckDBError);
	REQUIRE((duckdb_query(con, "SELECT * FROM test;", &result) != DuckDBError));

	auto count = duckdb_result_chunk_count(result);
	auto chunks = new duckdb_data_chunk[count];

	for (auto i = 0UL; i < count; i++) {
		chunks[i] = duckdb_result_get_chunk(result, i);
	}

	// Check column 0
	REQUIRE(duckdb_data_chunk_column_to_arrow_array(con, chunks, count, 0, (duckdb_arrow_array *)&i_arrow_array) == DuckDBSuccess);
	REQUIRE(i_arrow_array->length == 2);
	REQUIRE(i_arrow_array->n_buffers == 1);
	REQUIRE(i_arrow_array->n_children == 1);

	// Check column 1
	REQUIRE(duckdb_data_chunk_column_to_arrow_array(con, chunks, count, 1, (duckdb_arrow_array *)&s_arrow_array) == DuckDBSuccess);
	REQUIRE(s_arrow_array->length == 2);
	REQUIRE(s_arrow_array->n_buffers == 1);
	REQUIRE(s_arrow_array->n_children == 1);

	i_arrow_array->release(i_arrow_array);
	s_arrow_array->release(s_arrow_array);
	delete i_arrow_array;
	delete s_arrow_array;
	duckdb_destroy_result(&result); // segmentation failure happens here
	duckdb_disconnect(&con);
	duckdb_close(&db);
	for (auto i = 0UL; i < count; i++) {
		duckdb_destroy_data_chunk(&chunks[i]);
	}
	delete[] chunks;
}

TEST_CASE("Test DataChunk C API reference", "[cAnybaseApi]") {
	duckdb_logical_type types[2];
	types[0] = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
	types[1] = duckdb_create_logical_type(DUCKDB_TYPE_SMALLINT);

	auto data_chunk = duckdb_create_data_chunk(types, 2);
	REQUIRE(data_chunk);
	duckdb_data_chunk_set_size(data_chunk, 1);

	// append standard primitive values
	auto col1_ptr = (int64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(data_chunk, 0));
	*col1_ptr = 42;
	auto col2_ptr = (int16_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(data_chunk, 1));
	*col2_ptr = 84;

	auto other_chunk = duckdb_create_data_chunk_copy(&data_chunk);
	REQUIRE(other_chunk);

	auto other_col1_ptr = (int64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(other_chunk, 0));
	auto other_col2_ptr = (int16_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(other_chunk, 1));
	*other_col1_ptr = 88;

	REQUIRE(*other_col1_ptr == 88);
	REQUIRE(*other_col2_ptr == 84);
	REQUIRE(*col1_ptr == 42);

	duckdb_data_chunk_set_size(other_chunk, 2);

	*(other_col1_ptr + 8) = 77;
	*(other_col2_ptr + 2) = 12;

	REQUIRE(*(other_col1_ptr + 8) == 77);
	REQUIRE(*(other_col2_ptr + 2) == 12);
	REQUIRE(duckdb_data_chunk_get_size(data_chunk) == 1);



	duckdb_destroy_data_chunk(&data_chunk);
	duckdb_destroy_data_chunk(&other_chunk);
	duckdb_destroy_logical_type(&types[0]);
	duckdb_destroy_logical_type(&types[1]);
    printf("Test DataChunk C API reference passed\n");
}

TEST_CASE("Test MergeDataChunk in C API", "[cAnybaseApi]") {
	CAPITester tester;
	REQUIRE(tester.OpenDatabase(nullptr));
	REQUIRE(duckdb_vector_size() == STANDARD_VECTOR_SIZE);

	tester.Query("CREATE TABLE test(i INT PRIMARY KEY, j TIMESTAMP_MS);");
	tester.Query("CREATE TABLE test2(i INT PRIMARY KEY, j TIMESTAMPTZ);");
	tester.Query("INSERT INTO test2 VALUES (1, '1992-09-20 12:30:00.123456789+01:00');");
	auto result = tester.Query("SELECT * FROM test2");

	auto *arrow_array = new ArrowArray();

	REQUIRE(duckdb_result_to_arrow(&result->InternalResult(), (duckdb_arrow_array *)&arrow_array) == DuckDBSuccess);
	REQUIRE(arrow_array->length == 1);

	duckdb_logical_type types[2];
	types[0] = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	types[1] = duckdb_create_logical_type(DUCKDB_TYPE_TIMESTAMP_MS);
	const char *names[2] = {strdup("i"), strdup("j")};

	duckdb_appender appender;
	auto status = duckdb_merger_create(tester.connection, nullptr, "test", &appender);
	REQUIRE(status == DuckDBSuccess);
	duckdb_appender_add_column(appender, "i");
	duckdb_appender_add_column(appender, "j");

	duckdb_arrow_options arrow_options;
	duckdb_connection_get_arrow_options(tester.connection, &arrow_options);
	ArrowSchema arrow_schema;
	duckdb_to_arrow_schema(arrow_options, types, names, 2, &arrow_schema);
	duckdb_destroy_arrow_options(&arrow_options);

	duckdb_append_arrow(tester.connection, appender, arrow_array, &arrow_schema);
	duckdb_appender_close(appender);

	auto r2 = tester.Query("SELECT * FROM test");
	REQUIRE(r2.get()->row_count() == 1);

	duckdb_appender_destroy(&appender);
	duckdb_destroy_logical_type(&types[0]);
	duckdb_destroy_logical_type(&types[1]);
}

// TEST_CASE("Test MergeDataChunk in C API", "[cAnybaseApi]") {
// 	CAPITester tester;
// 	REQUIRE(tester.OpenDatabase(nullptr));
// 	REQUIRE(duckdb_vector_size() == STANDARD_VECTOR_SIZE);
//
// 	tester.Query("CREATE TABLE test(i INT PRIMARY KEY, j INT);");
// 	tester.Query("INSERT INTO test VALUES (1, 2);");
//
// 	duckdb_logical_type types[2];
// 	types[0] = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
// 	types[1] = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
//
// 	auto data_chunk = duckdb_create_data_chunk(types, 2);
// 	REQUIRE(data_chunk);
//
// 	auto i_data = (int32_t *) duckdb_vector_get_data(duckdb_data_chunk_get_vector(data_chunk, 0));
// 	auto j_data = (int32_t *) duckdb_vector_get_data(duckdb_data_chunk_get_vector(data_chunk, 1));
// 	i_data[0] = 1;
// 	j_data[0] = 1;
//
// 	duckdb_data_chunk_set_size(data_chunk, 1);
//
// 	duckdb_appender appender;
// 	auto status = duckdb_merger_create(tester.connection, nullptr, "test", &appender);
// 	REQUIRE(status == DuckDBSuccess);
//
// 	REQUIRE(duckdb_append_data_chunk(appender, data_chunk) == DuckDBSuccess);
// 	duckdb_appender_close(appender);
//
// 	auto result = tester.Query("SELECT i, j FROM test;");
// 	auto i = result->Fetch<int>(0, 0);
// 	REQUIRE(1 == i);
// 	auto j = result->Fetch<int>(1, 0);
// 	REQUIRE(1 == j);
//
// 	duckdb_appender_destroy(&appender);
// 	duckdb_destroy_data_chunk(&data_chunk);
// 	duckdb_destroy_logical_type(&types[0]);
// 	duckdb_destroy_logical_type(&types[1]);
// }

// TEST_CASE("Test MergeDataChunk updates versions in C API", "[cAnybaseApi]") {
// 	CAPITester tester;
// 	REQUIRE(tester.OpenDatabase(nullptr));
// 	REQUIRE(duckdb_vector_size() == STANDARD_VECTOR_SIZE);
//
// 	tester.Query("CREATE TABLE test(i INT PRIMARY KEY, j INT, f INT);");
// 	tester.Query("INSERT INTO test VALUES (1, 2, 3);");
//
// 	auto tableVersion = duckdb_get_table_version(tester.connection, nullptr, "test", nullptr);
// 	auto iColumnVersion = duckdb_get_column_version(tester.connection, nullptr, "test", "i", nullptr);
// 	auto jColumnVersion = duckdb_get_column_version(tester.connection, nullptr, "test", "j", nullptr);
//
// 	duckdb_logical_type types[2];
// 	types[0] = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
// 	types[1] = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
//
// 	auto data_chunk = duckdb_create_data_chunk(types, 2);
// 	REQUIRE(data_chunk);
//
// 	auto i_data = (int32_t *) duckdb_vector_get_data(duckdb_data_chunk_get_vector(data_chunk, 0));
// 	auto j_data = (int32_t *) duckdb_vector_get_data(duckdb_data_chunk_get_vector(data_chunk, 1));
// 	i_data[0] = 1;
// 	j_data[0] = 1;
//
// 	duckdb_data_chunk_set_size(data_chunk, 1);
//
// 	REQUIRE(duckdb_begin_transaction(tester.connection, 1000, 2, nullptr) == DuckDBSuccess);
// 	duckdb_appender appender;
// 	auto status = duckdb_merger_create(tester.connection, nullptr, "test", &appender);
// 	REQUIRE(status == DuckDBSuccess);
//
// 	duckdb_appender_add_column(appender, "i");
// 	duckdb_appender_add_column(appender, "j");
// 	REQUIRE(duckdb_append_data_chunk(appender, data_chunk) == DuckDBSuccess);
// 	duckdb_appender_close(appender);
// 	tester.Query("COMMIT;");
//
// 	duckdb_appender_destroy(&appender);
//
// 	i_data = (int32_t *) duckdb_vector_get_data(duckdb_data_chunk_get_vector(data_chunk, 0));
// 	j_data = (int32_t *) duckdb_vector_get_data(duckdb_data_chunk_get_vector(data_chunk, 1));
// 	i_data[0] = 1;
// 	j_data[0] = 22;
//
// 	duckdb_data_chunk_set_size(data_chunk, 1);
//
// 	// tester.Query("BEGIN TRANSACTION;");
// 	REQUIRE(duckdb_begin_transaction(tester.connection, 2000, 2, nullptr) == DuckDBSuccess);
// 	status = duckdb_merger_create(tester.connection, nullptr, "test", &appender);
// 	REQUIRE(status == DuckDBSuccess);
//
// 	duckdb_appender_add_column(appender, "i");
// 	duckdb_appender_add_column(appender, "j");
//
// 	REQUIRE(duckdb_append_data_chunk(appender, data_chunk) == DuckDBSuccess);
// 	duckdb_appender_close(appender);
// 	tester.Query("COMMIT;");
//
// 	auto newTableVersion = duckdb_get_table_version(tester.connection, nullptr, "test", nullptr);
// 	auto newIColumnVersion = duckdb_get_column_version(tester.connection, nullptr, "test", "i", nullptr);
// 	auto newJColumnVersion = duckdb_get_column_version(tester.connection, nullptr, "test", "j", nullptr);
//
// 	auto result = tester.Query("SELECT i, j FROM test;");
// 	auto i = result->Fetch<int>(0, 0);
// 	REQUIRE(1 == i);
// 	auto j = result->Fetch<int>(1, 0);
// 	REQUIRE(22 == j);
//
// 	REQUIRE(tableVersion + 2 == newTableVersion);
// 	REQUIRE(iColumnVersion == newIColumnVersion);
// 	REQUIRE(jColumnVersion + 2 == newJColumnVersion);
//
// 	duckdb_appender_destroy(&appender);
// 	duckdb_destroy_data_chunk(&data_chunk);
// 	duckdb_destroy_logical_type(&types[0]);
// 	duckdb_destroy_logical_type(&types[1]);
// }

// void some_func2(cdc_event_type type,
// 				const char *transaction_id,
// 				idx_t column_count,
// 				idx_t table_version,
// 				idx_t *updated_column_index,
// 				const char *table_name,
// 				const char **column_names,
// 				idx_t *column_versions,
// 				duckdb_data_chunk values,
// 				duckdb_data_chunk previous_values) {
//
// 	// printf("type: %d\n", type);
// 	printf("time: %s\n", transaction_id);
// 	printf("column_count: %llu\n", column_count);
// 	// printf("table_version: %llu\n", table_version);
// 	//
// 	// int totalSize = sizeof(column_names);
// 	// printf("columns: ");
// 	// if (column_count > 0) {
// 	// 	for (auto i = 0; i < column_count; i++) {
// 	// 		printf(column_names[i]);
// 	// 		if (i != column_count - 1) {
// 	// 			printf(", ");
// 	// 		}
// 	// 	}
// 	// }
// 	// printf("\n");
//
// 	if (values != nullptr) {
// 		duckdb_destroy_data_chunk(&values);
// 		values = nullptr;
// 	}
// 	if (previous_values != nullptr) {
// 		duckdb_destroy_data_chunk(&previous_values);
// 		previous_values = nullptr;
// 	}
// }
//
// TEST_CASE("Test WAL Generation2", "[capi]") {
// 	duckdb_database db;
// 	duckdb_connection connection1;
// 	duckdb_result result;
// 	duckdb_result errorMessage;
//
// 	// REQUIRE(duckdb_open("/Users/jeremyosterhoudt/Downloads/dbz_demo/test.db", &db) != DuckDBError);
// 	REQUIRE(duckdb_open(nullptr, &db) != DuckDBError);
// 	duckdb_set_cdc_callback(db, some_func2);
// 	REQUIRE(duckdb_connect(db, &connection1) != DuckDBError);
//
// 	// REQUIRE(duckdb_query(connection1, "create table person (id int, \"name\" varchar, address varchar, age int, title varchar, active boolean, primary key (id))", nullptr) != DuckDBError);
// 	REQUIRE(duckdb_query(connection1, "create table person (id int, \"name\" varchar, address varchar, age int, title varchar, active boolean)", nullptr) != DuckDBError);
// 	REQUIRE(duckdb_query(connection1, "INSERT INTO person VALUES (1, 'Jer', '132 fake street', 67, 'The main guy who writes shit', true), (2, 'Jen', '132 fake street', 67, 'HM', true), (1, 'Jer', '132 fake street', 67, 'The main guy who writes shit', true), (2, 'Gog', '132 fake street', 67, 'HM', true)", nullptr) != DuckDBError);
// 	// REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//
// 	REQUIRE(duckdb_begin_transaction(connection1, 12345, 1, nullptr) != DuckDBError);
// 	// REQUIRE(duckdb_query(connection1, "UPDATE person SET \"name\" = 'The Dude', age = 44 where id = 1", nullptr) != DuckDBError);
// 	REQUIRE(duckdb_query(connection1, "DELETE FROM person", nullptr) != DuckDBError);
// 	REQUIRE(duckdb_query(connection1, "COMMIT", nullptr) != DuckDBError);
// 	REQUIRE(duckdb_begin_transaction(connection1, 12345, 1, nullptr) != DuckDBError);
// 	REQUIRE(duckdb_query(connection1, "INSERT INTO person VALUES (2, 'Jen', '132 fake street', 67, 'HM', true)", nullptr) != DuckDBError);
// 	REQUIRE(duckdb_query(connection1, "COMMIT", nullptr) != DuckDBError);
// 	duckdb_query(connection1, "DELETE FROM person", nullptr);
// 	duckdb_disconnect(&connection1);
// 	duckdb_close(&db);
// }

// TEST_CASE("Test WAL Generation", "[capi]") {
//     duckdb_database db;
//     duckdb_connection connection1;
//     // duckdb_connection connection2;
//     // duckdb_result result;
//     // duckdb_result errorMessage;
//
//      // REQUIRE(duckdb_open("/Users/jeremyosterhoudt/Downloads/dbz_demo/test.db", &db) != DuckDBError);
//     REQUIRE(duckdb_open(nullptr, &db) != DuckDBError);
//     duckdb_set_cdc_callback(db, some_func2);
//     REQUIRE(duckdb_connect(db, &connection1) != DuckDBError);
//     // REQUIRE(duckdb_connect(db, &connection2) != DuckDBError);
//
// 	REQUIRE(duckdb_begin_transaction(connection1, 12345, 1, nullptr) != DuckDBError);
//
//     // REQUIRE(duckdb_query(connection1, "CREATE TABLE if not exists FOO(id BIGINT unique, val INTEGER default 22, xtra INTEGER default 11)", nullptr) != DuckDBError);
//     // REQUIRE(duckdb_query(connection1, "INSERT INTO FOO (id) VALUES (1), (2)", nullptr) != DuckDBError);
// 	// REQUIRE(duckdb_query(connection1, "BEGIN TRANSACTION", nullptr) != DuckDBError);
//
//     REQUIRE(duckdb_query(connection1, "CREATE TABLE if not exists FOO(id BIGINT, val INTEGER, xtra INTEGER, other integer)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
//     REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3), (2, 7, 4, 1)", nullptr) != DuckDBError);
// 	REQUIRE(duckdb_query(connection1, "COMMIT", nullptr) != DuckDBError);
//
//     // REQUIRE(duckdb_query(connection1, "UPDATE FOO SET val = 5 where val = 8 or other = 125", nullptr) != DuckDBError);
//     // REQUIRE(duckdb_query(connection1, "DELETE FROM FOO where id = 2", nullptr) != DuckDBError);
//     // REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (2, 8, 31, 125)", nullptr) != DuckDBError);
//     // REQUIRE(duckdb_query(connection1, "BEGIN TRANSACTION", nullptr) != DuckDBError);
//
//
// 	// // REQUIRE(duckdb_begin_transaction(connection1, 12345, 2, nullptr) != DuckDBError);
// 	// REQUIRE(duckdb_query(connection1, "BEGIN TRANSACTION", nullptr) != DuckDBError);
//  //    // REQUIRE(duckdb_query(connection1, "UPDATE FOO SET val = 5 where id = 2", nullptr) != DuckDBError);
//  //    REQUIRE(duckdb_query(connection1, "UPDATE FOO SET val = 44 where id = 1", nullptr) != DuckDBError);
//  //    REQUIRE(duckdb_query(connection1, "UPDATE FOO SET val = 5 where val = 8 and other = 125", nullptr) != DuckDBError);
//  //    // REQUIRE(duckdb_query(connection1, "UPDATE FOO SET val = 4 where val = 5", nullptr) != DuckDBError);
//  //    // REQUIRE(duckdb_query(connection1, "UPDATE FOO SET val = 3 where val = 4", nullptr) != DuckDBError);
//  //    REQUIRE(duckdb_query(connection1, "COMMIT", nullptr) != DuckDBError);
//  // REQUIRE(duckdb_query(connection1, "DELETE FROM FOO where id = 2", nullptr) != DuckDBError);
//
//     // duckdb_destroy_result(&errorMessage);
//     //duckdb_destroy_result(&result);
//     duckdb_disconnect(&connection1);
//     // duckdb_disconnect(&connection2);
//     duckdb_close(&db);
// }
