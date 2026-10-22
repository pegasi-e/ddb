#include "catch.hpp"
#include "duckdb/main/appender.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

// start Anybase changes
#include "duckdb.hpp"
#include "duckdb/storage/storage_info.hpp"
#include <numeric>
#include "duckdb/common/types/conflict_manager.hpp"
#include "duckdb/common/types/constraint_conflict_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/index/art/art_operator.hpp"
#include "duckdb/execution/index/art/node48.hpp"
#include "duckdb/execution/index/art/node256.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
// end Anybase changes

#include <vector>
#include <random>
#include <algorithm>
#ifndef _WIN32
#include <sys/time.h>
#endif
using namespace duckdb;
using namespace std;
//
// TEST_CASE("merger increments versions", "[merger]") {
// 	duckdb::unique_ptr<QueryResult> result;
// 	DuckDB db(nullptr);
// 	Connection con(db);
//
// 	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY, j INTEGER)"));
// 	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 16)"));
//
// 	idx_t tableVersion = 0;
// 	idx_t iColumnVersion = 0;
// 	idx_t jColumnVersion = 0;
//
// 	tableVersion = con.context->GetTableVersion("", "integers");
// 	jColumnVersion = con.context->GetColumnVersion("", "integers", "j");
// 	iColumnVersion = con.context->GetColumnVersion("", "integers", "i");
//
// 	result = con.Query("SELECT j FROM integers");
// 	REQUIRE(CHECK_COLUMN(result, 0, {16}));
//
// 	// REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
// 	con.context->BeginTransaction(duckdb::timestamp_t(1000), 1);
//
// 	Merger merger(con, "integers");
// 	merger.BeginRow();
// 	merger.Append<int32_t>(1);
// 	merger.Append<int32_t>(18);
// 	merger.EndRow();
//
// 	merger.Flush();
//
// 	REQUIRE_NO_FAIL(con.Query("COMMIT TRANSACTION"));
//
// 	auto newTableVersion = con.context->GetTableVersion("", "integers");
// 	auto newJColumnVersion = con.context->GetColumnVersion("", "integers", "j");
// 	auto newIColumnVersion = con.context->GetColumnVersion("", "integers", "i");
//
// 	REQUIRE(tableVersion + 1 == newTableVersion);
// 	REQUIRE(jColumnVersion + 1 == newJColumnVersion);
// 	REQUIRE(iColumnVersion == newIColumnVersion);
//
// 	con.context->BeginTransaction(duckdb::timestamp_t(2000), 1);
//
// 	Merger merger2(con, "integers");
// 	merger2.BeginRow();
// 	merger2.Append<int32_t>(1);
// 	merger2.Append<int32_t>(22);
// 	merger2.EndRow();
//
// 	merger2.Flush();
//
// 	REQUIRE_NO_FAIL(con.Query("COMMIT TRANSACTION"));
//
// 	newTableVersion = con.context->GetTableVersion("", "integers");
// 	newJColumnVersion = con.context->GetColumnVersion("", "integers", "j");
// 	newIColumnVersion = con.context->GetColumnVersion("", "integers", "i");
//
// 	REQUIRE(tableVersion + 2 == newTableVersion);
// 	REQUIRE(jColumnVersion + 2 == newJColumnVersion);
// 	REQUIRE(iColumnVersion == newIColumnVersion);
//
// 	result = con.Query("SELECT j FROM integers");
// 	REQUIRE(CHECK_COLUMN(result, 0, {22}));
// }
//
// TEST_CASE("Basic merger tests", "[merger]") {
// 	duckdb::unique_ptr<QueryResult> result;
// 	DuckDB db(nullptr);
// 	Connection con(db);
//
// 	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY, j INTEGER)"));
// 	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 16)"));
//
// 	result = con.Query("SELECT j FROM integers");
// 	REQUIRE(CHECK_COLUMN(result, 0, {16}));
//
// 	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
// 	Merger merger(con, "integers");
// 	merger.BeginRow();
// 	merger.Append<int32_t>(1);
// 	merger.Append<int32_t>(18);
// 	merger.EndRow();
//
// 	merger.Flush();
//
// 	REQUIRE_NO_FAIL(con.Query("COMMIT TRANSACTION"));
//
// 	result = con.Query("SELECT j FROM integers");
// 	REQUIRE(CHECK_COLUMN(result, 0, {18}));
// }
//
// TEST_CASE("Test multiple AppendRow", "[merger]") {
// 	duckdb::unique_ptr<QueryResult> result;
// 	DuckDB db(nullptr);
// 	Connection con(db);
//
// 	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY, j INTEGER)"));
//
// 	// append a bunch of values
// 	{
// 		Appender appender(con, "integers");
// 		for (size_t i = 0; i < 2000; i++) {
// 		  appender.AppendRow((int32_t)(i), (int32_t)(i));
// 		}
// 		appender.Close();
// 	}
//
// 	// merge rows
// 	// change even key value
// 	duckdb::vector<duckdb::Value> values;
// 	{
// 		Merger merger(con, "integers");
// 		for (size_t i = 0; i < 2000; i++) {
// 		  if ((i % 2 ) == 0) {
// 		    merger.AppendRow( (int32_t)(i),  (int32_t)(2*i));
// 		    values.push_back(duckdb::Value((int32_t)(2*i)));
// 		  }
// 		  else {
// 		    values.push_back(duckdb::Value((int32_t)i));
// 		  }
// 		}
// 		merger.Close();
// 	}
//
// 	result = con.Query("SELECT j FROM integers");
// 	REQUIRE(CHECK_COLUMN(result, 0, values));
// }
//
// TEST_CASE("Test merging out of key order", "[merger]") {
// 	duckdb::unique_ptr<QueryResult> result;
// 	DuckDB db(nullptr);
// 	Connection con(db);
//
// 	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(i BIGINT, j VARCHAR, b VARCHAR, PRIMARY KEY (i))"));
//
// 	auto number_of_values = 2000;
// 	std::vector<idx_t> key_ids;
// 	duckdb::vector<duckdb::Value> key_values;
// 	duckdb::vector<duckdb::Value> j_values;
// 	duckdb::vector<duckdb::Value> b_values;
//
// 	// append a bunch of values
// 	Appender appender(con, "test");
// 	for (int32_t i = 0; i < number_of_values; i++) {
// 		auto index_string = std::to_string(i);
// 		appender.AppendRow(i, (string_t)("j-" + index_string), (string_t)("b-" + index_string));
//
// 		key_ids.push_back(i);
// 		key_values.push_back(duckdb::Value(i));
// 		j_values.push_back(duckdb::Value("uj-" + index_string));
// 		b_values.push_back(duckdb::Value("ub-" + index_string));
// 	}
// 	appender.Close();
//
// 	auto rng = std::default_random_engine {random_device{}()};
// 	std::shuffle(std::begin(key_ids), std::end(key_ids), rng);
//
// 	// merge rows
// 	Merger merger(con, "test");
// 	for (idx_t i = 0; i < key_ids.size(); i++) {
// 		int32_t key_value = key_ids[i];
// 		auto j_update_value = j_values[key_value];
// 		auto b_update_value = b_values[key_value];
//
// 		merger.AppendRow(key_value, j_update_value, b_update_value);
// 	}
// 	merger.Close();
//
// 	result = con.Query("SELECT * FROM test");
// 	REQUIRE(CHECK_COLUMN(result, 0, key_values));
// 	REQUIRE(CHECK_COLUMN(result, 1, j_values));
// 	REQUIRE(CHECK_COLUMN(result, 2, b_values));
// }
//
// TEST_CASE("Partial merge tests", "[merger]") {
// 	duckdb::unique_ptr<QueryResult> result;
// 	DuckDB db(nullptr);
// 	Connection con(db);
//
// 	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i BIGINT, b VARCHAR default 'abc', j int, k int default 9, PRIMARY KEY (i))"));
//
// 	// append a bunch of values
// 	duckdb::vector<idx_t> values;
// 	Appender appender(con, "integers");
// 	for (idx_t i = 0; i < 1; i++) {
// 		auto num = i;
// 		auto key = "key-" + std::to_string(num);
// 		appender.AppendRow((int64_t)(num), duckdb::Value(key.c_str()), (int32_t) i, (int32_t) i);
// 		values.push_back(num);
// 	}
// 	appender.Close();
//
// 	duckdb::vector<std::string> columns = {"j", "i"};
//
// 	Merger merger(con, "integers");
// 	for (auto &column : columns) {
// 		merger.AddColumn(column);
// 	}
// 	merger.AppendRow(33, 0);
// 	merger.Close();
//
// 	result = con.Query("SELECT b, j, k FROM integers");
// 	REQUIRE(CHECK_COLUMN(result, 0, {"key-0"}));
// 	REQUIRE(CHECK_COLUMN(result, 1, {33}));
// 	REQUIRE(CHECK_COLUMN(result, 2, {0}));
//
// 	duckdb::vector<std::string> columns2 = {"k", "i"};
//
// 	Merger merger2(con, "integers");
// 	for (auto &column : columns2) {
// 		merger2.AddColumn(column);
// 	}
// 	merger2.AppendRow(44, 0);
// 	merger2.Close();
//
// 	result = con.Query("SELECT b, j, k FROM integers");
// 	REQUIRE(CHECK_COLUMN(result, 0, {"key-0"}));
// 	REQUIRE(CHECK_COLUMN(result, 1, {33}));
// 	REQUIRE(CHECK_COLUMN(result, 2, {44}));
// }
//
// TEST_CASE("Merger new rows add defaults", "[merger]") {
// 	duckdb::unique_ptr<QueryResult> result;
// 	DuckDB db(nullptr);
// 	Connection con(db);
//
// 	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i BIGINT, b VARCHAR default 'abc', j int, k int default 9, PRIMARY KEY (i))"));
//
// 	// append a bunch of values
// 	duckdb::vector<idx_t> values;
// 	Appender appender(con, "integers");
// 	for (idx_t i = 0; i < 1; i++) {
// 		auto num = i;
// 		auto key = "key-" + std::to_string(num);
// 		appender.AppendRow((int64_t)(num), duckdb::Value(key.c_str()), (int32_t) i, (int32_t) i);
// 		values.push_back(num);
// 	}
// 	appender.Close();
//
// 	duckdb::vector<std::string> columns = {"j", "i"};
//
// 	Merger merger(con, "integers");
// 	for (auto &column : columns) {
// 		merger.AddColumn(column);
// 	}
// 	merger.AppendRow(33, 1);
// 	merger.Close();
//
// 	result = con.Query("SELECT b, j, k FROM integers");
// 	REQUIRE(CHECK_COLUMN(result, 0, {"key-0", "abc"}));
// 	REQUIRE(CHECK_COLUMN(result, 1, {0, 33}));
// 	REQUIRE(CHECK_COLUMN(result, 2, {0, 9}));
// }

// start Anybase changes
static void GroupingQuery(Connection &con, const string &sql) {
	auto result = con.Query(sql);
	REQUIRE_FALSE(result->HasError());
}

TEST_CASE("Unordered Merger grouping preserves full buckets and partial row groups", "[merger][grouping]") {
	DuckDB db(nullptr);
	Connection con(db);
	const idx_t existing = DEFAULT_ROW_GROUP_SIZE + 37;
	const idx_t count = existing + 3;
	GroupingQuery(con, "CREATE TABLE grouping_target(id BIGINT PRIMARY KEY, n BIGINT, s VARCHAR)");
	GroupingQuery(con, "INSERT INTO grouping_target SELECT i, -1, 'initial' FROM range(" +
	                       std::to_string(existing) + ") t(i)");

	std::vector<idx_t> order(count);
	std::iota(order.begin(), order.end(), 0);
	SECTION("Shuffled row IDs within buckets") {
		std::mt19937 rng(20261021);
		std::shuffle(order.begin(), order.end(), rng);
	}
	SECTION("Sorted buckets presented in reverse order") {
		std::sort(order.begin(), order.end(), [](idx_t a, idx_t b) {
			auto a_bucket = a / STANDARD_VECTOR_SIZE;
			auto b_bucket = b / STANDARD_VECTOR_SIZE;
			return a_bucket == b_bucket ? a < b : a_bucket > b_bucket;
		});
	}
	DataChunk batch;
	batch.Initialize(Allocator::DefaultAllocator(), {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR}, count);
	batch.SetCardinality(count);
	for (idx_t r = 0; r < count; ++r) {
		auto id = order[r];
		batch.SetValue(0, r, Value::BIGINT(id));
		batch.SetValue(1, r, id % 7 == 0 ? Value(LogicalType::BIGINT) : Value::BIGINT(id * 3));
		batch.SetValue(2, r, id % 11 == 0 ? Value(LogicalType::VARCHAR)
		                                    : Value("updated payload for row " + std::to_string(id)));
	}
	GroupingQuery(con, "BEGIN");
	{
		Merger merger(con, "grouping_target");
		merger.AppendDataChunk(batch);
		merger.Close();
	}
	GroupingQuery(con, "COMMIT");
	auto verify = [&]() {
		auto result = con.Query("SELECT count(*), count(*) FILTER (WHERE "
		                        "n IS DISTINCT FROM CASE WHEN id % 7 = 0 THEN NULL ELSE id * 3 END OR "
		                        "s IS DISTINCT FROM CASE WHEN id % 11 = 0 THEN NULL "
		                        "ELSE 'updated payload for row ' || id::VARCHAR END) FROM grouping_target");
		REQUIRE_FALSE(result->HasError());
		REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == int64_t(count));
		REQUIRE(result->GetValue(1, 0).GetValue<int64_t>() == 0);
	};
	verify();

	// Full-bucket updates must retain transaction rollback behavior.
	for (idx_t r = 0; r < count; ++r) {
		batch.SetValue(1, r, Value::BIGINT(-777));
		batch.SetValue(2, r, Value("rolled back payload"));
	}
	GroupingQuery(con, "BEGIN");
	{
		Merger merger(con, "grouping_target");
		merger.AppendDataChunk(batch);
		merger.Close();
	}
	GroupingQuery(con, "ROLLBACK");
	verify();

	// A duplicate must be rejected before a vector-sized bucket could overflow.
	DataChunk duplicates;
	duplicates.Initialize(Allocator::DefaultAllocator(), batch.GetTypes(), STANDARD_VECTOR_SIZE + 1);
	duplicates.SetCardinality(STANDARD_VECTOR_SIZE + 1);
	for (idx_t r = 0; r <= STANDARD_VECTOR_SIZE; ++r) {
		duplicates.SetValue(0, r, Value::BIGINT(r == STANDARD_VECTOR_SIZE ? 0 : STANDARD_VECTOR_SIZE - 1 - r));
		duplicates.SetValue(1, r, Value::BIGINT(-999));
		duplicates.SetValue(2, r, Value("duplicate payload"));
	}
	GroupingQuery(con, "BEGIN");
	{
		Merger merger(con, "grouping_target");
		merger.AppendDataChunk(duplicates);
		string error;
		try {
			merger.Close();
		} catch (const std::exception &ex) {
			error = ex.what();
		}
		merger.Clear();
		REQUIRE(error.find("can not update the same row twice") != string::npos);
	}
	GroupingQuery(con, "ROLLBACK");
	verify();
}

TEST_CASE("Merger duplicate-target checks preserve ordered and sparse inputs", "[merger][duplicates]") {
	DuckDB db(nullptr);
	Connection con(db);
	GroupingQuery(con, "CREATE TABLE duplicate_target(id BIGINT PRIMARY KEY, n BIGINT)");
	GroupingQuery(con, "INSERT INTO duplicate_target SELECT i, 0 FROM range(8197) t(i) ORDER BY i");
	std::vector<int64_t> keys;
	bool duplicate = false;
	SECTION("Ordered unique targets") { keys = {0, 1, 63, 64, 65, 8196}; }
	SECTION("Unordered targets across bit boundaries") { keys = {64, 1, 63, 0, 65, 2, 127, 126}; }
	SECTION("Adjacent duplicate targets") { keys = {0, 63, 63, 8196}; duplicate = true; }
	SECTION("Nonadjacent duplicate targets in a compact range") { keys = {64, 0, 63, 64}; duplicate = true; }
	SECTION("Sparse unique targets") { keys = {8196, 0, 4096}; }
	SECTION("Sparse duplicate targets") { keys = {8196, 0, 8196}; duplicate = true; }
	SECTION("Single target") { keys = {64}; }

	DataChunk batch;
	batch.Initialize(Allocator::DefaultAllocator(), {LogicalType::BIGINT, LogicalType::BIGINT}, keys.size());
	batch.SetCardinality(keys.size());
	int64_t expected_sum = 0;
	for (idx_t i = 0; i < keys.size(); ++i) {
		batch.SetValue(0, i, Value::BIGINT(keys[i]));
		batch.SetValue(1, i, Value::BIGINT(keys[i] * 3 + 1));
		expected_sum += keys[i] * 3 + 1;
	}
	GroupingQuery(con, "BEGIN");
	{
		Merger merger(con, "duplicate_target");
		merger.AppendDataChunk(batch);
		string error;
		try {
			merger.Close();
		} catch (const std::exception &ex) {
			error = ex.what();
		}
		merger.Clear();
		if (duplicate) {
			REQUIRE(error.find("can not update the same row twice") != string::npos);
		} else {
			REQUIRE(error.empty());
			auto result = con.Query("SELECT sum(n), count(*) FILTER (WHERE n <> 0) FROM duplicate_target");
			REQUIRE_FALSE(result->HasError());
			REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == expected_sum);
			REQUIRE(result->GetValue(1, 0).GetValue<int64_t>() == int64_t(keys.size()));
		}
	}
	GroupingQuery(con, "ROLLBACK");
	auto result = con.Query("SELECT count(*), sum(n) FROM duplicate_target");
	REQUIRE_FALSE(result->HasError());
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 8197);
	REQUIRE(result->GetValue(1, 0).GetValue<int64_t>() == 0);
}

TEST_CASE("Merger duplicate-target checks handle transaction-local row IDs", "[merger][duplicates]") {
	DuckDB db(nullptr);
	Connection con(db);
	GroupingQuery(con, "CREATE TABLE local_duplicate_target(id BIGINT PRIMARY KEY, n BIGINT)");
	GroupingQuery(con, "INSERT INTO local_duplicate_target VALUES (-1, 0)");
	GroupingQuery(con, "BEGIN");
	GroupingQuery(con, "INSERT INTO local_duplicate_target SELECT i, 0 FROM range(8197) t(i) ORDER BY i");
	std::vector<int64_t> keys;
	bool duplicate = true;
	SECTION("Ordered local targets") { keys = {0, 8196}; duplicate = false; }
	SECTION("Compact unordered local duplicates") {
		for (int64_t i = 15; i >= 0; --i) keys.push_back(i);
		keys.push_back(15);
	}
	SECTION("Sparse local duplicates") { keys = {8196, 0, 8196}; }
	DataChunk batch;
	batch.Initialize(Allocator::DefaultAllocator(), {LogicalType::BIGINT, LogicalType::BIGINT}, keys.size());
	batch.SetCardinality(keys.size());
	for (idx_t i = 0; i < keys.size(); ++i) {
		batch.SetValue(0, i, Value::BIGINT(keys[i]));
		batch.SetValue(1, i, Value::BIGINT(17));
	}
	{
		Merger merger(con, "local_duplicate_target");
		merger.AppendDataChunk(batch);
		string error;
		try {
			merger.Close();
		} catch (const std::exception &ex) {
			error = ex.what();
		}
		merger.Clear();
		if (duplicate) {
			REQUIRE(error.find("can not update the same row twice") != string::npos);
		} else {
			REQUIRE(error.empty());
			auto result = con.Query("SELECT sum(n) FROM local_duplicate_target");
			REQUIRE_FALSE(result->HasError());
			REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 34);
		}
	}
	GroupingQuery(con, "ROLLBACK");
	auto result = con.Query("SELECT count(*), sum(n) FROM local_duplicate_target");
	REQUIRE_FALSE(result->HasError());
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 1);
	REQUIRE(result->GetValue(1, 0).GetValue<int64_t>() == 0);
}

TEST_CASE("Conflict recording deduplicates input positions across indexes", "[merger][conflicts]") {
	unordered_set<column_t> target;
	ConflictInfo info(target);
	const idx_t size = 8197;
	ConflictManager manager(VerifyExistenceType::APPEND, size, &info);
	manager.SetMode(ConflictManagerMode::SCAN);
	REQUIRE_FALSE(manager.HasConflicts());
	REQUIRE_FALSE(manager.AddNull(0));
	REQUIRE_FALSE(manager.HasConflicts());
	const idx_t positions[] = {4096, 0, 8196, 63, 64, 65, 2047, 2048};
	for (idx_t i = 0; i < 8; ++i) {
		REQUIRE_FALSE(manager.AddHit(positions[i], row_t(100 + i)));
	}
	manager.FinishLookup(); // Untargeted DO NOTHING can scan further unique indexes.
	for (idx_t i = 0; i < 8; ++i) {
		REQUIRE_FALSE(manager.AddHit(positions[i], row_t(900 + i)));
	}
	REQUIRE(manager.ConflictCount() == 8);
	for (idx_t i = 0; i < 8; ++i) {
		REQUIRE(manager.GetInvertedSel().get_index(i) == positions[i]);
		REQUIRE(FlatVector::GetData<row_t>(manager.GetRowIds())[i] == row_t(100 + i));
		REQUIRE(manager.GetFirstValidity().RowIsValid(positions[i]));
	}
	REQUIRE(manager.GetFirstInvalidIndex(size).GetIndex() == 0);
	REQUIRE(manager.GetFirstInvalidIndex(size, true).GetIndex() == 1);
	manager.SetMode(ConflictManagerMode::THROW);
	REQUIRE_FALSE(manager.AddHit(4096, 123));
	REQUIRE(manager.AddHit(1, 123));
}

TEST_CASE("Conflict recording preserves empty and finished manager behavior", "[merger][conflicts]") {
	ConflictManager throwing(VerifyExistenceType::APPEND, 2);
	REQUIRE(throwing.AddHit(0, 123));
	REQUIRE_FALSE(throwing.AddNull(1));
	REQUIRE_FALSE(throwing.HasConflicts());
	unordered_set<column_t> target {0};
	ConflictInfo info(target);
	ConflictManager manager(VerifyExistenceType::APPEND, 2, &info);
	manager.SetMode(ConflictManagerMode::SCAN);
	manager.FinishLookup(); // No hits must not finish a scan.
	REQUIRE_FALSE(manager.HasConflicts());
	REQUIRE_FALSE(manager.AddHit(1, 42));
	manager.FinishLookup(); // Duplicate matching indexes must not record new hits.
	REQUIRE_FALSE(manager.AddHit(0, 99));
	REQUIRE(manager.ConflictCount() == 1);
	manager.SetMode(ConflictManagerMode::THROW);
	REQUIRE_FALSE(manager.AddHit(1, 42));
	REQUIRE(manager.AddHit(0, 99));
}

TEST_CASE("Hit dispatch preserves secondary hits and foreign key null checks", "[merger][conflicts]") {
	unordered_set<column_t> target {0};
	ConflictInfo info(target);
	SECTION("Secondary hits respect THROW and finished scans") {
		ConflictManager manager(VerifyExistenceType::APPEND, 4, &info);
		REQUIRE(manager.AddSecondHit(0, 20));
		REQUIRE_FALSE(manager.HasConflicts());
		manager.SetMode(ConflictManagerMode::SCAN);
		REQUIRE_FALSE(manager.AddHit(2, 10));
		REQUIRE_FALSE(manager.AddSecondHit(2, 20));
		REQUIRE(manager.ConflictCount() == 1);
		REQUIRE(FlatVector::GetData<row_t>(manager.GetRowIds())[0] == 10);
		manager.FinishLookup();
		REQUIRE_FALSE(manager.AddHit(1, 30));
		REQUIRE_FALSE(manager.AddSecondHit(1, 40));
		REQUIRE(manager.ConflictCount() == 1);
		REQUIRE_FALSE(manager.GetFirstValidity().RowIsValid(1));
		manager.SetMode(ConflictManagerMode::THROW);
		REQUIRE_FALSE(manager.AddSecondHit(2, 99));
		REQUIRE(manager.AddSecondHit(1, 99));
	}
	SECTION("Foreign key NULL hits keep their existence-check semantics") {
		ConflictManager manager(VerifyExistenceType::APPEND_FK, 4, &info);
		REQUIRE(manager.AddNull(1));
		REQUIRE_FALSE(manager.HasConflicts());
		manager.SetMode(ConflictManagerMode::SCAN);
		REQUIRE_FALSE(manager.AddNull(1));
		REQUIRE(manager.ConflictCount() == 1);
		REQUIRE(manager.GetFirstValidity().RowIsValid(1));
		manager.SetMode(ConflictManagerMode::THROW);
		REQUIRE_FALSE(manager.AddNull(1));
		REQUIRE(manager.AddNull(2));
		ConflictManager deleting(VerifyExistenceType::DELETE_FK, 4, &info);
		REQUIRE_FALSE(deleting.AddNull(1));
		REQUIRE_FALSE(deleting.HasConflicts());
	}
}

static void ConflictQuery(Connection &con, const string &sql) {
	auto result = con.Query(sql);
	INFO(sql);
	REQUIRE_FALSE(result->HasError());
}
static int64_t ConflictScalar(Connection &con, const string &sql) {
	auto result = con.Query(sql);
	INFO(sql);
	REQUIRE_FALSE(result->HasError());
	return result->GetValue(0, 0).GetValue<int64_t>();
}

TEST_CASE("SQL conflict checks retain secondary unique and null semantics", "[merger][conflicts]") {
	DuckDB db(nullptr);
	Connection con(db);
	ConflictQuery(con, "CREATE TABLE conflict_target(id INTEGER PRIMARY KEY, u INTEGER UNIQUE, n INTEGER)");
	ConflictQuery(con, "INSERT INTO conflict_target VALUES (1, 10, 100), (2, 20, 200)");
	ConflictQuery(con, "INSERT INTO conflict_target VALUES (1, 10, 999), (3, 20, 999), (4, 40, 400) ON CONFLICT DO NOTHING");
	REQUIRE(ConflictScalar(con, "SELECT count(*) FROM conflict_target") == 3);
	REQUIRE(ConflictScalar(con, "SELECT sum(n) FROM conflict_target") == 700);
	ConflictQuery(con, "INSERT INTO conflict_target VALUES (1, 10, 101) ON CONFLICT(id) DO UPDATE SET n=excluded.n");
	REQUIRE(ConflictScalar(con, "SELECT n FROM conflict_target WHERE id=1") == 101);
	{
		auto result = con.Query("INSERT INTO conflict_target VALUES (5, 20, 500) ON CONFLICT(id) DO UPDATE SET n=excluded.n");
		REQUIRE(result->HasError());
	}
	{
		auto result = con.Query("INSERT INTO conflict_target VALUES (1, 50, 500)");
		REQUIRE(result->HasError());
	}
	// Separate statements avoid the fork's existing multi-row DO NOTHING NULL deduplication behavior.
	ConflictQuery(con, "INSERT INTO conflict_target VALUES (6, NULL, 600) ON CONFLICT DO NOTHING");
	ConflictQuery(con, "INSERT INTO conflict_target VALUES (7, NULL, 700) ON CONFLICT DO NOTHING");
	REQUIRE(ConflictScalar(con, "SELECT count(*) FROM conflict_target WHERE u IS NULL") == 2);
	ConflictQuery(con, "CREATE TABLE conflict_child(id INTEGER REFERENCES conflict_target(id))");
	ConflictQuery(con, "INSERT INTO conflict_child VALUES (1), (NULL)");
	{
		auto result = con.Query("INSERT INTO conflict_child VALUES (999)");
		REQUIRE(result->HasError());
	}
	{
		auto result = con.Query("DELETE FROM conflict_target WHERE id=1");
		REQUIRE(result->HasError());
	}
}

TEST_CASE("Conflict checks choose visible row IDs after delete and reinsertion", "[merger][conflicts]") {
	DuckDB db(nullptr);
	Connection writer(db), reader(db);
	ConflictQuery(writer, "CREATE TABLE conflict_visible(id INTEGER PRIMARY KEY, n INTEGER)");
	ConflictQuery(writer, "INSERT INTO conflict_visible VALUES (1, 10)");
	ConflictQuery(reader, "BEGIN");
	REQUIRE(ConflictScalar(reader, "SELECT n FROM conflict_visible WHERE id=1") == 10);
	ConflictQuery(writer, "BEGIN");
	ConflictQuery(writer, "DELETE FROM conflict_visible WHERE id=1");
	ConflictQuery(writer, "INSERT INTO conflict_visible VALUES (1, 20)");
	ConflictQuery(writer, "COMMIT");
	ConflictQuery(writer, "INSERT INTO conflict_visible VALUES (1, 30) ON CONFLICT(id) DO UPDATE SET n=excluded.n");
	REQUIRE(ConflictScalar(writer, "SELECT n FROM conflict_visible WHERE id=1") == 30);
	REQUIRE(ConflictScalar(reader, "SELECT n FROM conflict_visible WHERE id=1") == 10);
	ConflictQuery(reader, "COMMIT");
}

TEST_CASE("ART prefix comparisons preserve string hits and misses at word boundaries", "[merger][conflicts][artprefix]") {
	DuckDB db(nullptr);
	Connection con(db);
	std::vector<idx_t> lengths;
	for (idx_t n = 0; n <= 33; n++) {
		lengths.push_back(n);
	}
	for (idx_t n : {63, 64, 127, 238, 239, 240, 241, 479, 480, 511}) {
		lengths.push_back(n);
	}
	for (auto n : lengths) {
		INFO("shared prefix length: " << n);
		ConflictQuery(con, "CREATE TABLE prefix_target(k VARCHAR PRIMARY KEY, v INTEGER)");
		string base(n, 'a');
		set<string> keys {base, base + "suffix", base + "other", base.substr(0, n / 2),
		                  base + string("\0tail", 5)};
		set<idx_t> positions;
		for (idx_t i = 0; i < MinValue<idx_t>(n, 34); i++) {
			positions.insert(i);
		}
		if (n) {
			positions.insert(n / 2);
			positions.insert(n - 1);
		}
		if (n > 239) {
			positions.insert(239);
		}
		for (auto pos : positions) {
			auto mismatch = base;
			mismatch[pos] = 'b';
			keys.insert(mismatch);
		}
		int32_t value = 0;
		// Insert longer keys before their shorter prefixes to exercise bounded misses.
		for (auto it = keys.rbegin(); it != keys.rend(); ++it) {
			auto &key = *it;
			auto inserted = con.Query("INSERT INTO prefix_target VALUES (?, ?)", key, value);
			REQUIRE_FALSE(inserted->HasError());
			value++;
		}
		value = 10000;
		for (auto &key : keys) {
			auto duplicate = con.Query("INSERT INTO prefix_target VALUES (?, -1)", key);
			REQUIRE(duplicate->HasError());
			auto updated = con.Query("INSERT INTO prefix_target VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
			                         key, value);
			REQUIRE_FALSE(updated->HasError());
			auto fetched = con.Query("SELECT v FROM prefix_target WHERE k=?", key);
			REQUIRE_FALSE(fetched->HasError());
			auto chunk = fetched->Fetch();
			REQUIRE(chunk);
			REQUIRE(chunk->size() == 1);
			REQUIRE(chunk->GetValue(0, 0).GetValue<int32_t>() == value);
			value++;
		}
		auto missing = base + "__missing__";
		auto inserted = con.Query("INSERT INTO prefix_target VALUES (?, -7) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
		                          missing);
		REQUIRE_FALSE(inserted->HasError());
		REQUIRE(ConflictScalar(con, "SELECT count(*) FROM prefix_target") == int64_t(keys.size() + 1));
		auto fetched = con.Query("SELECT v FROM prefix_target WHERE k=?", missing);
		REQUIRE_FALSE(fetched->HasError());
		auto chunk = fetched->Fetch();
		REQUIRE(chunk);
		REQUIRE(chunk->size() == 1);
		REQUIRE(chunk->GetValue(0, 0).GetValue<int32_t>() == -7);
		ConflictQuery(con, "DROP TABLE prefix_target");
	}
}

TEST_CASE("ART lookup dispatch preserves node types and terminal node identity", "[merger][conflicts][artdispatch]") {
	DuckDB db(nullptr);
	Connection con(db);
	ConflictQuery(con, "CREATE TABLE dispatch_target(k INTEGER)");
	ConflictQuery(con, "CREATE UNIQUE INDEX dispatch_idx ON dispatch_target(k)");
	con.context->RunFunctionInTransaction([&]() {
		auto &table = Catalog::GetEntry<TableCatalogEntry>(*con.context, INVALID_CATALOG, DEFAULT_SCHEMA, "dispatch_target");
		auto index = table.GetStorage().GetDataTableInfo()->GetIndexes().Find("dispatch_idx");
		REQUIRE(index);
		auto &art = index->Cast<ART>();
		IndexLock lock;
		art.InitializeLock(lock);
		data_t byte = 0;
		ARTKey key(&byte, 1);
		ARTKey empty_key;
		Node empty;
		REQUIRE_FALSE(ARTOperator::Lookup(art, empty, empty_key, 0));
		for (auto type : {NType::LEAF, NType::LEAF_INLINED, NType::NODE_7_LEAF, NType::NODE_15_LEAF,
		                  NType::NODE_256_LEAF}) {
			Node terminal;
			terminal.SetMetadata(static_cast<uint8_t>(type));
			REQUIRE(ARTOperator::Lookup(art, terminal, empty_key, 0).get() == &terminal);
		}
		for (auto metadata : {1, 2, 3, 4, 5, 6, 42, 0}) {
			Node gate;
			gate.SetMetadata(static_cast<uint8_t>(metadata | 0x80));
			REQUIRE(ARTOperator::Lookup(art, gate, empty_key, 0).get() == &gate);
		}
		Node invalid;
		invalid.SetMetadata(42);
		REQUIRE_THROWS_WITH(ARTOperator::Lookup(art, invalid, key, 0),
		                    Catch::Matchers::Contains("Invalid node type for GetChildInternal: 42."));
		// Interleaved lookups must defer later errors and skip NULL lanes.
		{
			array<ARTKey, 8> batch_keys;
			array<ARTOperator::LookupResult, 8> results;
			for (idx_t i = 1; i < 8; i++) batch_keys[i] = key;
			ARTOperator::LookupBatch(art, invalid, batch_keys.data(), 5, results);
			REQUIRE_FALSE(results[0].leaf);
			REQUIRE_FALSE(results[0].error);
			for (idx_t i = 1; i < 5; i++) {
				REQUIRE_FALSE(results[i].leaf);
				REQUIRE(bool(results[i].error));
				REQUIRE_THROWS_WITH(std::rethrow_exception(results[i].error),
				                    Catch::Matchers::Contains("Invalid node type for GetChildInternal: 42."));
			}
			Node terminal;
			terminal.SetMetadata(static_cast<uint8_t>(NType::LEAF_INLINED));
			terminal.SetRowId(77);
			ARTOperator::LookupBatch(art, terminal, batch_keys.data(), 8, results);
			REQUIRE_FALSE(results[0].leaf);
			for (idx_t i = 1; i < 8; i++) {
				REQUIRE(results[i].leaf.get() == &terminal);
				REQUIRE_FALSE(results[i].error);
			}
		}
		const NType types[] = {NType::NODE_4, NType::NODE_16, NType::NODE_48, NType::NODE_256};
		const idx_t capacities[] = {4, 16, 48, 256};
		for (idx_t t = 0; t < 4; t++) {
			INFO("internal node type: " << static_cast<int>(types[t]));
			Node root;
			Node::New(art, root, types[t]);
			for (idx_t i = 0; i < capacities[t] - 1; i++) {
				auto child_byte = static_cast<uint8_t>((i * 73) & 255);
				Node child;
				child.SetMetadata(static_cast<uint8_t>(NType::LEAF_INLINED));
				child.SetRowId(1000 + child_byte);
				Node::InsertChild(art, root, child_byte, child);
			}
			REQUIRE(root.GetType() == types[t]);
			for (idx_t base = 0; base < 256; base += 8) {
				array<data_t, 8> bytes;
				array<ARTKey, 8> batch_keys;
				array<ARTOperator::LookupResult, 8> results;
				for (idx_t i = 0; i < 8; i++) {
					bytes[i] = static_cast<data_t>(base + i);
					batch_keys[i] = ARTKey(&bytes[i], 1);
				}
				ARTOperator::LookupBatch(art, root, batch_keys.data(), 8, results);
				for (idx_t i = 0; i < 8; i++) {
					REQUIRE_FALSE(results[i].error);
					REQUIRE(results[i].leaf.get() == ARTOperator::Lookup(art, root, batch_keys[i], 0).get());
				}
			}
			for (idx_t b = 0; b < 256; b++) {
				byte = static_cast<data_t>(b);
				auto expected = root.GetChild(art, byte);
				auto actual = ARTOperator::Lookup(art, root, key, 0);
				REQUIRE(actual.get() == expected.get());
				if (actual) {
					REQUIRE(actual->GetRowId() == row_t(1000 + b));
				}
			}
			// Small nodes retain an entry for an empty child and must report corruption.
			root.ReplaceChild(art, 0, Node());
			byte = 0;
			if (types[t] == NType::NODE_256) {
				REQUIRE_FALSE(ARTOperator::Lookup(art, root, key, 0));
			} else {
				REQUIRE_THROWS_AS(ARTOperator::Lookup(art, root, key, 0), InternalException);
			}
			Node::FreeNode(art, root);
		}
	});
}

TEST_CASE("ART buffer lookup pins survive collisions and unwind before allocator mutation", "[merger][bufferreuse]") {
	DuckDB db(nullptr);
	Connection con(db);
	ConflictQuery(con, "CREATE TABLE buffer_pin_target(k INTEGER)");
	ConflictQuery(con, "CREATE UNIQUE INDEX buffer_pin_idx ON buffer_pin_target(k)");
	con.context->RunFunctionInTransaction([&]() {
		auto &table = Catalog::GetEntry<TableCatalogEntry>(*con.context, INVALID_CATALOG, DEFAULT_SCHEMA, "buffer_pin_target");
		auto index = table.GetStorage().GetDataTableInfo()->GetIndexes().Find("buffer_pin_idx");
		REQUIRE(index);
		auto &art = index->Cast<ART>();
		IndexLock lock;
		art.InitializeLock(lock);
		auto &blocks = Node::GetAllocator(art, NType::NODE_256).block_manager;
		// Two segments per buffer force 300 buffers, exceeding the 256-entry cache bound.
		FixedSizeAllocator allocator(blocks.GetBlockSize() / 3, blocks);
		std::vector<IndexPointer> pointers;
		for (idx_t i = 0; i < 600; i++) {
			auto ptr = allocator.New();
			Store<uint64_t>(1000 + i, allocator.Get(ptr, true));
			pointers.push_back(ptr);
		}
		{
			FixedSizeAllocator::LookupCache cache;
			cache.Bind(allocator);
			for (idx_t pass = 0; pass < 3; pass++) {
				for (idx_t r = 0; r < pointers.size(); r++) {
					auto i = pass % 2 ? pointers.size() - 1 - r : r;
					REQUIRE(Load<uint64_t>(allocator.Get(pointers[i], false)) == 1000 + i);
				}
			}
			// Preserve a read-to-dirty transition for an already cached buffer.
			Store<uint64_t>(777, allocator.Get(pointers[0], true));
			REQUIRE(Load<uint64_t>(allocator.Get(pointers[0], false)) == 777);
		}
		try {
			FixedSizeAllocator::LookupCache cache;
			cache.Bind(allocator);
			REQUIRE(Load<uint64_t>(allocator.Get(pointers[0], false)) == 777);
			throw InvalidInputException("exercise cache cleanup");
		} catch (const InvalidInputException &) {
		}
		for (auto ptr : pointers) allocator.Free(ptr);
		allocator.Reset();
		auto ptr = allocator.New();
		Store<uint64_t>(123, allocator.Get(ptr, true));
		REQUIRE(Load<uint64_t>(allocator.Get(ptr, false)) == 123);
		allocator.Free(ptr);
	});
}

TEST_CASE("Large Merger buffer reuse survives cold indexes rollback and checkpoint", "[merger][bufferreuse]") {
	auto path = TestDirectoryPath() + "/merger_buffer_reuse.db";
	DeleteDatabase(path);
	const idx_t count = 8193;
	{
		DuckDB db(path);
		Connection con(db);
		ConflictQuery(con, "CREATE TABLE buffer_merge_target(k VARCHAR PRIMARY KEY, v BIGINT)");
		ConflictQuery(con, "INSERT INTO buffer_merge_target SELECT repeat(md5(i::VARCHAR),4) || i::VARCHAR, -1 FROM range(8193) t(i)");
		ConflictQuery(con, "CHECKPOINT");
	}
	{
		DuckDB db(path);
		Connection con(db);
		auto result = con.Query("SELECT k FROM buffer_merge_target ORDER BY rowid DESC");
		REQUIRE_FALSE(result->HasError());
		DataChunk batch;
		batch.Initialize(Allocator::DefaultAllocator(), {LogicalType::VARCHAR, LogicalType::BIGINT}, count);
		batch.SetCardinality(count);
		idx_t row = 0;
		while (auto chunk = result->Fetch()) {
			for (idx_t i = 0; i < chunk->size(); i++) {
				batch.SetValue(0, row, chunk->GetValue(0, i));
				batch.SetValue(1, row, Value::BIGINT(row * 7));
				row++;
			}
		}
		REQUIRE(row == count);
		for (bool rollback : {true, false}) {
			ConflictQuery(con, "BEGIN");
			{
				Merger merger(con, "buffer_merge_target");
				merger.AppendDataChunk(batch);
				merger.Close();
			}
			REQUIRE(ConflictScalar(con, "SELECT count(*) FROM buffer_merge_target WHERE v>=0") == int64_t(count));
			ConflictQuery(con, rollback ? "ROLLBACK" : "COMMIT");
			if (rollback) {
				REQUIRE(ConflictScalar(con, "SELECT count(*) FROM buffer_merge_target WHERE v=-1") == int64_t(count));
			}
			ConflictQuery(con, "CHECKPOINT");
		}
	}
	{
		DuckDB db(path);
		Connection con(db);
		REQUIRE(ConflictScalar(con, "SELECT count(*) FROM buffer_merge_target") == int64_t(count));
		REQUIRE(ConflictScalar(con, "SELECT sum(v) FROM buffer_merge_target") == int64_t(7 * count * (count - 1) / 2));
		ConflictQuery(con, "DROP TABLE buffer_merge_target");
		ConflictQuery(con, "CHECKPOINT");
	}
	DeleteDatabase(path);
}

TEST_CASE("Large Merger conflict verification reports the first input error after nulls", "[merger][bufferreuse][interleave]") {
	DuckDB db(nullptr);
	Connection con(db);
	ConflictQuery(con, "CREATE TABLE first_error_target(id BIGINT PRIMARY KEY, u BIGINT UNIQUE)");
	ConflictQuery(con, "INSERT INTO first_error_target SELECT i, i FROM range(8193) t(i)");
	DataChunk batch;
	batch.Initialize(Allocator::DefaultAllocator(), {LogicalType::BIGINT, LogicalType::BIGINT}, 8193);
	batch.SetCardinality(8193);
	for (idx_t i = 0; i < batch.size(); i++) {
		batch.SetValue(0, i, Value::BIGINT(8193 + i));
		batch.SetValue(1, i, i == 0 ? Value(LogicalType::BIGINT) : Value::BIGINT(i == 2 ? 0 : i == 3 ? 1 : 8193 + i));
	}
	ConflictQuery(con, "BEGIN");
	string error;
	{
		Merger merger(con, "first_error_target");
		try {
			merger.AppendDataChunk(batch);
			merger.Close();
		} catch (const std::exception &ex) {
			error = ex.what();
		}
		merger.Clear();
	}
	REQUIRE(error.find("u: 0") != string::npos);
	ConflictQuery(con, "ROLLBACK");
	REQUIRE(ConflictScalar(con, "SELECT count(*) FROM first_error_target") == 8193);
	ConflictQuery(con, "CHECKPOINT");
}
// end Anybase changes
