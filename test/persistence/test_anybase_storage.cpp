#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb.h"
#include "duckdb/common/types/data_chunk.hpp"
#include <algorithm>
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/update_segment.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

TEST_CASE("Anybase versions persist alongside 1.5 VARIANT metadata", "[anybase_storage]") {
	auto path = TestCreatePath("anybase_versions_15.db");
	DeleteDatabase(path);
	duckdb::vector<idx_t> before;
	idx_t table_version;
	const duckdb::vector<string> names = {"id", "text_value", "nested", "items", "fixed", "dynamic"};
	{
		DBConfig config;
		config.options.serialization_compatibility = SerializationCompatibility::FromString("v1.5.0");
		DuckDB db(path, &config);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE versions (id INTEGER, text_value VARCHAR, nested STRUCT(a INTEGER), "
		                          "items INTEGER[], fixed INTEGER[2], dynamic VARIANT)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO versions SELECT i, repeat('long-value', 20), {'a': i}, [i, i+1], "
		                          "[i, i+1]::INTEGER[2], {'a': i}::VARIANT FROM range(100) t(i)"));
		table_version = con.context->GetTableVersion("main", "versions");
		REQUIRE(table_version > 0);
		for (auto &name : names) {
			before.push_back(con.context->GetColumnVersion("main", "versions", name.c_str()));
			REQUIRE(before.back() > 0);
		}
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
	}
	{
		DuckDB db(path);
		Connection con(db);
		REQUIRE(con.context->GetTableVersion("main", "versions") == table_version);
		for (idx_t i = 0; i < names.size(); i++) {
			REQUIRE(con.context->GetColumnVersion("main", "versions", names[i].c_str()) == before[i]);
		}
		auto result = con.Query("SELECT count(*), sum(id) FROM versions WHERE dynamic.a::INTEGER = id");
		REQUIRE_NO_FAIL(*result);
		REQUIRE(CHECK_COLUMN(result, 0, {100}));
		REQUIRE(CHECK_COLUMN(result, 1, {4950}));
		REQUIRE_NO_FAIL(con.Query("UPDATE versions SET text_value = repeat('changed-value', 30) WHERE id IN (1,2)"));
		REQUIRE(con.context->GetTableVersion("main", "versions") == table_version + 1);
		REQUIRE(con.context->GetColumnVersion("main", "versions", "text_value") == before[1] + 1);
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
	}
	{
		DuckDB db(path);
		Connection con(db);
		REQUIRE(con.context->GetTableVersion("main", "versions") == table_version + 1);
		REQUIRE(con.context->GetColumnVersion("main", "versions", "text_value") == before[1] + 1);
	}
	DeleteDatabase(path);
}

TEST_CASE("Anybase merger preserves defaults and updates rows across row groups", "[anybase_storage][merger]") {
	auto path = TestCreatePath("anybase_merger_15.db");
	DeleteDatabase(path);
	{
		DuckDB db(nullptr);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS merge_db (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v1.5.0')"));
		REQUIRE_NO_FAIL(con.Query("USE merge_db"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE target(id BIGINT PRIMARY KEY, val VARCHAR DEFAULT 'default-value')"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO target SELECT i, 'original' FROM range(6000) t(i)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
		{
			Merger merger(con, "target");
			merger.AppendRow(int64_t(4097), "updated-high");
			merger.AppendRow(int64_t(10), "updated-low");
			merger.AppendRow(int64_t(7000), "inserted");
			merger.Close();
		}
		auto result = con.Query("SELECT id, val FROM target WHERE id IN (10,4097,7000) ORDER BY id");
		REQUIRE_NO_FAIL(*result);
		REQUIRE(CHECK_COLUMN(result, 0, {10,4097,7000}));
		REQUIRE(CHECK_COLUMN(result, 1, {"updated-low","updated-high","inserted"}));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
	}
	{
		DuckDB db(path);
		Connection con(db);
		auto result = con.Query("SELECT count(*) FROM target");
		REQUIRE_NO_FAIL(*result);
		REQUIRE(CHECK_COLUMN(result, 0, {6001}));
	}
	DeleteDatabase(path);
}

namespace {
struct AnybaseCDCValues {
	duckdb::vector<int64_t> previous_updates;
	duckdb::vector<int64_t> current_updates;
	duckdb::vector<int64_t> deleted_ids;
};
AnybaseCDCValues *anybase_cdc_values = nullptr;

void CaptureAnybaseCDC(cdc_event_type type, const char *, idx_t count, idx_t, idx_t *, const char *table,
                       const char **names, idx_t *, duckdb_data_chunk current, duckdb_data_chunk previous) {
	if (table && string(table) == "events" && anybase_cdc_values) {
		for (idx_t col = 0; col < count; col++) {
			if (type == DUCKDB_CDC_EVENT_UPDATE && string(names[col]) == "val") {
				auto &old_values = *reinterpret_cast<DataChunk *>(previous);
				auto &new_values = *reinterpret_cast<DataChunk *>(current);
				for (idx_t row = 0; row < old_values.size(); row++) {
					anybase_cdc_values->previous_updates.push_back(old_values.GetValue(col, row).GetValue<int64_t>());
					anybase_cdc_values->current_updates.push_back(new_values.GetValue(col, row).GetValue<int64_t>());
				}
			}
			if (type == DUCKDB_CDC_EVENT_DELETE && string(names[col]) == "id") {
				auto &old_values = *reinterpret_cast<DataChunk *>(previous);
				for (idx_t row = 0; row < old_values.size(); row++) {
					anybase_cdc_values->deleted_ids.push_back(old_values.GetValue(col, row).GetValue<int64_t>());
				}
			}
		}
	}
	if (current) {
		duckdb_destroy_data_chunk(&current);
	}
	if (previous) {
		duckdb_destroy_data_chunk(&previous);
	}
}
} // namespace

TEST_CASE("Anybase CDC uses absolute positions for separate 1.5 row groups", "[anybase_storage][anybase_cdc]") {
	auto path = TestCreatePath("anybase_cdc_15.db");
	DeleteDatabase(path);
	{
		DuckDB db(nullptr);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS cdc_db (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v1.5.0')"));
		REQUIRE_NO_FAIL(con.Query("USE cdc_db"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE events(id BIGINT, val BIGINT)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO events SELECT i, i FROM range(6000) t(i)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
		AnybaseCDCValues values;
		anybase_cdc_values = &values;
		auto &config = DBConfig::GetConfig(*con.context);
		config.change_data_capture.function = CaptureAnybaseCDC;
		REQUIRE_NO_FAIL(con.Query("UPDATE events SET val = val + 10000 WHERE id IN (10,4097)"));
		REQUIRE_NO_FAIL(con.Query("DELETE FROM events WHERE id = 4098"));
		config.change_data_capture.function = nullptr;
		anybase_cdc_values = nullptr;
		std::sort(values.previous_updates.begin(), values.previous_updates.end());
		std::sort(values.current_updates.begin(), values.current_updates.end());
		REQUIRE(values.previous_updates == duckdb::vector<int64_t> {10,4097});
		REQUIRE(values.current_updates == duckdb::vector<int64_t> {10010,14097});
		REQUIRE(values.deleted_ids == duckdb::vector<int64_t> {4098});
	}
	DeleteDatabase(path);
}

TEST_CASE("String-memory fix handles repeated long VARCHAR and BLOB updates", "[anybase_storage][anybase_string_memory]") {
	auto path = TestCreatePath("anybase_string_updates_15.db");
	DeleteDatabase(path);
	{
		DuckDB db(path);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(id INTEGER, text_value VARCHAR, binary_value BLOB)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO strings SELECT i, repeat('original',100), repeat('binary',100)::BLOB "
		                          "FROM range(100) t(i)"));
		for (idx_t cycle = 0; cycle < 12; cycle++) {
			REQUIRE_NO_FAIL(con.Query("UPDATE strings SET text_value = CASE WHEN id % 3 = 0 THEN NULL "
			                          "ELSE repeat('changed-value',100) END, "
			                          "binary_value = CASE WHEN id % 3 = 0 THEN NULL ELSE repeat('changed-blob',100)::BLOB END"));
			REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
			REQUIRE_NO_FAIL(con.Query("UPDATE strings SET text_value = repeat('restored',100), "
			                          "binary_value = repeat('restored-binary',100)::BLOB"));
			REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
		}
	}
	{
		DuckDB db(path);
		Connection con(db);
		auto result = con.Query("SELECT count(*) FROM strings WHERE text_value = repeat('restored',100) "
		                        "AND binary_value = repeat('restored-binary',100)::BLOB");
		REQUIRE_NO_FAIL(*result);
		REQUIRE(CHECK_COLUMN(result, 0, {100}));
	}
	DeleteDatabase(path);
}

TEST_CASE("No-op long-string updates do not grow the update heap", "[anybase_storage][anybase_string_memory]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(text_value VARCHAR, binary_value BLOB)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings SELECT 'value', 'binary'::BLOB FROM range(4)"));
	con.context->RunFunctionInTransaction([&]() {
		auto &table = Catalog::GetEntry<TableCatalogEntry>(*con.context, INVALID_CATALOG, DEFAULT_SCHEMA, "strings");
		auto &storage = table.GetStorage();
		auto row_group = storage.GetRowGroupCollection()->GetRowGroups()->GetSegment(0);
		REQUIRE(row_group);
		for (idx_t column_idx = 0; column_idx < 2; column_idx++) {
			auto &column = row_group->GetNode().GetRawColumnData(column_idx);
			UpdateSegment segment(column);
			const auto initial_bytes = segment.GetStringHeap().AllocationSize();
			const string long_value(65536, 'x');
			const auto value = column_idx == 0 ? Value(long_value) : Value::BLOB(long_value);
			for (bool include_null : {false, true}) {
				Vector base(column.type);
				for (idx_t row = 0; row < 4; row++) {
					base.SetValue(row, include_null && row == 3 ? Value(column.type) : value);
				}
				row_t ids[] = {0, 1, 2, 3};
				for (idx_t cycle = 0; cycle < 100; cycle++) {
					Vector update(column.type);
					for (idx_t row = 0; row < 4; row++) {
						update.SetValue(row, base.GetValue(row));
					}
					segment.Update(TransactionData(0, 0), storage, column_idx, update, ids, 4, base, 0);
				}
				REQUIRE_FALSE(segment.HasUpdates());
				REQUIRE(segment.GetStringHeap().AllocationSize() == initial_bytes);
			}
		}
	});
}
