#include "catch.hpp"
#include "duckdb/main/appender.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include <vector>
#include <random>
#include <algorithm>
#include <sys/time.h>

using namespace duckdb;
using namespace std;

TEST_CASE("Basic merger tests", "[merger]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 16)"));

	result = con.Query("SELECT j FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {16}));

	Merger merger(con, "integers");
	merger.BeginRow();
	merger.Append<int32_t>(1);
	merger.Append<int32_t>(18);	
	merger.EndRow();

	merger.Flush();

	result = con.Query("SELECT j FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {18}));
}

TEST_CASE("Test multiple AppendRow", "[merger]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY, j INTEGER)"));

	// append a bunch of values
	{
		Appender appender(con, "integers");
		for (size_t i = 0; i < 2000; i++) {
		  appender.AppendRow((int32_t)(i), (int32_t)(i));
		}
		appender.Close();
	}

	// merge rows
	// change even key value
	duckdb::vector<duckdb::Value> values;
	{
		Merger merger(con, "integers");
		for (size_t i = 0; i < 2000; i++) {
		  if ((i % 2 ) == 0) {
		    merger.AppendRow( (int32_t)(i),  (int32_t)(2*i));
		    values.push_back(duckdb::Value((int32_t)(2*i)));
		  }
		  else {
		    values.push_back(duckdb::Value((int32_t)i));
		  }
		}
		merger.Close();
	}
	
	result = con.Query("SELECT j FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, values));
}

TEST_CASE("Random key order updates test", "[merger]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i BIGINT, j VARCHAR, b VARCHAR, PRIMARY KEY (i))"));

	// append a bunch of values

	Appender appender(con, "integers");
	for (size_t i = 0; i < 1000000; i++) {
		appender.AppendRow((int64_t)(i), "key-", "val-");
	}
	appender.Close();

	duckdb::vector<duckdb::Value> values;
	auto maxValues = 100000;
	auto endValue = maxValues+1;
	auto startValue = 0;
	for (idx_t i = 0; i < maxValues; i++) {
		//		if (i % 2 == 0) {
		//			values.push_back(endValue--);
		//		}
		//		else {
		//			values.push_back(startValue++);
		//		}
		values.push_back((int) i);
	}

	for (idx_t c = 0; c < 1; c++) {
		auto rng = std::default_random_engine {};
		std::shuffle(std::begin(values), std::end(values), rng);

		struct timeval start_t;
		Printer::Print("LocalAppend upsert?");
		gettimeofday(&start_t, nullptr);

		// merge rows
		// change even key value
		Merger merger(con, "integers");
		for (size_t i = 0; i < maxValues; i++) {
			auto value = values[i];
			auto v = (int64_t)(value.GetValue<int64_t>());
			auto key = "update-key-" + std::to_string(v);
			auto val = "update-tst-" + std::to_string(v);
			merger.AppendRow(v, key.c_str(), val.c_str());
		}
		merger.Close();

		struct timeval now;
		gettimeofday(&now, nullptr);
		auto time = (now.tv_usec - start_t.tv_usec) / (double)1000.0 + (now.tv_sec - start_t.tv_sec) * (double)1000.0;
		Printer::Print("conflict time: " + std::to_string(time));
	}

	result = con.Query("SELECT * FROM integers limit 10000");
	auto &materialized = reinterpret_cast<duckdb::MaterializedQueryResult &>(*result);
	auto count = materialized.RowCount();
	auto string = materialized.ToString();
	Printer::Print(std::to_string(count));
}

TEST_CASE("Random key order updates with chunks test", "[merger]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i BIGINT, j VARCHAR, b VARCHAR, PRIMARY KEY (i))"));

	// append a bunch of values

#if DEBUG
	auto total_number_of_records = 10000;
#else
	auto total_number_of_records = 1000000;
#endif


	duckdb::vector<idx_t> values;
	Appender appender(con, "integers");
	for (idx_t i = 0; i < total_number_of_records; i++) {
		auto key = "key-" + std::to_string(i);
		auto val = "val-" + std::to_string(i);
		appender.AppendRow((int64_t)(i), duckdb::Value(key.c_str()), duckdb::Value(val.c_str()));
		values.push_back(i);
	}
	appender.Close();

	auto number_of_updates = 1000000;
//	auto end_value = number_of_updates+1;
//	auto start_value = 0;

	auto rng = std::default_random_engine {random_device{}()};
	std::shuffle(std::begin(values), std::end(values), rng);
	duckdb::vector<duckdb::DataChunk *> chunks;
	Printer::Print(std::to_string(values[1]));

	for (idx_t c = 0; c < 1; c++) {
//		con.Query("begin transaction");
		auto row = 0;
		for (idx_t i = 0; i < number_of_updates; i++) {
			if (i % STANDARD_VECTOR_SIZE == 0) {
				row = 0;
				auto chunk = new duckdb::DataChunk();
				duckdb::vector<LogicalType> types = {LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR};
				chunk->Initialize(duckdb::Allocator::DefaultAllocator(), types);
				chunks.push_back(chunk);
			}

			auto chunk_index = i / STANDARD_VECTOR_SIZE;
			auto current_chunk = chunks[chunk_index];

			for (idx_t c = 0; c < current_chunk->ColumnCount(); c++) {
				if (c == 0) {
					current_chunk->SetValue(c, row, duckdb::Value((int64_t)values[i]));
				} else if (c == 1) {
					auto key = "up key-" + std::to_string(values[i]);
					current_chunk->SetValue(c, row, duckdb::Value(key.c_str()));
				} else {
					auto val = "update val-" + std::to_string(values[i]);
					current_chunk->SetValue(c, row, duckdb::Value(val.c_str()));
				}
			}
			row++;
			current_chunk->SetCardinality(row);
		}

		struct timeval start_t;
		Printer::Print("LocalAppend upsert?");
		gettimeofday(&start_t, nullptr);

		// merge rows
		// change even key value
		Merger merger(con, "integers");
		for (size_t i = 0; i < chunks.size(); i++) {
			merger.AppendDataChunk(*chunks[i]);
		}
		merger.Close();

		struct timeval now;
		gettimeofday(&now, nullptr);
		auto time = (now.tv_usec - start_t.tv_usec) / (double)1000.0 + (now.tv_sec - start_t.tv_sec) * (double)1000.0;
		Printer::Print("conflict time: " + std::to_string(time));
//		con.Query("rollback");

		for (int i = 0; i < chunks.size(); i++) {
			delete chunks[i];
		}
		chunks.clear();
	}

	result = con.Query("SELECT * FROM integers");
	auto &materialized = reinterpret_cast<duckdb::MaterializedQueryResult &>(*result);
	auto count = materialized.RowCount();
	auto string = materialized.ToString();
	Printer::Print(std::to_string(count));
	Printer::Print(materialized.GetValue(1, values[1]).ToSQLString());
}
