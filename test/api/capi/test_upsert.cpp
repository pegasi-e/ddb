#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include <sys/time.h>

using namespace duckdb;
using namespace std;

TEST_CASE("Test upsert", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	con.Query("CREATE TABLE Info (Id short, \"Key\" VARCHAR default 'key-d', \"Val\" VARCHAR default 'val-d', PRIMARY KEY (Id))");

	auto number_of_records = 2;
	duckdb::string insert_string = "INSERT INTO Info VALUES ";
	for (auto i = 0; i < number_of_records; i++) {
		auto i_str = std::to_string(i);
//		insert_string += "(" + i_str + ", " + "'key-" + i_str + "', " + "'val-" + i_str + "'" + ")";
		insert_string += "(" + i_str + ", " + " default" + ", " + " 'abc'" + ")";
		if (i < number_of_records - 1) {
			insert_string += ", ";
		}
	}

	Printer::Print("Staring insert");

	auto insert_result = con.Query(insert_string);
	REQUIRE(insert_result->HasError() == false);

	Printer::Print("insert complete");

	auto number_of_updates = 20;
	string update_string = "INSERT INTO Info VALUES ";
	for (auto i = 0; i < number_of_updates; i++) {
		auto i_str = std::to_string(i);
		update_string += "(" + i_str + ", " + "'update-key-" + i_str + "', " + " 'update-val-" + i_str + "')";
		if (i < number_of_updates - 1) {
			update_string += ", ";
		}
	}

	update_string += " on conflict (Id) do update set \"Key\" = EXCLUDED.Key, \"Val\" = EXCLUDED.Val";

	Printer::Print(update_string);

	Printer::Print("starting update");
	struct timeval start_t;
	gettimeofday(&start_t, nullptr);

	auto merge_result = con.Query(update_string);
	Printer::Print(merge_result->ToString());
	REQUIRE(merge_result->HasError() == false);

	struct timeval now;
	gettimeofday(&now, nullptr);
	auto time = (now.tv_usec - start_t.tv_usec) / (double)1000.0 + (now.tv_sec - start_t.tv_sec) * (double)1000.0;

	Printer::Print("update complete: " + std::to_string(time));
}

TEST_CASE("Test upsert with appender", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	con.Query("CREATE TABLE Info (Id short, \"Key\" VARCHAR default 'key-d', \"Val\" VARCHAR default 'val-d', PRIMARY KEY (Id))");

	auto appender = make_uniq<Appender>(con, "Info");

	// we can use the appender
	appender->BeginRow();
	appender->Append<int32_t>(1);
	appender->Append<string_t>("abc");
	appender->Append<string_t>("233");
	appender->EndRow();
	appender->Close();

	auto appender2 = make_uniq<Appender>(con, "Info");

	appender2->BeginRow();
	appender2->Append<int32_t>(1);
	appender2->Append<string_t>("abc");
	appender2->Append<string_t>("233");
	appender2->EndRow();
	appender2->Close();
}