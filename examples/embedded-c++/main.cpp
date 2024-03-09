#include <random>
#include <vector>
#include <unordered_set>
#include <iostream>
#include <chrono>
#include "duckdb.hpp"

using namespace duckdb;

int main(int argc, char *argv[]) {
        const char *path = nullptr;
	bool query = false;
	if (argc > 1) {
	  path = argv[1];
	}

	if (argc > 2) {
	  query = true;
	}
	
	DuckDB db(path);

	Connection con(db);

	con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY, j INTEGER)");
	con.Query("CREATE TABLE temp(i INTEGER PRIMARY KEY, j INTEGER)");

	std::random_device rd;  // Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); // Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<uint32_t> dis(0, 2000000000);

	// insert initial rows into integers
	uint32_t initial_rows = 1024 * 1024;
	std::vector<int> keys;
	std::unordered_set<int> s;
	std::vector<int> values;
	for (int idx = 0 ; idx < initial_rows; ) {
	  uint32_t c1 = dis(gen);
	  uint32_t c2 = dis(gen);	  

	  if (s.find(c1) == s.end()) {
	    s.insert(c1);
	    keys.push_back(c1);
	    values.push_back(c2);
	    ++idx;
	  }
	}
	
	Appender appender(con, "integers");
	auto start_time = std::chrono::high_resolution_clock::now();
	con.Query("BEGIN TRANSACTION");
	for (int idx = 0 ; idx < initial_rows; ++idx) {
	  int c1 = keys[idx];
	  int c2 = values[idx];
	  appender.AppendRow(c1, c2);
	}
	appender.Close();
	con.Query("COMMIT TRANSACTION");
	auto end_time = std::chrono::high_resolution_clock::now();
	std::cout << "Time difference:" << std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count() << " milliseconds" << std::endl;
	auto result = con.Query("SELECT count(*) FROM integers");
	result->Print();
	uint32_t update_rows = 10240;
	auto start_time2 = std::chrono::high_resolution_clock::now();
	
	if (query) {
	  con.Query("BEGIN TRANSACTION");
	  Appender appender1(con, "temp");
	  for (int idx = 0 ; idx < update_rows; ++idx) {
	    appender1.AppendRow(keys[idx], values[idx]+1);
	  }
	  appender1.Close();
	  con.Query("INSERT INTO integers SELECT * FROM TEMP ON CONFLICT (i) DO UPDATE SET j = EXCLUDED.j");
	  con.Query("COMMIT TRANSACTION");
	  auto end_time2 = std::chrono::high_resolution_clock::now();
	  std::cout << "Time difference:" << std::chrono::duration_cast<std::chrono::milliseconds>(end_time2 - start_time2).count() << " milliseconds" << std::endl;
	} else {

	  Merger merger(con, "integers");

	  auto start_time1 = std::chrono::high_resolution_clock::now();
	  con.Query("BEGIN TRANSACTION");
	  for (int idx = 0 ; idx < update_rows; ++idx) {
	    int c1 = keys[idx];
	    int c2 = dis(gen);
	    merger.AppendRow(c1, c2);
	  }
	  merger.Close();
	  con.Query("COMMIT TRANSACTION");
	  auto end_time1 = std::chrono::high_resolution_clock::now();
	  std::cout << "Time difference:" << std::chrono::duration_cast<std::chrono::milliseconds>(end_time1 - start_time1).count() << " milliseconds" << std::endl;
	}

}
