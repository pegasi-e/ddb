#include <random>
#include <vector>
#include <unordered_set>
#include <iostream>
#include <chrono>
#include "duckdb.hpp"

using namespace duckdb;

int main(int argc, char *argv[]) {
	DuckDB db(nullptr);

	Connection con(db);

	con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY, j INTEGER)");
	con.Query("CREATE TABLE temp(i INTEGER PRIMARY KEY, j INTEGER)");

	std::random_device rd;  // Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); // Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<uint32_t> dis(0, 2 * 1024 * 1024 * 1024);
#if 1
	// insert initial rows into integers
	uint32_t initial_rows = 1024 * 1024;
	std::vector<int> keys;
	std::vector<int> values;
	for (int idx = 0 ; idx < initial_rows; ++idx) {
	  int c1 = dis(gen);
	  int c2 = dis(gen);
	  keys.push_back(c1);
	  values.push_back(c2);
	}
	std::unordered_set<int> s;
	Appender appender(con, "integers");
	auto start_time = std::chrono::high_resolution_clock::now();
	con.Query("BEGIN TRANSACTION");
	for (int idx = 0 ; idx < initial_rows; ++idx) {
	  int c1 = keys[idx];
	  int c2 = values[idx];
	  if (s.find(c1) == s.end()) {
	    s.insert(c1);
	    appender.AppendRow(c1, c2);
	  }
	}
	appender.Close();
	con.Query("COMMIT TRANSACTION");
	auto end_time = std::chrono::high_resolution_clock::now();
	std::cout << "Time difference:" << std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count() << " milliseconds" << std::endl;
	uint32_t update_rows = 10240;
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
#endif
#if 0
	// insert initial rows into integers
	uint32_t initial_rows = 1024 * 1024;
	Appender appender(con, "integers");

	auto start_time = std::chrono::high_resolution_clock::now();
	con.Query("BEGIN TRANSACTION");
	for (int idx = 0 ; idx < initial_rows; ++idx) {
	    appender.AppendRow(idx, idx);
	}
	appender.Close();
	con.Query("COMMIT TRANSACTION");
	auto end_time = std::chrono::high_resolution_clock::now();
	std::cout << "Time difference:" << std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count() << " milliseconds" << std::endl;
	
	uint32_t update_rows = 10240;
	Merger merger(con, "integers");
	auto start_time1 = std::chrono::high_resolution_clock::now();
	con.Query("BEGIN TRANSACTION");
	for (int idx = 0 ; idx < update_rows; ++idx) {
	  merger.AppendRow(idx, idx+1);
	}
	merger.Close();
	con.Query("COMMIT TRANSACTION");
	auto end_time1 = std::chrono::high_resolution_clock::now();
	std::cout << "Time difference:" << std::chrono::duration_cast<std::chrono::milliseconds>(end_time1 - start_time1).count() << " milliseconds" << std::endl;
#endif
}
