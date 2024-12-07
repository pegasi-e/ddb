#include "duckdb.hpp"

using namespace duckdb;

int main(int argc, char* argv[]) {
	DuckDB db(argv[1]);

	Connection con(db);
        std::string arg2(argv[2]);
        if (arg2 == "create")
          {
            con.Query("CREATE TABLE integers(i INTEGER, j INTEGER)");
            con.Query("INSERT INTO integers VALUES (1, 100),(2,200)");
            con.Query("CREATE UNIQUE INDEX idx ON integers (i)");
            //con.Query("INSERT INTO integers VALUES (1, 101),(2,201)");
            //auto result = con.Query("SELECT * FROM integers");
            //result->Print();
            //con.Query("CHECKPOINT 'db0'");
          }
        else
          {
            con.Query("INSERT INTO integers VALUES (1, 101),(2,201)");
            auto result = con.Query("SELECT * FROM integers");
            result->Print();
          }
}
