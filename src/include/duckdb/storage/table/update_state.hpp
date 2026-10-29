//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/update_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/append_state.hpp"

namespace duckdb {
class TableCatalogEntry;

struct TableUpdateState {
	unique_ptr<ConstraintState> constraint_state;
	// start Anybase changes
	//! Only set by Merger after global and local conflicts have been separated.
	bool merger_global_rows = false;
	// end Anybase changes
};

} // namespace duckdb
