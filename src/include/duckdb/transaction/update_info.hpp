//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/update_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/storage/table/column_data.hpp"

namespace duckdb {
class UpdateSegment;
class ColumnData;
struct DataTableInfo;

struct UpdateInfo {
	//! The update segment that this update info affects
	UpdateSegment *segment;
	//! The column index of which column we are updating
	idx_t column_index;
	//! The version number
	atomic<transaction_t> version_number;
	//! The vector index within the uncompressed segment
	idx_t vector_index;
	//! The amount of updated tuples
	sel_t N; // NOLINT
	//! The maximum amount of tuples that can fit into this UpdateInfo
	sel_t max;
	//! The row ids of the tuples that have been updated. This should always be kept sorted!
	sel_t *tuples;
	//! The data of the tuples
	data_ptr_t tuple_data;
	//! The previous update info (or nullptr if it is the base)
	UpdateInfo *prev;
	//! The next update info in the chain (or nullptr if it is the last)
	UpdateInfo *next;

	// Anybase additions
	ColumnData *column;
	DataTable *table;
	// end Anybase additions

	//! Loop over the update chain and execute the specified callback on all UpdateInfo's that are relevant for that
	//! transaction in-order of newest to oldest
	template <class T>
	static void UpdatesForTransaction(UpdateInfo *current, transaction_t start_time, transaction_t transaction_id, bool fetch_current_update,
	                                  T &&callback) {
		while (current) {
			if (current->version_number > start_time) {
				if ((fetch_current_update &&  current->version_number != transaction_id) ||
					(!fetch_current_update && current->version_number == transaction_id)) {
					callback(current);
				}
			}

			current = current->next;
		}
	}

	Value GetValue(idx_t index);
	string ToString();
	void Print();
	void Verify();
};

} // namespace duckdb
