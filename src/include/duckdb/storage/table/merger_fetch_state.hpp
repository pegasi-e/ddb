#pragma once

// start Anybase changes
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/types/vector_cache.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/column_data.hpp"

namespace duckdb {
class ColumnData;
class ColumnSegment;
class DataTable;

struct MergerColumnFetchState {
	explicit MergerColumnFetchState(const LogicalType &type)
	    : cache(Allocator::DefaultAllocator(), type), standard(nullptr), validity(nullptr), base(cache) {
	}
	VectorCache cache;
	// Keep segment objects alive until their compression states have been released.
	shared_ptr<ColumnSegment> standard_segment;
	shared_ptr<ColumnSegment> validity_segment;
	ColumnScanState standard;
	ColumnScanState validity;
	Vector base;
	shared_ptr<const ColumnData> column;
};

//! Reuse only within one Merger flush and this thread; never retain decoded data across calls.
class MergerFetchScope {
public:
	explicit MergerFetchScope(DataTable &table_p) : table(table_p), previous(Current()) {
		Current() = this;
	}
	~MergerFetchScope() {
		Current() = previous;
	}
	MergerFetchScope(const MergerFetchScope &) = delete;
	MergerFetchScope &operator=(const MergerFetchScope &) = delete;
	static MergerFetchScope *&Current() {
		static thread_local MergerFetchScope *current = nullptr;
		return current;
	}
	MergerColumnFetchState &Get(const ColumnData &column, idx_t index, const LogicalType &type) {
		auto &entry = states[index];
		if (!entry || entry->cache.GetType() != type) {
			entry = make_uniq<MergerColumnFetchState>(type);
		}
		// Reset validity and auxiliary data before releasing any previous scan pins.
		entry->base.ResetFromCache(entry->cache);
		if (entry->column.get() != &column) {
			entry->standard = ColumnScanState(nullptr);
			entry->validity = ColumnScanState(nullptr);
			entry->standard_segment.reset();
			entry->validity_segment.reset();
			entry->column = column.shared_from_this();
		}
		return *entry;
	}
	DataTable &table;
private:
	MergerFetchScope *previous;
	unordered_map<idx_t, unique_ptr<MergerColumnFetchState>> states;
};
} // namespace duckdb
// end Anybase changes
