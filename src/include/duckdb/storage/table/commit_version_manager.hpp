//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/commit_version_manager.hpp
//
//
//===----------------------------------------------------------------------===//

// Start Anybase changes
#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/serializer/serialization_traits.hpp"

namespace duckdb {

struct CommitHLC {
	// Keep fork-specific fields away from upstream's low-numbered serialization fields.
	static constexpr field_id_t TIMESTAMP_FIELD_ID = 60000;
	static constexpr field_id_t SEQUENCE_FIELD_ID = 60001;

	int64_t timestamp = 0;
	uint64_t sequence = 0;
};

//! Callers must synchronize access to the version manager.
struct CommitVersionManager {
public:
	void DidCommitTransaction(const transaction_t commit_id) {
		if (commit_id == last_commit_id) {
			return;
		}

		last_commit_id = commit_id;
		version++;
	}

	void DidCommitTransaction(transaction_t commit_id, CommitHLC commit_hlc) {
		if (commit_id == last_commit_id) {
			return;
		}
		DidCommitTransaction(commit_id);
		hlc = commit_hlc;
	}

	CommitHLC NextCommitHLC(int64_t timestamp_us) const {
		if (hlc.sequence == NumericLimits<uint64_t>::Maximum()) {
			throw TransactionException("Cannot commit: commit HLC sequence exhausted");
		}
		CommitHLC result;
		result.timestamp = MaxValue(hlc.timestamp, timestamp_us);
		result.sequence = hlc.sequence + 1;
		return result;
	}

	CommitHLC GetCommitHLC() const {
		return hlc;
	}

	void RestoreCommitHLC(CommitHLC commit_hlc) {
		if (commit_hlc.sequence > hlc.sequence) {
			hlc = commit_hlc;
		}
	}

	idx_t GetVersion() const {
		return version;
	}

	void SetVersion(idx_t version) {
		this->version = version;
	}

private:
	idx_t version = 0;
	transaction_t last_commit_id = 0;
	CommitHLC hlc;
};
} // namespace duckdb
// End Anybase changes