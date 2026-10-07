//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/transaction/transaction_data.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/atomic.hpp"

namespace duckdb {
class Catalog;
class SequenceCatalogEntry;
class SchemaCatalogEntry;

class AttachedDatabase;
class ColumnData;
class ClientContext;
class CatalogEntry;
class DataTable;
class DatabaseInstance;
class LocalStorage;
class MetaTransaction;
class TransactionManager;
class WriteAheadLog;

class ChunkVectorInfo;

struct DeleteInfo;
struct UpdateInfo;
struct DatabaseModificationType;

//! The transaction object holds information about a currently running or past
//! transaction
class Transaction {
public:
	DUCKDB_API Transaction(TransactionManager &manager, ClientContext &context);
	DUCKDB_API virtual ~Transaction();

	TransactionManager &manager;
	weak_ptr<ClientContext> context;
	//! The current active query for the transaction. Set to MAXIMUM_QUERY_ID if
	//! no query is active.
	atomic<transaction_t> active_query;

public:
	DUCKDB_API static Transaction &Get(ClientContext &context, AttachedDatabase &db);
	DUCKDB_API static Transaction &Get(ClientContext &context, Catalog &catalog);
	//! Returns the transaction for the given context if it has already been started
	DUCKDB_API static optional_ptr<Transaction> TryGet(ClientContext &context, AttachedDatabase &db);

	//! Whether or not the transaction has made any modifications to the database so far
	DUCKDB_API bool IsReadOnly();
	//! Promotes the transaction to a read-write transaction
	DUCKDB_API virtual void SetReadWrite();
	//! Sets the database modifications that are planned to be performed in this transaction
	DUCKDB_API virtual void SetModifications(DatabaseModificationType type);

	virtual bool IsDuckTransaction() const {
		return false;
	}

public:
	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}

private:
	bool is_read_only;
// start Anybase changes
	unordered_map<string, unordered_set<idx_t>> involved_columns;
	mutable std::mutex mu_;

public:
	virtual bool ShouldPublishCDCEvent() {
		return false;
	}

	void AddInvolvedColumn(const string &table_name, std::unordered_set<idx_t> &column_indices) {
		std::lock_guard<std::mutex> lock(mu_);
		auto& dest = involved_columns[table_name];
		dest.insert(column_indices.begin(), column_indices.end());
	}

	std::vector<idx_t> GetInvolvedColumns(const string &table_name) {
		std::lock_guard<std::mutex> lock(mu_);
		if (involved_columns.find(table_name) != involved_columns.end()) {
			auto& dest = involved_columns[table_name];
			return std::vector<idx_t>(dest.begin(), dest.end());
		}

		return {};
	}

	bool HasInvolvedColumns(const string &table_name) const {
		std::lock_guard<std::mutex> lock(mu_);
		return involved_columns.find(table_name) != involved_columns.end();
	}

// end Anybase changes
};

} // namespace duckdb
