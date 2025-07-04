#include "duckdb/storage/data_table.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/chrono.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/conflict_manager.hpp"
#include "duckdb/common/types/constraint_conflict_info.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/planner/constraints/list.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_binder/check_binder.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/checkpoint/table_data_writer.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/delete_state.hpp"
#include "duckdb/storage/table/persistent_table_data.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/standard_column_data.hpp"
#include "duckdb/storage/table/update_state.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

namespace duckdb {

DataTableInfo::DataTableInfo(AttachedDatabase &db, shared_ptr<TableIOManager> table_io_manager_p, string schema,
                             string table)
    : db(db), table_io_manager(std::move(table_io_manager_p)), schema(std::move(schema)), table(std::move(table)) {
}

void DataTableInfo::InitializeIndexes(ClientContext &context, const char *index_type) {
	indexes.InitializeIndexes(context, *this, index_type);
}

bool DataTableInfo::IsTemporary() const {
	return db.IsTemporary();
}

DataTable::DataTable(AttachedDatabase &db, shared_ptr<TableIOManager> table_io_manager_p, const string &schema,
                     const string &table, vector<ColumnDefinition> column_definitions_p,
                     unique_ptr<PersistentTableData> data)
    : db(db), info(make_shared_ptr<DataTableInfo>(db, std::move(table_io_manager_p), schema, table)),
      column_definitions(std::move(column_definitions_p)), version(DataTableVersion::MAIN_TABLE) {
	// initialize the table with the existing data from disk, if any
	auto types = GetTypes();
	auto &io_manager = TableIOManager::Get(*this);
	this->row_groups = make_shared_ptr<RowGroupCollection>(info, io_manager, types, 0);
	if (data && data->row_group_count > 0) {
		this->row_groups->Initialize(*data);
	} else {
		this->row_groups->InitializeEmpty();
		D_ASSERT(row_groups->GetTotalRows() == 0);
	}
	row_groups->Verify();
}

DataTable::DataTable(ClientContext &context, DataTable &parent, ColumnDefinition &new_column, Expression &default_value)
    : db(parent.db), info(parent.info), version(DataTableVersion::MAIN_TABLE) {
	// add the column definitions from this DataTable
	for (auto &column_def : parent.column_definitions) {
		column_definitions.emplace_back(column_def.Copy());
	}
	column_definitions.emplace_back(new_column.Copy());

	auto &local_storage = LocalStorage::Get(context, db);

	ExpressionExecutor default_executor(context);
	default_executor.AddExpression(default_value);

	// prevent any new tuples from being added to the parent
	lock_guard<mutex> parent_lock(parent.append_lock);

	this->row_groups = parent.row_groups->AddColumn(context, new_column, default_executor);

	// also add this column to client local storage
	local_storage.AddColumn(parent, *this, new_column, default_executor);

	// this table replaces the previous table, hence the parent is no longer the root DataTable
	parent.version = DataTableVersion::ALTERED;
}

DataTable::DataTable(ClientContext &context, DataTable &parent, idx_t removed_column)
    : db(parent.db), info(parent.info), version(DataTableVersion::MAIN_TABLE) {
	// prevent any new tuples from being added to the parent
	auto &local_storage = LocalStorage::Get(context, db);
	lock_guard<mutex> parent_lock(parent.append_lock);

	for (auto &column_def : parent.column_definitions) {
		column_definitions.emplace_back(column_def.Copy());
	}

	info->InitializeIndexes(context);

	// first check if there are any indexes that exist that point to the removed column
	info->indexes.Scan([&](Index &index) {
		for (auto &column_id : index.GetColumnIds()) {
			if (column_id == removed_column) {
				throw CatalogException("Cannot drop this column: an index depends on it!");
			} else if (column_id > removed_column) {
				throw CatalogException("Cannot drop this column: an index depends on a column after it!");
			}
		}
		return false;
	});

	// erase the column definitions from this DataTable
	D_ASSERT(removed_column < column_definitions.size());
	column_definitions.erase_at(removed_column);

	storage_t storage_idx = 0;
	for (idx_t i = 0; i < column_definitions.size(); i++) {
		auto &col = column_definitions[i];
		col.SetOid(i);
		if (col.Generated()) {
			continue;
		}
		col.SetStorageOid(storage_idx++);
	}

	// alter the row_groups and remove the column from each of them
	this->row_groups = parent.row_groups->RemoveColumn(removed_column);

	// scan the original table, and fill the new column with the transformed value
	local_storage.DropColumn(parent, *this, removed_column);

	// this table replaces the previous table, hence the parent is no longer the root DataTable
	parent.version = DataTableVersion::ALTERED;
}

DataTable::DataTable(ClientContext &context, DataTable &parent, BoundConstraint &constraint)
    : db(parent.db), info(parent.info), row_groups(parent.row_groups), version(DataTableVersion::MAIN_TABLE) {

	// ALTER COLUMN to add a new constraint.

	// Clone the storage info vector or the table.
	for (const auto &index_info : parent.info->index_storage_infos) {
		info->index_storage_infos.push_back(IndexStorageInfo(index_info.name));
	}
	info->InitializeIndexes(context);

	auto &local_storage = LocalStorage::Get(context, db);
	lock_guard<mutex> parent_lock(parent.append_lock);
	for (auto &column_def : parent.column_definitions) {
		column_definitions.emplace_back(column_def.Copy());
	}

	if (constraint.type != ConstraintType::UNIQUE) {
		VerifyNewConstraint(local_storage, parent, constraint);
	}
	local_storage.MoveStorage(parent, *this);
	parent.version = DataTableVersion::ALTERED;
}

DataTable::DataTable(ClientContext &context, DataTable &parent, idx_t changed_idx, const LogicalType &target_type,
                     const vector<StorageIndex> &bound_columns, Expression &cast_expr)
    : db(parent.db), info(parent.info), version(DataTableVersion::MAIN_TABLE) {
	auto &local_storage = LocalStorage::Get(context, db);
	// prevent any tuples from being added to the parent
	lock_guard<mutex> lock(append_lock);
	for (auto &column_def : parent.column_definitions) {
		column_definitions.emplace_back(column_def.Copy());
	}

	info->InitializeIndexes(context);

	// first check if there are any indexes that exist that point to the changed column
	info->indexes.Scan([&](Index &index) {
		for (auto &column_id : index.GetColumnIds()) {
			if (column_id == changed_idx) {
				throw CatalogException("Cannot change the type of this column: an index depends on it!");
			}
		}
		return false;
	});

	// change the type in this DataTable
	column_definitions[changed_idx].SetType(target_type);

	// set up the statistics for the table
	// the column that had its type changed will have the new statistics computed during conversion
	this->row_groups = parent.row_groups->AlterType(context, changed_idx, target_type, bound_columns, cast_expr);

	// scan the original table, and fill the new column with the transformed value
	local_storage.ChangeType(parent, *this, changed_idx, target_type, bound_columns, cast_expr);

	// this table replaces the previous table, hence the parent is no longer the root DataTable
	parent.version = DataTableVersion::ALTERED;
}

vector<LogicalType> DataTable::GetTypes() {
	vector<LogicalType> types;
	for (auto &it : column_definitions) {
		types.push_back(it.Type());
	}
	return types;
}

bool DataTable::IsTemporary() const {
	return info->IsTemporary();
}

AttachedDatabase &DataTable::GetAttached() {
	D_ASSERT(RefersToSameObject(db, info->db));
	return db;
}

const vector<ColumnDefinition> &DataTable::Columns() const {
	return column_definitions;
}

TableIOManager &DataTable::GetTableIOManager() {
	return *info->table_io_manager;
}

TableIOManager &TableIOManager::Get(DataTable &table) {
	return table.GetTableIOManager();
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void DataTable::InitializeScan(ClientContext &context, DuckTransaction &transaction, TableScanState &state,
                               const vector<StorageIndex> &column_ids, optional_ptr<TableFilterSet> table_filters) {
	state.checkpoint_lock = transaction.SharedLockTable(*info);
	auto &local_storage = LocalStorage::Get(transaction);
	state.Initialize(column_ids, context, table_filters);
	row_groups->InitializeScan(state.table_state, column_ids, table_filters);
	local_storage.InitializeScan(*this, state.local_state, table_filters);
}

void DataTable::InitializeScanWithOffset(DuckTransaction &transaction, TableScanState &state,
                                         const vector<StorageIndex> &column_ids, idx_t start_row, idx_t end_row) {
	state.checkpoint_lock = transaction.SharedLockTable(*info);
	state.Initialize(column_ids);
	row_groups->InitializeScanWithOffset(state.table_state, column_ids, start_row, end_row);
}

idx_t DataTable::GetRowGroupSize() const {
	return row_groups->GetRowGroupSize();
}

vector<PartitionStatistics> DataTable::GetPartitionStats(ClientContext &context) {
	auto result = row_groups->GetPartitionStats();
	auto &local_storage = LocalStorage::Get(context, db);
	auto local_partitions = local_storage.GetPartitionStats(*this);
	result.insert(result.end(), local_partitions.begin(), local_partitions.end());
	return result;
}

idx_t DataTable::MaxThreads(ClientContext &context) const {
	idx_t row_group_size = GetRowGroupSize();
	idx_t parallel_scan_vector_count = row_group_size / STANDARD_VECTOR_SIZE;
	if (ClientConfig::GetConfig(context).verify_parallelism) {
		parallel_scan_vector_count = 1;
	}
	idx_t parallel_scan_tuple_count = STANDARD_VECTOR_SIZE * parallel_scan_vector_count;
	return GetTotalRows() / parallel_scan_tuple_count + 1;
}

void DataTable::InitializeParallelScan(ClientContext &context, ParallelTableScanState &state) {
	auto &local_storage = LocalStorage::Get(context, db);
	auto &transaction = DuckTransaction::Get(context, db);
	state.checkpoint_lock = transaction.SharedLockTable(*info);
	row_groups->InitializeParallelScan(state.scan_state);

	local_storage.InitializeParallelScan(*this, state.local_state);
}

bool DataTable::NextParallelScan(ClientContext &context, ParallelTableScanState &state, TableScanState &scan_state) {
	if (row_groups->NextParallelScan(context, state.scan_state, scan_state.table_state)) {
		return true;
	}
	auto &local_storage = LocalStorage::Get(context, db);
	if (local_storage.NextParallelScan(context, *this, state.local_state, scan_state.local_state)) {
		return true;
	} else {
		// finished all scans: no more scans remaining
		return false;
	}
}

void DataTable::Scan(DuckTransaction &transaction, DataChunk &result, TableScanState &state) {
	// scan the persistent segments
	if (state.table_state.Scan(transaction, result)) {
		D_ASSERT(result.size() > 0);
		return;
	}

	// scan the transaction-local segments
	auto &local_storage = LocalStorage::Get(transaction);
	local_storage.Scan(state.local_state, state.GetColumnIds(), result);
}

bool DataTable::CreateIndexScan(TableScanState &state, DataChunk &result, TableScanType type) {
	return state.table_state.ScanCommitted(result, type);
}

//===--------------------------------------------------------------------===//
// Index Methods
//===--------------------------------------------------------------------===//
shared_ptr<DataTableInfo> &DataTable::GetDataTableInfo() {
	return info;
}

void DataTable::InitializeIndexes(ClientContext &context) {
	info->InitializeIndexes(context);
}

bool DataTable::HasIndexes() const {
	return !info->indexes.Empty();
}

bool DataTable::HasUniqueIndexes() const {
	if (!HasIndexes()) {
		return false;
	}
	bool has_unique_index = false;
	info->indexes.Scan([&](Index &index) {
		if (index.IsUnique()) {
			has_unique_index = true;
			return true;
		}
		return false;
	});
	return has_unique_index;
}

void DataTable::AddIndex(unique_ptr<Index> index) {
	info->indexes.AddIndex(std::move(index));
}

bool DataTable::HasForeignKeyIndex(const vector<PhysicalIndex> &keys, ForeignKeyType type) {
	auto index = info->indexes.FindForeignKeyIndex(keys, type);
	return index != nullptr;
}

void DataTable::SetIndexStorageInfo(vector<IndexStorageInfo> index_storage_info) {
	info->index_storage_infos = std::move(index_storage_info);
}

void DataTable::VacuumIndexes() {
	info->indexes.Scan([&](Index &index) {
		if (index.IsBound()) {
			index.Cast<BoundIndex>().Vacuum();
		}
		return false;
	});
}

void DataTable::VerifyIndexBuffers() {
	info->indexes.Scan([&](Index &index) {
		if (index.IsBound()) {
			index.Cast<BoundIndex>().VerifyBuffers();
		}
		return false;
	});
}

void DataTable::CleanupAppend(transaction_t lowest_transaction, idx_t start, idx_t count) {
	row_groups->CleanupAppend(lowest_transaction, start, count);
}

bool DataTable::IndexNameIsUnique(const string &name) {
	return info->indexes.NameIsUnique(name);
}

string DataTableInfo::GetSchemaName() {
	return schema;
}

string DataTableInfo::GetTableName() {
	lock_guard<mutex> l(name_lock);
	return table;
}

void DataTableInfo::SetTableName(string name) {
	lock_guard<mutex> l(name_lock);
	table = std::move(name);
}

string DataTable::GetTableName() const {
	return info->GetTableName();
}

void DataTable::SetTableName(string new_name) {
	info->SetTableName(std::move(new_name));
}

TableStorageInfo DataTable::GetStorageInfo() {
	TableStorageInfo result;
	result.cardinality = GetTotalRows();
	info->indexes.Scan([&](Index &index) {
		IndexInfo index_info;
		index_info.is_primary = index.IsPrimary();
		index_info.is_unique = index.IsUnique() || index_info.is_primary;
		index_info.is_foreign = index.IsForeign();
		index_info.column_set = index.GetColumnIdSet();
		result.index_info.push_back(std::move(index_info));
		return false;
	});
	return result;
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
// start Anybase changes
void DataTable::Fetch(DuckTransaction &transaction, DataChunk &result, const vector<StorageIndex> &column_ids,
                      const Vector &row_identifiers, idx_t fetch_count, ColumnFetchState &state,
                      bool fetch_current_update) {
	auto lock = info->checkpoint_lock.GetSharedLock();
	row_groups->Fetch(transaction, result, column_ids, row_identifiers, fetch_count, state, fetch_current_update);
}
// end Anybase changes

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
static void VerifyNotNullConstraint(TableCatalogEntry &table, Vector &vector, idx_t count, const string &col_name) {
	if (!VectorOperations::HasNull(vector, count)) {
		return;
	}

	throw ConstraintException("NOT NULL constraint failed: %s.%s", table.name, col_name);
}

// To avoid throwing an error at SELECT, instead this moves the error detection to INSERT
static void VerifyGeneratedExpressionSuccess(ClientContext &context, TableCatalogEntry &table, DataChunk &chunk,
                                             Expression &expr, column_t index) {
	auto &col = table.GetColumn(LogicalIndex(index));
	D_ASSERT(col.Generated());
	ExpressionExecutor executor(context, expr);
	Vector result(col.Type());
	try {
		executor.ExecuteExpression(chunk, result);
	} catch (InternalException &ex) {
		throw;
	} catch (std::exception &ex) {
		ErrorData error(ex);
		throw ConstraintException("Incorrect value for generated column \"%s %s AS (%s)\" : %s", col.Name(),
		                          col.Type().ToString(), col.GeneratedExpression().ToString(), error.RawMessage());
	}
}

static void VerifyCheckConstraint(ClientContext &context, TableCatalogEntry &table, Expression &expr, DataChunk &chunk,
                                  CheckConstraint &check) {
	ExpressionExecutor executor(context, expr);
	Vector result(LogicalType::INTEGER);
	try {
		executor.ExecuteExpression(chunk, result);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		throw ConstraintException("CHECK constraint failed on table %s with expression %s (Error: %s)", table.name,
		                          check.ToString(), error.RawMessage());
	} catch (...) {
		// LCOV_EXCL_START
		throw ConstraintException("CHECK constraint failed on table %s with expression %s (Unknown Error)", table.name,
		                          check.ToString());
	} // LCOV_EXCL_STOP
	UnifiedVectorFormat vdata;
	result.ToUnifiedFormat(chunk.size(), vdata);

	auto dataptr = UnifiedVectorFormat::GetData<int32_t>(vdata);
	for (idx_t i = 0; i < chunk.size(); i++) {
		auto idx = vdata.sel->get_index(i);
		if (vdata.validity.RowIsValid(idx) && dataptr[idx] == 0) {
			throw ConstraintException("CHECK constraint failed on table %s with expression %s", table.name,
			                          check.ToString());
		}
	}
}

// Find the first index that is not null, and did not find a match
static idx_t FirstMissingMatch(const ManagedSelection &matches) {
	idx_t match_idx = 0;

	for (idx_t i = 0; i < matches.Size(); i++) {
		auto match = matches.IndexMapsToLocation(match_idx, i);
		match_idx += match;
		if (!match) {
			// This index is missing in the matches vector
			return i;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t LocateErrorIndex(bool is_append, const ManagedSelection &matches) {
	// We expected to find nothing, so the first error is the first match.
	if (!is_append) {
		return matches[0];
	}
	// We expected to find matches for all of them, so the first missing match is the first error.
	return FirstMissingMatch(matches);
}

[[noreturn]] static void ThrowForeignKeyConstraintError(idx_t failed_index, bool is_append, Index &conflict_index,
                                                        DataChunk &input) {
	// The index that caused the conflict has to be bound by this point (or we would not have gotten here)
	D_ASSERT(conflict_index.IsBound());
	auto &index = conflict_index.Cast<BoundIndex>();
	auto verify_type = is_append ? VerifyExistenceType::APPEND_FK : VerifyExistenceType::DELETE_FK;
	D_ASSERT(failed_index != DConstants::INVALID_INDEX);
	auto message = index.GetConstraintViolationMessage(verify_type, failed_index, input);
	throw ConstraintException(message);
}

bool IsForeignKeyConstraintError(bool is_append, idx_t input_count, const ManagedSelection &matches) {
	if (is_append) {
		// We need to find a match for all values
		return matches.Count() != input_count;
	} else {
		// We should not find any matches
		return matches.Count() != 0;
	}
}

static bool IsAppend(VerifyExistenceType verify_type) {
	return verify_type == VerifyExistenceType::APPEND_FK;
}

void DataTable::VerifyForeignKeyConstraint(optional_ptr<LocalTableStorage> storage,
                                           const BoundForeignKeyConstraint &bound_foreign_key, ClientContext &context,
                                           DataChunk &chunk, VerifyExistenceType verify_type) {
	reference<const vector<PhysicalIndex>> src_keys_ptr = bound_foreign_key.info.fk_keys;
	reference<const vector<PhysicalIndex>> dst_keys_ptr = bound_foreign_key.info.pk_keys;

	bool is_append = IsAppend(verify_type);
	if (!is_append) {
		src_keys_ptr = bound_foreign_key.info.pk_keys;
		dst_keys_ptr = bound_foreign_key.info.fk_keys;
	}

	// Get the column types in their physical order.
	auto &table_entry = Catalog::GetEntry<TableCatalogEntry>(context, db.GetName(), bound_foreign_key.info.schema,
	                                                         bound_foreign_key.info.table);
	vector<LogicalType> types;
	for (auto &col : table_entry.GetColumns().Physical()) {
		types.emplace_back(col.Type());
	}

	// Create the data chunk that has to be verified.
	DataChunk dst_chunk;
	dst_chunk.InitializeEmpty(types);
	for (idx_t i = 0; i < src_keys_ptr.get().size(); i++) {
		auto &src_chunk = chunk.data[src_keys_ptr.get()[i].index];
		dst_chunk.data[dst_keys_ptr.get()[i].index].Reference(src_chunk);
	}

	auto count = chunk.size();
	dst_chunk.SetCardinality(count);
	if (count <= 0) {
		return;
	}

	// Record conflicts instead of throwing immediately.
	unordered_set<column_t> empty_column_list;
	ConflictInfo empty_conflict_info(empty_column_list, false);
	ConflictManager global_conflicts(verify_type, count, &empty_conflict_info);
	ConflictManager local_conflicts(verify_type, count, &empty_conflict_info);
	global_conflicts.SetMode(ConflictManagerMode::SCAN);
	local_conflicts.SetMode(ConflictManagerMode::SCAN);

	// Global constraint verification.
	auto &data_table = table_entry.GetStorage();
	data_table.info->indexes.VerifyForeignKey(storage, dst_keys_ptr, dst_chunk, global_conflicts);
	global_conflicts.Finalize();
	auto &global_matches = global_conflicts.Conflicts();

	// Check if we can insert the chunk into the local storage.
	auto &local_storage = LocalStorage::Get(context, db);
	bool local_error = false;
	auto local_verification = local_storage.Find(data_table);

	// Local constraint verification.
	if (local_verification) {
		auto &local_indexes = local_storage.GetIndexes(context, data_table);
		local_indexes.VerifyForeignKey(storage, dst_keys_ptr, dst_chunk, local_conflicts);
		local_conflicts.Finalize();
		auto &local_matches = local_conflicts.Conflicts();
		local_error = IsForeignKeyConstraintError(is_append, count, local_matches);
	}

	// No constraint violation.
	auto global_error = IsForeignKeyConstraintError(is_append, count, global_matches);
	if (!global_error && !local_error) {
		return;
	}

	// Some error occurred, and we likely want to throw
	optional_ptr<Index> index;
	optional_ptr<Index> transaction_index;

	auto fk_type = is_append ? ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE : ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE;
	// check whether or not the chunk can be inserted or deleted into the referenced table' storage
	index = data_table.info->indexes.FindForeignKeyIndex(dst_keys_ptr, fk_type);
	if (local_verification) {
		auto &transact_index = local_storage.GetIndexes(context, data_table);
		// check whether or not the chunk can be inserted or deleted into the referenced table' storage
		transaction_index = transact_index.FindForeignKeyIndex(dst_keys_ptr, fk_type);
	}

	if (!local_verification) {
		// Only local state is checked, throw the error
		D_ASSERT(global_error);
		auto failed_index = LocateErrorIndex(is_append, global_matches);
		D_ASSERT(failed_index != DConstants::INVALID_INDEX);
		ThrowForeignKeyConstraintError(failed_index, is_append, *index, dst_chunk);
	}
	if (local_error && global_error && is_append) {
		// When we want to do an append, we only throw if the foreign key does not exist in both transaction and local
		// storage
		auto &transaction_matches = local_conflicts.Conflicts();
		idx_t failed_index = DConstants::INVALID_INDEX;
		idx_t regular_idx = 0;
		idx_t transaction_idx = 0;
		for (idx_t i = 0; i < count; i++) {
			bool in_regular = global_matches.IndexMapsToLocation(regular_idx, i);
			regular_idx += in_regular;
			bool in_transaction = transaction_matches.IndexMapsToLocation(transaction_idx, i);
			transaction_idx += in_transaction;

			if (!in_regular && !in_transaction) {
				// We need to find a match for all of the input values
				// The failed index is i, it does not show up in either regular or transaction storage
				failed_index = i;
				break;
			}
		}
		if (failed_index == DConstants::INVALID_INDEX) {
			// We don't throw, every value was present in either regular or transaction storage
			return;
		}
		ThrowForeignKeyConstraintError(failed_index, true, *index, dst_chunk);
	}
	if (!is_append) {
		D_ASSERT(local_verification);
		auto &transaction_matches = local_conflicts.Conflicts();
		if (global_error) {
			auto failed_index = LocateErrorIndex(false, global_matches);
			D_ASSERT(failed_index != DConstants::INVALID_INDEX);
			ThrowForeignKeyConstraintError(failed_index, false, *index, dst_chunk);
		} else {
			D_ASSERT(local_error);
			D_ASSERT(transaction_matches.Count() != DConstants::INVALID_INDEX);
			auto failed_index = LocateErrorIndex(false, transaction_matches);
			D_ASSERT(failed_index != DConstants::INVALID_INDEX);
			ThrowForeignKeyConstraintError(failed_index, false, *transaction_index, dst_chunk);
		}
	}
}

void DataTable::VerifyAppendForeignKeyConstraint(optional_ptr<LocalTableStorage> storage,
                                                 const BoundForeignKeyConstraint &bound_foreign_key,
                                                 ClientContext &context, DataChunk &chunk) {
	VerifyForeignKeyConstraint(storage, bound_foreign_key, context, chunk, VerifyExistenceType::APPEND_FK);
}

void DataTable::VerifyDeleteForeignKeyConstraint(optional_ptr<LocalTableStorage> storage,
                                                 const BoundForeignKeyConstraint &bound_foreign_key,
                                                 ClientContext &context, DataChunk &chunk) {
	VerifyForeignKeyConstraint(storage, bound_foreign_key, context, chunk, VerifyExistenceType::DELETE_FK);
}

void DataTable::VerifyNewConstraint(LocalStorage &local_storage, DataTable &parent, const BoundConstraint &constraint) {
	if (constraint.type != ConstraintType::NOT_NULL) {
		throw NotImplementedException("FIXME: ALTER COLUMN with such constraint is not supported yet");
	}

	parent.row_groups->VerifyNewConstraint(parent, constraint);
	local_storage.VerifyNewConstraint(parent, constraint);
}

// start Anybase changes
void DataTable::VerifyUniqueIndexes(TableIndexList &indexes, optional_ptr<LocalTableStorage> storage, DataChunk &chunk,
                                    optional_ptr<ConflictManager> manager, bool allow_non_standard_vector_sizes) {
	// Verify the constraint without a conflict manager.
	if (!manager) {
		return indexes.ScanBound<ART>([&](ART &art) {
			if (!art.IsUnique()) {
				return false;
			}

			if (storage) {
				auto delete_index = storage->delete_indexes.Find(art.GetIndexName());
				IndexAppendInfo index_append_info(IndexAppendMode::DEFAULT, delete_index);
				art.VerifyAppend(chunk, index_append_info, nullptr, allow_non_standard_vector_sizes);
			} else {
				IndexAppendInfo index_append_info;
				art.VerifyAppend(chunk, index_append_info, nullptr, allow_non_standard_vector_sizes);
			}
			return false;
		});
	}

	// The conflict manager is only provided for statements containing ON CONFLICT.
	auto &conflict_info = manager->GetConflictInfo();

	// Find all indexes matching the conflict target.
	indexes.ScanBound<ART>([&](ART &art) {
		if (!art.IsUnique()) {
			return false;
		}
		if (!conflict_info.ConflictTargetMatches(art)) {
			return false;
		}

		if (storage) {
			auto delete_index = storage->delete_indexes.Find(art.GetIndexName());
			manager->AddIndex(art, delete_index);
		} else {
			manager->AddIndex(art, nullptr);
		}
		return false;
	});

	// Verify indexes matching the conflict target.
	manager->SetMode(ConflictManagerMode::SCAN);
	auto &matched_indexes = manager->MatchedIndexes();
	auto &matched_delete_indexes = manager->MatchedDeleteIndexes();
	IndexAppendInfo index_append_info(IndexAppendMode::DEFAULT, nullptr);
	for (idx_t i = 0; i < matched_indexes.size(); i++) {
		index_append_info.delete_index = matched_delete_indexes[i];
		matched_indexes[i].get().VerifyAppend(chunk, index_append_info, *manager, allow_non_standard_vector_sizes);
	}

	// Scan the other indexes and throw, if there are any conflicts.
	manager->SetMode(ConflictManagerMode::THROW);
	indexes.ScanBound<ART>([&](ART &art) {
		if (!art.IsUnique()) {
			return false;
		}
		if (manager->MatchedIndex(art)) {
			return false;
		}

		if (storage) {
			auto delete_index = storage->delete_indexes.Find(art.GetIndexName());
			IndexAppendInfo index_append_info(IndexAppendMode::DEFAULT, delete_index);
			art.VerifyAppend(chunk, index_append_info, *manager, allow_non_standard_vector_sizes);
		} else {
			IndexAppendInfo index_append_info;
			art.VerifyAppend(chunk, index_append_info, *manager, allow_non_standard_vector_sizes);
		}
		return false;
	});
}

void DataTable::VerifyAppendConstraints(ConstraintState &constraint_state, ClientContext &context, DataChunk &chunk,
                                        optional_ptr<LocalTableStorage> storage,
                                        optional_ptr<ConflictManager> manager,
                                        bool allow_non_standard_vector_sizes) {

	auto &table = constraint_state.table;
	if (table.HasGeneratedColumns()) {
		// Verify the generated columns against the inserted values.
		auto binder = Binder::CreateBinder(context);
		physical_index_set_t bound_columns;
		CheckBinder generated_check_binder(*binder, context, table.name, table.GetColumns(), bound_columns);
		for (auto &col : table.GetColumns().Logical()) {
			if (!col.Generated()) {
				continue;
			}
			D_ASSERT(col.Type().id() != LogicalTypeId::ANY);
			generated_check_binder.target_type = col.Type();
			auto to_be_bound_expression = col.GeneratedExpression().Copy();
			auto bound_expression = generated_check_binder.Bind(to_be_bound_expression);
			VerifyGeneratedExpressionSuccess(context, table, chunk, *bound_expression, col.Oid());
		}
	}

	if (HasUniqueIndexes()) {
		VerifyUniqueIndexes(info->indexes, storage, chunk, manager, allow_non_standard_vector_sizes);
	}

	auto &constraints = table.GetConstraints();
	for (idx_t i = 0; i < constraint_state.bound_constraints.size(); i++) {
		auto &base_constraint = constraints[i];
		auto &constraint = constraint_state.bound_constraints[i];
		switch (base_constraint->type) {
		case ConstraintType::NOT_NULL: {
			auto &bound_not_null = constraint->Cast<BoundNotNullConstraint>();
			auto &not_null = base_constraint->Cast<NotNullConstraint>();
			auto &col = table.GetColumns().GetColumn(LogicalIndex(not_null.index));
			VerifyNotNullConstraint(table, chunk.data[bound_not_null.index.index], chunk.size(), col.Name());
			break;
		}
		case ConstraintType::CHECK: {
			auto &check = base_constraint->Cast<CheckConstraint>();
			auto &bound_check = constraint->Cast<BoundCheckConstraint>();
			VerifyCheckConstraint(context, table, *bound_check.expression, chunk, check);
			break;
		}
		case ConstraintType::UNIQUE: {
			// These were handled earlier.
			break;
		}
		case ConstraintType::FOREIGN_KEY: {
			auto &bound_foreign_key = constraint->Cast<BoundForeignKeyConstraint>();
			if (bound_foreign_key.info.IsAppendConstraint()) {
				VerifyAppendForeignKeyConstraint(storage, bound_foreign_key, context, chunk);
			}
			break;
		}
		default:
			throw InternalException("invalid constraint type");
		}
	}
}
// end Anybase changes

// start Anybase changes
bool AllMergeConflictsMeetCondition(DataChunk &result) {
	result.Flatten();
	auto data = FlatVector::GetData<bool>(result.data[0]);
	for (idx_t i = 0; i < result.size(); i++) {
		if (!data[i]) {
			return false;
		}
	}
	return true;
}

void CheckOnConflictCondition(ClientContext &context, DataChunk &conflicts, const unique_ptr<Expression> &condition,
                              DataChunk &result) {
	ExpressionExecutor executor(context, *condition);
	result.Initialize(context, {LogicalType::BOOLEAN});
	executor.Execute(conflicts, result);
	result.SetCardinality(conflicts.size());
}

static void CombineExistingAndInsertTuples(DataChunk &result, DataChunk &scan_chunk, DataChunk &input_chunk,
                                           ClientContext &client,
                                           const optional_ptr<const vector<LogicalType>> &insert_types,
                                           const optional_ptr<const vector<LogicalType>> &types_to_fetch) {

	if (types_to_fetch == nullptr || types_to_fetch->empty()) {
		// We have not scanned the initial table, so we can just duplicate the initial chunk
		result.Initialize(client, input_chunk.GetTypes());
		result.Reference(input_chunk);
		result.SetCardinality(input_chunk);
		return;
	}

	D_ASSERT(insert_types != nullptr);
	vector<LogicalType> combined_types;
	combined_types.reserve(insert_types->size() + types_to_fetch->size());
	combined_types.insert(combined_types.end(), insert_types->begin(), insert_types->end());
	combined_types.insert(combined_types.end(), types_to_fetch->begin(), types_to_fetch->end());

	result.Initialize(client, combined_types);
	result.Reset();
	// Add the VALUES list
	for (idx_t i = 0; i < insert_types->size(); i++) {
		idx_t col_idx = i;
		auto &other_col = input_chunk.data[i];
		auto &this_col = result.data[col_idx];
		D_ASSERT(other_col.GetType() == this_col.GetType());
		this_col.Reference(other_col);
	}
	// Add the columns from the original conflicting tuples
	for (idx_t i = 0; i < types_to_fetch->size(); i++) {
		idx_t col_idx = i + insert_types->size();
		auto &other_col = scan_chunk.data[i];
		auto &this_col = result.data[col_idx];
		D_ASSERT(other_col.GetType() == this_col.GetType());
		this_col.Reference(other_col);
	}
	// This is guaranteed by the requirement of a conflict target to have a condition or set expressions
	// Only when we have any sort of condition or SET expression that references the existing table is this possible
	// to not be true.
	// We can have a SET expression without a conflict target ONLY if there is only 1 Index on the table
	// In which case this also can't cause a discrepancy between existing tuple count and insert tuple count
	D_ASSERT(input_chunk.size() == scan_chunk.size());
	result.SetCardinality(input_chunk.size());
}

// TODO: should we use a hash table to keep track of this instead?
template <bool GLOBAL>
static bool CheckForDuplicateTargets(const Vector &row_ids, idx_t count) {
	// Insert all rows, if any of the rows has already been updated before, we throw an error
	auto data = FlatVector::GetData<row_t>(row_ids);

	// The rowids in the transaction-local ART aren't final yet so we have to separately keep track of the two sets of
	// rowids
	// Rows that have been updated by a DO UPDATE conflict
	unordered_set<row_t> updated_global_rows;
	// Rows in the transaction-local storage that have been updated by a DO UPDATE conflict
	unordered_set<row_t> updated_local_rows;
	unordered_set<row_t> &updated_rows = GLOBAL ? updated_global_rows :updated_local_rows;
	bool is_sorted = true;
	for (idx_t i = 0; i < count; i++) {
		auto result = updated_rows.insert(data[i]);
		if (result.second == false) {
			throw InvalidInputException(
			    "ON CONFLICT DO UPDATE can not update the same row twice in the same command. Ensure that no rows "
			    "proposed for insertion within the same command have duplicate constrained values");
		}

		if (i > 0 && data[i - 1] > data[i]) {
			is_sorted = false;
		}
	}

	return is_sorted;
}

static vector<LogicalType> GetTypesOfSetColumns(const vector<PhysicalIndex>& set_columns, TableCatalogEntry &table) {
	vector<LogicalType> update_types;
	for (idx_t i = 0; i < set_columns.size(); i++) {
		update_types.push_back(table.GetColumns().GetColumn(set_columns[i]).Type());
	}

	return update_types;
}

static void CreateUpdateChunk(ClientContext &context, DataChunk &chunk, TableCatalogEntry &table, const vector<PhysicalIndex>& set_columns, Vector &row_ids, DataChunk &update_chunk,
                              const vector<LogicalType> &set_types,
                              const optional_ptr<const vector<unique_ptr<Expression>>> &set_expressions,
                              const optional_ptr<const unique_ptr<Expression>> &do_update_condition) {

	if (set_expressions) {
		// Check the optional condition for the DO UPDATE clause, to filter which rows will be updated
		if (do_update_condition && *do_update_condition) {
			DataChunk do_update_filter_result;
			do_update_filter_result.Initialize(context, {LogicalType::BOOLEAN});
			ExpressionExecutor where_executor(context, **do_update_condition);
			where_executor.Execute(chunk, do_update_filter_result);
			do_update_filter_result.SetCardinality(chunk.size());
			do_update_filter_result.Flatten();

			ManagedSelection selection(chunk.size());

			auto where_data = FlatVector::GetData<bool>(do_update_filter_result.data[0]);
			for (idx_t i = 0; i < chunk.size(); i++) {
				if (where_data[i]) {
					selection.Append(i);
				}
			}
			if (selection.Count() != selection.Size()) {
				// Not all conflicts met the condition, need to filter out the ones that don't
				chunk.Slice(selection.Selection(), selection.Count());
				chunk.SetCardinality(selection.Count());
				// Also apply this Slice to the to-update row_ids
				row_ids.Slice(selection.Selection(), selection.Count());
			}
		}

		// Execute the SET expressions
		update_chunk.Initialize(context, set_types);
		ExpressionExecutor executor(context, *set_expressions);
		executor.Execute(chunk, update_chunk);
		update_chunk.SetCardinality(chunk);
	} else {
		D_ASSERT(table.GetColumns().Physical().Size() == chunk.ColumnCount());
		auto update_types = GetTypesOfSetColumns(set_columns, table);
		update_chunk.Initialize(context, set_types, chunk.size());
		for (idx_t i = 0; i < set_columns.size(); i++) {
			update_chunk.data[i].Reference(chunk.data[set_columns[i].index]);
		}

		update_chunk.SetCardinality(chunk);
	}
}

template <bool GLOBAL>
static idx_t PerformOnConflictAction(ClientContext &context, DataChunk &chunk, TableCatalogEntry &table,
                                     Vector &row_ids, const vector<PhysicalIndex>& set_columns,
                                     const vector<unique_ptr<BoundConstraint>> &bound_constraints,
                                     const vector<LogicalType> &set_types,
                                     const optional_ptr<const vector<unique_ptr<Expression>>> &set_expressions,
                                     const optional_ptr<const unique_ptr<Expression>> &do_update_condition) {
	DataChunk update_chunk;
	CreateUpdateChunk(context, chunk, table, set_columns, row_ids, update_chunk, set_types, set_expressions, do_update_condition);

	auto &data_table = table.GetStorage();
	// Perform the update, using the results of the SET expressions
	if (GLOBAL) {
	  	auto update_state = data_table.InitializeUpdate(table, context, bound_constraints);
	  	data_table.Update(*update_state, context, row_ids, set_columns, update_chunk);
	} else {
		auto &local_storage = LocalStorage::Get(context, data_table.db);
		// Perform the update, using the results of the SET expressions
		local_storage.Update(data_table, row_ids, set_columns, update_chunk);
	}
	return update_chunk.size();
}

template <bool GLOBAL>
static idx_t PerformOrderedUpdate(TableCatalogEntry &table, ClientContext &context,
                                  const vector<unique_ptr<BoundConstraint>> &bound_constraints,
                                  const vector<PhysicalIndex> &set_columns,
                                  Vector &row_ids, DataChunk &conflict_chunk,
                                  const vector<LogicalType> &set_types,
                                  const optional_ptr<const vector<unique_ptr<Expression>>> &set_expressions,
                                  const optional_ptr<const unique_ptr<Expression>> &do_update_condition) {

	idx_t updated_tuples = 0;
	auto standard_chunk_count = (idx_t) ceil((double) conflict_chunk.size() / STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < standard_chunk_count; i++) {
		auto chunk_size = std::min<idx_t>(conflict_chunk.size() - updated_tuples, STANDARD_VECTOR_SIZE);
		auto offset = i * STANDARD_VECTOR_SIZE;
		DataChunk data_chunk;
		data_chunk.Initialize(context, conflict_chunk.GetTypes());
		data_chunk.Reference(conflict_chunk);
		data_chunk.Slice(offset, chunk_size);

		Vector group_row_ids(row_ids);
		group_row_ids.Slice(row_ids, offset, offset + chunk_size);

		updated_tuples += PerformOnConflictAction<GLOBAL>(context, data_chunk, table, group_row_ids, set_columns, bound_constraints, set_types, set_expressions, do_update_condition);
	}

	return updated_tuples;
}

struct GroupedUpdate {
	explicit GroupedUpdate() {
		count = 0;
	}

	void Initialize(idx_t size) {
		sel.Initialize(size);
	}

	SelectionVector sel;
	idx_t count;
	vector<row_t> ids;
};

static map<std::tuple<idx_t, idx_t>, GroupedUpdate> GroupUpdatesByRowGroup(DataChunk &conflict_chunk, Vector &row_ids,
                                                                           const shared_ptr<RowGroupCollection> &row_groups) {
	D_ASSERT(row_ids.GetType().InternalType() == ROW_TYPE);

	auto flat_row_ids = FlatVector::GetData<row_t>(row_ids);
	map<std::tuple<idx_t, idx_t>, GroupedUpdate> grouped_updates;
	for (idx_t i = 0; i < conflict_chunk.size(); i++) {
		auto row_group = row_groups->GetRowGroupByRowNumber(UnsafeNumericCast<idx_t>(flat_row_ids[i]));
		auto rg_index = row_group->index;
		auto c_index = ((idx_t)flat_row_ids[i] - row_group->start) / STANDARD_VECTOR_SIZE;
		auto key = std::make_tuple(rg_index, c_index);

		if (grouped_updates.find(key) == grouped_updates.end()) {
			GroupedUpdate grouped_update;
			grouped_update.Initialize(row_group->count);
			grouped_updates[key] = std::move(grouped_update);
		}

		auto grouped_update = std::move(grouped_updates[key]);

		grouped_update.sel.set_index(grouped_update.count, i);
		grouped_update.count++;
		grouped_updates[key] = std::move(grouped_update);
	}

	return grouped_updates;
}

template <bool GLOBAL>
static idx_t PerformUnOrderedUpdate(TableCatalogEntry &table, ClientContext &context,
                                    const vector<unique_ptr<BoundConstraint>> &bound_constraints,
                                    const vector<PhysicalIndex> &set_columns,
                                    Vector &row_ids, DataChunk &conflict_chunk, const shared_ptr<RowGroupCollection> &row_groups,
                                    const vector<LogicalType> &set_types,
                                    const optional_ptr<const vector<unique_ptr<Expression>>> &set_expressions,
                                    const optional_ptr<const unique_ptr<Expression>> &do_update_condition) {

	idx_t updated_tuples = 0;
	auto grouped_updates = GroupUpdatesByRowGroup(conflict_chunk, row_ids, row_groups);

	for (auto &kvp : grouped_updates) {
		duckdb::DataChunk data_chunk;
		data_chunk.Initialize(context, conflict_chunk.GetTypes());
		data_chunk.Reference(conflict_chunk);
		data_chunk.Slice(kvp.second.sel, kvp.second.count);

		auto row_group_ids = Vector(row_ids);
		row_group_ids.Slice(kvp.second.sel, kvp.second.count);

		updated_tuples += PerformOnConflictAction<GLOBAL>(context, data_chunk, table, row_group_ids,
		                                                  set_columns, bound_constraints, set_types, set_expressions, do_update_condition);
	}

	return updated_tuples;
}

template <bool GLOBAL>
static idx_t HandleInsertConflicts(TableCatalogEntry &table, ClientContext &context, DataChunk &insert_chunk,
                                   DataTable &data_table, const vector<unique_ptr<BoundConstraint>> &bound_constraints,
                                   const unordered_set<column_t> &conflict_target, const vector<PhysicalIndex> &set_columns,
                                   const vector<LogicalType> &set_types,
                                   const optional_ptr<shared_ptr<RowGroupCollection>> &row_groups,
                                   const optional_ptr<const vector<LogicalType>> &insert_types,
                                   const optional_ptr<const vector<LogicalType>> &types_to_fetch,
                                   const optional_ptr<const unique_ptr<Expression>> &conflict_condition,
                                   const optional_ptr<const vector<StorageIndex>> &columns_to_fetch,
                                   const optional_ptr<const vector<unique_ptr<Expression>>> &set_expressions,
                                   const optional_ptr<const unique_ptr<Expression>> &do_update_condition
                                   ) {

	auto &local_storage = LocalStorage::Get(context, data_table.db);
	auto local_table_storage = local_storage.GetStorage(table.GetStorage());

	// We either want to do nothing, or perform an update when conflicts arise
	ConflictInfo conflict_info(conflict_target);
	ConflictManager conflict_manager(VerifyExistenceType::APPEND, insert_chunk.size(), &conflict_info);

	if (GLOBAL) {
		auto constraint_state = data_table.InitializeConstraintState(table, bound_constraints);
		data_table.VerifyAppendConstraints(*constraint_state, context, insert_chunk, local_table_storage, &conflict_manager, true);
	} else {
		DataTable::VerifyUniqueIndexes(local_storage.GetIndexes(context, data_table), local_table_storage, insert_chunk, &conflict_manager);
	}

	conflict_manager.Finalize();
	if (conflict_manager.ConflictCount() == 0) {
		// No conflicts found, 0 updates performed
		return 0;
	}

	auto &conflicts = conflict_manager.Conflicts();
	auto &row_ids = conflict_manager.RowIds();

	DataChunk conflict_chunk; // contains only the conflicting values
	DataChunk scan_chunk;     // contains the original values, that caused the conflict
	DataChunk combined_chunk; // contains conflict_chunk + scan_chunk (wide)

	// Filter out everything but the conflicting rows
	conflict_chunk.Initialize(context, insert_chunk.GetTypes());
	conflict_chunk.Reference(insert_chunk);
	conflict_chunk.Slice(conflicts.Selection(), conflicts.Count());
	conflict_chunk.SetCardinality(conflicts.Count());

	// Start CDC changes
	auto &current_transaction = DuckTransaction::Get(context, table.catalog);
	auto columnMap = unordered_map<column_t, vector<column_t>>();
	auto involved_columns = vector<idx_t>(conflict_target.begin(), conflict_target.end());
	for (idx_t i = 0; i < set_columns.size(); ++i) {
		involved_columns.push_back(set_columns[i].index);
	}

	for (auto &t : set_columns) {
		columnMap[t.index] = involved_columns;
	}

	current_transaction.involved_columns[data_table.GetTableName()] = columnMap;
	// End CDC changes

	// Holds the pins for the fetched rows
	unique_ptr<ColumnFetchState> fetch_state;
	if (types_to_fetch != nullptr && !types_to_fetch->empty()) {
		D_ASSERT(scan_chunk.size() == 0);
		// When these values are required for the conditions or the SET expressions,
		// then we scan the existing table for the conflicting tuples, using the rowids
		scan_chunk.Initialize(context, *types_to_fetch);
		fetch_state = make_uniq<ColumnFetchState>();
		if (GLOBAL) {
			auto &transaction = DuckTransaction::Get(context, table.catalog);
			data_table.Fetch(transaction, scan_chunk, *columns_to_fetch, row_ids, conflicts.Count(), *fetch_state);
		} else {
			local_storage.FetchChunk(data_table, row_ids, conflicts.Count(), *columns_to_fetch, scan_chunk,
			                         *fetch_state);
		}
	}

	// Splice the Input chunk and the fetched chunk together
	CombineExistingAndInsertTuples(combined_chunk, scan_chunk, conflict_chunk, context, insert_types, types_to_fetch);

	if (conflict_condition && *conflict_condition) {
		DataChunk conflict_condition_result;
		CheckOnConflictCondition(context, combined_chunk, *conflict_condition, conflict_condition_result);
		bool conditions_met = AllMergeConflictsMeetCondition(conflict_condition_result);
		if (!conditions_met) {
			// Filter out the tuples that did pass the filter, then run the verify again
			ManagedSelection sel(combined_chunk.size());
			auto data = FlatVector::GetData<bool>(conflict_condition_result.data[0]);
			for (idx_t i = 0; i < combined_chunk.size(); i++) {
				if (!data[i]) {
					// Only populate the selection vector with the tuples that did not meet the condition
					sel.Append(i);
				}
			}
			combined_chunk.Slice(sel.Selection(), sel.Count());
			row_ids.Slice(sel.Selection(), sel.Count());
			if (GLOBAL) {
				auto constraint_state = data_table.InitializeConstraintState(table, bound_constraints);
				data_table.VerifyAppendConstraints(*constraint_state, context, combined_chunk, local_table_storage, nullptr);
			} else {
				DataTable::VerifyUniqueIndexes(local_storage.GetIndexes(context, data_table), local_table_storage,
				                               insert_chunk, nullptr);
			}
			throw InternalException("The previous operation was expected to throw but didn't");
		}
	}

	auto is_sorted = CheckForDuplicateTargets<GLOBAL>(row_ids, combined_chunk.size());

	idx_t updated_tuples = 0;

	if (!set_types.empty()) {
		if (is_sorted || row_groups == nullptr) {
			// fast path for sorted updates
			updated_tuples = PerformOrderedUpdate<GLOBAL>(table, context, bound_constraints, set_columns, row_ids,
														  combined_chunk, set_types, set_expressions, do_update_condition);
		} else {
			// fast path for non-sorted updates
			updated_tuples = PerformUnOrderedUpdate<GLOBAL>(table, context, bound_constraints, set_columns, row_ids,
															combined_chunk, *row_groups,
															set_types, set_expressions, do_update_condition);
		}
	}

	// Remove the conflicting tuples from the insert chunk
	SelectionVector sel_vec(insert_chunk.size());
	idx_t new_size = SelectionVector::Inverted(conflicts.Selection(), sel_vec, conflicts.Count(), insert_chunk.size());
	insert_chunk.Slice(sel_vec, new_size);
	insert_chunk.SetCardinality(new_size);

	return updated_tuples;
}

static idx_t OnConflictHandling(TableCatalogEntry &table, ClientContext &context,
                                DataChunk& chunk, const vector<unique_ptr<BoundConstraint>> &bound_constraints,
                                const unordered_set<column_t> &conflict_target, const vector<PhysicalIndex> &set_columns,
                                const vector<LogicalType> &set_types,
                                const optional_ptr<shared_ptr<RowGroupCollection>> &row_groups,
                                const optional_ptr<const vector<LogicalType>> &insert_types,
                                const optional_ptr<const vector<LogicalType>> &types_to_fetch,
                                const optional_ptr<const unique_ptr<Expression>> &conflict_condition,
                                const optional_ptr<const vector<StorageIndex>> &columns_to_fetch,
                                const optional_ptr<const vector<unique_ptr<Expression>>> &set_expressions,
                                const optional_ptr<const unique_ptr<Expression>> &do_update_condition
                                ) {
	auto &data_table = table.GetStorage();
	idx_t updated_tuples = 0;

	updated_tuples += HandleInsertConflicts<true>(table, context, chunk, data_table, bound_constraints, conflict_target, set_columns,
	                                              set_types, row_groups, insert_types, types_to_fetch, conflict_condition,
	                                              columns_to_fetch, set_expressions, do_update_condition);
	// Also check the transaction-local storage+ART so we can detect conflicts within this transaction
	updated_tuples += HandleInsertConflicts<false>(table, context, chunk, data_table, bound_constraints, conflict_target, set_columns,
	                                               set_types, row_groups, insert_types, types_to_fetch, conflict_condition,
	                                               columns_to_fetch, set_expressions, do_update_condition);

	return updated_tuples;
}

static void AppendInsertChunks(TableCatalogEntry &table, ClientContext &context, DataTable &storage, DataChunk &insert_chunk, LocalAppendState &append_state) {
	idx_t insert_count = 0;
	auto standard_chunk_count = (idx_t) ceil((double) insert_chunk.size() / STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < standard_chunk_count; i++) {
		auto chunk_size = std::min<idx_t>(insert_chunk.size() - insert_count, STANDARD_VECTOR_SIZE);
		auto offset = i * STANDARD_VECTOR_SIZE;
		DataChunk data_chunk;
		data_chunk.Initialize(context, insert_chunk.GetTypes());
		data_chunk.Reference(insert_chunk);
		data_chunk.Slice(offset, chunk_size);

		storage.LocalAppend(append_state, context, data_chunk, true);
		insert_count += chunk_size;
	}
}

void DataTable::Merge(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk, const vector<unique_ptr<BoundConstraint>> &bound_constraints,
                           const unordered_set<column_t> &conflict_target, const vector<PhysicalIndex> &set_columns,
                           LocalAppendState &append_state, bool finalize_on_conflict, bool do_appends,
                           idx_t &update_count, idx_t &insert_count, const vector<LogicalType> &types_to_fetch,
                           const vector<LogicalType> &insert_types, const unique_ptr<Expression> &conflict_condition,
                           const vector<StorageIndex> &columns_to_fetch, const vector<unique_ptr<Expression>> &set_expressions,
                           const vector<LogicalType> &set_types, const unique_ptr<Expression> &do_update_condition) {

	auto &storage = table.GetStorage();

	DataChunk insert_chunk;
	insert_chunk.Initialize(context, chunk.GetTypes());
	insert_chunk.Reference(chunk);
	update_count = OnConflictHandling(table, context, insert_chunk, bound_constraints, conflict_target, set_columns,
	                                  set_types, nullptr, insert_types, types_to_fetch, conflict_condition,
	                                  columns_to_fetch, set_expressions, do_update_condition);

	if (do_appends) {
		storage.LocalAppend(append_state, context, insert_chunk, true);
		insert_count = insert_chunk.size();
	} else {
		insert_count = 0;
	}

	if (finalize_on_conflict) {
		storage.FinalizeLocalAppend(append_state);
	}

	chunk.Reset();
	chunk.Reference(insert_chunk);
}

void DataTable::Merge(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk, const vector<unique_ptr<BoundConstraint>> &bound_constraints,
                           const unordered_set<column_t> &conflict_target, const vector<PhysicalIndex> &set_columns,
                           LocalAppendState &append_state, bool do_appends, idx_t &update_count, idx_t &insert_count) {

	auto &storage = table.GetStorage();

	DataChunk insert_chunk;
	insert_chunk.Initialize(context, chunk.GetTypes());
	insert_chunk.Reference(chunk);
	auto set_types = GetTypesOfSetColumns(set_columns, table);
	update_count = OnConflictHandling(table, context, insert_chunk, bound_constraints, conflict_target, set_columns, set_types,
	                                  nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr);

	if (do_appends) {
		storage.LocalAppend(append_state, context, insert_chunk, true);
		insert_count = insert_chunk.size();
	} else {
		insert_count = 0;
	}

	storage.FinalizeLocalAppend(append_state);

	chunk.Reset();
	chunk.Reference(insert_chunk);
}

void DataTable::Merge(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk, const vector<unique_ptr<BoundConstraint>> &bound_constraints,
                           const unordered_set<column_t> &conflict_target, const vector<PhysicalIndex> &set_columns) {
	LocalAppendState append_state;
	idx_t update_count, insert_count;
	Merge(table, context, chunk, bound_constraints, conflict_target, set_columns, append_state, true, update_count, insert_count);
}

void DataTable::Merge(TableCatalogEntry &table, ClientContext &context, ColumnDataCollection &collection, const vector<unique_ptr<BoundConstraint>> &bound_constraints,
                           const unordered_set<column_t> &conflict_target, const vector<PhysicalIndex> &set_columns) {
	LocalAppendState append_state;
	auto &storage = table.GetStorage();
	storage.InitializeLocalAppend(append_state, table, context, bound_constraints);

	DataChunk insert_chunk;
	insert_chunk.Initialize(context, collection.Types(), collection.Count());
	for (auto &chunk : collection.Chunks()) {
		if (chunk.size() <= STANDARD_VECTOR_SIZE && collection.ChunkCount() == 1) {
			insert_chunk.Reference(chunk);
		} else {
			//Ideally appending a reference would be better than this copy, but for now this works fairly well.
			insert_chunk.Append(chunk, false);
		}
	}

	auto set_types = GetTypesOfSetColumns(set_columns, table);
	OnConflictHandling(table, context, insert_chunk, bound_constraints, conflict_target, set_columns, set_types, row_groups,
	                   nullptr, nullptr, nullptr, nullptr, nullptr, nullptr);
	AppendInsertChunks(table, context, storage, insert_chunk, append_state);
	storage.FinalizeLocalAppend(append_state);
}
// end Anybase changes

unique_ptr<ConstraintState>
DataTable::InitializeConstraintState(TableCatalogEntry &table,
                                     const vector<unique_ptr<BoundConstraint>> &bound_constraints) {
	return make_uniq<ConstraintState>(table, bound_constraints);
}

string DataTable::TableModification() const {
	switch (version.load()) {
	case DataTableVersion::MAIN_TABLE:
		return "no changes";
	case DataTableVersion::ALTERED:
		return "altered";
	case DataTableVersion::DROPPED:
		return "dropped";
	default:
		throw InternalException("Unrecognized table version");
	}
}

void DataTable::InitializeLocalAppend(LocalAppendState &state, TableCatalogEntry &table, ClientContext &context,
                                      const vector<unique_ptr<BoundConstraint>> &bound_constraints) {
	if (!IsMainTable()) {
		throw TransactionException("Transaction conflict: attempting to insert into table \"%s\" but it has been %s by "
		                           "a different transaction",
		                           GetTableName(), TableModification());
	}
	auto &local_storage = LocalStorage::Get(context, db);
	local_storage.InitializeAppend(state, *this);
	state.constraint_state = InitializeConstraintState(table, bound_constraints);
}

void DataTable::InitializeLocalStorage(LocalAppendState &state, TableCatalogEntry &table, ClientContext &context,
                                       const vector<unique_ptr<BoundConstraint>> &bound_constraints) {
	if (!IsMainTable()) {
		throw TransactionException("Transaction conflict: attempting to insert into table \"%s\" but it has been %s by "
		                           "a different transaction",
		                           GetTableName(), TableModification());
	}

	auto &local_storage = LocalStorage::Get(context, db);
	local_storage.InitializeStorage(state, *this);
	state.constraint_state = InitializeConstraintState(table, bound_constraints);
}

void DataTable::LocalAppend(LocalAppendState &state, ClientContext &context, DataChunk &chunk, bool unsafe) {
	if (chunk.size() == 0) {
		return;
	}
	if (!IsMainTable()) {
		throw TransactionException("Transaction conflict: attempting to insert into table \"%s\" but it has been %s by "
		                           "a different transaction",
		                           GetTableName(), TableModification());
	}
	chunk.Verify();

	// Insert any row ids into the DELETE ART and verify constraints afterward.
	// This happens only for the global indexes.
	if (!unsafe) {
		auto &constraint_state = *state.constraint_state;
		VerifyAppendConstraints(constraint_state, context, chunk, *state.storage, nullptr);
	}

	// Append to the transaction-local data.
	LocalStorage::Append(state, chunk);
}

void DataTable::LocalAppend(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk,
                            const vector<unique_ptr<BoundConstraint>> &bound_constraints) {
	LocalAppendState append_state;
	InitializeLocalAppend(append_state, table, context, bound_constraints);
	LocalAppend(append_state, context, chunk, false);
	FinalizeLocalAppend(append_state);
}

void DataTable::FinalizeLocalAppend(LocalAppendState &state) {
	LocalStorage::FinalizeAppend(state);
}

PhysicalIndex DataTable::CreateOptimisticCollection(ClientContext &context, unique_ptr<RowGroupCollection> collection) {
	auto &local_storage = LocalStorage::Get(context, db);
	return local_storage.CreateOptimisticCollection(*this, std::move(collection));
}

RowGroupCollection &DataTable::GetOptimisticCollection(ClientContext &context, const PhysicalIndex collection_index) {
	auto &local_storage = LocalStorage::Get(context, db);
	return local_storage.GetOptimisticCollection(*this, collection_index);
}

void DataTable::ResetOptimisticCollection(ClientContext &context, const PhysicalIndex collection_index) {
	auto &local_storage = LocalStorage::Get(context, db);
	local_storage.ResetOptimisticCollection(*this, collection_index);
}

OptimisticDataWriter &DataTable::GetOptimisticWriter(ClientContext &context) {
	auto &local_storage = LocalStorage::Get(context, db);
	return local_storage.GetOptimisticWriter(*this);
}

void DataTable::LocalMerge(ClientContext &context, RowGroupCollection &collection) {
	auto &local_storage = LocalStorage::Get(context, db);
	local_storage.LocalMerge(*this, collection);
}

void DataTable::LocalWALAppend(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk,
                               const vector<unique_ptr<BoundConstraint>> &bound_constraints) {
	LocalAppendState append_state;
	auto &storage = table.GetStorage();
	storage.InitializeLocalAppend(append_state, table, context, bound_constraints);

	storage.LocalAppend(append_state, context, chunk, true);
	append_state.storage->index_append_mode = IndexAppendMode::INSERT_DUPLICATES;
	storage.FinalizeLocalAppend(append_state);
}

void DataTable::LocalAppend(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk,
                            const vector<unique_ptr<BoundConstraint>> &bound_constraints, Vector &row_ids,
                            DataChunk &delete_chunk) {
	LocalAppendState append_state;
	auto &storage = table.GetStorage();
	storage.InitializeLocalAppend(append_state, table, context, bound_constraints);
	append_state.storage->AppendToDeleteIndexes(row_ids, delete_chunk);

	storage.LocalAppend(append_state, context, chunk, false);
	storage.FinalizeLocalAppend(append_state);
}

void DataTable::LocalAppend(TableCatalogEntry &table, ClientContext &context, ColumnDataCollection &collection,
                            const vector<unique_ptr<BoundConstraint>> &bound_constraints,
                            optional_ptr<const vector<LogicalIndex>> column_ids) {

	LocalAppendState append_state;
	auto &storage = table.GetStorage();
	storage.InitializeLocalAppend(append_state, table, context, bound_constraints);

	if (!column_ids || column_ids->empty()) {
		for (auto &chunk : collection.Chunks()) {
			storage.LocalAppend(append_state, context, chunk, false);
		}
		storage.FinalizeLocalAppend(append_state);
		return;
	}

	auto &column_list = table.GetColumns();
	map<PhysicalIndex, unique_ptr<Expression>> active_expressions;
	for (idx_t i = 0; i < column_ids->size(); i++) {
		auto &col = column_list.GetColumn((*column_ids)[i]);
		auto expr = make_uniq<BoundReferenceExpression>(col.Name(), col.Type(), i);
		active_expressions[col.Physical()] = std::move(expr);
	}

	auto binder = Binder::CreateBinder(context);
	ConstantBinder default_binder(*binder, context, "DEFAULT value");
	vector<unique_ptr<Expression>> expressions;
	for (idx_t i = 0; i < column_list.PhysicalColumnCount(); i++) {
		auto expr = active_expressions.find(PhysicalIndex(i));
		if (expr != active_expressions.end()) {
			expressions.push_back(std::move(expr->second));
			continue;
		}

		auto &col = column_list.GetColumn(PhysicalIndex(i));
		if (!col.HasDefaultValue()) {
			auto null_expr = make_uniq<BoundConstantExpression>(Value(col.Type()));
			expressions.push_back(std::move(null_expr));
			continue;
		}

		auto default_copy = col.DefaultValue().Copy();
		default_binder.target_type = col.Type();
		auto bound_default = default_binder.Bind(default_copy);
		expressions.push_back(std::move(bound_default));
	}

	ExpressionExecutor expression_executor(context, expressions);
	DataChunk result;
	result.Initialize(context, table.GetTypes());

	for (auto &chunk : collection.Chunks()) {
		expression_executor.Execute(chunk, result);
		storage.LocalAppend(append_state, context, result, false);
		result.Reset();
	}
	storage.FinalizeLocalAppend(append_state);
}

void DataTable::AppendLock(TableAppendState &state) {
	state.append_lock = unique_lock<mutex>(append_lock);
	if (!IsMainTable()) {
		throw TransactionException("Transaction conflict: attempting to insert into table \"%s\" but it has been %s by "
		                           "a different transaction",
		                           GetTableName(), TableModification());
	}
	state.row_start = NumericCast<row_t>(row_groups->GetTotalRows());
	state.current_row = state.row_start;
}

void DataTable::InitializeAppend(DuckTransaction &transaction, TableAppendState &state) {
	// obtain the append lock for this table
	if (!state.append_lock) {
		throw InternalException("DataTable::AppendLock should be called before DataTable::InitializeAppend");
	}
	row_groups->InitializeAppend(transaction, state);
}

void DataTable::Append(DataChunk &chunk, TableAppendState &state) {
	D_ASSERT(IsMainTable());
	row_groups->Append(chunk, state);
}

void DataTable::FinalizeAppend(DuckTransaction &transaction, TableAppendState &state) {
	row_groups->FinalizeAppend(transaction, state);
}

void DataTable::ScanTableSegment(DuckTransaction &transaction, idx_t row_start, idx_t count,
                                 const std::function<void(DataChunk &chunk)> &function) {
	if (count == 0) {
		return;
	}
	idx_t end = row_start + count;

	vector<StorageIndex> column_ids;
	vector<LogicalType> types;
	for (idx_t i = 0; i < this->column_definitions.size(); i++) {
		auto &col = this->column_definitions[i];
		column_ids.emplace_back(i);
		types.push_back(col.Type());
	}
	DataChunk chunk;
	chunk.Initialize(Allocator::Get(db), types);

	CreateIndexScanState state;

	InitializeScanWithOffset(transaction, state, column_ids, row_start, row_start + count);
	auto row_start_aligned = state.table_state.row_group->start + state.table_state.vector_index * STANDARD_VECTOR_SIZE;

	idx_t current_row = row_start_aligned;
	while (current_row < end) {
		state.table_state.ScanCommitted(chunk, TableScanType::TABLE_SCAN_COMMITTED_ROWS);
		if (chunk.size() == 0) {
			break;
		}
		idx_t end_row = current_row + chunk.size();
		// start of chunk is current_row
		// end of chunk is end_row
		// figure out if we need to write the entire chunk or just part of it
		idx_t chunk_start = MaxValue<idx_t>(current_row, row_start);
		idx_t chunk_end = MinValue<idx_t>(end_row, end);
		D_ASSERT(chunk_start < chunk_end);
		idx_t chunk_count = chunk_end - chunk_start;
		if (chunk_count != chunk.size()) {
			D_ASSERT(chunk_count <= chunk.size());
			// need to slice the chunk before insert
			idx_t start_in_chunk;
			if (current_row >= row_start) {
				start_in_chunk = 0;
			} else {
				start_in_chunk = row_start - current_row;
			}
			SelectionVector sel(start_in_chunk, chunk_count);
			chunk.Slice(sel, chunk_count);
			chunk.Verify();
		}
		function(chunk);
		chunk.Reset();
		current_row = end_row;
	}
}

void DataTable::MergeStorage(RowGroupCollection &data, TableIndexList &,
                             optional_ptr<StorageCommitState> commit_state) {
	row_groups->MergeStorage(data, this, commit_state);
	row_groups->Verify();
}

void DataTable::WriteToLog(DuckTransaction &transaction, WriteAheadLog &log, idx_t row_start, idx_t count,
                           optional_ptr<StorageCommitState> commit_state) {
	log.WriteSetTable(info->schema, info->table);
	if (commit_state) {
		idx_t optimistic_count = 0;
		auto entry = commit_state->GetRowGroupData(*this, row_start, optimistic_count);
		if (entry) {
			D_ASSERT(optimistic_count > 0);
			log.WriteRowGroupData(*entry);
			if (optimistic_count > count) {
				throw InternalException(
				    "Optimistically written count cannot exceed actual count (got %llu, but expected count is %llu)",
				    optimistic_count, count);
			}
			// write any remaining (non-optimistically written) rows to the WAL normally
			row_start += optimistic_count;
			count -= optimistic_count;
			if (count == 0) {
				return;
			}
		}
	}
	ScanTableSegment(transaction, row_start, count, [&](DataChunk &chunk) { log.WriteInsert(chunk); });
}

void DataTable::CommitAppend(transaction_t commit_id, idx_t row_start, idx_t count) {
	lock_guard<mutex> lock(append_lock);
	row_groups->CommitAppend(commit_id, row_start, count);
}

void DataTable::RevertAppendInternal(idx_t start_row) {
	D_ASSERT(IsMainTable());
	// revert appends made to row_groups
	row_groups->RevertAppendInternal(start_row);
}

void DataTable::RevertAppend(DuckTransaction &transaction, idx_t start_row, idx_t count) {
	lock_guard<mutex> lock(append_lock);

	// revert any appends to indexes
	if (!info->indexes.Empty()) {
		idx_t current_row_base = start_row;
		row_t row_data[STANDARD_VECTOR_SIZE];
		Vector row_identifiers(LogicalType::ROW_TYPE, data_ptr_cast(row_data));
		idx_t scan_count = MinValue<idx_t>(count, row_groups->GetTotalRows() - start_row);
		ScanTableSegment(transaction, start_row, scan_count, [&](DataChunk &chunk) {
			for (idx_t i = 0; i < chunk.size(); i++) {
				row_data[i] = NumericCast<row_t>(current_row_base + i);
			}
			info->indexes.Scan([&](Index &index) {
				// We cant add to unbound indexes anyways, so there is no need to revert them
				if (index.IsBound()) {
					index.Cast<BoundIndex>().Delete(chunk, row_identifiers);
				}
				return false;
			});
			current_row_base += chunk.size();
		});
	}

#ifdef DEBUG
	// Verify that our index memory is stable.
	info->indexes.Scan([&](Index &index) {
		if (index.IsBound()) {
			index.Cast<BoundIndex>().VerifyBuffers();
		}
		return false;
	});
#endif

	// revert the data table append
	RevertAppendInternal(start_row);
}

//===--------------------------------------------------------------------===//
// Indexes
//===--------------------------------------------------------------------===//
ErrorData DataTable::AppendToIndexes(TableIndexList &indexes, optional_ptr<TableIndexList> delete_indexes,
                                     DataChunk &chunk, row_t row_start, const IndexAppendMode index_append_mode) {
	ErrorData error;
	if (indexes.Empty()) {
		return error;
	}

	// first generate the vector of row identifiers
	Vector row_ids(LogicalType::ROW_TYPE);
	VectorOperations::GenerateSequence(row_ids, chunk.size(), row_start, 1);

	vector<BoundIndex *> already_appended;
	bool append_failed = false;
	// now append the entries to the indices
	indexes.Scan([&](Index &index_to_append) {
		if (!index_to_append.IsBound()) {
			throw InternalException("unbound index in DataTable::AppendToIndexes");
		}
		auto &index = index_to_append.Cast<BoundIndex>();

		// Find the matching delete index.
		optional_ptr<BoundIndex> delete_index;
		if (index.IsUnique()) {
			if (delete_indexes) {
				delete_index = delete_indexes->Find(index.name);
			}
		}

		try {
			IndexAppendInfo index_append_info(index_append_mode, delete_index);
			error = index.Append(chunk, row_ids, index_append_info);
		} catch (std::exception &ex) {
			error = ErrorData(ex);
		}

		if (error.HasError()) {
			append_failed = true;
			return true;
		}
		already_appended.push_back(&index);
		return false;
	});

	if (append_failed) {
		// constraint violation!
		// remove any appended entries from previous indexes (if any)
		for (auto *index : already_appended) {
			index->Delete(chunk, row_ids);
		}
	}
	return error;
}

ErrorData DataTable::AppendToIndexes(optional_ptr<TableIndexList> delete_indexes, DataChunk &chunk, row_t row_start,
                                     const IndexAppendMode index_append_mode) {
	D_ASSERT(IsMainTable());
	return AppendToIndexes(info->indexes, delete_indexes, chunk, row_start, index_append_mode);
}

void DataTable::RemoveFromIndexes(TableAppendState &state, DataChunk &chunk, row_t row_start) {
	D_ASSERT(IsMainTable());
	if (info->indexes.Empty()) {
		return;
	}
	// first generate the vector of row identifiers
	Vector row_identifiers(LogicalType::ROW_TYPE);
	VectorOperations::GenerateSequence(row_identifiers, chunk.size(), row_start, 1);

	// now remove the entries from the indices
	RemoveFromIndexes(state, chunk, row_identifiers);
}

void DataTable::RemoveFromIndexes(TableAppendState &state, DataChunk &chunk, Vector &row_identifiers) {
	D_ASSERT(IsMainTable());
	info->indexes.Scan([&](Index &index) {
		if (!index.IsBound()) {
			throw InternalException("Unbound index found in DataTable::RemoveFromIndexes");
		}
		auto &bound_index = index.Cast<BoundIndex>();
		bound_index.Delete(chunk, row_identifiers);
		return false;
	});
}

void DataTable::RemoveFromIndexes(Vector &row_identifiers, idx_t count) {
	D_ASSERT(IsMainTable());
	row_groups->RemoveFromIndexes(info->indexes, row_identifiers, count);
}

//===--------------------------------------------------------------------===//
// Delete
//===--------------------------------------------------------------------===//
static bool TableHasDeleteConstraints(TableCatalogEntry &table) {
	for (auto &constraint : table.GetConstraints()) {
		switch (constraint->type) {
		case ConstraintType::NOT_NULL:
		case ConstraintType::CHECK:
		case ConstraintType::UNIQUE:
			break;
		case ConstraintType::FOREIGN_KEY: {
			auto &foreign_key = constraint->Cast<ForeignKeyConstraint>();
			if (foreign_key.info.IsDeleteConstraint()) {
				return true;
			}
			break;
		}
		default:
			throw NotImplementedException("Constraint type not implemented!");
		}
	}
	return false;
}

void DataTable::VerifyDeleteConstraints(optional_ptr<LocalTableStorage> storage, TableDeleteState &state,
                                        ClientContext &context, DataChunk &chunk) {
	for (auto &constraint : state.constraint_state->bound_constraints) {
		switch (constraint->type) {
		case ConstraintType::NOT_NULL:
		case ConstraintType::CHECK:
		case ConstraintType::UNIQUE:
			break;
		case ConstraintType::FOREIGN_KEY: {
			auto &bound_foreign_key = constraint->Cast<BoundForeignKeyConstraint>();
			if (bound_foreign_key.info.IsDeleteConstraint()) {
				VerifyDeleteForeignKeyConstraint(storage, bound_foreign_key, context, chunk);
			}
			break;
		}
		default:
			throw NotImplementedException("Constraint type not implemented!");
		}
	}
}

unique_ptr<TableDeleteState> DataTable::InitializeDelete(TableCatalogEntry &table, ClientContext &context,
                                                         const vector<unique_ptr<BoundConstraint>> &bound_constraints) {
	// initialize indexes (if any)
	info->InitializeIndexes(context);

	auto binder = Binder::CreateBinder(context);
	vector<LogicalType> types;
	auto result = make_uniq<TableDeleteState>();
	result->has_delete_constraints = TableHasDeleteConstraints(table);
	if (result->has_delete_constraints) {
		// initialize the chunk if there are any constraints to verify
		for (idx_t i = 0; i < column_definitions.size(); i++) {
			result->col_ids.emplace_back(column_definitions[i].StorageOid());
			types.emplace_back(column_definitions[i].Type());
		}
		result->verify_chunk.Initialize(Allocator::Get(context), types);
		result->constraint_state = make_uniq<ConstraintState>(table, bound_constraints);
	}
	return result;
}

idx_t DataTable::Delete(TableDeleteState &state, ClientContext &context, Vector &row_identifiers, idx_t count) {
	D_ASSERT(row_identifiers.GetType().InternalType() == ROW_TYPE);
	if (count == 0) {
		return 0;
	}

	auto &transaction = DuckTransaction::Get(context, db);
	auto &local_storage = LocalStorage::Get(transaction);
	auto storage = local_storage.GetStorage(*this);

	row_identifiers.Flatten(count);
	auto ids = FlatVector::GetData<row_t>(row_identifiers);

	idx_t pos = 0;
	idx_t delete_count = 0;
	while (pos < count) {
		idx_t start = pos;
		bool is_transaction_delete = ids[pos] >= MAX_ROW_ID;
		// figure out which batch of rows to delete now
		for (pos++; pos < count; pos++) {
			bool row_is_transaction_delete = ids[pos] >= MAX_ROW_ID;
			if (row_is_transaction_delete != is_transaction_delete) {
				break;
			}
		}
		idx_t current_offset = start;
		idx_t current_count = pos - start;

		Vector offset_ids(row_identifiers, current_offset, pos);

		// This is a transaction-local DELETE.
		if (is_transaction_delete) {
			if (state.has_delete_constraints) {
				// Verify any delete constraints.
				ColumnFetchState fetch_state;
				local_storage.FetchChunk(*this, offset_ids, current_count, state.col_ids, state.verify_chunk,
				                         fetch_state);
				VerifyDeleteConstraints(storage, state, context, state.verify_chunk);
			}
			delete_count += local_storage.Delete(*this, offset_ids, current_count);
			continue;
		}

		// This is a regular DELETE.
		if (state.has_delete_constraints) {
			// Verify any delete constraints.
			ColumnFetchState fetch_state;
			Fetch(transaction, state.verify_chunk, state.col_ids, offset_ids, current_count, fetch_state);
			VerifyDeleteConstraints(storage, state, context, state.verify_chunk);
		}
		delete_count += row_groups->Delete(transaction, *this, ids + current_offset, current_count);
	}
	return delete_count;
}

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
static void CreateMockChunk(vector<LogicalType> &types, const vector<PhysicalIndex> &column_ids, DataChunk &chunk,
                            DataChunk &mock_chunk) {
	// construct a mock DataChunk
	mock_chunk.InitializeEmpty(types);
	for (column_t i = 0; i < column_ids.size(); i++) {
		mock_chunk.data[column_ids[i].index].Reference(chunk.data[i]);
	}
	mock_chunk.SetCardinality(chunk.size());
}

static bool CreateMockChunk(TableCatalogEntry &table, const vector<PhysicalIndex> &column_ids,
                            physical_index_set_t &desired_column_ids, DataChunk &chunk, DataChunk &mock_chunk) {
	idx_t found_columns = 0;
	// check whether the desired columns are present in the UPDATE clause
	for (column_t i = 0; i < column_ids.size(); i++) {
		if (desired_column_ids.find(column_ids[i]) != desired_column_ids.end()) {
			found_columns++;
		}
	}
	if (found_columns == 0) {
		// no columns were found: no need to check the constraint again
		return false;
	}
	if (found_columns != desired_column_ids.size()) {
		// not all columns in UPDATE clause are present!
		// this should not be triggered at all as the binder should add these columns
		throw InternalException("Not all columns required for the CHECK constraint are present in the UPDATED chunk!");
	}
	// construct a mock DataChunk
	auto types = table.GetTypes();
	CreateMockChunk(types, column_ids, chunk, mock_chunk);
	return true;
}

void DataTable::VerifyUpdateConstraints(ConstraintState &state, ClientContext &context, DataChunk &chunk,
                                        const vector<PhysicalIndex> &column_ids) {
	auto &table = state.table;
	auto &constraints = table.GetConstraints();
	auto &bound_constraints = state.bound_constraints;
	for (idx_t constr_idx = 0; constr_idx < bound_constraints.size(); constr_idx++) {
		auto &base_constraint = constraints[constr_idx];
		auto &constraint = bound_constraints[constr_idx];
		switch (constraint->type) {
		case ConstraintType::NOT_NULL: {
			auto &bound_not_null = constraint->Cast<BoundNotNullConstraint>();
			auto &not_null = base_constraint->Cast<NotNullConstraint>();
			// check if the constraint is in the list of column_ids
			for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
				if (column_ids[col_idx] == bound_not_null.index) {
					// found the column id: check the data in
					auto &col = table.GetColumn(LogicalIndex(not_null.index));
					VerifyNotNullConstraint(table, chunk.data[col_idx], chunk.size(), col.Name());
					break;
				}
			}
			break;
		}
		case ConstraintType::CHECK: {
			auto &check = base_constraint->Cast<CheckConstraint>();
			auto &bound_check = constraint->Cast<BoundCheckConstraint>();

			DataChunk mock_chunk;
			if (CreateMockChunk(table, column_ids, bound_check.bound_columns, chunk, mock_chunk)) {
				VerifyCheckConstraint(context, table, *bound_check.expression, mock_chunk, check);
			}
			break;
		}
		case ConstraintType::UNIQUE:
		case ConstraintType::FOREIGN_KEY:
			break;
		default:
			throw NotImplementedException("Constraint type not implemented!");
		}
	}

#ifdef DEBUG
	// Ensure that we never call UPDATE for indexed columns.
	// Instead, we must rewrite these updates into DELETE + INSERT.
	info->indexes.Scan([&](Index &index) {
		D_ASSERT(index.IsBound());
		D_ASSERT(!index.Cast<BoundIndex>().IndexIsUpdated(column_ids));
		return false;
	});
#endif
}

unique_ptr<TableUpdateState> DataTable::InitializeUpdate(TableCatalogEntry &table, ClientContext &context,
                                                         const vector<unique_ptr<BoundConstraint>> &bound_constraints) {
	// check that there are no unknown indexes
	info->InitializeIndexes(context);

	auto result = make_uniq<TableUpdateState>();
	result->constraint_state = InitializeConstraintState(table, bound_constraints);
	return result;
}

void DataTable::Update(TableUpdateState &state, ClientContext &context, Vector &row_ids,
                       const vector<PhysicalIndex> &column_ids, DataChunk &updates) {
	D_ASSERT(row_ids.GetType().InternalType() == ROW_TYPE);
	D_ASSERT(column_ids.size() == updates.ColumnCount());
	updates.Verify();

	auto count = updates.size();
	if (count == 0) {
		return;
	}

	if (!IsMainTable()) {
		throw TransactionException(
		    "Transaction conflict: attempting to update table \"%s\" but it has been %s by a different transaction",
		    GetTableName(), TableModification());
	}

	// first verify that no constraints are violated
	VerifyUpdateConstraints(*state.constraint_state, context, updates, column_ids);

	// now perform the actual update
	Vector max_row_id_vec(Value::BIGINT(MAX_ROW_ID));
	Vector row_ids_slice(LogicalType::BIGINT);
	DataChunk updates_slice;
	updates_slice.InitializeEmpty(updates.GetTypes());

	SelectionVector sel_local_update(count), sel_global_update(count);
	auto n_local_update = VectorOperations::GreaterThanEquals(row_ids, max_row_id_vec, nullptr, count,
	                                                          &sel_local_update, &sel_global_update);
	auto n_global_update = count - n_local_update;

	// row id > MAX_ROW_ID? transaction-local storage
	if (n_local_update > 0) {
		updates_slice.Slice(updates, sel_local_update, n_local_update);
		updates_slice.Flatten();
		row_ids_slice.Slice(row_ids, sel_local_update, n_local_update);
		row_ids_slice.Flatten(n_local_update);

		LocalStorage::Get(context, db).Update(*this, row_ids_slice, column_ids, updates_slice);
	}

	// otherwise global storage
	if (n_global_update > 0) {
		auto &transaction = DuckTransaction::Get(context, db);
		transaction.ModifyTable(*this);
		updates_slice.Slice(updates, sel_global_update, n_global_update);
		updates_slice.Flatten();
		row_ids_slice.Slice(row_ids, sel_global_update, n_global_update);
		row_ids_slice.Flatten(n_global_update);

// start Anybase changes
		row_groups->Update(transaction, *this, FlatVector::GetData<row_t>(row_ids_slice), column_ids, updates_slice);
// end Anybase changes
	}
}

void DataTable::UpdateColumn(TableCatalogEntry &table, ClientContext &context, Vector &row_ids,
                             const vector<column_t> &column_path, DataChunk &updates) {
	D_ASSERT(row_ids.GetType().InternalType() == ROW_TYPE);
	D_ASSERT(updates.ColumnCount() == 1);
	updates.Verify();
	if (updates.size() == 0) {
		return;
	}

	if (!IsMainTable()) {
		throw TransactionException(
		    "Transaction conflict: attempting to update table \"%s\" but it has been %s by a different transaction",
		    GetTableName(), TableModification());
	}

	// now perform the actual update
	auto &transaction = DuckTransaction::Get(context, db);

	updates.Flatten();
	row_ids.Flatten(updates.size());
// start Anybase changes
	row_groups->UpdateColumn(transaction, *this, row_ids, column_path, updates);
// end Anybase changes
}

//===--------------------------------------------------------------------===//
// Statistics
//===--------------------------------------------------------------------===//
unique_ptr<BaseStatistics> DataTable::GetStatistics(ClientContext &context, column_t column_id) {
	if (column_id == COLUMN_IDENTIFIER_ROW_ID) {
		return nullptr;
	}
	return row_groups->CopyStats(column_id);
}

void DataTable::SetDistinct(column_t column_id, unique_ptr<DistinctStatistics> distinct_stats) {
	D_ASSERT(column_id != COLUMN_IDENTIFIER_ROW_ID);
	row_groups->SetDistinct(column_id, std::move(distinct_stats));
}

unique_ptr<BlockingSample> DataTable::GetSample() {
	return row_groups->GetSample();
}

//===--------------------------------------------------------------------===//
// Checkpoint
//===--------------------------------------------------------------------===//
unique_ptr<StorageLockKey> DataTable::GetSharedCheckpointLock() {
	return info->checkpoint_lock.GetSharedLock();
}

unique_ptr<StorageLockKey> DataTable::GetCheckpointLock() {
	return info->checkpoint_lock.GetExclusiveLock();
}

void DataTable::Checkpoint(TableDataWriter &writer, Serializer &serializer) {
	// checkpoint each individual row group
	TableStatistics global_stats;
	row_groups->CopyStats(global_stats);
	row_groups->Checkpoint(writer, global_stats);
	// The row group payload data has been written. Now write:
	//   sample
	//   column stats
	//   row-group pointers
	//   table pointer
	//   index data
	writer.FinalizeTable(global_stats, info.get(), serializer);
}

void DataTable::CommitDropColumn(const idx_t column_index) {
	row_groups->CommitDropColumn(column_index);
}

idx_t DataTable::ColumnCount() const {
	return column_definitions.size();
}

idx_t DataTable::GetTotalRows() const {
	return row_groups->GetTotalRows();
}

void DataTable::CommitDropTable() {
	// commit a drop of this table: mark all blocks as modified, so they can be reclaimed later on
	row_groups->CommitDropTable();

	// propagate dropping this table to its indexes: frees all index memory
	info->indexes.Scan([&](Index &index) {
		D_ASSERT(index.IsBound());
		index.Cast<BoundIndex>().CommitDrop();
		return false;
	});
}

//===--------------------------------------------------------------------===//
// Column Segment Info
//===--------------------------------------------------------------------===//
vector<ColumnSegmentInfo> DataTable::GetColumnSegmentInfo() {
	auto lock = GetSharedCheckpointLock();
	return row_groups->GetColumnSegmentInfo();
}

//===--------------------------------------------------------------------===//
// Index Constraint Creation
//===--------------------------------------------------------------------===//
void DataTable::AddIndex(const ColumnList &columns, const vector<LogicalIndex> &column_indexes,
                         const IndexConstraintType type, const IndexStorageInfo &index_info) {
	if (!IsMainTable()) {
		throw TransactionException("Transaction conflict: attempting to add an index to table \"%s\" but it has been "
		                           "%s by a different transaction",
		                           GetTableName(), TableModification());
	}

	// Fetch the column types and create bound column reference expressions.
	vector<column_t> physical_ids;
	vector<unique_ptr<Expression>> expressions;

	for (const auto column_index : column_indexes) {
		auto binding = ColumnBinding(0, physical_ids.size());
		auto &col = columns.GetColumn(column_index);
		auto ref = make_uniq<BoundColumnRefExpression>(col.Name(), col.Type(), binding);
		expressions.push_back(std::move(ref));
		physical_ids.push_back(col.Physical().index);
	}

	// Create an ART around the expressions.
	auto &io_manager = TableIOManager::Get(*this);
	auto art = make_uniq<ART>(index_info.name, type, physical_ids, io_manager, std::move(expressions), db, nullptr,
	                          index_info);
	info->indexes.AddIndex(std::move(art));
}

// start Anybase changes
void DataTable::DidCommitTransaction(const transaction_t commit_id, bool update_all_columns) const {
	info->commit_version_manager.DidCommitTransaction(commit_id);
	if (update_all_columns) {
		row_groups->UpdateColumnVersions(commit_id);
	}
}

idx_t DataTable::GetVersion() const {
	return info->commit_version_manager.GetVersion();
}

idx_t DataTable::GetColumnVersion(const column_t idx) const {
	return row_groups->GetVersion(idx);
}

// Anybase changes
void DataTable::ScanTableSegment(DuckTransaction &transaction, idx_t row_start, idx_t count, vector<StorageIndex> &column_ids,
								 vector<LogicalType> types, const std::function<void(DataChunk &chunk)> &function) {
	if (count == 0) {
		return;
	}
	idx_t end = row_start + count;

	DataChunk chunk;
	chunk.Initialize(Allocator::Get(db), types);

	CreateIndexScanState state;

	InitializeScanWithOffset(transaction, state, column_ids, row_start, row_start + count);
	auto row_start_aligned = state.table_state.row_group->start + state.table_state.vector_index * STANDARD_VECTOR_SIZE;

	// idx_t current_row = row_start;//row_start_aligned;
	idx_t current_row = row_start_aligned;
	while (current_row < end) {
		state.table_state.ScanCommitted(chunk, TableScanType::TABLE_SCAN_COMMITTED_ROWS);
		if (chunk.size() == 0) {
			break;
		}
		idx_t end_row = current_row + chunk.size();
		// start of chunk is current_row
		// end of chunk is end_row
		// figure out if we need to write the entire chunk or just part of it
		idx_t chunk_start = MaxValue<idx_t>(current_row, row_start);
		idx_t chunk_end = MinValue<idx_t>(end_row, end);
		D_ASSERT(chunk_start < chunk_end);
		idx_t chunk_count = chunk_end - chunk_start;
		if (chunk_count != chunk.size()) {
			D_ASSERT(chunk_count <= chunk.size());
			// need to slice the chunk before insert
			idx_t start_in_chunk;
			if (current_row >= row_start) {
				start_in_chunk = 0;
			} else {
				start_in_chunk = row_start - current_row;
			}
			SelectionVector sel(start_in_chunk, chunk_count);
			chunk.Slice(sel, chunk_count);
			chunk.Verify();
		}
		function(chunk);
		chunk.Reset();
		current_row = end_row;
	}
}
// end Anybase changes

} // namespace duckdb
