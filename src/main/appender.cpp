#include "duckdb/main/appender.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/decimal_cast_operators.hpp"
#include "duckdb/common/operator/string_cast.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/common/types/conflict_manager.hpp"

namespace duckdb {

BaseAppender::BaseAppender(Allocator &allocator, AppenderType type_p)
    : allocator(allocator), column(0), appender_type(type_p) {
}

BaseAppender::BaseAppender(Allocator &allocator_p, vector<LogicalType> types_p, AppenderType type_p)
    : allocator(allocator_p), types(std::move(types_p)), collection(make_uniq<ColumnDataCollection>(allocator, types)),
      column(0), appender_type(type_p) {
	InitializeChunk();
}

BaseAppender::~BaseAppender() {
}

void BaseAppender::Destructor() {
	if (Exception::UncaughtException()) {
		return;
	}
	// flush any remaining chunks, but only if we are not cleaning up the appender as part of an exception stack unwind
	// wrapped in a try/catch because Close() can throw if the table was dropped in the meantime
	try {
		Close();
	} catch (...) { // NOLINT
	}
}

InternalAppender::InternalAppender(ClientContext &context_p, TableCatalogEntry &table_p)
    : BaseAppender(Allocator::DefaultAllocator(), table_p.GetTypes(), AppenderType::PHYSICAL), context(context_p),
      table(table_p) {
}

InternalAppender::~InternalAppender() {
	Destructor();
}

Appender::Appender(Connection &con, const string &schema_name, const string &table_name)
    : BaseAppender(Allocator::DefaultAllocator(), AppenderType::LOGICAL), context(con.context) {
	description = con.TableInfo(schema_name, table_name);
	if (!description) {
		// table could not be found
		throw CatalogException(StringUtil::Format("Table \"%s.%s\" could not be found", schema_name, table_name));
	}
	for (auto &column : description->columns) {
		types.push_back(column.Type());
	}
	InitializeChunk();
	collection = make_uniq<ColumnDataCollection>(allocator, types);
}

Appender::Appender(Connection &con, const string &table_name) : Appender(con, DEFAULT_SCHEMA, table_name) {
}

Appender::~Appender() {
	Destructor();
}

void BaseAppender::InitializeChunk() {
	chunk.Initialize(allocator, types);
}

void BaseAppender::BeginRow() {
}

void BaseAppender::EndRow() {
	// check that all rows have been appended to
	if (column != chunk.ColumnCount()) {
		throw InvalidInputException("Call to EndRow before all rows have been appended to!");
	}
	column = 0;
	chunk.SetCardinality(chunk.size() + 1);
	if (chunk.size() >= STANDARD_VECTOR_SIZE) {
		FlushChunk();
	}
}

template <class SRC, class DST>
void BaseAppender::AppendValueInternal(Vector &col, SRC input) {
	FlatVector::GetData<DST>(col)[chunk.size()] = Cast::Operation<SRC, DST>(input);
}

template <class SRC, class DST>
void BaseAppender::AppendDecimalValueInternal(Vector &col, SRC input) {
	switch (appender_type) {
	case AppenderType::LOGICAL: {
		auto &type = col.GetType();
		D_ASSERT(type.id() == LogicalTypeId::DECIMAL);
		auto width = DecimalType::GetWidth(type);
		auto scale = DecimalType::GetScale(type);
		CastParameters parameters;
		TryCastToDecimal::Operation<SRC, DST>(input, FlatVector::GetData<DST>(col)[chunk.size()], parameters, width,
		                                      scale);
		return;
	}
	case AppenderType::PHYSICAL: {
		AppendValueInternal<SRC, DST>(col, input);
		return;
	}
	default:
		throw InternalException("Type not implemented for AppenderType");
	}
}

template <class T>
void BaseAppender::AppendValueInternal(T input) {
	if (column >= types.size()) {
		throw InvalidInputException("Too many appends for chunk!");
	}
	auto &col = chunk.data[column];
	switch (col.GetType().id()) {
	case LogicalTypeId::BOOLEAN:
		AppendValueInternal<T, bool>(col, input);
		break;
	case LogicalTypeId::UTINYINT:
		AppendValueInternal<T, uint8_t>(col, input);
		break;
	case LogicalTypeId::TINYINT:
		AppendValueInternal<T, int8_t>(col, input);
		break;
	case LogicalTypeId::USMALLINT:
		AppendValueInternal<T, uint16_t>(col, input);
		break;
	case LogicalTypeId::SMALLINT:
		AppendValueInternal<T, int16_t>(col, input);
		break;
	case LogicalTypeId::UINTEGER:
		AppendValueInternal<T, uint32_t>(col, input);
		break;
	case LogicalTypeId::INTEGER:
		AppendValueInternal<T, int32_t>(col, input);
		break;
	case LogicalTypeId::UBIGINT:
		AppendValueInternal<T, uint64_t>(col, input);
		break;
	case LogicalTypeId::BIGINT:
		AppendValueInternal<T, int64_t>(col, input);
		break;
	case LogicalTypeId::HUGEINT:
		AppendValueInternal<T, hugeint_t>(col, input);
		break;
	case LogicalTypeId::UHUGEINT:
		AppendValueInternal<T, uhugeint_t>(col, input);
		break;
	case LogicalTypeId::FLOAT:
		AppendValueInternal<T, float>(col, input);
		break;
	case LogicalTypeId::DOUBLE:
		AppendValueInternal<T, double>(col, input);
		break;
	case LogicalTypeId::DECIMAL:
		switch (col.GetType().InternalType()) {
		case PhysicalType::INT16:
			AppendDecimalValueInternal<T, int16_t>(col, input);
			break;
		case PhysicalType::INT32:
			AppendDecimalValueInternal<T, int32_t>(col, input);
			break;
		case PhysicalType::INT64:
			AppendDecimalValueInternal<T, int64_t>(col, input);
			break;
		case PhysicalType::INT128:
			AppendDecimalValueInternal<T, hugeint_t>(col, input);
			break;
		default:
			throw InternalException("Internal type not recognized for Decimal");
		}
		break;
	case LogicalTypeId::DATE:
		AppendValueInternal<T, date_t>(col, input);
		break;
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		AppendValueInternal<T, timestamp_t>(col, input);
		break;
	case LogicalTypeId::TIME:
		AppendValueInternal<T, dtime_t>(col, input);
		break;
	case LogicalTypeId::TIME_TZ:
		AppendValueInternal<T, dtime_tz_t>(col, input);
		break;
	case LogicalTypeId::INTERVAL:
		AppendValueInternal<T, interval_t>(col, input);
		break;
	case LogicalTypeId::VARCHAR:
		FlatVector::GetData<string_t>(col)[chunk.size()] = StringCast::Operation<T>(input, col);
		break;
	default:
		AppendValue(Value::CreateValue<T>(input));
		return;
	}
	column++;
}

template <>
void BaseAppender::Append(bool value) {
	AppendValueInternal<bool>(value);
}

template <>
void BaseAppender::Append(int8_t value) {
	AppendValueInternal<int8_t>(value);
}

template <>
void BaseAppender::Append(int16_t value) {
	AppendValueInternal<int16_t>(value);
}

template <>
void BaseAppender::Append(int32_t value) {
	AppendValueInternal<int32_t>(value);
}

template <>
void BaseAppender::Append(int64_t value) {
	AppendValueInternal<int64_t>(value);
}

template <>
void BaseAppender::Append(hugeint_t value) {
	AppendValueInternal<hugeint_t>(value);
}

template <>
void BaseAppender::Append(uhugeint_t value) {
	AppendValueInternal<uhugeint_t>(value);
}

template <>
void BaseAppender::Append(uint8_t value) {
	AppendValueInternal<uint8_t>(value);
}

template <>
void BaseAppender::Append(uint16_t value) {
	AppendValueInternal<uint16_t>(value);
}

template <>
void BaseAppender::Append(uint32_t value) {
	AppendValueInternal<uint32_t>(value);
}

template <>
void BaseAppender::Append(uint64_t value) {
	AppendValueInternal<uint64_t>(value);
}

template <>
void BaseAppender::Append(const char *value) {
	AppendValueInternal<string_t>(string_t(value));
}

void BaseAppender::Append(const char *value, uint32_t length) {
	AppendValueInternal<string_t>(string_t(value, length));
}

template <>
void BaseAppender::Append(string_t value) {
	AppendValueInternal<string_t>(value);
}

template <>
void BaseAppender::Append(float value) {
	AppendValueInternal<float>(value);
}

template <>
void BaseAppender::Append(double value) {
	AppendValueInternal<double>(value);
}

template <>
void BaseAppender::Append(date_t value) {
	AppendValueInternal<date_t>(value);
}

template <>
void BaseAppender::Append(dtime_t value) {
	AppendValueInternal<dtime_t>(value);
}

template <>
void BaseAppender::Append(timestamp_t value) {
	AppendValueInternal<timestamp_t>(value);
}

template <>
void BaseAppender::Append(interval_t value) {
	AppendValueInternal<interval_t>(value);
}

template <>
void BaseAppender::Append(Value value) { // NOLINT: template shtuff
	if (column >= chunk.ColumnCount()) {
		throw InvalidInputException("Too many appends for chunk!");
	}
	AppendValue(value);
}

template <>
void BaseAppender::Append(std::nullptr_t value) {
	if (column >= chunk.ColumnCount()) {
		throw InvalidInputException("Too many appends for chunk!");
	}
	auto &col = chunk.data[column++];
	FlatVector::SetNull(col, chunk.size(), true);
}

void BaseAppender::AppendValue(const Value &value) {
	chunk.SetValue(column, chunk.size(), value);
	column++;
}

void BaseAppender::AppendDataChunk(DataChunk &chunk) {
	auto chunk_types = chunk.GetTypes();
	if (chunk_types != types) {
		for (idx_t i = 0; i < chunk.ColumnCount(); i++) {
			if (chunk.data[i].GetType() != types[i]) {
				throw InvalidInputException("Type mismatch in Append DataChunk and the types required for appender, "
				                            "expected %s but got %s for column %d",
				                            types[i].ToString(), chunk.data[i].GetType().ToString(), i + 1);
			}
		}
	}
	collection->Append(chunk);
	if (collection->Count() >= FLUSH_COUNT) {
		Flush();
	}
}

//void InternalAppender::ResolveDefaults(TableCatalogEntry &table, DataChunk &chunk, DataChunk &chunk_with_defaults) {
////	chunk.Flatten();
//	auto g_state = new GlobalSinkState();
//	auto l_state = new LocalSinkState();
//	LocalAppendState l_a_state;
////	OperatorSinkInput sink_input {*g_state, *l_state, interrupt_state};
//	delete g_state;
//	delete l_state;
//
////	for (auto &chunk : collection->Chunks()) {
////		table.GetStorage().LocalAppend(l_a_state, table, context, chunk, true);
////	}
//
//}

//void BaseAppender::OnConflictHandling() {
//	idx_t updated_tuples = 0;
//	this.
//}
//

void BaseAppender::FlushChunk() {
	if (chunk.size() == 0) {
		return;
	}



	collection->Append(chunk);
	chunk.Reset();
	if (collection->Count() >= FLUSH_COUNT) {
		Flush();
	}
}

void BaseAppender::Flush() {
	// check that all vectors have the same length before appending
	if (column != 0) {
		throw InvalidInputException("Failed to Flush appender: incomplete append to row!");
	}

	FlushChunk();
	if (collection->Count() == 0) {
		return;
	}
	FlushInternal(*collection);

	collection->Reset();
	column = 0;
}

void Appender::FlushInternal(ColumnDataCollection &collection) {
	context->Append(*description, collection);
}

void InternalAppender::FlushInternal(ColumnDataCollection &collection) {
	auto binder = Binder::CreateBinder(context);
	auto bound_constraints = binder->BindConstraints(table);
	table.GetStorage().LocalAppend(table, context, collection, bound_constraints);
//	table.GetStorage().LocalAppend(table, context, collection, bound_constraints);
}



static void CombineExistingAndInsertTuples(DataChunk &result, DataChunk &scan_chunk, DataChunk &input_chunk,
                                           ClientContext &client) {

	// We have not scanned the initial table, so we can just duplicate the initial chunk
	result.Initialize(client, input_chunk.GetTypes());
	result.Reference(input_chunk);
	result.SetCardinality(input_chunk);
}

template <bool GLOBAL>
static void RegisterUpdatedRows(const Vector &row_ids, idx_t count) {
	// Insert all rows, if any of the rows has already been updated before, we throw an error
	auto data = FlatVector::GetData<row_t>(row_ids);

	// The rowids in the transaction-local ART aren't final yet so we have to separately keep track of the two sets of
	// rowids
	// Rows that have been updated by a DO UPDATE conflict
	unordered_set<row_t> updated_global_rows;
	// Rows in the transaction-local storage that have been updated by a DO UPDATE conflict
	unordered_set<row_t> updated_local_rows;
	unordered_set<row_t> &updated_rows = GLOBAL ? updated_global_rows :updated_local_rows;
	for (idx_t i = 0; i < count; i++) {
		auto result = updated_rows.insert(data[i]);
		if (result.second == false) {
			throw InvalidInputException(
			    "ON CONFLICT DO UPDATE can not update the same row twice in the same command. Ensure that no rows "
			    "proposed for insertion within the same command have duplicate constrained values");
		}
	}
}

template <bool GLOBAL>
static idx_t PerformOnConflictAction(ClientContext &context, DataChunk &chunk, TableCatalogEntry &table,
                                     Vector &row_ids, vector<PhysicalIndex>& set_columns) {

	DataChunk update_chunk;
	CreateUpdateChunk(context, chunk, table, row_ids, update_chunk);

	auto &data_table = table.GetStorage();
	// Perform the update, using the results of the SET expressions
	if (GLOBAL) {
		data_table.Update(table, context, row_ids, set_columns, update_chunk);
	} else {
		auto &local_storage = LocalStorage::Get(context, data_table.db);
		// Perform the update, using the results of the SET expressions
		local_storage.Update(data_table, row_ids, set_columns, update_chunk);
	}
	return update_chunk.size();
}

template <bool GLOBAL>
static idx_t HandleInsertConflicts(TableCatalogEntry &table, ClientContext &context, DataChunk &insert_chunk,  DataTable &data_table) {
	vector<PhysicalIndex> set_columns;
	// The column ids to apply the ON CONFLICT on
	unordered_set<column_t> conflict_target;

	data_table.info->indexes.Scan([&](Index &index) {
		if (index.IsPrimary()) {
			conflict_target = index.column_id_set;
			return true;
		}
		return false;
	});

	for (idx_t i = 0; i < data_table.column_definitions.size(); i++) {
		auto &col = data_table.column_definitions[i];
		column_t oid = col.Oid();
		if (conflict_target.find(oid) == conflict_target.end()) {
			set_columns.push_back(col.Physical());
		}
	}

	auto &local_storage = LocalStorage::Get(context, data_table.db);
	insert_chunk.Print();
	// We either want to do nothing, or perform an update when conflicts arise
	ConflictInfo conflict_info(conflict_target);
	ConflictManager conflict_manager(VerifyExistenceType::APPEND, insert_chunk.size(), &conflict_info);
	if (GLOBAL) {
		data_table.VerifyAppendConstraints(table, context, insert_chunk, &conflict_manager);
	} else {
		DataTable::VerifyUniqueIndexes(local_storage.GetIndexes(data_table), context, insert_chunk,
		                               &conflict_manager);
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

	// Splice the Input chunk and the fetched chunk together
	CombineExistingAndInsertTuples(combined_chunk, scan_chunk, conflict_chunk, context);

	RegisterUpdatedRows<GLOBAL>(row_ids, combined_chunk.size());

	idx_t updated_tuples = PerformOnConflictAction<GLOBAL>(context, combined_chunk, table, row_ids, set_columns);

	// Remove the conflicting tuples from the insert chunk
	SelectionVector sel_vec(insert_chunk.size());
	idx_t new_size =
	    SelectionVector::Inverted(conflicts.Selection(), sel_vec, conflicts.Count(), insert_chunk.size());
	insert_chunk.Slice(sel_vec, new_size);
	insert_chunk.SetCardinality(new_size);
	return updated_tuples;
}

template <bool GLOBAL>
static idx_t HandleInsertConflicts(TableCatalogEntry &table, ClientContext &context, DataChunk &insert_chunk, DataTable &data_table, const vector<unique_ptr<BoundConstraint>> &bound_constraints) {
	vector<PhysicalIndex> set_columns;
	// The column ids to apply the ON CONFLICT on
	unordered_set<column_t> conflict_target;

	auto storage_info = data_table.GetStorageInfo();
	for (idx_t i = 0; i < storage_info.index_info.size(); i++) {
		if (storage_info.index_info[i].is_primary) {
			conflict_target = storage_info.index_info[i].column_set;
			break;
		}
	}

	if (conflict_target.empty()) {
		//throw!
	}

	for (idx_t i = 0; i < data_table.Columns().size(); i++) {
		auto &col = data_table.Columns()[i];
		column_t oid = col.Oid();
		if (conflict_target.find(oid) == conflict_target.end()) {
			set_columns.push_back(col.Physical());
		}
	}

	auto &local_storage = LocalStorage::Get(context, data_table.db);
	insert_chunk.Print();
	// We either want to do nothing, or perform an update when conflicts arise
	ConflictInfo conflict_info(conflict_target);
	ConflictManager conflict_manager(VerifyExistenceType::APPEND, insert_chunk.size(), &conflict_info);
	if (GLOBAL) {
		auto constraint_state = data_table.InitializeConstraintState(table, bound_constraints);
		data_table.VerifyAppendConstraints(*constraint_state, context, insert_chunk, &conflict_manager);
	} else {
		DataTable::VerifyUniqueIndexes(local_storage.GetIndexes(data_table), context, insert_chunk,
		                               &conflict_manager);
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

	// Splice the Input chunk and the fetched chunk together
	CombineExistingAndInsertTuples(combined_chunk, scan_chunk, conflict_chunk, context);

	RegisterUpdatedRows<GLOBAL>(row_ids, combined_chunk.size());

	idx_t updated_tuples = PerformOnConflictAction<GLOBAL>(context, combined_chunk, table, row_ids, set_columns);

	// Remove the conflicting tuples from the insert chunk
	SelectionVector sel_vec(insert_chunk.size());
	idx_t new_size =
	    SelectionVector::Inverted(conflicts.Selection(), sel_vec, conflicts.Count(), insert_chunk.size());
	insert_chunk.Slice(sel_vec, new_size);
	insert_chunk.SetCardinality(new_size);
	return updated_tuples;
}

idx_t InternalAppender::OnConflictHandling(TableCatalogEntry &table, ClientContext &context, DataChunk &insert_chunk, DataTable &data_table, const vector<unique_ptr<BoundConstraint>> &bound_constraints) {
	// Check whether any conflicts arise, and if they all meet the conflict_target + condition
	// If that's not the case - We throw the first error
	idx_t updated_tuples = 0;
	updated_tuples += HandleInsertConflicts<true>(table, context, insert_chunk, data_table, bound_constraints);
	// Also check the transaction-local storage+ART so we can detect conflicts within this transaction
	updated_tuples += HandleInsertConflicts<false>(table, context, insert_chunk, data_table, bound_constraints);

	return updated_tuples;
}

void BaseAppender::Close() {
	if (column == 0 || column == types.size()) {
		Flush();
	}
}

} // namespace duckdb
