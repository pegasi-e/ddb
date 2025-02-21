#include "duckdb/transaction/cdc_write_state.hpp"

#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/chunk_info.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_version_manager.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/transaction/append_info.hpp"
#include "duckdb/transaction/delete_info.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/update_info.hpp"
#include "duckdb/function/change_data_capature_function.hpp"

namespace duckdb {

void ChangeDataCapture::EmitChange(
			const cdc_event_type type,
			const idx_t transactionId,
			const idx_t column_count,
			const idx_t table_version,
			idx_t *updated_column_index,
			const char *table_name,
			const char **column_names,
			idx_t *column_versions,
			const duckdb_data_chunk values,
			const duckdb_data_chunk previous_values) const {

	if (function != nullptr) {

		function(type, transactionId, column_count, table_version, updated_column_index, table_name, column_names, column_versions, values, previous_values);

		if (column_count > 0 && column_names != nullptr) {
			for (idx_t i = 0; i < column_count; i++) {
				free((void *) column_names[i]);
			}
		}
	}
}

CDCWriteState::CDCWriteState(DuckTransaction &transaction_p)
    : transaction(transaction_p) {
}

void CDCWriteState::EmitDelete(DeleteInfo &info) {
	auto &table = info.table;
	vector<column_t> column_ids;
	for (idx_t i = 0; i < table->ColumnCount(); i++) {
		column_ids.emplace_back(i);
	}

	auto delete_chunk = make_uniq<DataChunk>();
	auto ptr = transaction.context.lock();
	delete_chunk->Initialize(*ptr, table->GetTypes());

	Vector row_ids(LogicalType::ROW_TYPE);
	if (info.is_consecutive) {
		for (idx_t i = 0; i < info.count; i++) {
			row_ids.SetValue(i, UnsafeNumericCast<int64_t>(info.base_row + i));
		}
	} else {
		auto delete_rows = info.GetRows();
		for (idx_t i = 0; i < info.count; i++) {
			row_ids.SetValue(i, UnsafeNumericCast<int64_t>(info.base_row) + delete_rows[i]);
		}
	}
	auto cfs = ColumnFetchState();

	auto table_version = table->GetVersion();
	auto &column_definitions = table->Columns();
	auto columnCount = table->ColumnCount();
	auto column_names = vector<const char*>(columnCount);
	auto column_versions = vector<uint64_t>(columnCount);
	for (idx_t i = 0; i < columnCount; i++) {
		column_names[i] = strdup(column_definitions[i].GetName().c_str());
		column_versions[i] = table->GetColumnVersion(i);
	}

	table->Fetch(transaction, *delete_chunk, column_ids, row_ids, info.count, cfs, false);

	auto &config = DBConfig::GetConfig(info.table->db.GetDatabase());
	config.change_data_capture.EmitChange(
		DUCKDB_CDC_EVENT_DELETE,
		transaction.transaction_id,
		columnCount,
		table_version,
		nullptr,
		table->GetTableName().c_str(),
		column_names.data(),
		column_versions.data(),
		nullptr,
		reinterpret_cast<duckdb_data_chunk>(delete_chunk.release())
		);
}

void CDCWriteState::EmitUpdate(UpdateInfo &info) {
	auto &table = info.table;
	auto table_types = table->GetTypes();
	auto &column_definitions = table->Columns();
	idx_t start = info.column->start + info.vector_index * STANDARD_VECTOR_SIZE;
	auto table_version = table->GetVersion();

	vector<column_t> column_ids;
	if (transaction.involved_columns.find(table->GetTableName()) != transaction.involved_columns.end()) {
		auto column_map = transaction.involved_columns[table->GetTableName()];
		if (column_map.find(info.column_index) != column_map.end()) {
			column_ids = column_map[info.column_index];
		}
	}

	vector<const char*> column_names;
	vector<uint64_t> column_versions;
	vector<LogicalType> update_types;
	vector<column_t> column_indexes;
	auto did_add_target = false;
	for (idx_t i = 0; i < column_ids.size(); i++) {
		auto column_index = column_ids[i];
		column_names.push_back(strdup(column_definitions[column_index].GetName().c_str()));
		column_versions.push_back(table->GetColumnVersion(column_index));
		update_types.emplace_back(table_types[column_index]);
		column_indexes.push_back(column_index);
		if (column_index == info.column_index) {
			did_add_target = true;
		}
	}

	if (!did_add_target) {
		column_names.push_back(strdup(column_definitions[info.column_index].GetName().c_str()));
		column_versions.push_back(table->GetColumnVersion(info.column_index));
		update_types.emplace_back(table_types[info.column_index]);
		column_indexes.push_back(info.column_index);
	}

	auto update_chunk = make_uniq<DataChunk>();
	auto ptr = transaction.context.lock();
	update_chunk->Initialize(*ptr, update_types);

	auto previous_chunk = make_uniq<DataChunk>();
	previous_chunk->Initialize(*ptr, update_types);

	auto cfs = ColumnFetchState();
	Vector row_ids(LogicalType::ROW_TYPE);

	for (idx_t i = 0; i < info.N; i++) {
		row_ids.SetValue(i, UnsafeNumericCast<int64_t>(start + info.tuples[i]));
	}

	table->Fetch(transaction, *update_chunk, column_indexes, row_ids, info.N, cfs);
	table->Fetch(transaction, *previous_chunk, column_indexes, row_ids, info.N, cfs, false);

	auto &config = DBConfig::GetConfig(info.table->db.GetDatabase());

	config.change_data_capture.EmitChange(
		DUCKDB_CDC_EVENT_UPDATE,
		transaction.transaction_id,
		column_indexes.size(),
		table_version,
		&info.column_index,
		table->GetTableName().c_str(),
		column_names.data(),
		column_versions.data(),
		reinterpret_cast<duckdb_data_chunk>(update_chunk.release()),
		reinterpret_cast<duckdb_data_chunk>(previous_chunk.release())
		);
}

void CDCWriteState::EmitInsert(AppendInfo &info) {
	auto &table = info.table;
	auto table_version = table->GetVersion();
	auto table_types = table->GetTypes();

	auto insert_chunk = make_uniq<DataChunk>();
	auto ptr = transaction.context.lock();
	insert_chunk->Initialize(*ptr, table->GetTypes(), info.count);

	Vector row_ids(LogicalType::ROW_TYPE, info.count);
	for (idx_t i = 0; i < info.count; i++) {
		row_ids.SetValue(i, UnsafeNumericCast<int64_t>(info.start_row + i));
	}

	vector<column_t> column_ids;
	for (idx_t i = 0; i < table->ColumnCount(); i++) {
		column_ids.emplace_back(i);
	}

	auto cfs = ColumnFetchState();

	auto &column_definitions = table->Columns();
	auto columnCount = table->ColumnCount();
	auto column_names = vector<const char*>(columnCount);
	auto column_versions = vector<uint64_t>(columnCount);
	for (idx_t i = 0; i < columnCount; i++) {
		column_names[i] = strdup(column_definitions[i].GetName().c_str());
		column_versions[i] = table->GetColumnVersion(i);
	}

	table->Fetch(transaction, *insert_chunk, column_ids, row_ids, info.count, cfs);

	auto &config = DBConfig::GetConfig(info.table->db.GetDatabase());
	config.change_data_capture.EmitChange(
		DUCKDB_CDC_EVENT_INSERT,
		transaction.transaction_id,
		columnCount,
		table_version,
		nullptr,
		table->GetTableName().c_str(),
		column_names.data(),
		column_versions.data(),
		reinterpret_cast<duckdb_data_chunk>(insert_chunk.release()),
		nullptr
		);
}

void CDCWriteState::EmitEntry(UndoFlags type, data_ptr_t data) {
	switch (type) {
		case UndoFlags::CATALOG_ENTRY: {
			//Not supported
			break;
		}
		case UndoFlags::INSERT_TUPLE: {
			// append:
			auto info = reinterpret_cast<AppendInfo *>(data);
			if (!info->table->IsTemporary()) {
				EmitInsert(*info);
			}
			break;
		}
		case UndoFlags::DELETE_TUPLE: {
			// deletion:
			auto info = reinterpret_cast<DeleteInfo *>(data);
			if (!info->table->IsTemporary()) {
				EmitDelete(*info);
			}
			break;
		}
		case UndoFlags::UPDATE_TUPLE: {
			// update:
			auto info = reinterpret_cast<UpdateInfo *>(data);
			if (!info->segment->column_data.GetTableInfo().IsTemporary()) {
				EmitUpdate(*info);
			}
			break;
		}
		case UndoFlags::SEQUENCE_VALUE: {
			//Not Supported
			break;
		}
		default:
			throw InternalException("UndoBuffer - don't know how to commit this type!");
	}
}

void CDCWriteState::EmitTransactionEntry(CDC_EVENT_TYPE type){
	if (transaction.context.expired()) {
		return;
	}

	auto context = transaction.context.lock();
	auto &config = DBConfig::GetConfig(*context);
	config.change_data_capture.EmitChange(
		type,
		transaction.transaction_id,
		0,
		0,
		nullptr,
		nullptr,
		nullptr,
		nullptr,
		nullptr,
		nullptr
		);
}
} // namespace duckdb
