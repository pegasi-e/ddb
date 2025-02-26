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
	}
}

CDCWriteState::CDCWriteState(DuckTransaction &transaction_p)
    : transaction(transaction_p) {
}

void CDCWriteState::EmitDelete(DeleteInfo &info) {
	auto &table = info.table;

	auto table_version = table->GetVersion();
	auto &column_definitions = table->Columns();
	auto columnCount = table->ColumnCount();
	auto column_names = vector<const char*>(columnCount);
	auto column_versions = vector<uint64_t>(columnCount);
	for (idx_t i = 0; i < columnCount; i++) {
		column_names[i] = strdup(column_definitions[i].GetName().c_str());
		column_versions[i] = table->GetColumnVersion(i);
	}

	auto ptr = transaction.context.lock();
	auto &config = DBConfig::GetConfig(info.table->db.GetDatabase());
	table->ScanTableSegment(info.base_row, info.count, [&](DataChunk &chunk) {
		auto delete_chunk = make_uniq<DataChunk>();
		delete_chunk->Initialize(*ptr, chunk.GetTypes(), chunk.size());
		delete_chunk->Reference(chunk);

		if (!info.is_consecutive) {
			ManagedSelection sel(info.count);
			auto delete_rows = info.GetRows();
			for (idx_t i = 0; i < info.count; i++) {
				sel.Append(delete_rows[i]);
			}
			delete_chunk->Slice(sel.Selection(), sel.Count());
		}

		delete_chunk->Flatten();

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
	});

	if (columnCount > 0) {
		for (idx_t i = 0; i < columnCount; i++) {
			free((void *) column_names[i]);
		}
	}
}

void handle_update(const DuckTransaction &transaction, UpdateInfo &info, DataTable *&table, const idx_t table_version, vector<const char *> column_names,
	vector<uint64_t> column_versions, const vector<LogicalType> &update_types, const vector<column_t> &column_indexes,
	const idx_t update_column_index, const shared_ptr<ClientContext> &ptr,
	UpdateInfo &adjusted_info, const SelectionVector &sel, const DBConfig &config, DataChunk &chunk) {

	auto current_chunk = make_uniq<DataChunk>();
	auto previous_chunk = make_uniq<DataChunk>();

	// current_chunk->Initialize(*ptr, update_types, chunk.size());//Initialize(*ptr, update_types, chunk.size());
	current_chunk->Initialize(*ptr, update_types, chunk.size());
	previous_chunk->Initialize(*ptr, update_types, chunk.size());

	current_chunk->Append(chunk);
	previous_chunk->Append(chunk);
	// previous_chunk->data[update_column_index].Initialize(true, chunk.size());
	adjusted_info.segment->FetchAndApplyUpdate(&adjusted_info, previous_chunk->data[update_column_index]);
	// for (idx_t i = 0; i < column_indexes.size(); i++) {
	// 	if (column_indexes[i] == update_column_index) {
	// 		// previous_chunk->data[i].Initialize(true, chunk.size());
	// 		adjusted_info.segment->FetchAndApplyUpdate(&adjusted_info, previous_chunk->data[i]);
	// 		break;
	// 	}
	// }
	// // previous_chunk->Reference(chunk);
	// //
	// // // Reference columns that did not change
	// for (auto i = 0; i < column_indexes.size(); i++) {
	// 	if (column_indexes[i] != update_column_index) {
	// 		previous_chunk->data[i].Reference(chunk.data[i]);
	// 	} else {
	// 		previous_chunk->data[i].Initialize(true, chunk.size());
	// 		Printer::Print("Cache post init");
	// 		// VectorOperations::Copy(chunk.data[i], previous_chunk->data[i], chunk.size(), 0, previous_chunk->size());
	// 		// memcpy(previous_chunk->data[i].GetData(), chunk.data[i].GetData(), chunk.size());
	//
	// 		if (adjusted_info.N == STANDARD_VECTOR_SIZE) {
	// 			// special case: update touches ALL tuples of this vector
	// 			// in this case we can just memcpy the data
	// 			// since the layout of the update info is guaranteed to be [0, 1, 2, 3, ...]
	// 			memcpy(previous_chunk->data[i].GetData(), info.tuple_data, sizeof(GetTypeIdSize(update_types[i].InternalType())) * adjusted_info.N);
	// 		} else {
	// 			for (idx_t x = 0; x < adjusted_info.N; x++) {
	// 				previous_chunk->data[i].SetValue(info.tuples[x], info.tuple_data[x]);
	// 				// result_data[current->tuples[i]] = info_data[i];
	// 			}
	// 		}
	//
	//
	//
	// 		// adjusted_info.segment->FetchAndApplyUpdate(&adjusted_info, previous_chunk->data[i]);
	// 		Printer::Print("Cache apply update");
	// 		// previous_chunk->data[i].Initialize(false, chunk.size());
	// 		// break;
	// 		// previous_chunk->data[i].CopyBuffer(chunk.data[i]);
	// 		// memcpy(previous_chunk->data[i].GetData(), chunk.data[i].GetData(), chunk.size());
	// 	}
	// }
	// previous_chunk->SetCapacity(chunk);
	// previous_chunk->SetCardinality(chunk);


	// previous_chunk->Append(chunk);
	// previous_chunk->SetCapacity(chunk);
	// previous_chunk->SetCardinality(chunk);

	// Reference columns that did not change
	// for (auto i = 0; i < column_indexes.size(); i++) {
	// if (column_indexes[i] != update_column_index) {
	// previous_chunk->data[i].Reference(chunk.data[i]);
	// } else {
	// previous_chunk->data[i].Initialize();
	// }
	// }


	current_chunk->Slice(sel, adjusted_info.N);
	previous_chunk->Slice(sel, adjusted_info.N);
	// Printer::Print("Cache post slice");
	// previous_chunk->SetCardinality(adjusted_info.N);
	// current_chunk->SetCardinality(adjusted_info.N);

	current_chunk->Flatten();
	previous_chunk->Flatten();
	// Printer::Print("Cache post flatten");


	// previous_chunk->Verify();

	config.change_data_capture.EmitChange(
		DUCKDB_CDC_EVENT_UPDATE,
		transaction.transaction_id,
		column_indexes.size(),
		table_version,
		&info.column_index,
		table->GetTableName().c_str(),
		column_names.data(),
		column_versions.data(),
		reinterpret_cast<duckdb_data_chunk>(current_chunk.release()),
		reinterpret_cast<duckdb_data_chunk>(previous_chunk.release())
	);
}

void CDCWriteState::EmitUpdate(UpdateInfo &info) {
	auto &table = info.table;
	auto table_types = table->GetTypes();
	auto &column_definitions = table->Columns();
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
	idx_t update_column_index = info.column_index;
	auto did_add_target = false;
	for (idx_t i = 0; i < column_ids.size(); i++) {
		auto column_index = column_ids[i];
		column_names.push_back(strdup(column_definitions[column_index].GetName().c_str()));
		column_versions.push_back(table->GetColumnVersion(column_index));
		update_types.emplace_back(table_types[column_index]);
		column_indexes.push_back(column_index);
		if (column_index == info.column_index) {
			update_column_index = i;
			did_add_target = true;
		}
	}

	if (!did_add_target) {
		update_column_index = column_names.size();
		column_names.push_back(strdup(column_definitions[info.column_index].GetName().c_str()));
		column_versions.push_back(table->GetColumnVersion(info.column_index));
		update_types.emplace_back(table_types[info.column_index]);
		column_indexes.push_back(info.column_index);
	}

	auto ptr = transaction.context.lock();
	// auto firstIndex = info.tuples[0];
	// auto count = info.tuples[info.N - 1] + 1 - firstIndex;
	// auto start_offset = info.vector_index * STANDARD_VECTOR_SIZE + firstIndex;
	//
	// // Create a modified update info
	// UpdateInfo adjusted_info;
	// adjusted_info.N = info.N;
	//
	// // for some reason the unique pointer causes a segmentation fault, so we allocate for ourselves
	// // adjusted_info.tuples = make_uniq_array_uninitialized<sel_t>(info.N).get();
	// adjusted_info.tuples = new sel_t[info.N];
	// memcpy(adjusted_info.tuples, info.tuples, info.N * sizeof(sel_t));
	// adjusted_info.tuple_data = info.tuple_data;
	// adjusted_info.version_number.store(info.version_number.load());
	// adjusted_info.vector_index = info.vector_index;
	// adjusted_info.segment = info.segment;
	//
	// // ManagedSelection sel(info.N);
	// for (idx_t i = 0; i < info.N; i++) {
	// 	adjusted_info.tuples[i] = info.tuples[i] - firstIndex;
	// 	// sel.Append(adjusted_info.tuples[i]);
	// }

	UpdateInfo &adjusted_info = info;
	SelectionVector sel(adjusted_info.tuples);
	auto &config = DBConfig::GetConfig(info.table->db.GetDatabase());

	if (last_table == table->GetTableName() && last_vector_index == info.vector_index && scanned_chunk != nullptr) {
		auto &chunk = *(scanned_chunk.get());
		handle_update(transaction, info, table, table_version, column_names, column_versions, update_types, column_indexes,
		              update_column_index, ptr, adjusted_info, sel, config, chunk);
	} else {
		// table->ScanTableSegment(start_offset, count, column_indexes, update_types, [&](DataChunk &chunk) {
		table->ScanTableSegment(info.vector_index * STANDARD_VECTOR_SIZE, STANDARD_VECTOR_SIZE, column_indexes, update_types, [&](DataChunk &chunk) {
			last_table = table->GetTableName();
			last_vector_index = info.vector_index;
			scanned_chunk = make_uniq<DataChunk>();
			scanned_chunk->InitializeEmpty(update_types);
			// scanned_chunk->Initialize(*ptr, update_types, chunk.size());
			scanned_chunk->Reference(chunk);

			handle_update(transaction, info, table, table_version, column_names, column_versions, update_types, column_indexes,
					  update_column_index, ptr, adjusted_info, sel, config, chunk);

			// auto current_chunk = make_uniq<DataChunk>();
			// auto previous_chunk = make_uniq<DataChunk>();
			//
			// current_chunk->Initialize(*ptr, update_types, chunk.size());//Initialize(*ptr, update_types, chunk.size());
			// previous_chunk->Initialize(*ptr, update_types, chunk.size());//Initialize(*ptr, update_types, chunk.size());
			//
			// current_chunk->Reference(chunk);
			// previous_chunk->Append(chunk);
			// for (idx_t i = 0; i < column_indexes.size(); i++) {
			// 	if (column_indexes[i] == update_column_index) {
			// 		// previous_chunk->data[i].Initialize(true, chunk.size());
			// 		adjusted_info.segment->FetchAndApplyUpdate(&adjusted_info, previous_chunk->data[i]);
			// 		break;
			// 	}
			// }
			// // previous_chunk->Reference(chunk);
			// //
			// // // Reference columns that did not change
			// // for (auto i = 0; i < column_indexes.size(); i++) {
			// // 	if (column_indexes[i] != update_column_index) {
			// // 		previous_chunk->data[i].Reference(chunk.data[i]);
			// // 	} else {
			// // 		previous_chunk->data[i].Initialize(true, chunk.size());
			// // 		Printer::Print("Cache post init");
			// // 		// VectorOperations::Copy(chunk.data[i], previous_chunk->data[i], chunk.size(), 0, previous_chunk->size());
			// // 		// memcpy(previous_chunk->data[i].GetData(), chunk.data[i].GetData(), chunk.size());
			// //
			// // 		if (adjusted_info.N == STANDARD_VECTOR_SIZE) {
			// // 			// special case: update touches ALL tuples of this vector
			// // 			// in this case we can just memcpy the data
			// // 			// since the layout of the update info is guaranteed to be [0, 1, 2, 3, ...]
			// // 			memcpy(previous_chunk->data[i].GetData(), info.tuple_data, sizeof(GetTypeIdSize(update_types[i].InternalType())) * adjusted_info.N);
			// // 		} else {
			// // 			for (idx_t x = 0; x < adjusted_info.N; x++) {
			// // 				previous_chunk->data[i].SetValue(info.tuples[x], info.tuple_data[x]);
			// // 				// result_data[current->tuples[i]] = info_data[i];
			// // 			}
			// // 		}
			// // 		Printer::Print("post apply update");
			// // 		// previous_chunk->data[i].Initialize(false, chunk.size());
			// // 		// break;
			// // 		// previous_chunk->data[i].CopyBuffer(chunk.data[i]);
			// // 		// memcpy(previous_chunk->data[i].GetData(), chunk.data[i].GetData(), chunk.size());
			// // 	}
			// // }
			// // previous_chunk->SetCapacity(chunk);
			// // previous_chunk->SetCardinality(chunk);
			//
			//
			// // previous_chunk->Append(chunk);
			// // previous_chunk->SetCapacity(chunk);
			// // previous_chunk->SetCardinality(chunk);
			//
			// // Reference columns that did not change
			// // for (auto i = 0; i < column_indexes.size(); i++) {
			// 	// if (column_indexes[i] != update_column_index) {
			// 		// previous_chunk->data[i].Reference(chunk.data[i]);
			// 	// } else {
			// 		// previous_chunk->data[i].Initialize();
			// 	// }
			// // }
			//
			//
			// current_chunk->Slice(sel, adjusted_info.N);
			// previous_chunk->Slice(sel, adjusted_info.N);
			// // Printer::Print("post slice");
			// // previous_chunk->SetCardinality(adjusted_info.N);
			// // current_chunk->SetCardinality(adjusted_info.N);
			//
			// current_chunk->Flatten();
			// previous_chunk->Flatten();
			// // Printer::Print("post flatten");
			//
			//
			// // previous_chunk->Verify();
			//
			// config.change_data_capture.EmitChange(
			// 	DUCKDB_CDC_EVENT_UPDATE,
			// 	transaction.transaction_id,
			// 	column_indexes.size(),
			// 	table_version,
			// 	&info.column_index,
			// 	table->GetTableName().c_str(),
			// 	column_names.data(),
			// 	column_versions.data(),
			// 	reinterpret_cast<duckdb_data_chunk>(current_chunk.release()),
			// 	reinterpret_cast<duckdb_data_chunk>(previous_chunk.release())
			// 	);
		});
	}

	// delete[] adjusted_info.tuples;
	if (!column_names.empty()) {
		for (idx_t i = 0; i < column_names.size(); i++) {
			free((void *) column_names[i]);
		}
	}
}

void CDCWriteState::EmitInsert(AppendInfo &info) {
	auto &table = info.table;
	auto table_version = table->GetVersion();

	auto &column_definitions = table->Columns();
	auto columnCount = table->ColumnCount();
	auto column_names = vector<const char*>(columnCount);
	auto column_versions = vector<uint64_t>(columnCount);
	for (idx_t i = 0; i < columnCount; i++) {
		column_names[i] = strdup(column_definitions[i].GetName().c_str());
		column_versions[i] = table->GetColumnVersion(i);
	}
	auto ptr = transaction.context.lock();

	table->ScanTableSegment(info.start_row, info.count, [&](DataChunk &chunk) {
		auto insert_chunk = make_uniq<DataChunk>();
		insert_chunk->Initialize(*ptr, chunk.GetTypes(), chunk.size());
		insert_chunk->Reference(chunk);
		insert_chunk->Flatten();

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
	});

	if (columnCount > 0) {
		for (idx_t i = 0; i < columnCount; i++) {
			free((void *) column_names[i]);
		}
	}
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
