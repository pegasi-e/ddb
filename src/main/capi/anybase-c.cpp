#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/transaction/timestamp_manager.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/common/helper.hpp"

using duckdb::ArrowConverter;
using duckdb::ArrowAppender;
using duckdb::ArrowResultWrapper;
using duckdb::Connection;
using duckdb::DataChunk;
using duckdb::LogicalType;
using duckdb::ErrorData;
using duckdb::ArrowTableFunction;
using duckdb::Appender;
using duckdb::AppenderWrapper;

uint64_t duckdb_get_hlc_timestamp() {
	return duckdb::TimestampManager::GetHLCTimestamp();
}

void duckdb_set_hlc_timestamp(uint64_t ts) {
	duckdb::TimestampManager::SetHLCTimestamp(ts);
}

uint64_t duckdb_get_snapshot_id(duckdb_connection connection)
{
  Connection *conn = reinterpret_cast<Connection *>(connection);
  return conn->GetSnapshotId();
}

uint64_t duckdb_checkpoint_and_get_snapshot_id(duckdb_connection connection)
{
  Connection *conn = reinterpret_cast<Connection *>(connection);
  return conn->CheckpointAndGetSnapshotId();
}

duckdb_state duckdb_create_arrow_appender(duckdb_result *result, duckdb_arrow_appender *out_arrow_appender) {
	if (!result || !out_arrow_appender) {
		return DuckDBError;
	}

	if (!result->internal_data) {
		return DuckDBError;
	}

	auto &result_data = *(reinterpret_cast<duckdb::DuckDBResultData *>(result->internal_data));
	if (result_data.result_set_type == duckdb::CAPIResultSetType::CAPI_RESULT_TYPE_DEPRECATED) {
		return DuckDBError;
	}
	if (result_data.result->type != duckdb::QueryResultType::MATERIALIZED_RESULT) {
		// This API is only supported for materialized query results
		return DuckDBError;
	}

	auto options = result_data.result->client_properties;
	options.uuid_as_binary_array = true;
	auto extension_type_cast = duckdb::ArrowTypeExtensionData::GetExtensionTypes(
		*result_data.result->client_properties.client_context, result_data.result->types);
	auto &materialized = reinterpret_cast<duckdb::MaterializedQueryResult &>(*result_data.result);
	auto &collection = materialized.Collection();
	auto chunk_count = collection.ChunkCount();

	auto appender = new ArrowAppender(collection.Types(), chunk_count * STANDARD_VECTOR_SIZE, options, extension_type_cast);
	*out_arrow_appender = reinterpret_cast<duckdb_arrow_appender>(appender);

	return DuckDBSuccess;
}

duckdb_state duckdb_arrow_appender_destroy(duckdb_arrow_appender *arrow_appender) {
	if (!arrow_appender || !*arrow_appender) {
		return DuckDBError;
	}
	auto appender = reinterpret_cast<ArrowAppender *>(*arrow_appender);
	if (appender) {
		delete appender;
	}
	*arrow_appender = nullptr;
	return DuckDBSuccess;
}

duckdb_state duckdb_arrow_appender_append_chunk(duckdb_arrow_appender arrow_appender, duckdb_data_chunk chunk) {
	if (!arrow_appender || !chunk) {
		return DuckDBError;
	}
	auto appender = reinterpret_cast<ArrowAppender *>(arrow_appender);
	auto dchunk = reinterpret_cast<duckdb::DataChunk *>(chunk);
	appender->Append(*dchunk, 0, dchunk->size(), dchunk->size());

	return DuckDBSuccess;
}

duckdb_state duckdb_arrow_appender_finalize(duckdb_arrow_appender arrow_appender, duckdb_arrow_array *out_array) {
	if (!arrow_appender || !out_array) {
		return DuckDBError;
	}
	auto appender = reinterpret_cast<ArrowAppender *>(arrow_appender);
	auto *p_array = reinterpret_cast<ArrowArray *>(*out_array);
	*p_array = appender->Finalize();

	return DuckDBSuccess;
}

duckdb_state duckdb_result_to_arrow(duckdb_result *result, duckdb_arrow_array *out_array) {
	if (!out_array) {
		return DuckDBSuccess;
	}

	if (!result->internal_data) {
		return DuckDBError;
	}
	auto &result_data = *(reinterpret_cast<duckdb::DuckDBResultData *>(result->internal_data));
	if (result_data.result_set_type == duckdb::CAPIResultSetType::CAPI_RESULT_TYPE_DEPRECATED) {
		return DuckDBError;
	}
	if (result_data.result->type != duckdb::QueryResultType::MATERIALIZED_RESULT) {
		// This API is only supported for materialized query results
		return DuckDBError;
	}
	result_data.result_set_type = duckdb::CAPIResultSetType::CAPI_RESULT_TYPE_MATERIALIZED;
	auto &materialized = reinterpret_cast<duckdb::MaterializedQueryResult &>(*result_data.result);
	auto &collection = materialized.Collection();
	auto options = materialized.client_properties;
	options.uuid_as_binary_array = true;

	auto chunk_count = collection.ChunkCount();
	auto extension_type_cast = duckdb::ArrowTypeExtensionData::GetExtensionTypes(
		*result_data.result->client_properties.client_context, result_data.result->types);
	ArrowAppender appender(collection.Types(), chunk_count * STANDARD_VECTOR_SIZE, options, extension_type_cast);

	for (idx_t i = 0; i < chunk_count; i++) {
		auto chunk = duckdb::unique_ptr<duckdb::DataChunk>();
		chunk->Initialize(duckdb::Allocator::DefaultAllocator(), collection.Types());
		collection.FetchChunk(i, *chunk);
		appender.Append(*chunk, 0, chunk->size(), chunk->size());
	}

	auto *p_array = reinterpret_cast<ArrowArray *>(*out_array);
	*p_array = appender.Finalize();

	return DuckDBSuccess;
}

void duckdb_result_chunk_arrow_array(duckdb_result result, duckdb_data_chunk chunk, duckdb_arrow_array *out_array) {
	if (!out_array) {
		return;
	}
	auto dchunk = reinterpret_cast<duckdb::DataChunk *>(chunk);
	auto &result_data = *(reinterpret_cast<duckdb::DuckDBResultData *>(result.internal_data));
	auto extension_type_cast = duckdb::ArrowTypeExtensionData::GetExtensionTypes(
		*result_data.result->client_properties.client_context, result_data.result->types);
	auto options = result_data.result->client_properties;
	options.uuid_as_binary_array = true;

	ArrowConverter::ToArrowArray(*dchunk, reinterpret_cast<ArrowArray *>(*out_array),
								 options, extension_type_cast);
}

duckdb_state duckdb_data_chunks_to_arrow_array(duckdb_result result, duckdb_data_chunk *chunks, idx_t number_of_chunks, duckdb_arrow_array *out_array) {
	if (!chunks || number_of_chunks == 0 || !out_array)  {
		return DuckDBSuccess;
	}

	auto &result_data = *(reinterpret_cast<duckdb::DuckDBResultData *>(result.internal_data));
	auto options = result_data.result->client_properties;
	options.uuid_as_binary_array = true;
	auto chunk_count = number_of_chunks;
	auto first_chunk = reinterpret_cast<duckdb::DataChunk *>(chunks[0]);
	auto types = first_chunk->GetTypes();
	auto extension_type_cast = duckdb::ArrowTypeExtensionData::GetExtensionTypes(
		*result_data.result->client_properties.client_context, result_data.result->types);
	ArrowAppender appender(types, chunk_count * STANDARD_VECTOR_SIZE, options, extension_type_cast);
	for (idx_t i = 0; i < chunk_count; i++) {
		auto chunk = reinterpret_cast<duckdb::DataChunk *>(chunks[i]);
		appender.Append(*chunk, 0, chunk->size(), chunk->size());
	}

	auto *p_array = reinterpret_cast<ArrowArray *>(*out_array);
	*p_array = appender.Finalize();

	return DuckDBSuccess;
}

duckdb_state duckdb_data_chunk_column_to_arrow_array(duckdb_connection connection, duckdb_data_chunk *chunks, idx_t number_of_chunks, idx_t column_index, duckdb_arrow_array *out_array) {
	if (!chunks || number_of_chunks == 0 || !out_array)  {
		return DuckDBSuccess;
	}

	auto options = ((Connection *)connection)->context->GetClientProperties();
	auto chunk_count = number_of_chunks;
	auto first_chunk = reinterpret_cast<duckdb::DataChunk *>(chunks[0]);
	auto type = first_chunk->GetTypes()[column_index];
	if (type == LogicalType::UUID) {
		options.uuid_as_binary_array = true;
	}
	auto types = duckdb::vector<duckdb::LogicalType>{type};
	std::unordered_map<idx_t, const duckdb::shared_ptr<duckdb::ArrowTypeExtensionData>> extension_type_cast;
	ArrowAppender appender(types, chunk_count * STANDARD_VECTOR_SIZE, options, extension_type_cast);
	for (idx_t i = 0; i < chunk_count; i++) {
		auto chunk = reinterpret_cast<duckdb::DataChunk *>(chunks[i]);
		appender.Append(*chunk, 0, column_index, 0, chunk->size(), chunk->size());
	}

	auto *p_array = reinterpret_cast<ArrowArray *>(*out_array);
	*p_array = appender.Finalize();

	return DuckDBSuccess;
}

duckdb_data_chunk duckdb_create_data_chunk_copy(duckdb_data_chunk *chunk) {
	if (!chunk) {
		return nullptr;
	}
	
	auto dchunk = reinterpret_cast<duckdb::DataChunk *>(*chunk);

	auto new_chunk = new duckdb::DataChunk();
	new_chunk->Initialize(duckdb::Allocator::DefaultAllocator(), dchunk->GetTypes());

	dchunk->Copy(*new_chunk);

	return reinterpret_cast<duckdb_data_chunk>(new_chunk);
}

idx_t duckdb_get_table_version(const duckdb_connection connection, const char *schema, const char *table, char **error) {
	auto *ddbConnection = reinterpret_cast<Connection *>(connection);

	try {
		return ddbConnection->context->GetTableVersion(schema, table);
	} catch (std::exception &ex) {
		if (error) {
			ErrorData parsed_error(ex);
			*error = strdup(parsed_error.Message().c_str());
		}
		return 0;
	} catch (...) { // LCOV_EXCL_START
		if (error) {
			*error = strdup("Unknown error");
		}
		return 0;
	} // LCOV_EXCL_STOP
}

idx_t duckdb_get_column_version(const duckdb_connection connection, const char *schema, const char *table, const char *column, char **error) {
	auto *ddbConnection = reinterpret_cast<Connection *>(connection);

	try {
		return ddbConnection->context->GetColumnVersion(schema, table, column);
	} catch (std::exception &ex) {
		if (error) {
			ErrorData parsed_error(ex);
			*error = strdup(parsed_error.Message().c_str());
		}
		return 0;
	} catch (...) { // LCOV_EXCL_START
		if (error) {
			*error = strdup("Unknown error");
		}
		return 0;
	} // LCOV_EXCL_STOP
}

duckdb_state duckdb_begin_transaction(const duckdb_connection connection, const int64_t micro_seconds, const uint64_t sequence, char **error) {
	const auto *ddbConnection = reinterpret_cast<Connection *>(connection);

	try {
		ddbConnection->context->BeginTransaction(duckdb::timestamp_t(micro_seconds), sequence);
		return DuckDBSuccess;
	} catch (std::exception &ex) {
		if (error) {
			ErrorData parsed_error(ex);
			*error = strdup(parsed_error.Message().c_str());
		}
	} catch (...) { // LCOV_EXCL_START
		if (error) {
			*error = strdup("Unknown error");
		}
	} // LCOV_EXCL_STOP

	return DuckDBError;
}

idx_t duckdb_estimated_row_count(const duckdb_connection connection, const char *catalog, const char *schema, const char *table, char **error) {
	auto *ddbConnection = reinterpret_cast<Connection *>(connection);

	try {
		return ddbConnection->context->GetTotalRows(catalog, schema, table);
	} catch (std::exception &ex) {
		if (error) {
			ErrorData parsed_error(ex);
			*error = strdup(parsed_error.Message().c_str());
		}
		return 0;
	} catch (...) { // LCOV_EXCL_START
		if (error) {
			*error = strdup("Unknown error");
		}
		return 0;
	} // LCOV_EXCL_STOP
}

void duckdb_set_cdc_callback(duckdb_database db, duckdb_change_data_capture_callback_t function) {
	auto wrapper = reinterpret_cast<duckdb::DatabaseWrapper *>(db);
	auto &config = duckdb::DBConfig::GetConfig(*wrapper->database->instance);
	config.change_data_capture.function = function;
}

duckdb_error_data duckdb_append_arrow(duckdb_connection connection, duckdb_appender appender, struct ArrowArray *arrow_array, struct ArrowSchema *schema) {
	if (!connection || !schema || !arrow_array || !appender) {
		return duckdb_create_error_data(DUCKDB_ERROR_INVALID_INPUT,
										"Invalid argument(s) to duckdb_append_arrow");
	}

	const auto ddbConnection = reinterpret_cast<Connection *>(connection);
	auto arrow_schema = duckdb::make_uniq<duckdb::ArrowTableSchema>();
	try {
		duckdb::ArrowTableFunction::PopulateArrowTableSchema(duckdb::DBConfig::GetConfig(*ddbConnection->context), *arrow_schema, *schema);
	} catch (const duckdb::Exception &ex) {
		return duckdb_create_error_data(DUCKDB_ERROR_INVALID_INPUT, ex.what());
	} catch (const std::exception &ex) {
		return duckdb_create_error_data(DUCKDB_ERROR_INVALID_INPUT, ex.what());
	} catch (...) {
		return duckdb_create_error_data(DUCKDB_ERROR_INVALID_INPUT, "Unknown error occurred during conversion");
	}

	auto &types = arrow_schema->GetTypes();
	auto &arrow_types = arrow_schema->GetColumns();
	auto *appender_wrapper = reinterpret_cast<AppenderWrapper *>(appender);
	auto &appender_instance = appender_wrapper->appender;

	auto dchunk = duckdb::make_uniq<duckdb::DataChunk>();
	dchunk->Initialize(*ddbConnection->context, types, duckdb::NumericCast<idx_t>(arrow_array->length));
	dchunk->SetCardinality(duckdb::NumericCast<idx_t>(arrow_array->length));

	for (idx_t i = 0; i < dchunk->ColumnCount(); i++) {
		auto &parent_array = *arrow_array;
		auto &array = parent_array.children[i];
		auto arrow_type = arrow_types.at(i);
		auto array_physical_type = arrow_type->GetPhysicalType();
		auto array_state = duckdb::make_uniq<duckdb::ArrowArrayScanState>(*ddbConnection->context);
		// We need to make sure that our chunk will hold the ownership
		array_state->owned_data = duckdb::make_shared_ptr<duckdb::ArrowArrayWrapper>();
		array_state->owned_data->arrow_array = *arrow_array;
		// We set it to nullptr to effectively transfer the ownership
		arrow_array->release = nullptr;

		try {
			switch (array_physical_type) {
			case duckdb::ArrowArrayPhysicalType::DICTIONARY_ENCODED:
				duckdb::ArrowToDuckDBConversion::ColumnArrowToDuckDBDictionary(dchunk->data[i], *array, 0, *array_state,
				                                                               dchunk->size(), *arrow_type);
				break;
			case duckdb::ArrowArrayPhysicalType::RUN_END_ENCODED:
				duckdb::ArrowToDuckDBConversion::ColumnArrowToDuckDBRunEndEncoded(
				    dchunk->data[i], *array, 0, *array_state, dchunk->size(), *arrow_type);
				break;
			case duckdb::ArrowArrayPhysicalType::DEFAULT:
				duckdb::ArrowToDuckDBConversion::SetValidityMask(dchunk->data[i], *array, 0, dchunk->size(),
				                                                 parent_array.offset, -1);

				duckdb::ArrowToDuckDBConversion::ColumnArrowToDuckDB(dchunk->data[i], *array, 0, *array_state,
				                                                     dchunk->size(), *arrow_type);
				break;
			default:
				return duckdb_create_error_data(DUCKDB_ERROR_NOT_IMPLEMENTED,
				                                "Only Default Physical Types are currently supported");
			}
		} catch (const duckdb::Exception &ex) {
			return duckdb_create_error_data(DUCKDB_ERROR_INVALID_INPUT, ex.what());
		} catch (const std::exception &ex) {
			return duckdb_create_error_data(DUCKDB_ERROR_INVALID_INPUT, ex.what());
		} catch (...) {
			return duckdb_create_error_data(DUCKDB_ERROR_INVALID_INPUT, "Unknown error occurred during conversion");
		}
	}

	DataChunk slice;
	slice.InitializeEmpty(types);
	const idx_t total_size = dchunk->size();

	for (idx_t offset = 0; offset < total_size; offset += STANDARD_VECTOR_SIZE) {
		idx_t count = duckdb::MinValue<idx_t>(STANDARD_VECTOR_SIZE, total_size - offset);
		slice.Reference(*dchunk);
		slice.Slice(offset, count);
		try {
			appender_instance->AppendDataChunk(slice);
		} catch (std::exception &ex) {
			return duckdb_create_error_data(DUCKDB_ERROR_INVALID_INPUT, ex.what());
		} catch (...) { // LCOV_EXCL_START
			return duckdb_create_error_data(DUCKDB_ERROR_INVALID_INPUT, "Unknown appender error");
		} // LCOV_EXCL_STOP
	}

	return nullptr;
}

