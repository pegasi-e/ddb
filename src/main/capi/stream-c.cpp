#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/printer.hpp"
duckdb_data_chunk duckdb_stream_fetch_chunk(duckdb_result result) {
	if (!result.internal_data) {
		return nullptr;
	}
	auto &result_data = *((duckdb::DuckDBResultData *)result.internal_data);
	if (result_data.result->type != duckdb::QueryResultType::STREAM_RESULT) {
		// We can only fetch from a StreamQueryResult
		return nullptr;
	}
	return duckdb_fetch_chunk(result);
}

duckdb_data_chunk duckdb_fetch_chunk(duckdb_result result) {
  duckdb::Printer::PrintF("duckdb_fetch_chunk1\n");
	if (!result.internal_data) {
		return nullptr;
	}
          duckdb::Printer::PrintF("duckdb_fetch_chunk2\n");
          auto &result_data = *(reinterpret_cast<duckdb::DuckDBResultData *>(result.internal_data));
          duckdb::Printer::PrintF("duckdb_fetch_chunk3\n");
	if (result_data.result_set_type == duckdb::CAPIResultSetType::CAPI_RESULT_TYPE_DEPRECATED) {
		return nullptr;
	}
          duckdb::Printer::PrintF("duckdb_fetch_chunk4a\n");
	result_data.result_set_type = duckdb::CAPIResultSetType::CAPI_RESULT_TYPE_STREAMING;
	auto &result_instance = (duckdb::StreamQueryResult &)*result_data.result;
        if (result_data.result == nullptr)
          {
            duckdb::Printer::PrintF("duckdb_fetch_chunk : null ptr\n");
          }
        else
          {
            duckdb::Printer::PrintF("duckdb_fetch_chunk : not null ptr\n");
          }
          duckdb::Printer::PrintF("duckdb_fetch_chunk5\n");
	// FetchRaw ? Do we care about flattening them?
	try {
            duckdb::Printer::PrintF("duckdb_fetch_chunk6\n");
            //auto chunk = result_instance.FetchRaw();
            auto chunk = ((duckdb::StreamQueryResult *)(result_data.result))->FetchRaw();
                  duckdb::Printer::PrintF("duckdb_fetch_chunk7\n");
		return reinterpret_cast<duckdb_data_chunk>(chunk.release());
	} catch (std::exception &e) {
            duckdb::Printer::PrintF("duckdb_fetch_chunk8\n");
		return nullptr;
	}
}
