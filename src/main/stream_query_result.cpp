#include "duckdb/main/stream_query_result.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/common/box_renderer.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

StreamQueryResult::StreamQueryResult(StatementType statement_type, StatementProperties properties,
                                     vector<LogicalType> types, vector<string> names,
                                     ClientProperties client_properties, shared_ptr<BufferedData> data)
    : QueryResult(QueryResultType::STREAM_RESULT, statement_type, std::move(properties), std::move(types),
                  std::move(names), std::move(client_properties)),
      buffered_data(std::move(data)) {
	context = buffered_data->GetContext();
}

StreamQueryResult::~StreamQueryResult() {
}

string StreamQueryResult::ToString() {
	string result;
	if (success) {
		result = HeaderToString();
		result += "[[STREAM RESULT]]";
	} else {
		result = GetError() + "\n";
	}
	return result;
}

unique_ptr<ClientContextLock> StreamQueryResult::LockContext() {
	if (!context) {
		string error_str = "Attempting to execute an unsuccessful or closed pending query result";
		if (HasError()) {
			error_str += StringUtil::Format("\nError: %s", GetError());
		}
		throw InvalidInputException(error_str);
	}
	return context->LockContext();
}

unique_ptr<DataChunk> StreamQueryResult::FetchInternal(ClientContextLock &lock) {
    duckdb::Printer::PrintF("FetchInternal\n");
	bool invalidate_query = true;
	unique_ptr<DataChunk> chunk;
	try {
		// fetch the chunk and return it
		auto res = buffered_data->ReplenishBuffer(*this, lock);
                    duckdb::Printer::PrintF("FetchInternal1\n");
		if (res == PendingExecutionResult::EXECUTION_ERROR) {
                      duckdb::Printer::PrintF("FetchInternal2\n");
			return chunk;
		}
    duckdb::Printer::PrintF("FetchInternal3\n");
                chunk = buffered_data->Scan();
                    duckdb::Printer::PrintF("FetchInternal4\n");
		if (!chunk || chunk->ColumnCount() == 0 || chunk->size() == 0) {
                      duckdb::Printer::PrintF("FetchInternal5\n");
			context->CleanupInternal(lock, this);
			chunk = nullptr;
		}
                    duckdb::Printer::PrintF("FetchInternal6\n");
		return chunk;
	} catch (std::exception &ex) {
              duckdb::Printer::PrintF("FetchInternal7\n");
		ErrorData error(ex);
		if (!Exception::InvalidatesTransaction(error.Type())) {
			// standard exceptions do not invalidate the current transaction
			invalidate_query = false;
		} else if (Exception::InvalidatesDatabase(error.Type())) {
			// fatal exceptions invalidate the entire database
			auto &config = context->config;
			if (!config.query_verification_enabled) {
				auto &db_instance = DatabaseInstance::GetDatabase(*context);
				ValidChecker::Invalidate(db_instance, error.RawMessage());
			}
		}
		context->ProcessError(error, context->GetCurrentQuery());
		SetError(std::move(error));
                    duckdb::Printer::PrintF("FetchInternal8\n");
	} catch (...) { // LCOV_EXCL_START
		SetError(ErrorData("Unhandled exception in FetchInternal"));
	} // LCOV_EXCL_STOP
	context->CleanupInternal(lock, this, invalidate_query);
            duckdb::Printer::PrintF("FetchInternal9\n");
	return nullptr;
}

unique_ptr<DataChunk> StreamQueryResult::FetchRaw() {
  duckdb::Printer::PrintF("FetchRaw\n");
	unique_ptr<DataChunk> chunk;
	{
		auto lock = LockContext();
		CheckExecutableInternal(*lock);
		chunk = FetchInternal(*lock);
	}
	if (!chunk || chunk->ColumnCount() == 0 || chunk->size() == 0) {
		Close();
		return nullptr;
	}
	return chunk;
}

unique_ptr<MaterializedQueryResult> StreamQueryResult::Materialize() {
	if (HasError() || !context) {
		return make_uniq<MaterializedQueryResult>(GetErrorObject());
	}
	auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);

	ColumnDataAppendState append_state;
	collection->InitializeAppend(append_state);
	while (true) {
		auto chunk = Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		collection->Append(append_state, *chunk);
	}
	auto result =
	    make_uniq<MaterializedQueryResult>(statement_type, properties, names, std::move(collection), client_properties);
	if (HasError()) {
		return make_uniq<MaterializedQueryResult>(GetErrorObject());
	}
	return result;
}

bool StreamQueryResult::IsOpenInternal(ClientContextLock &lock) {
	bool invalidated = !success || !context;
	if (!invalidated) {
		invalidated = !context->IsActiveResult(lock, *this);
	}
	return !invalidated;
}

void StreamQueryResult::CheckExecutableInternal(ClientContextLock &lock) {
	if (!IsOpenInternal(lock)) {
		string error_str = "Attempting to execute an unsuccessful or closed pending query result";
		if (HasError()) {
			error_str += StringUtil::Format("\nError: %s", GetError());
		}
		throw InvalidInputException(error_str);
	}
}

bool StreamQueryResult::IsOpen() {
	if (!success || !context) {
		return false;
	}
	auto lock = LockContext();
	return IsOpenInternal(*lock);
}

void StreamQueryResult::Close() {
	buffered_data->Close();
	context.reset();
}

} // namespace duckdb
