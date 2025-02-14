//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/cdc_write_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/undo_buffer.hpp"
#include "duckdb/common/vector_size.hpp"

namespace duckdb {
class DuckTransaction;

struct DeleteInfo;
struct UpdateInfo;
struct AppendInfo;

class CDCWriteState {
public:
    explicit CDCWriteState(DuckTransaction &transaction, optional_ptr<StorageCommitState> commit_state);

public:
    void EmitEntry(UndoFlags type, data_ptr_t data);

private:
    void EmitDelete(DeleteInfo &info);
    void EmitUpdate(UpdateInfo &info);
    void EmitInsert(AppendInfo &info);

private:
    DuckTransaction &transaction;
    optional_ptr<StorageCommitState> commit_state;
};

} // namespace duckdb
