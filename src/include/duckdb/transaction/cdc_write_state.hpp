//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/cdc_write_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {
class DuckTransaction;
struct DeleteInfo;
struct UpdateInfo;
struct AppendInfo;

class CDCWriteState {
public:
    explicit CDCWriteState(DuckTransaction &transaction);

public:
    void EmitEntry(UndoFlags type, data_ptr_t data);
    void EmitTransactionEntry(CDC_EVENT_TYPE type);
    void Flush();

private:
    void EmitDelete(DeleteInfo &info);
    void EmitUpdate(UpdateInfo &info);
    void EmitInsert(AppendInfo &info);
    bool CanApplyUpdate(UpdateInfo &info);


private:
    DuckTransaction &transaction;
    unique_ptr<DataChunk> current_update_chunk;
    unique_ptr<DataChunk> previous_update_chunk;
    vector<string> update_column_names;
    vector<uint64_t> column_versions;
    vector<column_t> column_indexes;
    idx_t update_table_version;
    UpdateInfo last_update_info;
};

} // namespace duckdb
