//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/client_properties.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <utility>

#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

enum class ArrowOffsetSize : uint8_t { REGULAR, LARGE };

//! A set of properties from the client context that can be used to interpret the query result
struct ClientProperties {
	ClientProperties(string time_zone_p, ArrowOffsetSize arrow_offset_size_p, bool arrow_use_list_view_p,
	                 bool produce_arrow_string_view_p, bool lossless_conversion,
	                 optional_ptr<ClientContext> client_context)
	    : time_zone(std::move(time_zone_p)), arrow_offset_size(arrow_offset_size_p),
	      arrow_use_list_view(arrow_use_list_view_p), produce_arrow_string_view(produce_arrow_string_view_p),
	      arrow_lossless_conversion(lossless_conversion), client_context(client_context) {
	}
	// start Anybase changes
	ClientProperties() {};
	// end Anybase changes
	string time_zone = "UTC";
	ArrowOffsetSize arrow_offset_size = ArrowOffsetSize::REGULAR;
	bool arrow_use_list_view = false;
	bool produce_arrow_string_view = false;
	bool arrow_lossless_conversion = false;
	optional_ptr<ClientContext> client_context;
};
} // namespace duckdb
