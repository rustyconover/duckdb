//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_column_metadata.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/parser/column_annotation.hpp"

namespace duckdb {

//! The comment and tags of a file's columns, stored as JSON in its key-value metadata
struct ParquetColumnMetadata {
	//! The key-value metadata key
	static constexpr const char *KEY = "duckdb:column_metadata";

	//! Renders the comment and tags of the named columns as JSON, or returns an empty string if no column has any
	static string Write(const vector<string> &names, const vector<ColumnAnnotation> &annotations);
	//! Parses JSON written by Write into the comment and tags of each named column, ignoring malformed entries
	static unordered_map<string, ColumnAnnotation> Read(const string &json);
};

} // namespace duckdb
