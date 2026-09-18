//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/column_annotation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

class Serializer;
class Deserializer;

//! The COMMENT and TAGS declared for a column in a SELECT list
struct ColumnAnnotation {
	//! The comment, or NULL if none was declared
	Value comment;
	//! The tags, in declaration order
	InsertionOrderPreservingMap<string> tags;

public:
	//! Renders the annotation as it appears after the column alias, e.g. " COMMENT 'text' TAGS {'k': 'v'}"
	string ToString() const;

	void Serialize(Serializer &serializer) const;
	static shared_ptr<ColumnAnnotation> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
