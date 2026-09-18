#pragma once
#include "duckdb/common/common.hpp"
#include "duckdb/common/pair.hpp"

namespace duckdb {

enum class ColumnAnnotationClauseType : uint8_t { COMMENT, TAGS };

//! A COMMENT or TAGS clause that follows the alias of a select-list entry
struct ColumnAnnotationClause {
	ColumnAnnotationClauseType type = ColumnAnnotationClauseType::COMMENT;
	//! The text of a COMMENT clause
	string comment;
	//! The entries of a TAGS clause, in declaration order
	vector<pair<string, string>> tags;
};
} // namespace duckdb
