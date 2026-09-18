#include "duckdb/parser/column_annotation.hpp"

#include "duckdb/common/sql_identifier.hpp"

namespace duckdb {

string ColumnAnnotation::ToString() const {
	string result;
	if (!comment.IsNull()) {
		result += " COMMENT " + SQLString::ToString(comment.GetValue<string>());
	}
	if (!tags.empty()) {
		result += " TAGS {";
		bool first = true;
		for (auto &tag : tags) {
			if (!first) {
				result += ", ";
			}
			first = false;
			result += SQLString::ToString(tag.first) + ": " + SQLString::ToString(tag.second);
		}
		result += "}";
	}
	return result;
}

} // namespace duckdb
