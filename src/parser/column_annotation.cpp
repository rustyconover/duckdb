#include "duckdb/parser/column_annotation.hpp"

#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/parser/column_definition.hpp"

namespace duckdb {

string ColumnAnnotation::ToString() const {
	return ToString(comment, tags);
}

string ColumnAnnotation::ToString(const Value &comment, const InsertionOrderPreservingMap<string> &tags) {
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

void ColumnAnnotation::ApplyTo(ColumnDefinition &column) const {
	if (!comment.IsNull()) {
		column.SetComment(comment);
	}
	if (!tags.empty()) {
		column.SetTags(tags);
	}
}

} // namespace duckdb
