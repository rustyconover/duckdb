#include "parquet_column_metadata.hpp"

#include "yyjson.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

string ParquetColumnMetadata::Write(const vector<string> &names, const vector<ColumnAnnotation> &annotations) {
	D_ASSERT(names.size() == annotations.size());
	auto doc = yyjson_mut_doc_new(nullptr);
	auto root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);
	bool any = false;
	for (idx_t i = 0; i < names.size(); i++) {
		auto &annotation = annotations[i];
		if (annotation.comment.IsNull() && annotation.tags.empty()) {
			continue;
		}
		any = true;
		auto column = yyjson_mut_obj(doc);
		if (!annotation.comment.IsNull()) {
			yyjson_mut_obj_add_strcpy(doc, column, "comment", annotation.comment.ToString().c_str());
		}
		if (!annotation.tags.empty()) {
			auto tags = yyjson_mut_obj(doc);
			for (auto &tag : annotation.tags) {
				yyjson_mut_obj_put(tags, yyjson_mut_strncpy(doc, tag.first.c_str(), tag.first.size()),
				                   yyjson_mut_strncpy(doc, tag.second.c_str(), tag.second.size()));
			}
			yyjson_mut_obj_add_val(doc, column, "tags", tags);
		}
		yyjson_mut_obj_put(root, yyjson_mut_strncpy(doc, names[i].c_str(), names[i].size()), column);
	}
	string result;
	if (any) {
		size_t len;
		auto json = yyjson_mut_write(doc, 0, &len);
		if (json) {
			result = string(json, len);
			free(json);
		}
	}
	yyjson_mut_doc_free(doc);
	return result;
}

unordered_map<string, ColumnAnnotation> ParquetColumnMetadata::Read(const string &json) {
	unordered_map<string, ColumnAnnotation> result;
	auto doc = yyjson_read(json.c_str(), json.size(), 0);
	if (!doc) {
		return result;
	}
	auto root = yyjson_doc_get_root(doc);
	if (yyjson_is_obj(root)) {
		size_t idx, max;
		yyjson_val *key, *column;
		yyjson_obj_foreach(root, idx, max, key, column) {
			if (!yyjson_is_obj(column)) {
				continue;
			}
			ColumnAnnotation annotation;
			auto comment = yyjson_obj_get(column, "comment");
			if (yyjson_is_str(comment)) {
				annotation.comment = Value(string(yyjson_get_str(comment), yyjson_get_len(comment)));
			}
			auto tags = yyjson_obj_get(column, "tags");
			if (yyjson_is_obj(tags)) {
				size_t tag_idx, tag_max;
				yyjson_val *tag_key, *tag_value;
				yyjson_obj_foreach(tags, tag_idx, tag_max, tag_key, tag_value) {
					if (yyjson_is_str(tag_value)) {
						annotation.tags[string(yyjson_get_str(tag_key), yyjson_get_len(tag_key))] =
						    string(yyjson_get_str(tag_value), yyjson_get_len(tag_value));
					}
				}
			}
			result[string(yyjson_get_str(key), yyjson_get_len(key))] = std::move(annotation);
		}
	}
	yyjson_doc_free(doc);
	return result;
}

} // namespace duckdb
