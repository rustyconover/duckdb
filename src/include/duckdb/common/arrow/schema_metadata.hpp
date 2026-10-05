//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/arrow/schema_metadata.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/arrow/arrow_type_extension.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {
class ArrowSchemaMetadata {
public:
	//! Constructor used to read a metadata schema, used when importing an arrow object
	explicit ArrowSchemaMetadata(const char *metadata);
	//! Constructor used to create a metadata schema, used when exporting an arrow object
	ArrowSchemaMetadata();
	//! Adds an option to the metadata
	void AddOption(const string &key, const string &value);
	//! Gets an option from the metadata, returns an empty string if it does not exist.
	string GetOption(const string &key) const;
	//! Transforms metadata to a char*, used when creating an arrow object
	unsafe_unique_array<char> SerializeMetadata() const;
	//! If the arrow extension is set
	bool HasExtension() const;
	//! The column comment stored in the metadata, or NULL if there is none
	Value GetComment() const;
	//! The column tags stored in the metadata
	InsertionOrderPreservingMap<string> GetTags() const;
	//! Stores a column comment and tags in the metadata
	void AddCommentAndTags(const Value &comment, const InsertionOrderPreservingMap<string> &tags);

	ArrowExtensionMetadata GetExtensionInfo(string format);
	//! Get the extension name if set, otherwise returns empty
	string GetExtensionName() const;
	//! Key for encode of the extension type name
	static constexpr const char *ARROW_EXTENSION_NAME = "ARROW:extension:name";
	//! Key for encode of the metadata key
	static constexpr const char *ARROW_METADATA_KEY = "ARROW:extension:metadata";
	//! Creates the metadata based on an extension name
	static ArrowSchemaMetadata ArrowCanonicalType(const string &extension_name);
	//! Creates the metadata based on an extension name
	static ArrowSchemaMetadata NonCanonicalType(const string &type_name, const string &vendor_name);
	//! Key of a column comment
	static constexpr const char *DUCKDB_COMMENT_KEY = "duckdb:comment";
	//! Prefix of the keys of column tags, followed by the tag key
	static constexpr const char *DUCKDB_TAG_KEY_PREFIX = "duckdb:tag:";
	//! Key of a column comment written by other producers, read when there is no duckdb:comment
	static constexpr const char *COMMENT_KEY = "comment";

private:
	//! The metadata, in the order of its keys
	InsertionOrderPreservingMap<string, string, unordered_map<string, idx_t>> schema_metadata_map;
	//! The extension metadata, parsed into a flat key -> value map, currently only used for internal types in
	//! arrow.opaque
	unordered_map<string, string> extension_metadata_map;
};
} // namespace duckdb
