#include "catch.hpp"

#include "arrow/arrow_test_helper.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/schema_metadata.hpp"
#include "duckdb/common/identifier.hpp"

using namespace duckdb;

namespace {

//! Exports the schema of a query result to Arrow
class ExportedSchema {
public:
	explicit ExportedSchema(QueryResult &result) {
		ArrowConverter::ToArrowSchema(&schema, result.GetTypes(), IdentifiersToStrings(result.GetNames()),
		                              result.client_properties);
	}
	~ExportedSchema() {
		if (schema.release) {
			schema.release(&schema);
		}
	}

	ArrowSchemaMetadata Metadata(idx_t col_idx) {
		return ArrowSchemaMetadata(schema.children[col_idx]->metadata);
	}
	bool HasMetadata(idx_t col_idx) {
		return schema.children[col_idx]->metadata != nullptr;
	}

	ArrowSchema schema;
};

//! An Arrow scan over a query result, whose schema carries the result's column metadata
unique_ptr<ArrowTestFactory> ResultFactory(Connection &con, const string &query) {
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	auto types = result->GetTypes();
	auto names = IdentifiersToStrings(result->GetNames());
	auto properties = result->client_properties;
	return make_uniq<ArrowTestFactory>(types, names, std::move(result), std::move(properties), *con.context);
}

} // namespace

TEST_CASE("Arrow export writes column comments and tags as field metadata", "[arrow]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE orders (id INTEGER, email VARCHAR TAGS {'pii': 'email'}, "
	                          "amount DECIMAL(10, 2) COMMENT 'Order total' TAGS {'unit': 'USD', 'scale': '2'})"));

	auto result = con.Query("SELECT id, email, amount, amount / 1.2 AS net COMMENT 'Before VAT' TAGS {'unit': 'USD'}, "
	                        "id + 1 AS computed FROM orders");
	REQUIRE_NO_FAIL(*result);
	ExportedSchema exported(*result);
	REQUIRE(exported.schema.n_children == 5);

	REQUIRE(!exported.HasMetadata(0));
	REQUIRE(exported.Metadata(1).GetComment().IsNull());
	REQUIRE(exported.Metadata(1).GetOption("duckdb:tag:pii") == "email");

	auto amount = exported.Metadata(2);
	REQUIRE(amount.GetOption("duckdb:comment") == "Order total");
	REQUIRE(amount.GetOption("duckdb:tag:unit") == "USD");
	REQUIRE(amount.GetOption("duckdb:tag:scale") == "2");
	auto amount_tags = amount.GetTags();
	REQUIRE(amount_tags.size() == 2);
	// tags keep their declaration order
	REQUIRE(amount_tags.begin()->first == "unit");

	REQUIRE(exported.Metadata(3).GetOption("duckdb:comment") == "Before VAT");
	REQUIRE(exported.Metadata(3).GetOption("duckdb:tag:unit") == "USD");
	REQUIRE(!exported.HasMetadata(4));
}

TEST_CASE("Arrow export of column metadata can be disabled", "[arrow]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET arrow_output_column_metadata = false"));
	auto result = con.Query("SELECT 42 AS a COMMENT 'answer' TAGS {'k': 'v'}");
	REQUIRE_NO_FAIL(*result);
	ExportedSchema exported(*result);
	REQUIRE(!exported.HasMetadata(0));
}

TEST_CASE("Arrow export merges column metadata with extension type metadata", "[arrow]") {
	DuckDB db(nullptr);
	Connection con(db);
	// UUID is exported as the arrow.uuid extension type with lossless conversion
	REQUIRE_NO_FAIL(con.Query("SET arrow_lossless_conversion = true"));
	auto result = con.Query("SELECT gen_random_uuid() AS u COMMENT 'identifier' TAGS {'k': 'v'}");
	REQUIRE_NO_FAIL(*result);
	ExportedSchema exported(*result);
	auto metadata = exported.Metadata(0);
	REQUIRE(metadata.GetOption(ArrowSchemaMetadata::ARROW_EXTENSION_NAME) == "arrow.uuid");
	REQUIRE(metadata.GetOption("duckdb:comment") == "identifier");
	REQUIRE(metadata.GetOption("duckdb:tag:k") == "v");
}

TEST_CASE("Arrow export of column metadata from a prepared statement", "[arrow]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto prepared = con.Prepare("SELECT $1::INTEGER AS a COMMENT 'parameter'");
	REQUIRE(!prepared->HasError());
	auto result = prepared->Execute(42);
	REQUIRE_NO_FAIL(*result);
	ExportedSchema exported(*result);
	REQUIRE(exported.Metadata(0).GetOption("duckdb:comment") == "parameter");

	REQUIRE_NO_FAIL(con.Query("PREPARE p AS SELECT 1 AS b TAGS {'k': 'v'}"));
	auto executed = con.Query("EXECUTE p");
	REQUIRE_NO_FAIL(*executed);
	ExportedSchema exported_execute(*executed);
	REQUIRE(exported_execute.Metadata(0).GetOption("duckdb:tag:k") == "v");
}

TEST_CASE("Arrow scan reads column comments and tags from field metadata", "[arrow]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto factory = ResultFactory(con, "SELECT 1 AS id, 'a@b.c' AS email TAGS {'pii': 'email'}, "
	                                  "12.5 AS amount COMMENT 'Order total' TAGS {'unit': 'USD'}");
	auto relation = con.TableFunction("arrow_scan", {}, {}, ArrowTestHelper::ConstructArrowScan(*factory));

	// DESCRIBE of the scan reports the metadata, which passes through queries over it
	auto describe = relation->Query(Identifier("arrow_orders"),
	                                "SELECT column_name, extra FROM (DESCRIBE SELECT * FROM arrow_orders)");
	REQUIRE(CHECK_COLUMN(describe, 0, {"id", "email", "amount"}));
	REQUIRE(CHECK_COLUMN(
	    describe, 1,
	    {Value(), "{\"tags\":{\"pii\":\"email\"}}", "{\"comment\":\"Order total\",\"tags\":{\"unit\":\"USD\"}}"}));

	// and CREATE TABLE AS stores it; the test factory does not push down projections, so all columns are read
	auto created = relation->Query(Identifier("arrow_orders"), "CREATE TABLE copied AS SELECT * FROM arrow_orders");
	REQUIRE_NO_FAIL(*created);
	auto columns = con.Query("SELECT column_name, comment, tags::VARCHAR FROM duckdb_columns() "
	                         "WHERE table_name = 'copied' ORDER BY column_index");
	REQUIRE(CHECK_COLUMN(columns, 0, {"id", "email", "amount"}));
	REQUIRE(CHECK_COLUMN(columns, 1, {Value(), Value(), "Order total"}));
	REQUIRE(CHECK_COLUMN(columns, 2, {"{}", "{pii=email}", "{unit=USD}"}));
}

TEST_CASE("Arrow scan reads a plain comment key written by other producers", "[arrow]") {
	DuckDB db(nullptr);
	Connection con(db);

	ArrowSchemaMetadata metadata;
	metadata.AddOption("comment", "from another producer");
	metadata.AddOption("duckdb:tag:", "ignored");
	metadata.AddOption("other", "ignored");
	auto serialized = metadata.SerializeMetadata();
	ArrowSchemaMetadata parsed(serialized.get());
	REQUIRE(parsed.GetComment() == Value("from another producer"));
	REQUIRE(parsed.GetTags().empty());

	// duckdb:comment takes precedence over comment
	metadata.AddOption("duckdb:comment", "from duckdb");
	serialized = metadata.SerializeMetadata();
	ArrowSchemaMetadata parsed_both(serialized.get());
	REQUIRE(parsed_both.GetComment() == Value("from duckdb"));
}
