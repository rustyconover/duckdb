#include "catch.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/table_filter_set.hpp"
#include "test_helpers.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

using namespace duckdb;

// Dummy TableInOutFunction that:
// - sums all INTEGER values in each row
// - only emits 1 row per call to ThrottlingSum::Function, caching the remainder
// - during flushing of caching operators still emits only 1 row sum per call, meaning that multiple flushes are
// required to correctly process this operator
struct ThrottlingSum {
	struct ThrottlingSumLocalData : public LocalTableFunctionState {
		ThrottlingSumLocalData() {
		}
		duckdb::vector<int> row_sums;
		idx_t current_idx = 0;
	};

	static duckdb::unique_ptr<GlobalTableFunctionState> ThrottlingSumGlobalInit(ClientContext &context,
	                                                                            TableFunctionInitInput &input) {
		return make_uniq<GlobalTableFunctionState>();
	}

	static duckdb::unique_ptr<LocalTableFunctionState> ThrottlingSumLocalInit(ExecutionContext &context,
	                                                                          TableFunctionInitInput &input,
	                                                                          GlobalTableFunctionState *global_state) {
		return make_uniq<ThrottlingSumLocalData>();
	}

	static duckdb::unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                             duckdb::vector<LogicalType> &return_types,
	                                             duckdb::vector<Identifier> &names) {
		return_types.emplace_back(LogicalType::INTEGER);
		names.emplace_back("total");
		return make_uniq<TableFunctionData>();
	}

	static OperatorResultType Function(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &input,
	                                   DataChunk &output) {
		auto &local_state = data_p.local_state->Cast<ThrottlingSum::ThrottlingSumLocalData>();

		for (idx_t row_idx = 0; row_idx < input.size(); row_idx++) {
			int sum = 0;
			for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
				if (input.data[col_idx].GetType() == LogicalType::INTEGER) {
					sum += input.data[col_idx].GetValue(row_idx).GetValue<int>();
				}
			}
			local_state.row_sums.push_back(sum);
		}

		if (PhysicalOperator::SelectOperatorCachingMode(context) == OperatorCachingMode::UNORDERED) {
			// Caching is allowed
			if (local_state.current_idx < local_state.row_sums.size()) {
				output.data[0].Append(Value(local_state.row_sums[local_state.current_idx++]));
				output.SetChildCardinality(1);
			} else {
				output.SetChildCardinality(0);
			}
		} else {
			// Caching is not allowed, we should emit everything!
			auto to_emit = local_state.row_sums.size() - local_state.current_idx;
			auto &sum_col = output.data[0];
			for (idx_t i = 0; i < to_emit; i++) {
				sum_col.Append(Value(local_state.row_sums[local_state.current_idx + i]));
			}
			local_state.current_idx += to_emit;
			output.SetChildCardinality(to_emit);
		}

		return OperatorResultType::NEED_MORE_INPUT;
	}

	static OperatorFinalizeResultType Finalize(ExecutionContext &context, TableFunctionInput &data_p,
	                                           DataChunk &output) {
		auto &local_state = data_p.local_state->Cast<ThrottlingSum::ThrottlingSumLocalData>();

		if (local_state.current_idx < local_state.row_sums.size()) {
			output.data[0].Append(Value(local_state.row_sums[local_state.current_idx++]));
			output.SetChildCardinality(1);
			return OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
		} else {
			return OperatorFinalizeResultType::FINISHED;
		}
	}

	static void Register(Connection &con) {
		// Create our test TableFunction
		con.BeginTransaction();
		auto &client_context = *con.context;
		auto &catalog = Catalog::GetSystemCatalog(client_context);
		TableFunction caching_table_in_out("throttling_sum", {LogicalType::TABLE}, nullptr, ThrottlingSum::Bind,
		                                   ThrottlingSum::ThrottlingSumGlobalInit,
		                                   ThrottlingSum::ThrottlingSumLocalInit);
		caching_table_in_out.in_out_function = ThrottlingSum::Function;
		caching_table_in_out.in_out_function_final = ThrottlingSum::Finalize;
		CreateTableFunctionInfo caching_table_in_out_info(caching_table_in_out);
		catalog.CreateTableFunction(*con.context, caching_table_in_out_info);
		con.Commit();
	}
};

struct LateralStructEcho {
	static duckdb::unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                             duckdb::vector<LogicalType> &return_types,
	                                             duckdb::vector<Identifier> &names) {
		return_types.emplace_back(LogicalType::BIGINT);
		names.emplace_back("outer_i");
		return_types.emplace_back(LogicalType::BIGINT);
		names.emplace_back("limit_value");
		return_types.emplace_back(LogicalType::VARCHAR);
		names.emplace_back("label_value");
		return make_uniq<TableFunctionData>();
	}

	static OperatorResultType Function(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &input,
	                                   DataChunk &output) {
		for (idx_t row_idx = 0; row_idx < input.size(); row_idx++) {
			auto struct_value = input.data[0].GetValue(row_idx);
			auto &children = StructValue::GetChildren(struct_value);
			output.data[0].Append(children[0]);
			output.data[1].Append(children[1]);
			output.data[2].Append(children[2]);
		}
		output.SetChildCardinality(input.size());
		return OperatorResultType::NEED_MORE_INPUT;
	}

	static void Register(Connection &con) {
		con.BeginTransaction();
		auto &client_context = *con.context;
		auto &catalog = Catalog::GetSystemCatalog(client_context);
		auto struct_type = LogicalType::STRUCT(
		    {{"outer_i", LogicalType::BIGINT}, {"limit", LogicalType::BIGINT}, {"label", LogicalType::VARCHAR}});
		TableFunction lateral_struct_echo("lateral_struct_echo", {struct_type}, nullptr, LateralStructEcho::Bind);
		lateral_struct_echo.in_out_function = LateralStructEcho::Function;
		CreateTableFunctionInfo lateral_struct_echo_info(lateral_struct_echo);
		catalog.CreateTableFunction(*con.context, lateral_struct_echo_info);
		con.Commit();
	}
};

// Builds a genuine N-child operator from the bound TABLE arguments: a nested cross
// product of every input, capped with a projection at the function's bind index.
struct MultiTableCrossProduct {
	static unique_ptr<LogicalOperator> BindOperator(ClientContext &context, TableFunctionBindInput &input,
	                                                TableIndex bind_index, vector<Identifier> &return_names) {
		auto &relations = input.InputRelations();
		if (relations.size() < 2) {
			throw InvalidInputException("Expected at least two TABLE arguments");
		}
		unique_ptr<LogicalOperator> combined;
		vector<unique_ptr<Expression>> projections;
		for (idx_t relation_idx = 0; relation_idx < relations.size(); relation_idx++) {
			auto &relation = relations[relation_idx];
			if (relation.types.size() != relation.names.size()) {
				throw InvalidInputException("TABLE argument schema is inconsistent");
			}
			auto bindings = relation.plan->GetColumnBindings();
			for (idx_t column_idx = 0; column_idx < bindings.size(); column_idx++) {
				auto name = Identifier("arg" + to_string(relation.argument_index) + "_" +
				                       relation.names[column_idx].GetIdentifierName());
				projections.push_back(
				    make_uniq<BoundColumnRefExpression>(name, relation.types[column_idx], bindings[column_idx]));
				return_names.push_back(name);
			}
			// consume the plan - the binder verifies every TABLE argument was taken
			auto child = input.TakeInputPlan(relation_idx);
			combined = combined ? LogicalCrossProduct::Create(std::move(combined), std::move(child)) : std::move(child);
		}
		auto projection = make_uniq<LogicalProjection>(bind_index, std::move(projections));
		projection->children.push_back(std::move(combined));
		return std::move(projection);
	}

	static void Register(Connection &con, const string &name, vector<LogicalType> arguments) {
		con.BeginTransaction();
		auto &catalog = Catalog::GetSystemCatalog(*con.context);
		TableFunction function(Identifier(name), std::move(arguments), nullptr, nullptr);
		function.bind_operator = BindOperator;
		CreateTableFunctionInfo info(function);
		catalog.CreateTableFunction(*con.context, info);
		con.Commit();
	}
};

// Consumes only its first TABLE argument - the binder must reject the leftover plan.
struct MultiTablePartialBindOperator {
	static unique_ptr<LogicalOperator> BindOperator(ClientContext &context, TableFunctionBindInput &input,
	                                                TableIndex bind_index, vector<Identifier> &return_names) {
		auto &first = input.InputRelations()[0];
		auto bindings = first.plan->GetColumnBindings();
		vector<unique_ptr<Expression>> projections;
		for (idx_t column_idx = 0; column_idx < bindings.size(); column_idx++) {
			projections.push_back(make_uniq<BoundColumnRefExpression>(first.names[column_idx], first.types[column_idx],
			                                                          bindings[column_idx]));
			return_names.push_back(first.names[column_idx]);
		}
		auto projection = make_uniq<LogicalProjection>(bind_index, std::move(projections));
		projection->children.push_back(input.TakeInputPlan(0));
		// the second TABLE argument's plan is deliberately left behind
		return std::move(projection);
	}

	static void Register(Connection &con, const string &name, vector<LogicalType> arguments) {
		con.BeginTransaction();
		auto &catalog = Catalog::GetSystemCatalog(*con.context);
		TableFunction function(Identifier(name), std::move(arguments), nullptr, nullptr);
		function.bind_operator = BindOperator;
		CreateTableFunctionInfo info(function);
		catalog.CreateTableFunction(*con.context, info);
		con.Commit();
	}
};

struct TableInputBindOperator {
	static unique_ptr<LogicalOperator> BindOperator(ClientContext &context, TableFunctionBindInput &input,
	                                                TableIndex bind_index, vector<Identifier> &return_names) {
		if (!input.input_plan || !*input.input_plan) {
			throw InvalidInputException("Expected a TABLE input plan");
		}
		auto child = std::move(*input.input_plan);
		auto bindings = child->GetColumnBindings();
		if (bindings.size() != input.input_table_types.size()) {
			throw InvalidInputException("Unexpected TABLE input schema");
		}
		vector<unique_ptr<Expression>> expressions;
		for (idx_t column_idx = 0; column_idx < bindings.size(); column_idx++) {
			expressions.push_back(make_uniq<BoundColumnRefExpression>(
			    input.input_table_names[column_idx], input.input_table_types[column_idx], bindings[column_idx]));
		}
		return_names = input.input_table_names;
		auto projection = make_uniq<LogicalProjection>(bind_index, std::move(expressions));
		projection->children.push_back(std::move(child));
		return std::move(projection);
	}

	static void Register(Connection &con, const string &name, vector<LogicalType> arguments) {
		con.BeginTransaction();
		auto &catalog = Catalog::GetSystemCatalog(*con.context);
		TableFunction function(Identifier(name), std::move(arguments), nullptr, nullptr);
		function.bind_operator = BindOperator;
		CreateTableFunctionInfo info(function);
		catalog.CreateTableFunction(*con.context, info);
		con.Commit();
	}
};

TEST_CASE("TABLE bind operators receive a single bound input plan", "[tablefunction]") {
	DuckDB db(nullptr);
	Connection con(db);
	TableInputBindOperator::Register(con, "single_table_bind_operator", {LogicalType::TABLE});

	auto single = con.Query(R"(
		SELECT i, s FROM single_table_bind_operator(
			(SELECT 42 AS i, 'value' AS s))
	)");
	REQUIRE_NO_FAIL(*single);
	REQUIRE(CHECK_COLUMN(single, 0, {42}));
	REQUIRE(CHECK_COLUMN(single, 1, {"value"}));
}

TEST_CASE("Bind operators receive every bound TABLE argument", "[tablefunction]") {
	DuckDB db(nullptr);
	Connection con(db);
	MultiTableCrossProduct::Register(con, "multi_table_pair", {LogicalType::TABLE, LogicalType::TABLE});
	MultiTableCrossProduct::Register(con, "multi_table_triple",
	                                 {LogicalType::TABLE, LogicalType::TABLE, LogicalType::TABLE});
	MultiTableCrossProduct::Register(con, "multi_table_mixed",
	                                 {LogicalType::TABLE, LogicalType::VARCHAR, LogicalType::TABLE});
	REQUIRE_NO_FAIL(*con.Query("SET debug_verify_serializer=true"));

	// two inputs with distinct schemas, both reachable
	auto pair = con.Query(R"(
		SELECT arg0_i, arg1_s
		FROM multi_table_pair((SELECT i FROM range(2) t(i)), (SELECT s FROM (VALUES ('a'), ('b')) t(s)))
		ORDER BY arg0_i, arg1_s
	)");
	REQUIRE_NO_FAIL(*pair);
	REQUIRE(CHECK_COLUMN(pair, 0, {0, 0, 1, 1}));
	REQUIRE(CHECK_COLUMN(pair, 1, {"a", "b", "a", "b"}));

	// three inputs with identical schemas stay distinguishable by argument position
	auto triple = con.Query(R"(
		SELECT arg0_i, arg1_i, arg2_i
		FROM multi_table_triple((SELECT 10::INTEGER AS i), (SELECT 20::INTEGER AS i), (SELECT 30::INTEGER AS i))
	)");
	REQUIRE_NO_FAIL(*triple);
	REQUIRE(CHECK_COLUMN(triple, 0, {10}));
	REQUIRE(CHECK_COLUMN(triple, 1, {20}));
	REQUIRE(CHECK_COLUMN(triple, 2, {30}));

	// a scalar argument between two TABLE arguments keeps the positional indexes
	auto mixed = con.Query(R"(
		SELECT arg0_i, arg2_i
		FROM multi_table_mixed((SELECT 1::INTEGER AS i), 'label', (SELECT 2::INTEGER AS i))
	)");
	REQUIRE_NO_FAIL(*mixed);
	REQUIRE(CHECK_COLUMN(mixed, 0, {1}));
	REQUIRE(CHECK_COLUMN(mixed, 1, {2}));

	// an empty input propagates through the operator the function built
	auto empty = con.Query(R"(
		SELECT count(*) FROM multi_table_pair((SELECT i FROM range(0) t(i)), (SELECT 42::BIGINT AS i))
	)");
	REQUIRE_NO_FAIL(*empty);
	REQUIRE(CHECK_COLUMN(empty, 0, {0}));

	// multiple vectors per input
	auto many = con.Query(R"(
		SELECT count(*) FROM multi_table_pair((SELECT i FROM range(5000) t(i)), (SELECT i FROM range(3) t(i)))
	)");
	REQUIRE_NO_FAIL(*many);
	REQUIRE(CHECK_COLUMN(many, 0, {15000}));

	// nested types survive the handover
	auto nested = con.Query(R"(
		SELECT arg0_items, arg1_nested.x
		FROM multi_table_pair((SELECT [1, NULL]::INTEGER[] AS items), (SELECT {'x': 42::INTEGER} AS nested))
	)");
	REQUIRE_NO_FAIL(*nested);
	REQUIRE(CHECK_COLUMN(nested, 0, {Value::LIST(LogicalType::INTEGER, {1, Value()})}));
	REQUIRE(CHECK_COLUMN(nested, 1, {42}));
}

TEST_CASE("Correlated TABLE arguments reach the bind operator", "[tablefunction]") {
	DuckDB db(nullptr);
	Connection con(db);
	MultiTableCrossProduct::Register(con, "multi_table_lateral", {LogicalType::TABLE, LogicalType::TABLE});

	auto same_outer = con.Query(R"(
		SELECT outer_rows.i, echoed.arg0_v, echoed.arg1_v
		FROM range(2) outer_rows(i), LATERAL multi_table_lateral(
			(SELECT outer_rows.i AS v), (SELECT outer_rows.i AS v)) echoed
		ORDER BY outer_rows.i
	)");
	REQUIRE_NO_FAIL(*same_outer);
	REQUIRE(CHECK_COLUMN(same_outer, 0, {0, 1}));
	REQUIRE(CHECK_COLUMN(same_outer, 1, {0, 1}));
	REQUIRE(CHECK_COLUMN(same_outer, 2, {0, 1}));

	auto different_outer = con.Query(R"(
		SELECT outer_rows.i, echoed.arg0_v, echoed.arg1_v
		FROM (VALUES (0, 100), (1, 101)) outer_rows(i, j), LATERAL multi_table_lateral(
			(SELECT outer_rows.i AS v), (SELECT outer_rows.j AS v)) echoed
		ORDER BY outer_rows.i
	)");
	REQUIRE_NO_FAIL(*different_outer);
	REQUIRE(CHECK_COLUMN(different_outer, 1, {0, 1}));
	REQUIRE(CHECK_COLUMN(different_outer, 2, {100, 101}));

	// only one side correlated
	auto mixed = con.Query(R"(
		SELECT outer_rows.i, echoed.arg0_v, echoed.arg1_v
		FROM range(2) outer_rows(i), LATERAL multi_table_lateral(
			(SELECT outer_rows.i AS v), (SELECT 42::BIGINT AS v)) echoed
		ORDER BY outer_rows.i
	)");
	REQUIRE_NO_FAIL(*mixed);
	REQUIRE(CHECK_COLUMN(mixed, 1, {0, 1}));
	REQUIRE(CHECK_COLUMN(mixed, 2, {42, 42}));

	// a correlated input that produces no rows
	auto correlated_empty = con.Query(R"(
		SELECT count(*)
		FROM range(2) outer_rows(i), LATERAL multi_table_lateral(
			(SELECT outer_rows.i AS v WHERE false), (SELECT outer_rows.i + 10 AS v)) echoed
	)");
	REQUIRE_NO_FAIL(*correlated_empty);
	REQUIRE(CHECK_COLUMN(correlated_empty, 0, {0}));
}

TEST_CASE("Multiple TABLE arguments require a bind operator", "[tablefunction]") {
	DuckDB db(nullptr);
	Connection con(db);

	con.BeginTransaction();
	{
		auto &catalog = Catalog::GetSystemCatalog(*con.context);
		TableFunction function("multi_table_in_out", {LogicalType::TABLE, LogicalType::TABLE}, nullptr,
		                       LateralStructEcho::Bind);
		function.in_out_function = LateralStructEcho::Function;
		CreateTableFunctionInfo info(function);
		catalog.CreateTableFunction(*con.context, info);
	}
	con.Commit();

	auto rejected = con.Query("SELECT * FROM multi_table_in_out((SELECT 1 AS a), (SELECT 2 AS b))");
	REQUIRE(rejected->HasError());
	REQUIRE(StringUtil::Contains(rejected->GetError(), "bind_operator"));
}

TEST_CASE("Unconsumed TABLE arguments are rejected", "[tablefunction]") {
	DuckDB db(nullptr);
	Connection con(db);
	MultiTablePartialBindOperator::Register(con, "multi_table_leaks", {LogicalType::TABLE, LogicalType::TABLE});

	auto leaked = con.Query("SELECT * FROM multi_table_leaks((SELECT 1 AS a), (SELECT 2 AS b))");
	REQUIRE(leaked->HasError());
	REQUIRE(StringUtil::Contains(leaked->GetError(), "did not consume"));
}

struct FilterPushdownEcho {
	struct GlobalState : public GlobalTableFunctionState {
		GlobalState(optional_ptr<TableFilterSet> filters_p, vector<column_t> column_ids_p,
		            vector<idx_t> projection_ids_p)
		    : filters(filters_p && (filters_p->HasFilters() || filters_p->HasMultiColumnFilters()) ? filters_p
		                                                                                           : nullptr),
		      column_ids(std::move(column_ids_p)), projection_ids(std::move(projection_ids_p)),
		      received_filters(filters != nullptr) {
		}

		optional_ptr<TableFilterSet> filters;
		vector<column_t> column_ids;
		vector<idx_t> projection_ids;
		bool received_filters;
	};

	struct LocalState : public LocalTableFunctionState {
		explicit LocalState(bool received_filters_p) : received_filters(received_filters_p) {
		}

		bool received_filters;
	};

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<Identifier> &names) {
		return_types.emplace_back(LogicalType::INTEGER);
		names.emplace_back("value");
		return_types.emplace_back(LogicalType::INTEGER);
		names.emplace_back("filter_state");
		return make_uniq<TableFunctionData>();
	}

	static unique_ptr<GlobalTableFunctionState> GlobalInit(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<GlobalState>(input.filters, input.column_ids, input.projection_ids);
	}

	static unique_ptr<LocalTableFunctionState> LocalInit(ExecutionContext &context, TableFunctionInitInput &input,
	                                                     GlobalTableFunctionState *global_state) {
		return make_uniq<LocalState>(input.filters &&
		                             (input.filters->HasFilters() || input.filters->HasMultiColumnFilters()));
	}

	static OperatorResultType Function(ExecutionContext &context, TableFunctionInput &data, DataChunk &input,
	                                   DataChunk &output) {
		auto &global_state = data.global_state->Cast<GlobalState>();
		auto &local_state = data.local_state->Cast<LocalState>();
		vector<LogicalType> output_types(global_state.column_ids.size(), LogicalType::INTEGER);
		DataChunk candidates;
		candidates.Initialize(context.client, output_types);
		for (idx_t output_idx = 0; output_idx < global_state.column_ids.size(); output_idx++) {
			switch (global_state.column_ids[output_idx]) {
			case 0:
				candidates.data[output_idx].Reference(input.data[0]);
				break;
			case 1: {
				// Bit 0 records global delivery; bit 1 records local delivery.
				auto filter_state = static_cast<int32_t>(global_state.received_filters) +
				                    2 * static_cast<int32_t>(local_state.received_filters);
				candidates.data[output_idx].Reference(Value::INTEGER(filter_state), count_t(input.size()));
				break;
			}
			default:
				throw InternalException("Unexpected filter pushdown test column id");
			}
		}
		candidates.SetChildCardinality(input.size());

		if (global_state.filters) {
			D_ASSERT(!global_state.filters->HasMultiColumnFilters());
			for (const auto &entry : *global_state.filters) {
				auto filter_idx = entry.GetIndex().GetIndex();
				auto column = make_uniq<BoundReferenceExpression>(candidates.data[filter_idx].GetType(), filter_idx);
				auto expression = entry.Filter().ToExpression(*column);
				ExpressionExecutor executor(context.client, *expression);
				SelectionVector selection(candidates.size());
				auto count = executor.SelectExpression(candidates, selection);
				candidates.Slice(selection, count);
			}
		}

		auto output_count =
		    global_state.projection_ids.empty() ? candidates.ColumnCount() : global_state.projection_ids.size();
		for (idx_t output_idx = 0; output_idx < output_count; output_idx++) {
			auto source_idx =
			    global_state.projection_ids.empty() ? output_idx : global_state.projection_ids[output_idx];
			output.data[output_idx].Reference(candidates.data[source_idx]);
		}
		output.SetChildCardinality(candidates.size());
		return OperatorResultType::NEED_MORE_INPUT;
	}

	static bool SupportsPushdownType(const FunctionData &bind_data, idx_t column_idx) {
		return column_idx == 0;
	}

	static void Register(Connection &con, const string &name, vector<LogicalType> arguments, bool filter_prune = true,
	                     bool type_filter = false) {
		con.BeginTransaction();
		auto &catalog = Catalog::GetSystemCatalog(*con.context);
		TableFunction function(Identifier(name), std::move(arguments), nullptr, Bind, GlobalInit, LocalInit);
		function.in_out_function = Function;
		function.projection_pushdown = true;
		function.filter_pushdown = true;
		function.filter_prune = filter_prune;
		if (type_filter) {
			function.supports_pushdown_type = SupportsPushdownType;
		}
		CreateTableFunctionInfo info(function);
		catalog.CreateTableFunction(*con.context, info);
		con.Commit();
	}
};

TEST_CASE("Caching TableInOutFunction", "[filter][.]") {
	DuckDB db(nullptr);
	Connection con(db);

	ThrottlingSum::Register(con);

	// Check result
	auto result2 =
	    con.Query("SELECT * FROM throttling_sum((select i::INTEGER, (i+1)::INTEGER as j from range(0,3) tbl(i)));");
	REQUIRE(result2->ColumnCount() == 1);
	REQUIRE(CHECK_COLUMN(result2, 0, {1, 3, 5}));

	// TODO: streaming these is currently unsupported

	// Large result into aggregation
	auto result3 = con.Query(
	    "SELECT sum(total) FROM throttling_sum((select i::INTEGER, (i+1)::INTEGER as j from range(0,130000) tbl(i)));");
	REQUIRE(result3->ColumnCount() == 1);
	REQUIRE(CHECK_COLUMN(result3, 0, {Value::BIGINT(16900000000)}));
}

TEST_CASE("Parallel execution with caching table in out functions", "[filter][.]") {
	DuckDB db(nullptr);
	Connection con(db);

	ThrottlingSum::Register(con);

	auto result = con.Query("CREATE TABLE test_data as select i::INTEGER from range(0,200000) tbl(i);");
	auto result2 = con.Query("SELECT * FROM throttling_sum((select * from test_data));");

	REQUIRE(result2->ColumnCount() == 1);
	REQUIRE(result2->RowCount() == 200000);
	REQUIRE(CHECK_COLUMN(result2, 0, {0, 1, 2, 3, 4, 5}));
}

TEST_CASE("Lateral table in out function preserves constant struct fields", "[tablefunction]") {
	DuckDB db(nullptr);
	Connection con(db);

	LateralStructEcho::Register(con);

	auto result = con.Query(R"(
		SELECT echoed.outer_i, echoed.limit_value, echoed.label_value
		FROM range(3) outer_rows(i)
		CROSS JOIN LATERAL lateral_struct_echo({'outer_i': i, 'limit': 1, 'label': 'fixed'}) AS echoed
		ORDER BY echoed.outer_i
	)");
	if (result->HasError()) {
		INFO(result->GetError());
	}
	REQUIRE(!result->HasError());
	REQUIRE(result->ColumnCount() == 3);
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 1, 1}));
	REQUIRE(CHECK_COLUMN(result, 2, {"fixed", "fixed", "fixed"}));
}

TEST_CASE("Filter pushdown into table in-out functions", "[tablefunction]") {
	DuckDB db(nullptr);
	Connection con(db);

	FilterPushdownEcho::Register(con, "filter_pushdown_echo", {LogicalType::TABLE});

	auto filtered = con.Query(R"(
		SELECT value, filter_state
		FROM filter_pushdown_echo((SELECT i::INTEGER AS value FROM range(3) t(i)))
		WHERE value = 1
	)");
	REQUIRE_NO_FAIL(*filtered);
	REQUIRE(CHECK_COLUMN(filtered, 0, {1}));
	REQUIRE(CHECK_COLUMN(filtered, 1, {3}));

	auto filter_only_column = con.Query(R"(
		SELECT filter_state
		FROM filter_pushdown_echo((SELECT i::INTEGER AS value FROM range(3) t(i)))
		WHERE value > 0 AND value < 2
	)");
	REQUIRE_NO_FAIL(*filter_only_column);
	REQUIRE(CHECK_COLUMN(filter_only_column, 0, {3}));

	FilterPushdownEcho::Register(con, "filter_pushdown_echo_no_prune", {LogicalType::TABLE}, false);
	auto no_filter_pruning = con.Query(R"(
		SELECT filter_state
		FROM filter_pushdown_echo_no_prune((SELECT i::INTEGER AS value FROM range(3) t(i)))
		WHERE value = 1
	)");
	REQUIRE_NO_FAIL(*no_filter_pruning);
	REQUIRE(CHECK_COLUMN(no_filter_pruning, 0, {3}));
}

TEST_CASE("Type-limited filters remain above table in-out functions", "[tablefunction]") {
	DuckDB db(nullptr);
	Connection con(db);

	FilterPushdownEcho::Register(con, "type_limited_filter_echo", {LogicalType::TABLE}, false, true);

	auto supported_filter = con.Query(R"(
		SELECT value, filter_state
		FROM type_limited_filter_echo((SELECT i::INTEGER AS value FROM range(3) t(i)))
		WHERE value = 1
	)");
	REQUIRE_NO_FAIL(*supported_filter);
	REQUIRE(CHECK_COLUMN(supported_filter, 0, {1}));
	REQUIRE(CHECK_COLUMN(supported_filter, 1, {0}));

	auto unsupported_filter = con.Query(R"(
		SELECT count(*)
		FROM type_limited_filter_echo((SELECT i::INTEGER AS value FROM range(3) t(i)))
		WHERE filter_state = 3
	)");
	REQUIRE_NO_FAIL(*unsupported_filter);
	REQUIRE(CHECK_COLUMN(unsupported_filter, 0, {0}));
}

TEST_CASE("Correlated filters remain above table in-out functions", "[tablefunction]") {
	DuckDB db(nullptr);
	Connection con(db);

	FilterPushdownEcho::Register(con, "lateral_filter_pushdown_echo", {LogicalType::INTEGER});

	auto result = con.Query(R"(
		SELECT echoed.value, echoed.filter_state
		FROM range(3) outer_rows(i),
		LATERAL lateral_filter_pushdown_echo(i::INTEGER) echoed
		WHERE echoed.value = outer_rows.i % 2
		ORDER BY echoed.value
	)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 0}));

	FilterPushdownEcho::Register(con, "lateral_type_limited_filter_echo", {LogicalType::INTEGER}, true, true);
	auto type_limited_filter = con.Query(R"(
		SELECT outer_rows.i, echoed.value
		FROM range(3) outer_rows(i),
		LATERAL lateral_type_limited_filter_echo(i::INTEGER) echoed
		WHERE echoed.filter_state = 3
		ORDER BY outer_rows.i
	)");
	REQUIRE_NO_FAIL(*type_limited_filter);
	REQUIRE(type_limited_filter->RowCount() == 0);
}

// Emits value = 1..n for every input row n and records whether it received pushed filters.
// Used to verify that WITH ORDINALITY keeps filters above the function and reports unfiltered positions.
struct OrdinalityEcho {
	//! Column count returned by Bind - the binder appends the ordinality column after these
	static constexpr idx_t BOUND_COLUMN_COUNT = 2;

	struct LocalState : public LocalTableFunctionState {
		idx_t offset = 0;
	};

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<Identifier> &names) {
		return_types.emplace_back(LogicalType::INTEGER);
		names.emplace_back("value");
		return_types.emplace_back(LogicalType::INTEGER);
		names.emplace_back("filter_state");
		return make_uniq<TableFunctionData>();
	}

	static unique_ptr<GlobalTableFunctionState> GlobalInit(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<FilterPushdownEcho::GlobalState>(input.filters, input.column_ids, input.projection_ids);
	}

	static unique_ptr<LocalTableFunctionState> LocalInit(ExecutionContext &context, TableFunctionInitInput &input,
	                                                     GlobalTableFunctionState *global_state) {
		return make_uniq<LocalState>();
	}

	static OperatorResultType Function(ExecutionContext &context, TableFunctionInput &data, DataChunk &input,
	                                   DataChunk &output) {
		auto &global_state = data.global_state->Cast<FilterPushdownEcho::GlobalState>();
		auto &local_state = data.local_state->Cast<LocalState>();
		auto count = NumericCast<idx_t>(input.data[0].GetValue(0).GetValue<int32_t>());
		auto batch_count = MinValue<idx_t>(count - local_state.offset, STANDARD_VECTOR_SIZE);

		vector<LogicalType> candidate_types;
		for (auto column_id : global_state.column_ids) {
			candidate_types.push_back(column_id < BOUND_COLUMN_COUNT ? LogicalType::INTEGER : LogicalType::BIGINT);
		}
		DataChunk candidates;
		candidates.Initialize(context.client, candidate_types);
		for (idx_t output_idx = 0; output_idx < global_state.column_ids.size(); output_idx++) {
			auto &candidate = candidates.data[output_idx];
			switch (global_state.column_ids[output_idx]) {
			case 0: {
				auto writer = FlatVector::Writer<int32_t>(candidate, batch_count);
				for (idx_t i = 0; i < batch_count; i++) {
					writer.WriteValue(NumericCast<int32_t>(local_state.offset + i + 1));
				}
				break;
			}
			case 1:
				candidate.Reference(Value::INTEGER(static_cast<int32_t>(global_state.received_filters)),
				                    count_t(batch_count));
				break;
			default:
				// the ordinality column is filled in by the operator
				candidate.Reference(Value::BIGINT(0), count_t(batch_count));
				break;
			}
		}
		candidates.SetChildCardinality(batch_count);

		auto projected_count =
		    global_state.projection_ids.empty() ? candidates.ColumnCount() : global_state.projection_ids.size();
		for (idx_t output_idx = 0; output_idx < projected_count; output_idx++) {
			auto source_idx =
			    global_state.projection_ids.empty() ? output_idx : global_state.projection_ids[output_idx];
			output.data[output_idx].Reference(candidates.data[source_idx]);
		}
		output.SetChildCardinality(candidates.size());

		local_state.offset += batch_count;
		if (local_state.offset < count) {
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}
		local_state.offset = 0;
		return OperatorResultType::NEED_MORE_INPUT;
	}

	static void PushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data,
	                                  vector<unique_ptr<Expression>> &filters) {
		filters.clear();
	}

	static void Register(Connection &con, const string &name, bool complex_filter = false) {
		con.BeginTransaction();
		auto &catalog = Catalog::GetSystemCatalog(*con.context);
		TableFunction function(Identifier(name), {LogicalType::INTEGER}, nullptr, Bind, GlobalInit, LocalInit);
		function.in_out_function = Function;
		function.projection_pushdown = true;
		function.filter_pushdown = true;
		function.filter_prune = true;
		if (complex_filter) {
			function.pushdown_complex_filter = PushdownComplexFilter;
		}
		CreateTableFunctionInfo info(function);
		catalog.CreateTableFunction(*con.context, info);
		con.Commit();
	}
};

TEST_CASE("WITH ORDINALITY reports unfiltered positions for table in-out functions", "[tablefunction]") {
	DuckDB db(nullptr);
	Connection con(db);

	OrdinalityEcho::Register(con, "ordinality_echo");
	OrdinalityEcho::Register(con, "complex_ordinality_echo", true);

	// without a filter the function emits 1..5 and is numbered 1..5
	auto unfiltered = con.Query(R"(
		SELECT echoed.value, echoed.ordinality, echoed.filter_state
		FROM range(1) outer_rows(i),
		LATERAL ordinality_echo((5 + outer_rows.i)::INTEGER) WITH ORDINALITY echoed
		ORDER BY echoed.value
	)");
	REQUIRE_NO_FAIL(*unfiltered);
	REQUIRE(CHECK_COLUMN(unfiltered, 0, {1, 2, 3, 4, 5}));
	REQUIRE(CHECK_COLUMN(unfiltered, 1, {1, 2, 3, 4, 5}));
	REQUIRE(CHECK_COLUMN(unfiltered, 2, {0, 0, 0, 0, 0}));

	// the filter must not be pushed into the function: the surviving row keeps its original ordinality
	auto filtered = con.Query(R"(
		SELECT echoed.value, echoed.ordinality, echoed.filter_state
		FROM range(1) outer_rows(i),
		LATERAL ordinality_echo((5 + outer_rows.i)::INTEGER) WITH ORDINALITY echoed
		WHERE echoed.value = 4
	)");
	REQUIRE_NO_FAIL(*filtered);
	REQUIRE(CHECK_COLUMN(filtered, 0, {4}));
	REQUIRE(CHECK_COLUMN(filtered, 1, {4}));
	REQUIRE(CHECK_COLUMN(filtered, 2, {0}));

	// the ordinality restarts at 1 for every input row
	auto per_row = con.Query(R"(
		SELECT outer_rows.i, echoed.value, echoed.ordinality
		FROM range(1, 4) outer_rows(i),
		LATERAL ordinality_echo(i::INTEGER) WITH ORDINALITY echoed
		WHERE echoed.value >= 2
		ORDER BY outer_rows.i, echoed.value
	)");
	REQUIRE_NO_FAIL(*per_row);
	REQUIRE(CHECK_COLUMN(per_row, 0, {2, 3, 3}));
	REQUIRE(CHECK_COLUMN(per_row, 1, {2, 2, 3}));
	REQUIRE(CHECK_COLUMN(per_row, 2, {2, 2, 3}));

	// a filter on the ordinality column itself is also evaluated above the function
	auto ordinality_filter = con.Query(R"(
		SELECT echoed.value, echoed.ordinality
		FROM range(1) outer_rows(i),
		LATERAL ordinality_echo((5 + outer_rows.i)::INTEGER) WITH ORDINALITY echoed
		WHERE echoed.ordinality > 3
		ORDER BY echoed.ordinality
	)");
	REQUIRE_NO_FAIL(*ordinality_filter);
	REQUIRE(CHECK_COLUMN(ordinality_filter, 0, {4, 5}));
	REQUIRE(CHECK_COLUMN(ordinality_filter, 1, {4, 5}));

	// a complex-filter callback must not consume the predicate before ordinality is assigned
	auto complex_filter = con.Query(R"(
		SELECT echoed.value, echoed.ordinality
		FROM range(1) outer_rows(i),
		LATERAL complex_ordinality_echo((5 + outer_rows.i)::INTEGER) WITH ORDINALITY echoed
		WHERE echoed.value = 4
	)");
	REQUIRE_NO_FAIL(*complex_filter);
	REQUIRE(CHECK_COLUMN(complex_filter, 0, {4}));
	REQUIRE(CHECK_COLUMN(complex_filter, 1, {4}));
}
