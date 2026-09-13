#include "catch.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
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

struct MultiTableEcho {
	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<Identifier> &names) {
		if (!input.IsMultiTableInput()) {
			throw InvalidInputException("Expected a multi-TABLE input");
		}
		for (idx_t table_idx = 0; table_idx < input.GetTableInputCount(); table_idx++) {
			auto argument_idx = input.GetTableArgumentIndex(table_idx);
			if (input.GetTableInputMemberName(table_idx) != Identifier("arg_" + to_string(argument_idx))) {
				throw InvalidInputException("Unexpected multi-TABLE input tag");
			}
			if (input.GetTableInputType(table_idx).id() != LogicalTypeId::STRUCT) {
				throw InvalidInputException("Expected STRUCT table rows");
			}
		}
		return_types.emplace_back(LogicalType::UBIGINT);
		names.emplace_back("table_index");
		return_types.push_back(input.input_table_types[0]);
		names.emplace_back("row_value");
		return_types.push_back(LogicalType::BOOLEAN);
		names.emplace_back("selected_row_valid");
		return make_uniq<TableFunctionData>();
	}

	static OperatorResultType Function(ExecutionContext &context, TableFunctionInput &data, DataChunk &input,
	                                   DataChunk &output) {
		auto table_indices = FlatVector::Writer<uint64_t>(output.data[0], input.size());
		auto selected_row_valid = FlatVector::Writer<bool>(output.data[2], input.size());
		for (idx_t row_idx = 0; row_idx < input.size(); row_idx++) {
			idx_t table_idx;
			if (!MultiTableFunctionInput::TryGetTableIndex(input, row_idx, table_idx)) {
				table_indices.WriteNull();
				selected_row_valid.WriteNull();
				continue;
			}
			auto selected_row = MultiTableFunctionInput::GetTableRows(input, table_idx).GetValue(row_idx);
			table_indices.WriteValue(table_idx);
			selected_row_valid.WriteValue(!selected_row.IsNull());
		}
		output.data[1].Reference(input.data[0]);
		output.SetChildCardinality(input.size());
		return OperatorResultType::NEED_MORE_INPUT;
	}

	static void Register(Connection &con, const string &name, vector<LogicalType> arguments) {
		con.BeginTransaction();
		auto &catalog = Catalog::GetSystemCatalog(*con.context);
		TableFunction function(Identifier(name), std::move(arguments), nullptr, Bind);
		function.in_out_function = Function;
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

struct MultiTableLifecycle {
	struct LocalState : public LocalTableFunctionState {
		vector<int64_t> values;
		idx_t offset = 0;
		bool loaded_input = false;
		bool emitted_final = false;
	};

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<Identifier> &names) {
		if (!input.IsMultiTableInput()) {
			throw InvalidInputException("Expected a multi-TABLE input");
		}
		return_types.push_back(LogicalType::BIGINT);
		names.emplace_back("value");
		return make_uniq<TableFunctionData>();
	}

	static unique_ptr<LocalTableFunctionState> LocalInit(ExecutionContext &context, TableFunctionInitInput &input,
	                                                     GlobalTableFunctionState *global_state) {
		return make_uniq<LocalState>();
	}

	static OperatorResultType Function(ExecutionContext &context, TableFunctionInput &data, DataChunk &input,
	                                   DataChunk &output) {
		auto &state = data.local_state->Cast<LocalState>();
		if (!state.loaded_input) {
			for (idx_t row_idx = 0; row_idx < input.size(); row_idx++) {
				idx_t table_idx;
				if (!MultiTableFunctionInput::TryGetTableIndex(input, row_idx, table_idx)) {
					continue;
				}
				auto row = MultiTableFunctionInput::GetTableRows(input, table_idx).GetValue(row_idx);
				state.values.push_back(StructValue::GetChildren(row)[0].GetValue<int64_t>());
			}
			state.loaded_input = true;
		}
		if (state.values.empty()) {
			state.loaded_input = false;
			return OperatorResultType::NEED_MORE_INPUT;
		}

		auto writer = FlatVector::Writer<int64_t>(output.data[0], 1);
		writer.WriteValue(state.values[state.offset++]);
		output.SetChildCardinality(1);
		if (state.offset < state.values.size()) {
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}
		state.values.clear();
		state.offset = 0;
		state.loaded_input = false;
		return OperatorResultType::NEED_MORE_INPUT;
	}

	static OperatorFinalizeResultType Finalize(ExecutionContext &context, TableFunctionInput &data, DataChunk &output) {
		auto &state = data.local_state->Cast<LocalState>();
		if (state.emitted_final) {
			return OperatorFinalizeResultType::FINISHED;
		}
		auto writer = FlatVector::Writer<int64_t>(output.data[0], 1);
		writer.WriteValue(-1);
		output.SetChildCardinality(1);
		state.emitted_final = true;
		return OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
	}

	static void Register(Connection &con) {
		con.BeginTransaction();
		auto &catalog = Catalog::GetSystemCatalog(*con.context);
		TableFunction function("multi_table_lifecycle", {LogicalType::TABLE, LogicalType::TABLE}, nullptr, Bind,
		                       nullptr, LocalInit);
		function.in_out_function = Function;
		function.in_out_function_final = Finalize;
		CreateTableFunctionInfo info(function);
		catalog.CreateTableFunction(*con.context, info);
		con.Commit();
	}
};

TEST_CASE("TABLE bind operators can consume single and normalized multi-input plans", "[tablefunction]") {
	DuckDB db(nullptr);
	Connection con(db);
	TableInputBindOperator::Register(con, "single_table_bind_operator", {LogicalType::TABLE});
	TableInputBindOperator::Register(con, "multi_table_bind_operator", {LogicalType::TABLE, LogicalType::TABLE});

	auto single = con.Query(R"(
		SELECT i, s FROM single_table_bind_operator(
			(SELECT 42 AS i, 'value' AS s))
	)");
	REQUIRE_NO_FAIL(*single);
	REQUIRE(CHECK_COLUMN(single, 0, {42}));
	REQUIRE(CHECK_COLUMN(single, 1, {"value"}));

	auto multiple = con.Query(R"(
		SELECT input.arg_0.i, input.arg_1.i
		FROM multi_table_bind_operator(
			(SELECT 10 AS i), (SELECT 20 AS i))
		ORDER BY union_tag(input)
	)");
	REQUIRE_NO_FAIL(*multiple);
	REQUIRE(CHECK_COLUMN(multiple, 0, {10, Value()}));
	REQUIRE(CHECK_COLUMN(multiple, 1, {Value(), 20}));
}

TEST_CASE("Multiple TABLE inputs preserve HAVE_MORE_OUTPUT and pipeline finalization", "[tablefunction]") {
	DuckDB db(nullptr);
	Connection con(db);
	MultiTableLifecycle::Register(con);

	auto result = con.Query(R"(
		SELECT count(*) FILTER (value >= 0), count(*) FILTER (value = -1)
		FROM multi_table_lifecycle(
			(SELECT i FROM range(5000) t(i)),
			(SELECT i FROM range(3000) t(i)))
	)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {8000}));
	REQUIRE(result->GetValue(1, 0).GetValue<int64_t>() > 0);

	auto empty_branch = con.Query(R"(
		SELECT count(*) FILTER (value >= 0), count(*) FILTER (value = -1)
		FROM multi_table_lifecycle(
			(SELECT i FROM range(0) t(i)),
			(SELECT i FROM range(3) t(i)))
	)");
	REQUIRE_NO_FAIL(*empty_branch);
	REQUIRE(CHECK_COLUMN(empty_branch, 0, {3}));
	REQUIRE(empty_branch->GetValue(1, 0).GetValue<int64_t>() > 0);
}

TEST_CASE("Table in-out functions accept multiple TABLE parameters", "[tablefunction]") {
	DuckDB db(nullptr);
	Connection con(db);

	MultiTableEcho::Register(con, "multi_table_echo", {LogicalType::TABLE, LogicalType::TABLE});
	MultiTableEcho::Register(con, "three_table_echo", {LogicalType::TABLE, LogicalType::TABLE, LogicalType::TABLE});
	MultiTableEcho::Register(con, "mixed_table_echo", {LogicalType::TABLE, LogicalType::VARCHAR, LogicalType::TABLE});
	REQUIRE_NO_FAIL(*con.Query("SET debug_verify_serializer=true"));

	auto unrelated = con.Query(R"(
		SELECT table_index, row_value.arg_0.i, row_value.arg_1.s, selected_row_valid
		FROM multi_table_echo(
			(SELECT i FROM range(2) t(i)),
			(SELECT s FROM (VALUES ('a'), ('b')) t(s)))
		ORDER BY table_index, coalesce(row_value.arg_0.i, 0), row_value.arg_1.s
	)");
	REQUIRE_NO_FAIL(*unrelated);
	REQUIRE(CHECK_COLUMN(unrelated, 0, {0, 0, 1, 1}));
	REQUIRE(CHECK_COLUMN(unrelated, 1, {0, 1, Value(), Value()}));
	REQUIRE(CHECK_COLUMN(unrelated, 2, {Value(), Value(), "a", "b"}));
	REQUIRE(CHECK_COLUMN(unrelated, 3, {true, true, true, true}));

	auto identical = con.Query(R"(
		SELECT table_index, row_value.arg_0.i, row_value.arg_1.i, row_value.arg_2.i
		FROM three_table_echo(
			(SELECT 10::INTEGER AS i),
			(SELECT 20::INTEGER AS i),
			(SELECT 30::INTEGER AS i))
		ORDER BY table_index
	)");
	REQUIRE_NO_FAIL(*identical);
	REQUIRE(CHECK_COLUMN(identical, 0, {0, 1, 2}));
	REQUIRE(CHECK_COLUMN(identical, 1, {10, Value(), Value()}));
	REQUIRE(CHECK_COLUMN(identical, 2, {Value(), 20, Value()}));
	REQUIRE(CHECK_COLUMN(identical, 3, {Value(), Value(), 30}));

	auto mixed = con.Query(R"(
		SELECT table_index, row_value.arg_0.i, row_value.arg_2.i
		FROM mixed_table_echo(
			(SELECT 1::INTEGER AS i), 'label', (SELECT 2::INTEGER AS i))
		ORDER BY table_index
	)");
	REQUIRE_NO_FAIL(*mixed);
	REQUIRE(CHECK_COLUMN(mixed, 0, {0, 1}));
	REQUIRE(CHECK_COLUMN(mixed, 1, {1, Value()}));
	REQUIRE(CHECK_COLUMN(mixed, 2, {Value(), 2}));

	auto empty = con.Query(R"(
		SELECT table_index, row_value.arg_0.i, row_value.arg_1.i, row_value.arg_2.i
		FROM three_table_echo(
			(SELECT i FROM range(0) t(i)),
			(SELECT 42::BIGINT AS i),
			(SELECT i FROM range(0) t(i)))
	)");
	REQUIRE_NO_FAIL(*empty);
	REQUIRE(CHECK_COLUMN(empty, 0, {1}));
	REQUIRE(CHECK_COLUMN(empty, 1, {Value()}));
	REQUIRE(CHECK_COLUMN(empty, 2, {42}));
	REQUIRE(CHECK_COLUMN(empty, 3, {Value()}));

	auto duplicate_names = con.Query(R"(
		SELECT row_value.arg_0.a, row_value.arg_0.a_1
		FROM multi_table_echo(
			(SELECT 1 AS a, 2 AS a),
			(SELECT NULL::INTEGER AS unused WHERE false))
	)");
	REQUIRE_NO_FAIL(*duplicate_names);
	REQUIRE(CHECK_COLUMN(duplicate_names, 0, {1}));
	REQUIRE(CHECK_COLUMN(duplicate_names, 1, {2}));

	auto nested = con.Query(R"(
		SELECT table_index, row_value.arg_0.items, row_value.arg_1.nested.x
		FROM multi_table_echo(
			(SELECT [1, NULL]::INTEGER[] AS items),
			(SELECT {'x': 42::INTEGER} AS nested))
		ORDER BY table_index
	)");
	REQUIRE_NO_FAIL(*nested);
	REQUIRE(CHECK_COLUMN(nested, 0, {0, 1}));
	REQUIRE(CHECK_COLUMN(nested, 1, {Value::LIST(LogicalType::INTEGER, {1, Value()}), Value()}));
	REQUIRE(CHECK_COLUMN(nested, 2, {Value(), 42}));

	auto multiple_vectors = con.Query(R"(
		SELECT table_index, count(*)
		FROM multi_table_echo(
			(SELECT i FROM range(5000) t(i)),
			(SELECT i FROM range(7000) t(i)))
		GROUP BY table_index
		ORDER BY table_index
	)");
	REQUIRE_NO_FAIL(*multiple_vectors);
	REQUIRE(CHECK_COLUMN(multiple_vectors, 0, {0, 1}));
	REQUIRE(CHECK_COLUMN(multiple_vectors, 1, {5000, 7000}));

	auto all_empty = con.Query(R"(
		SELECT count(*)
		FROM multi_table_echo(
			(SELECT i FROM range(0) t(i)),
			(SELECT i FROM range(0) t(i)))
	)");
	REQUIRE_NO_FAIL(*all_empty);
	REQUIRE(CHECK_COLUMN(all_empty, 0, {0}));

	auto unresolved_null = con.Query(R"(
		SELECT table_index, row_value.arg_0.v IS NULL
		FROM multi_table_echo(
			(SELECT NULL AS v), (SELECT 1 AS v WHERE false))
	)");
	REQUIRE_NO_FAIL(*unresolved_null);
	REQUIRE(CHECK_COLUMN(unresolved_null, 0, {0}));
	REQUIRE(CHECK_COLUMN(unresolved_null, 1, {true}));
}

TEST_CASE("Correlated multiple TABLE parameters", "[tablefunction]") {
	DuckDB db(nullptr);
	Connection con(db);
	MultiTableEcho::Register(con, "multi_table_lateral_echo", {LogicalType::TABLE, LogicalType::TABLE});

	auto same_outer = con.Query(R"(
		SELECT outer_rows.i, echoed.table_index, echoed.row_value.arg_0.v, echoed.row_value.arg_1.v
		FROM range(2) outer_rows(i), LATERAL multi_table_lateral_echo(
			(SELECT outer_rows.i AS v), (SELECT outer_rows.i AS v)) echoed
		ORDER BY outer_rows.i, echoed.table_index
	)");
	REQUIRE_NO_FAIL(*same_outer);
	REQUIRE(CHECK_COLUMN(same_outer, 0, {0, 0, 1, 1}));
	REQUIRE(CHECK_COLUMN(same_outer, 1, {0, 1, 0, 1}));

	auto different_outer = con.Query(R"(
		SELECT outer_rows.i, echoed.table_index, echoed.row_value.arg_0.v, echoed.row_value.arg_1.v
		FROM (VALUES (0, 100), (1, 101)) outer_rows(i, j), LATERAL multi_table_lateral_echo(
			(SELECT outer_rows.i AS v), (SELECT outer_rows.j AS v)) echoed
		ORDER BY outer_rows.i, echoed.table_index
	)");
	REQUIRE_NO_FAIL(*different_outer);
	REQUIRE(CHECK_COLUMN(different_outer, 2, {0, Value(), 1, Value()}));
	REQUIRE(CHECK_COLUMN(different_outer, 3, {Value(), 100, Value(), 101}));

	auto mixed_correlation = con.Query(R"(
		SELECT outer_rows.i, echoed.table_index, echoed.row_value.arg_0.v, echoed.row_value.arg_1.v
		FROM range(2) outer_rows(i), LATERAL multi_table_lateral_echo(
			(SELECT outer_rows.i AS v), (SELECT 42::BIGINT AS v)) echoed
		ORDER BY outer_rows.i, echoed.table_index
	)");
	REQUIRE_NO_FAIL(*mixed_correlation);
	REQUIRE(CHECK_COLUMN(mixed_correlation, 2, {0, Value(), 1, Value()}));
	REQUIRE(CHECK_COLUMN(mixed_correlation, 3, {Value(), 42, Value(), 42}));

	auto correlated_empty = con.Query(R"(
		SELECT outer_rows.i, echoed.table_index, echoed.row_value.arg_1.v
		FROM range(2) outer_rows(i), LATERAL multi_table_lateral_echo(
			(SELECT outer_rows.i AS v WHERE false), (SELECT outer_rows.i + 10 AS v)) echoed
		ORDER BY outer_rows.i, echoed.table_index
	)");
	REQUIRE_NO_FAIL(*correlated_empty);
	REQUIRE(CHECK_COLUMN(correlated_empty, 0, {0, 1}));
	REQUIRE(CHECK_COLUMN(correlated_empty, 1, {1, 1}));
	REQUIRE(CHECK_COLUMN(correlated_empty, 2, {10, 11}));
}

TEST_CASE("Multiple TABLE parameters enforce the UNION member limit", "[tablefunction]") {
	DuckDB db(nullptr);
	Connection con(db);
	MultiTableEcho::Register(con, "maximum_table_inputs",
	                         vector<LogicalType>(UnionType::MAX_UNION_MEMBERS, LogicalType::TABLE));
	MultiTableEcho::Register(con, "too_many_table_inputs",
	                         vector<LogicalType>(UnionType::MAX_UNION_MEMBERS + 1, LogicalType::TABLE));

	auto create_query = [](const string &name, idx_t table_count) {
		string query = "SELECT count(*) FROM " + name + "(";
		for (idx_t table_idx = 0; table_idx < table_count; table_idx++) {
			if (table_idx > 0) {
				query += ", ";
			}
			query += "(SELECT 1 AS i)";
		}
		return query + ")";
	};
	auto maximum = con.Query(create_query("maximum_table_inputs", UnionType::MAX_UNION_MEMBERS));
	REQUIRE_NO_FAIL(*maximum);
	REQUIRE(CHECK_COLUMN(maximum, 0, {static_cast<int64_t>(UnionType::MAX_UNION_MEMBERS)}));

	auto too_many = con.Query(create_query("too_many_table_inputs", UnionType::MAX_UNION_MEMBERS + 1));
	REQUIRE(too_many->HasError());
	REQUIRE(StringUtil::Contains(too_many->GetError(), "at most 255 are supported"));
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
