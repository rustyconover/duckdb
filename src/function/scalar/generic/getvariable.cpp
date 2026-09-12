#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

namespace duckdb {

namespace {
struct GetVariableBindData : FunctionData {
	explicit GetVariableBindData(Value value_p) : value(std::move(value_p)) {
	}

	Value value;

	bool Equals(const FunctionData &other_p) const override {
		const auto &other = other_p.Cast<GetVariableBindData>();
		return Value::NotDistinctFrom(value, other.value);
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<GetVariableBindData>(value);
	}
};

unique_ptr<FunctionData> GetVariableBindInternal(BindScalarFunctionInput &input, bool internal) {
	auto &context = input.GetClientContext();
	auto &arguments = input.GetArguments();
	auto &function = input.GetBoundFunction();

	if (arguments[0]->GetReturnType().id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}
	auto variable_name = input.GetConstant(0);
	Value value;
	if (!variable_name.IsNull()) {
		auto &config = ClientConfig::GetConfig(context);
		const auto identifier = Identifier(variable_name.ToString());
		if (internal) {
			config.GetInternalVariable(identifier, value);
		} else {
			config.GetUserVariable(identifier, value);
		}
	}
	function.SetReturnType(value.type());
	return make_uniq<GetVariableBindData>(std::move(value));
}

unique_ptr<FunctionData> GetVariableBind(BindScalarFunctionInput &input) {
	return GetVariableBindInternal(input, false);
}

unique_ptr<FunctionData> GetInternalVariableBind(BindScalarFunctionInput &input) {
	return GetVariableBindInternal(input, true);
}

unique_ptr<Expression> BindGetVariableExpression(FunctionBindExpressionInput &input) {
	if (!input.bind_data) {
		// unknown type
		throw InternalException("input.bind_data should be set");
	}
	auto &bind_data = input.bind_data->Cast<GetVariableBindData>();
	// emit a constant expression
	return make_uniq<BoundConstantExpression>(bind_data.value);
}

} // namespace

ScalarFunction GetVariableFun::GetFunction() {
	ScalarFunction getvar("getvariable", {{"variable_name", LogicalType::VARCHAR}}, LogicalType::ANY, nullptr,
	                      GetVariableBind, nullptr);
	getvar.SetBindExpressionCallback(BindGetVariableExpression);
	return getvar;
}

ScalarFunction GetInternalVariableFun::GetFunction() {
	ScalarFunction getvar("__internal_getvariable", {{"variable_name", LogicalType::VARCHAR}}, LogicalType::ANY,
	                      nullptr, GetInternalVariableBind, nullptr);
	getvar.SetBindExpressionCallback(BindGetVariableExpression);
	return getvar;
}

} // namespace duckdb
