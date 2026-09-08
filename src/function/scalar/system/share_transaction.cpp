#include "duckdb/function/scalar/system_functions.hpp"

#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

namespace duckdb {

namespace {

struct ShareTransactionLocalState : FunctionLocalState {
	explicit ShareTransactionLocalState(string transaction_id_p) : transaction_id(std::move(transaction_id_p)) {
	}

	string transaction_id;
};

unique_ptr<FunctionLocalState> ShareTransactionInit(ExpressionState &state, const BoundFunctionExpression &,
                                                    FunctionData *) {
	auto &context = state.GetContext();
	if (!context.transaction.HasActiveTransaction() || context.transaction.IsAutoCommit()) {
		throw TransactionException("duckdb_share_transaction() must be called inside an explicit transaction");
	}
	auto &meta_transaction = context.transaction.ActiveTransaction();
	optional_ptr<AttachedDatabase> database = meta_transaction.ModifiedDatabase();
	if (!database) {
		auto &database_manager = DatabaseManager::Get(context);
		auto name = database_manager.GetDefaultDatabase(context);
		auto default_database = database_manager.GetDatabase(context, name);
		if (!default_database) {
			throw TransactionException("duckdb_share_transaction(): default database '%s' does not exist", name);
		}
		database = default_database.get();
	}
	auto &transaction = meta_transaction.GetTransaction(*database);
	if (!transaction.IsDuckTransaction()) {
		throw TransactionException("Database '%s' does not support shared transactions", database->GetName());
	}
	auto &duck_transaction = transaction.Cast<DuckTransaction>();
	shared_ptr<DuckTransaction> handle;
	auto token = duck_transaction.GetTransactionManager().ShareTransaction(duck_transaction, handle);
	meta_transaction.SetSharedTransaction(*database, std::move(handle));
	auto transaction_id = StringUtil::Format("%s/%s", token, database->GetName().GetIdentifierName());
	return make_uniq<ShareTransactionLocalState>(std::move(transaction_id));
}

void ShareTransactionFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &data = ExecuteFunctionState::GetFunctionState(state)->Cast<ShareTransactionLocalState>();
	result.Reference(Value(data.transaction_id), count_t(input.size()));
}

} // namespace

ScalarFunction ShareTransactionFun::GetFunction() {
	ScalarFunction function({}, LogicalType::VARCHAR, ShareTransactionFunction, nullptr, nullptr, ShareTransactionInit);
	function.SetVolatile();
	function.SetFallible();
	function.SetRequiresOrderedExecution(true);
	return function;
}

} // namespace duckdb
