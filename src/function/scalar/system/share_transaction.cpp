#include "duckdb/function/scalar/system_functions.hpp"

#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

namespace duckdb {

namespace {

struct ShareTransactionData : FunctionData {
	explicit ShareTransactionData(string transaction_id_p) : transaction_id(std::move(transaction_id_p)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<ShareTransactionData>(transaction_id);
	}

	bool Equals(const FunctionData &other_p) const override {
		return transaction_id == other_p.Cast<ShareTransactionData>().transaction_id;
	}

	string transaction_id;
};

unique_ptr<FunctionData> ShareTransactionBind(BindScalarFunctionInput &input) {
	auto &context = input.GetClientContext();
	if (!context.transaction.HasActiveTransaction() || context.transaction.IsAutoCommit()) {
		throw TransactionException("duckdb_share_transaction() must be called inside an explicit transaction");
	}
	auto &meta_transaction = context.transaction.ActiveTransaction();
	if (!meta_transaction.GetSharedTransactionId().empty()) {
		return make_uniq<ShareTransactionData>(meta_transaction.GetSharedTransactionId());
	}

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
	auto transaction_id = StringUtil::Format("%llu/%s", static_cast<uint64_t>(context.GetConnectionId()),
	                                         database->GetName().GetIdentifierName());
	meta_transaction.SetSharedTransactionId(transaction_id);
	return make_uniq<ShareTransactionData>(std::move(transaction_id));
}

void ShareTransactionFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &expression = state.expr.Cast<BoundFunctionExpression>();
	auto &data = expression.BindInfo()->Cast<ShareTransactionData>();
	result.Reference(Value(data.transaction_id), count_t(input.size()));
}

} // namespace

ScalarFunction ShareTransactionFun::GetFunction() {
	return ScalarFunction({}, LogicalType::VARCHAR, ShareTransactionFunction, ShareTransactionBind, nullptr, nullptr,
	                      LogicalType(LogicalTypeId::INVALID), FunctionStability::VOLATILE);
}

} // namespace duckdb
