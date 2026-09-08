#include "duckdb/transaction/transaction_context.hpp"
#include "duckdb/logging/log_manager.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/common/operator/cast_operators.hpp"

namespace duckdb {

TransactionContext::TransactionContext(ClientContext &context)
    : context(context), auto_commit(true), invalidation_policy(TransactionInvalidationPolicy::STANDARD_POLICY),
      auto_rollback(false), current_transaction(nullptr) {
}

TransactionContext::~TransactionContext() {
	if (current_transaction) {
		try {
			Rollback(nullptr);
		} catch (std::exception &ex) {
			ErrorData data(ex);
			try {
				DUCKDB_LOG_ERROR(context, "TransactionContext::~TransactionContext()\t\t" + data.Message());
			} catch (...) { // NOLINT
			}
		} catch (...) { // NOLINT
		}
	}
}

void TransactionContext::BeginTransaction() {
	if (current_transaction) {
		throw TransactionException("cannot start a transaction within a transaction");
	}
	auto start_timestamp = Timestamp::GetCurrentTimestamp();
	auto global_transaction_id = context.db->GetDatabaseManager().GetNewTransactionNumber();
	{
		lock_guard<mutex> guard(transaction_lock);
		current_transaction = make_uniq<MetaTransaction>(context, start_timestamp, global_transaction_id);
	}

	// Notify any registered state of transaction begin
	for (auto &state : context.registered_state->States()) {
		state->TransactionBegin(*current_transaction, context);
	}
}

void TransactionContext::SetInvalidationPolicy(TransactionInvalidationPolicy new_invalidation_policy) {
	if (new_invalidation_policy == TransactionInvalidationPolicy::STANDARD_POLICY) {
		// if no policy is specified explicitly use the default one from the settings
		new_invalidation_policy = Settings::Get<DefaultTransactionInvalidationPolicySetting>(context);
	}
	invalidation_policy = new_invalidation_policy;
}

void TransactionContext::SetAutocheckpointError(ErrorData error) {
	autocheckpoint_error = std::move(error);
}

void TransactionContext::Commit() {
	if (!current_transaction) {
		throw TransactionException("failed to commit: no transaction active");
	}
	autocheckpoint_error = ErrorData();
	unique_ptr<MetaTransaction> transaction;
	{
		lock_guard<mutex> guard(transaction_lock);
		transaction = std::move(current_transaction);
	}
	ClearTransaction();
	auto error = transaction->Commit();
	// Notify any registered state of transaction commit
	if (error.HasError()) {
		for (auto const &s : context.registered_state->States()) {
			s->TransactionRollback(*transaction, context, error);
		}
		if (Exception::InvalidatesDatabase(error.Type()) || error.Type() == ExceptionType::INTERNAL) {
			// throw fatal / internal exceptions directly
			error.Throw();
		}
		throw TransactionException("Failed to commit: %s", error.RawMessage());
	}
	for (auto &state : context.registered_state->States()) {
		state->TransactionCommit(*transaction, context);
	}
	transaction->Finalize();
	if (autocheckpoint_error.HasError()) {
		auto err = std::move(autocheckpoint_error);
		autocheckpoint_error = ErrorData();
		err.Throw();
	}
}

void TransactionContext::SetAutoCommit(bool value) {
	auto_commit = value;
	if (!auto_commit && !current_transaction) {
		BeginTransaction();
	}
}

void TransactionContext::SetReadOnly() {
	current_transaction->SetReadOnly();
}

void TransactionContext::Rollback(optional_ptr<ErrorData> error) {
	if (!current_transaction) {
		throw TransactionException("failed to rollback: no transaction active");
	}
	unique_ptr<MetaTransaction> transaction;
	{
		lock_guard<mutex> guard(transaction_lock);
		transaction = std::move(current_transaction);
	}
	ClearTransaction();
	context.client_data->profiler->Reset();

	ErrorData rollback_error;
	try {
		transaction->Rollback();
	} catch (std::exception &ex) {
		rollback_error = ErrorData(ex);
	}
	// Notify any registered state of transaction rollback
	for (auto const &s : context.registered_state->States()) {
		s->TransactionRollback(*transaction, context, error);
	}
	if (rollback_error.HasError()) {
		rollback_error.Throw();
	}
	transaction->Finalize();
}

void TransactionContext::ClearTransaction() {
	SetAutoCommit(true);
	lock_guard<mutex> guard(transaction_lock);
	current_transaction = nullptr;
}

ForeignTransactionHandle TransactionContext::ForeignTransactionLookup(const Identifier &db_name) {
	lock_guard<mutex> guard(transaction_lock);
	if (!current_transaction) {
		return {};
	}
	return current_transaction->GetSharedDuckTransaction(db_name);
}

void TransactionContext::JoinTransaction(const string &transaction_id) {
	if (auto_commit || !current_transaction) {
		throw TransactionException("JOIN TRANSACTION can only be used inside an explicit transaction");
	}
	constexpr idx_t MAX_TRANSACTION_ID_LENGTH = 1024;
	if (transaction_id.empty()) {
		throw TransactionException("JOIN TRANSACTION requires a non-empty transaction id");
	}
	if (transaction_id.size() > MAX_TRANSACTION_ID_LENGTH) {
		throw TransactionException("JOIN TRANSACTION id exceeds the maximum length of %llu bytes",
		                           static_cast<uint64_t>(MAX_TRANSACTION_ID_LENGTH));
	}
	for (auto character : transaction_id) {
		auto byte = static_cast<unsigned char>(character);
		if (byte < 0x20 || byte == 0x7f) {
			throw TransactionException("JOIN TRANSACTION id contains a control character");
		}
	}

	// Split at the first slash so database names may themselves contain slashes.
	auto slash = transaction_id.find('/');
	if (slash == string::npos || slash == 0 || slash + 1 == transaction_id.size()) {
		throw TransactionException("Invalid transaction id '%s': expected '<connection_id>/<database_name>'",
		                           transaction_id);
	}
	auto connection_string = transaction_id.substr(0, slash);
	auto database_name = Identifier(transaction_id.substr(slash + 1));
	uint64_t raw_connection_id;
	if (!TryCast::Operation<string_t, uint64_t>(string_t(connection_string), raw_connection_id)) {
		throw TransactionException("Invalid transaction id '%s': connection id is not a number", transaction_id);
	}
	auto connection_id = static_cast<connection_t>(raw_connection_id);
	if (connection_id == context.GetConnectionId()) {
		throw TransactionException("Cannot join a transaction owned by the same connection");
	}

	auto owner = ConnectionManager::Get(context).FindByConnectionId(connection_id);
	if (!owner) {
		throw TransactionException("Invalid transaction id '%s': owning connection is no longer available",
		                           transaction_id);
	}
	auto handle = owner->transaction.ForeignTransactionLookup(database_name);
	if (!handle.transaction) {
		throw TransactionException("Invalid transaction id '%s': owning transaction is no longer available",
		                           transaction_id);
	}

	try {
		current_transaction->Adopt(*handle.database, handle.transaction);
	} catch (...) {
		// Complete the handoff if the owner detached while this lookup was in flight.
		shared_ptr<Transaction> transaction = std::move(handle.transaction);
		(void)transaction->Finalize(transaction, context, false);
		throw;
	}
}

idx_t TransactionContext::GetActiveQuery() {
	if (!current_transaction) {
		throw InternalException("GetActiveQuery called without active transaction");
	}
	return current_transaction->GetActiveQuery();
}

void TransactionContext::ResetActiveQuery() {
	if (current_transaction) {
		SetActiveQuery(MAXIMUM_QUERY_ID);
	}
}

void TransactionContext::SetActiveQuery(transaction_t query_number) {
	if (!current_transaction) {
		throw InternalException("SetActiveQuery called without active transaction");
	}
	current_transaction->SetActiveQuery(query_number);
}

} // namespace duckdb
