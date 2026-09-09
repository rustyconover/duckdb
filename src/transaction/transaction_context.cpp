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
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

namespace duckdb {

TransactionContext::TransactionContext(ClientContext &context)
    : context(context), auto_commit(true), invalidation_policy(TransactionInvalidationPolicy::STANDARD_POLICY),
      auto_rollback(false), current_transaction(nullptr) {
}

TransactionContext::~TransactionContext() {
	try {
		FinalizePendingTransactions();
	} catch (...) { // NOLINT
	}
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
	FinalizePendingTransactions();
	auto start_timestamp = Timestamp::GetCurrentTimestamp();
	auto global_transaction_id = context.db->GetDatabaseManager().GetNewTransactionNumber();
	current_transaction = make_uniq<MetaTransaction>(context, start_timestamp, global_transaction_id);

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
	auto transaction = std::move(current_transaction);
	auto shared_state = transaction->GetSharedTransactionState();
	shared_ptr<ClientContext> pending_context;
	if (shared_state) {
		pending_transactions.reserve(pending_transactions.size() + 1);
		shared_state->ReservePendingContext();
		pending_context = context.shared_from_this();
	}
	ClearTransaction();
	auto error = transaction->Commit();
	if (shared_state && !error.HasError()) {
		context.AddSharedTransactionPin();
		if (!shared_state->AddPendingContext(pending_context)) {
			context.RemoveSharedTransactionPin();
			if (shared_state->outcome.load() == SharedTransactionOutcome::ROLLED_BACK) {
				error = ErrorData(ExceptionType::TRANSACTION,
				                  "Cannot commit shared transaction: another connection has rolled back");
			}
		} else {
			PendingSharedTransaction pending;
			pending.transaction = std::move(transaction);
			pending.state = std::move(shared_state);
			pending_transactions.push_back(std::move(pending));
			return;
		}
	}
	FinalizePendingTransactions();
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

void TransactionContext::FinalizePendingTransactions() {
	for (idx_t i = 0; i < pending_transactions.size();) {
		auto outcome = pending_transactions[i].state->outcome.load();
		if (outcome == SharedTransactionOutcome::PENDING) {
			i++;
			continue;
		}
		auto transaction = std::move(pending_transactions[i].transaction);
		pending_transactions.erase_at(i);
		if (outcome == SharedTransactionOutcome::COMMITTED) {
			for (auto &state : context.registered_state->States()) {
				state->TransactionCommit(*transaction, context);
			}
		} else {
			ErrorData error(ExceptionType::TRANSACTION, "Shared transaction was rolled back by another participant");
			for (auto &state : context.registered_state->States()) {
				state->TransactionRollback(*transaction, context, error);
			}
		}
		transaction->Finalize();
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
	auto transaction = std::move(current_transaction);
	ClearTransaction();
	context.client_data->profiler->Reset();

	ErrorData rollback_error;
	try {
		transaction->Rollback();
	} catch (std::exception &ex) {
		rollback_error = ErrorData(ex);
	}
	FinalizePendingTransactions();
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
	current_transaction = nullptr;
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
	if (ValidChecker::IsInvalidated(*current_transaction)) {
		throw TransactionException("Cannot join a shared transaction from an invalidated transaction");
	}

	auto &database_manager = DatabaseManager::Get(context);
	auto database = database_manager.GetSharedTransactionDatabase(transaction_id);
	if (!database) {
		throw TransactionException("Shared transaction is no longer available");
	}
	if (ValidChecker::IsInvalidated(*database)) {
		throw TransactionException("Cannot join shared transaction: %s", ValidChecker::InvalidatedMessage(*database));
	}
	auto &transaction_manager = database->GetTransactionManager();
	if (!transaction_manager.IsDuckTransactionManager()) {
		throw TransactionException("Database '%s' does not support shared transactions", database->GetName());
	}
	auto &duck_manager = transaction_manager.Cast<DuckTransactionManager>();
	current_transaction->ValidateAdoption(*database);
	context.GuardSharedTransaction(duck_manager.GetSharedTransactionLock(transaction_id));
	auto &transaction = duck_manager.JoinTransaction(transaction_id);
	try {
		current_transaction->Adopt(*database, transaction);
	} catch (...) {
		duck_manager.CancelJoin(transaction);
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
