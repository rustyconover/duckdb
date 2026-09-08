#include "duckdb/transaction/meta_transaction.hpp"

#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/secret/secret_storage.hpp"

namespace duckdb {

namespace {

shared_ptr<Transaction> WrapNonOwning(Transaction &transaction) {
	return shared_ptr<Transaction>(&transaction, [](Transaction *) {});
}

shared_ptr<Transaction> StartTransaction(AttachedDatabase &db, ClientContext &context) {
	auto &manager = db.GetTransactionManager();
	if (manager.IsDuckTransactionManager()) {
		return manager.Cast<DuckTransactionManager>().StartTransactionShared(context);
	}
	return WrapNonOwning(manager.StartTransaction(context));
}

} // namespace

MetaTransaction::MetaTransaction(ClientContext &context_p, timestamp_t start_timestamp_p,
                                 transaction_t transaction_id_p)
    : context(context_p), start_timestamp(start_timestamp_p), global_transaction_id(transaction_id_p),
      transaction_validity(*context_p.db), active_query(MAXIMUM_QUERY_ID), modified_database(nullptr),
      is_read_only(false) {
}

MetaTransaction::~MetaTransaction() = default;

MetaTransaction &MetaTransaction::Get(ClientContext &context) {
	return context.transaction.ActiveTransaction();
}

ValidChecker &ValidChecker::Get(MetaTransaction &transaction) {
	return transaction.transaction_validity;
}

Transaction &Transaction::Get(ClientContext &context, AttachedDatabase &db) {
	auto &meta_transaction = MetaTransaction::Get(context);
	return meta_transaction.GetTransaction(db);
}

optional_ptr<Transaction> Transaction::TryGet(ClientContext &context, AttachedDatabase &db) {
	auto &meta_transaction = MetaTransaction::Get(context);
	return meta_transaction.TryGetTransaction(db);
}

#ifdef DEBUG
static void VerifyAllTransactionsUnique(AttachedDatabase &db, vector<reference<AttachedDatabase>> &all_transactions) {
	for (auto &tx : all_transactions) {
		if (RefersToSameObject(db, tx.get())) {
			throw InternalException("Database is already present in all_transactions");
		}
	}
}
#endif

optional_ptr<Transaction> MetaTransaction::TryGetTransaction(AttachedDatabase &db) {
	lock_guard<mutex> guard(lock);
	auto entry = transactions.find(db);
	if (entry == transactions.end()) {
		return nullptr;
	}
	return entry->second.transaction.get();
}

Transaction &MetaTransaction::GetTransaction(AttachedDatabase &db) {
	if (ValidChecker::IsInvalidated(db)) {
		throw IOException("%s", ValidChecker::InvalidatedMessage(db));
	}
	lock_guard<mutex> guard(lock);
	auto entry = transactions.find(db);
	if (entry == transactions.end()) {
		auto new_transaction = StartTransaction(db, context);
		new_transaction->active_query = active_query.load();
#ifdef DEBUG
		VerifyAllTransactionsUnique(db, all_transactions);
#endif
		// Rollback looks every entry of all_transactions up in transactions, so the two must not get out of sync:
		// reserve first, then insert, so that a failing allocation happens before either is modified and the
		// push_back that follows cannot allocate.
		all_transactions.reserve(all_transactions.size() + 1);
		auto &result = *new_transaction;
		transactions.insert({reference<AttachedDatabase>(db), TransactionReference(std::move(new_transaction))});
		all_transactions.push_back(db);
		auto shared_db = db.shared_from_this();
		UseDatabase(shared_db);

		return result;
	}
	auto &transaction = entry->second.transaction;
	D_ASSERT(transaction.use_count() > 2 || transaction->active_query == active_query);
	return *transaction;
}

void MetaTransaction::RemoveTransaction(AttachedDatabase &db) {
	auto entry = transactions.find(db);
	if (entry == transactions.end()) {
		throw InternalException("MetaTransaction::RemoveTransaction called but meta transaction did not have a "
		                        "transaction for this database");
	}
	transactions.erase(entry);
	for (idx_t i = 0; i < all_transactions.size(); i++) {
		auto &db_entry = all_transactions[i];
		if (RefersToSameObject(db_entry.get(), db)) {
			all_transactions.erase_at(i);
			break;
		}
	}
}

void MetaTransaction::SetReadOnly() {
	if (modified_database) {
		throw InternalException("Cannot set MetaTransaction to read only - modifications have already been made");
	}
	this->is_read_only = true;
}

bool MetaTransaction::IsReadOnly() const {
	return is_read_only;
}

Transaction &Transaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog.GetAttached());
}

void MetaTransaction::Adopt(AttachedDatabase &db, shared_ptr<Transaction> transaction) {
	D_ASSERT(transaction);
	lock_guard<mutex> guard(lock);
	if (transactions.find(db) != transactions.end()) {
		throw TransactionException("Cannot join transaction for database '%s': this connection already has a "
		                           "transaction open against that database",
		                           db.GetName());
	}
#ifdef DEBUG
	VerifyAllTransactionsUnique(db, all_transactions);
#endif
	auto shared_db = db.shared_from_this();
	UseDatabase(shared_db);
	all_transactions.reserve(all_transactions.size() + 1);
	transactions.insert({reference<AttachedDatabase>(db), TransactionReference(std::move(transaction))});
	all_transactions.push_back(db);
}

ForeignTransactionHandle MetaTransaction::GetSharedDuckTransaction(const Identifier &db_name) {
	lock_guard<mutex> guard(lock);
	for (auto &db_ref : all_transactions) {
		auto &db = db_ref.get();
		if (db.GetName() != db_name) {
			continue;
		}
		auto entry = transactions.find(db);
		if (entry == transactions.end() || !entry->second.transaction ||
		    !entry->second.transaction->IsDuckTransaction()) {
			return {};
		}
		ForeignTransactionHandle result;
		result.database = db.shared_from_this();
		result.transaction = shared_ptr_cast<Transaction, DuckTransaction>(entry->second.transaction);
		return result;
	}
	return {};
}

bool MetaTransaction::IsParticipatingInSharedTransaction() {
	lock_guard<mutex> guard(lock);
	for (auto &entry : transactions) {
		auto &transaction = entry.second.transaction;
		if (!transaction || !transaction->IsDuckTransaction()) {
			continue;
		}
		if (transaction.use_count() > 2 || transaction->Cast<DuckTransaction>().RollbackRequested()) {
			return true;
		}
	}
	return false;
}

ErrorData MetaTransaction::FinalizeAll(bool rollback) {
	ErrorData error;
	vector<reference<AttachedDatabase>> databases;
	vector<shared_ptr<Transaction>> transaction_handles;
	{
		lock_guard<mutex> guard(lock);
		for (idx_t i = all_transactions.size(); i > 0; i--) {
			auto &db = all_transactions[i - 1].get();
			auto entry = transactions.find(db);
			if (entry == transactions.end()) {
				throw InternalException("Could not find transaction corresponding to database in MetaTransaction");
			}
			if (entry->second.state != TransactionState::UNCOMMITTED) {
				continue;
			}
			databases.emplace_back(db);
			transaction_handles.push_back(std::move(entry->second.transaction));
		}
	}
#ifdef DEBUG
	reference_set_t<AttachedDatabase> committed_tx;
#endif
	for (idx_t i = 0; i < databases.size(); i++) {
		auto &db = databases[i].get();
		auto transaction = std::move(transaction_handles[i]);

#ifdef DEBUG
		auto already_committed = committed_tx.insert(db).second == false;
		if (already_committed) {
			throw InternalException("All databases inside all_transactions should be unique, invariant broken!");
		}
#endif

		bool invalidated = ValidChecker::IsInvalidated(db);
		if (invalidated) {
			error.Merge(ErrorData(IOException("%s", ValidChecker::InvalidatedMessage(db))));
		}
		auto vote_rollback = rollback || invalidated || error.HasError();
		auto finalize_error = transaction->Finalize(transaction, context, vote_rollback);
		if (finalize_error.HasError()) {
			error.Merge(finalize_error);
		}
		lock_guard<mutex> guard(lock);
		auto entry = transactions.find(db);
		if (entry != transactions.end()) {
			entry->second.state = (vote_rollback || finalize_error.HasError()) ? TransactionState::ROLLED_BACK
			                                                                   : TransactionState::COMMITTED;
		}
	}
	return error;
}

ErrorData MetaTransaction::Commit() {
	return FinalizeAll(false);
}

void MetaTransaction::Rollback() {
	auto error = FinalizeAll(true);
	if (error.HasError()) {
		error.Throw();
	}
}

void MetaTransaction::Finalize() {
	// Try to checkpoint any attached databases potentially still held by this transaction.
	for (auto &database : referenced_databases) {
		// If the use count is down to one, then we already detached the database.
		// That means new transactions can no longer obtain a shared pointer to it.
		AttachedDatabase::InvokeCloseIfLastReference(database.second, context);
	}
}

idx_t MetaTransaction::GetActiveQuery() {
	return active_query;
}

void MetaTransaction::SetActiveQuery(transaction_t query_number) {
	lock_guard<mutex> guard(lock);
	active_query = query_number;
	for (auto &entry : transactions) {
		auto &transaction = entry.second.transaction;
		if (!transaction) {
			continue;
		}
		if (transaction->IsDuckTransaction() && transaction.use_count() > 2) {
			continue;
		}
		transaction->active_query = query_number;
	}
}

optional_ptr<AttachedDatabase> MetaTransaction::GetReferencedDatabase(const Identifier &name) {
	lock_guard<mutex> guard(referenced_database_lock);
	auto entry = used_databases.find(name);
	if (entry != used_databases.end()) {
		return entry->second.get();
	}
	return nullptr;
}

shared_ptr<AttachedDatabase> MetaTransaction::GetReferencedDatabaseOwning(const Identifier &name) {
	lock_guard<mutex> guard(referenced_database_lock);
	for (auto &entry : referenced_databases) {
		if (entry.first.get().name == name) {
			return entry.second;
		}
	}
	return nullptr;
}

void MetaTransaction::DetachDatabase(AttachedDatabase &database) {
	lock_guard<mutex> guard(referenced_database_lock);
	used_databases.erase(database.GetName());
}

AttachedDatabase &MetaTransaction::UseDatabase(shared_ptr<AttachedDatabase> &database) {
	auto &db_ref = *database;
	lock_guard<mutex> guard(referenced_database_lock);
	auto entry = referenced_databases.find(db_ref);
	if (entry == referenced_databases.end()) {
		auto used_entry = used_databases.emplace(db_ref.GetName(), db_ref);
		if (!used_entry.second) {
			// return used_entry.first->second.get();
			throw InternalException(
			    "Database name %s was already used by a different database for this meta transaction",
			    db_ref.GetName());
		}
		referenced_databases.emplace(reference<AttachedDatabase>(db_ref), database);
	}
	return db_ref;
}

void MetaTransaction::ModifyDatabase(AttachedDatabase &db, DatabaseModificationType modification) {
	if (IsReadOnly()) {
		throw TransactionException("Cannot write to database \"%s\" - transaction is launched in read-only mode",
		                           db.GetName());
	}
	auto &transaction = GetTransaction(db);
	if (transaction.IsReadOnly()) {
		transaction.SetReadWrite();
	}
	transaction.SetModifications(modification);
	if (db.IsSystem() || db.IsTemporary()) {
		// we can always modify the system and temp databases
		return;
	}
	if (!modified_database) {
		modified_database = &db;
		return;
	}
	if (&db != modified_database.get()) {
		throw TransactionException(
		    "Attempting to write to database %s in a transaction that has already modified database %s - a "
		    "single transaction can only write to a single attached database.",
		    db.GetName(), modified_database->GetName());
	}
}

} // namespace duckdb
