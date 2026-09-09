#include "catch.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/transaction/shared_transaction_lock.hpp"
#include "test_helpers.hpp"

#include <condition_variable>
#include <thread>

using namespace duckdb;

static string ShareTransaction(Connection &connection) {
	auto result = connection.Query("SELECT duckdb_share_transaction()");
	REQUIRE_NO_FAIL(*result);
	return result->GetValue(0, 0).GetValue<string>();
}

static void JoinTransaction(Connection &connection, const string &transaction_id) {
	REQUIRE_NO_FAIL(connection.Query("BEGIN"));
	REQUIRE_NO_FAIL(connection.Query("JOIN TRANSACTION '" + transaction_id + "'"));
}

struct SharedTransactionHookState : ClientContextState {
	idx_t commits = 0;
	idx_t rollbacks = 0;

	void TransactionCommit(MetaTransaction &, ClientContext &) override {
		commits++;
	}
	void TransactionRollback(MetaTransaction &, ClientContext &) override {
		rollbacks++;
	}
};

struct CaptureTransactionState {
	mutex lock;
	std::condition_variable signal;
	string token;
	bool captured = false;
	bool release = false;
};

static void RegisterCaptureTransactionFunction(Connection &connection,
                                               const shared_ptr<CaptureTransactionState> &capture) {
	ScalarFunction function("capture_shared_transaction", {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                        [capture](DataChunk &input, ExpressionState &, Vector &result) {
		                        auto token = input.GetValue(0, 0).GetValue<string>();
		                        {
			                        unique_lock<mutex> guard(capture->lock);
			                        capture->token = token;
			                        capture->captured = true;
			                        capture->signal.notify_all();
			                        capture->signal.wait(guard, [&]() { return capture->release; });
		                        }
		                        result.Reference(Value(token), count_t(input.size()));
	                        });
	CreateScalarFunctionInfo info(function);
	connection.context->RunFunctionInTransaction(
	    [&]() { Catalog::GetSystemCatalog(*connection.context).CreateFunction(*connection.context, info); });
}

TEST_CASE("Transactions can be shared between connections", "[api][join_transaction]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);
	Connection observer(database);

	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (1)"));
	auto transaction_id = ShareTransaction(owner);
	JoinTransaction(joiner, transaction_id);

	auto result = joiner.Query("SELECT value FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE_NO_FAIL(joiner.Query("INSERT INTO shared_values VALUES (2)"));
	result = owner.Query("SELECT value FROM shared_values ORDER BY value");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));

	// The first participant to commit only releases its reference.
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	result = observer.Query("SELECT count(*) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));

	// The last participant performs the storage-layer commit.
	REQUIRE_NO_FAIL(joiner.Query("COMMIT"));
	result = observer.Query("SELECT value FROM shared_values ORDER BY value");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
}

TEST_CASE("Participants can join after the exporter commits", "[api][join_transaction]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection first_joiner(database);
	Connection late_joiner(database);

	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (1)"));
	auto transaction_id = ShareTransaction(owner);
	JoinTransaction(first_joiner, transaction_id);
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));

	JoinTransaction(late_joiner, transaction_id);
	auto result = late_joiner.Query("SELECT value FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE_NO_FAIL(first_joiner.Query("COMMIT"));
	REQUIRE_NO_FAIL(late_joiner.Query("COMMIT"));
}

TEST_CASE("Rollback dooms a shared transaction", "[api][join_transaction]") {
	DuckDB database(nullptr);
	Connection setup(database);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value INTEGER)"));

	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (42)"));
	JoinTransaction(joiner, ShareTransaction(owner));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_FAIL(owner.Query("COMMIT"));

	auto result = setup.Query("SELECT count(*) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
}

TEST_CASE("Closing a participant rolls back a shared transaction", "[api][join_transaction]") {
	DuckDB database(nullptr);
	Connection setup(database);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value INTEGER)"));

	Connection joiner(database);
	{
		Connection owner(database);
		REQUIRE_NO_FAIL(owner.Query("BEGIN"));
		REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (42)"));
		JoinTransaction(joiner, ShareTransaction(owner));
	}
	REQUIRE_FAIL(joiner.Query("COMMIT"));

	auto result = setup.Query("SELECT count(*) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
}

TEST_CASE("Shared transaction statements are serialized", "[api][join_transaction]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	JoinTransaction(joiner, ShareTransaction(owner));

	atomic<bool> success {true};
	auto insert_values = [&success](Connection &connection, idx_t offset) {
		for (idx_t i = 0; i < 50; i++) {
			auto result = connection.Query("INSERT INTO shared_values VALUES (" + to_string(offset + i) + ")");
			if (result->HasError()) {
				success = false;
				return;
			}
		}
	};
	std::thread owner_thread(insert_values, std::ref(owner), 0);
	std::thread joiner_thread(insert_values, std::ref(joiner), 50);
	owner_thread.join();
	joiner_thread.join();
	REQUIRE(success);

	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	REQUIRE_NO_FAIL(joiner.Query("COMMIT"));
	auto result = owner.Query("SELECT count(*), count(DISTINCT value) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {100}));
	REQUIRE(CHECK_COLUMN(result, 1, {100}));
}

TEST_CASE("Shared transaction ids are stable and preserve catalog names", "[api][join_transaction]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);

	REQUIRE_NO_FAIL(owner.Query("ATTACH ':memory:' AS \"catalog/with/slash\""));
	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE \"catalog/with/slash\".main.values_table (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO \"catalog/with/slash\".main.values_table VALUES (7)"));
	auto result = owner.Query("SELECT duckdb_share_transaction('catalog/with/slash')");
	REQUIRE_NO_FAIL(*result);
	auto transaction_id = result->GetValue(0, 0).GetValue<string>();
	REQUIRE(ShareTransaction(owner) == transaction_id);
	JoinTransaction(joiner, transaction_id);
	REQUIRE(ShareTransaction(joiner) == transaction_id);

	result = joiner.Query("SELECT value FROM \"catalog/with/slash\".main.values_table");
	REQUIRE(CHECK_COLUMN(result, 0, {7}));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	REQUIRE_NO_FAIL(joiner.Query("COMMIT"));
}

TEST_CASE("Shared transactions use an explicit database boundary", "[api][join_transaction]") {
	DuckDB database(nullptr);
	Connection setup(database);
	REQUIRE_NO_FAIL(setup.Query("ATTACH ':memory:' AS database_a"));
	REQUIRE_NO_FAIL(setup.Query("ATTACH ':memory:' AS database_b"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE database_a.main.values_table (value INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE database_b.main.values_table (value INTEGER)"));

	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("SELECT * FROM database_a.main.values_table"));
	REQUIRE_NO_FAIL(owner.Query("SELECT * FROM database_b.main.values_table"));
	auto result = owner.Query("SELECT duckdb_share_transaction('database_a')");
	REQUIRE_NO_FAIL(*result);
	auto transaction_id = result->GetValue(0, 0).GetValue<string>();
	JoinTransaction(joiner, transaction_id);
	REQUIRE_NO_FAIL(joiner.Query("INSERT INTO database_a.main.values_table VALUES (42)"));
	REQUIRE_FAIL(joiner.Query("INSERT INTO database_b.main.values_table VALUES (84)"));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_FAIL(owner.Query("COMMIT"));

	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO database_b.main.values_table VALUES (84)"));
	REQUIRE_FAIL(owner.Query("SELECT duckdb_share_transaction('database_a')"));
	REQUIRE_NO_FAIL(owner.Query("ROLLBACK"));

	Connection ambiguous(database);
	REQUIRE_NO_FAIL(ambiguous.Query("BEGIN"));
	REQUIRE_NO_FAIL(ambiguous.Query("SELECT * FROM database_a.main.values_table"));
	REQUIRE_NO_FAIL(ambiguous.Query("SELECT * FROM database_b.main.values_table"));
	REQUIRE_FAIL(ambiguous.Query("SELECT duckdb_share_transaction()"));
	REQUIRE_NO_FAIL(ambiguous.Query("ROLLBACK"));
}

TEST_CASE("Sharing occurs when the function executes", "[api][join_transaction]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);

	// Binding and explaining the function must not require or export a transaction.
	auto prepared = owner.Prepare("SELECT duckdb_share_transaction() FROM range(4097)");
	REQUIRE(!prepared->HasError());
	REQUIRE_NO_FAIL(owner.Query("EXPLAIN SELECT duckdb_share_transaction()"));

	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	auto result = prepared->Execute();
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetResultType() == QueryResultType::MATERIALIZED_RESULT);
	auto chunk = result->Fetch();
	REQUIRE(chunk);
	auto first_id = chunk->GetValue(0, 0).GetValue<string>();
	result.reset();
	JoinTransaction(joiner, first_id);
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	REQUIRE_NO_FAIL(joiner.Query("COMMIT"));

	// A new execution must export the current transaction instead of returning a cached capability.
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	result = prepared->Execute();
	REQUIRE_NO_FAIL(*result);
	chunk = result->Fetch();
	REQUIRE(chunk);
	auto second_id = chunk->GetValue(0, 0).GetValue<string>();
	result.reset();
	REQUIRE(second_id != first_id);
	JoinTransaction(joiner, second_id);
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	REQUIRE_NO_FAIL(joiner.Query("COMMIT"));
}

TEST_CASE("Shared transaction ids are exact capabilities and expire", "[api][join_transaction]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);

	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (1)"));
	auto transaction_id = ShareTransaction(owner);
	auto tampered_id = transaction_id;
	tampered_id[0] = tampered_id[0] == '0' ? '1' : '0';
	REQUIRE_NO_FAIL(joiner.Query("BEGIN"));
	REQUIRE_FAIL(joiner.Query("JOIN TRANSACTION '" + tampered_id + "'"));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));

	// Starting another transaction against the same database must not revive the old capability.
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("SELECT count(*) FROM shared_values"));
	REQUIRE_NO_FAIL(joiner.Query("BEGIN"));
	REQUIRE_FAIL(joiner.Query("JOIN TRANSACTION '" + transaction_id + "'"));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("ROLLBACK"));
}

TEST_CASE("JOIN TRANSACTION replaces an unused local DuckTransaction", "[api][join_transaction]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);

	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (42)"));
	auto transaction_id = ShareTransaction(owner);

	REQUIRE_NO_FAIL(joiner.Query("BEGIN"));
	REQUIRE_NO_FAIL(joiner.Query("SELECT count(*) FROM shared_values"));
	REQUIRE_NO_FAIL(joiner.Query("JOIN TRANSACTION '" + transaction_id + "'"));
	auto result = joiner.Query("SELECT value FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	REQUIRE_NO_FAIL(joiner.Query("COMMIT"));
}

TEST_CASE("A meta transaction can only participate in one shared database", "[api][join_transaction]") {
	DuckDB database(nullptr);
	Connection setup(database);
	REQUIRE_NO_FAIL(setup.Query("ATTACH ':memory:' AS database_a"));
	REQUIRE_NO_FAIL(setup.Query("ATTACH ':memory:' AS database_b"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE database_a.main.values_table (value INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE database_b.main.values_table (value INTEGER)"));

	Connection owner_a(database);
	REQUIRE_NO_FAIL(owner_a.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner_a.Query("INSERT INTO database_a.main.values_table VALUES (1)"));
	auto transaction_a = ShareTransaction(owner_a);

	Connection owner_b(database);
	REQUIRE_NO_FAIL(owner_b.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner_b.Query("INSERT INTO database_b.main.values_table VALUES (2)"));
	auto transaction_b = ShareTransaction(owner_b);

	Connection joiner_a(database);
	JoinTransaction(joiner_a, transaction_a);
	REQUIRE_FAIL(joiner_a.Query("JOIN TRANSACTION '" + transaction_b + "'"));
	REQUIRE_NO_FAIL(joiner_a.Query("ROLLBACK"));
	REQUIRE_FAIL(owner_a.Query("COMMIT"));
	REQUIRE_NO_FAIL(owner_b.Query("COMMIT"));
}

TEST_CASE("A shared transaction outlives its originating connection", "[api][join_transaction]") {
	DuckDB database(nullptr);
	Connection setup(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value INTEGER)"));

	{
		Connection owner(database);
		REQUIRE_NO_FAIL(owner.Query("BEGIN"));
		REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (1)"));
		JoinTransaction(joiner, ShareTransaction(owner));
		REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	}

	REQUIRE_NO_FAIL(joiner.Query("INSERT INTO shared_values VALUES (2)"));
	REQUIRE_NO_FAIL(joiner.Query("COMMIT"));
	auto result = setup.Query("SELECT value FROM shared_values ORDER BY value");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
}

TEST_CASE("Closing the exporter preserves its context during a joiner statement", "[api][join_transaction]") {
	DuckDB database(nullptr);
	Connection setup(database);
	Connection joiner(database);
	auto capture = make_shared_ptr<CaptureTransactionState>();
	RegisterCaptureTransactionFunction(setup, capture);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("INSERT INTO shared_values VALUES (1)"));

	auto owner = make_uniq<Connection>(database);
	REQUIRE_NO_FAIL(owner->Query("BEGIN"));
	JoinTransaction(joiner, ShareTransaction(*owner));
	unique_ptr<QueryResult> joiner_result;
	std::thread joiner_thread([&]() {
		joiner_result = joiner.Query("SELECT capture_shared_transaction(CAST(value AS VARCHAR)) FROM shared_values");
	});
	{
		unique_lock<mutex> guard(capture->lock);
		REQUIRE(capture->signal.wait_for(guard, std::chrono::seconds(2), [&]() { return capture->captured; }));
	}

	// Closing the exporter requests rollback, but its context remains alive until the joiner leaves the statement.
	owner.reset();
	{
		lock_guard<mutex> guard(capture->lock);
		capture->release = true;
		capture->signal.notify_all();
	}
	joiner_thread.join();
	REQUIRE_NO_FAIL(*joiner_result);
	REQUIRE_FAIL(joiner.Query("SELECT 42"));
	REQUIRE_FAIL(joiner.Query("COMMIT"));
	auto result = setup.Query("SELECT value FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("Closing an exporter rolls back its later transaction", "[api][join_transaction]") {
	DuckDB database(nullptr);
	Connection setup(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE private_values (id INTEGER PRIMARY KEY, value INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("INSERT INTO private_values VALUES (1, 0)"));

	{
		Connection owner(database);
		REQUIRE_NO_FAIL(owner.Query("BEGIN"));
		REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (1)"));
		JoinTransaction(joiner, ShareTransaction(owner));
		REQUIRE_NO_FAIL(owner.Query("COMMIT"));

		REQUIRE_NO_FAIL(owner.Query("BEGIN"));
		REQUIRE_NO_FAIL(owner.Query("UPDATE private_values SET value = 1 WHERE id = 1"));
	}

	// The later transaction is rolled back as soon as the owner connection closes.
	REQUIRE_NO_FAIL(setup.Query("UPDATE private_values SET value = 2 WHERE id = 1"));
	REQUIRE_NO_FAIL(joiner.Query("COMMIT"));
	auto result = setup.Query("SELECT value FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = setup.Query("SELECT value FROM private_values");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
}

TEST_CASE("An explicitly read-only transaction remains read-only after JOIN", "[api][join_transaction]") {
	DuckDB database(nullptr);
	Connection setup(database);
	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN TRANSACTION READ ONLY"));
	auto transaction_id = ShareTransaction(owner);
	JoinTransaction(joiner, transaction_id);
	auto insert_result = joiner.Query("INSERT INTO shared_values VALUES (1)");
	REQUIRE_FAIL(insert_result);
	REQUIRE(insert_result->GetError().find("database \"memory\" - shared transaction is read-only") != string::npos);
	REQUIRE(insert_result->GetError().find("\"\"memory\"\"") == string::npos);
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_FAIL(owner.Query("COMMIT"));
	auto result = setup.Query("SELECT count(*) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
}

TEST_CASE("Doomed shared transactions reject joins and statements", "[api][join_transaction]") {
	DuckDB database(nullptr);
	Connection setup(database);
	Connection owner(database);
	Connection peer(database);
	Connection late_joiner(database);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	auto transaction_id = ShareTransaction(owner);
	JoinTransaction(peer, transaction_id);
	REQUIRE_NO_FAIL(peer.Query("ROLLBACK"));
	REQUIRE_FAIL(late_joiner.Query("BEGIN; JOIN TRANSACTION '" + transaction_id + "'"));
	REQUIRE_FAIL(owner.Query("INSERT INTO shared_values VALUES (1)"));
	REQUIRE_FAIL(owner.Query("COMMIT"));
	auto result = setup.Query("SELECT count(*) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
}

TEST_CASE("JOIN rejects an invalidated local transaction without affecting the exporter", "[api][join_transaction]") {
	DuckDB database(nullptr);
	Connection setup(database);
	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (1)"));
	auto transaction_id = ShareTransaction(owner);
	REQUIRE_NO_FAIL(joiner.Query("BEGIN"));
	REQUIRE_FAIL(joiner.Query("SELECT CAST('not an integer' AS INTEGER)"));
	REQUIRE_FAIL(joiner.Query("JOIN TRANSACTION '" + transaction_id + "'"));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	auto result = setup.Query("SELECT value FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("JOIN validates local state before registering a participant", "[api][join_transaction]") {
	DuckDB database(nullptr);
	Connection setup(database);
	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (1)"));
	auto transaction_id = ShareTransaction(owner);
	REQUIRE_NO_FAIL(joiner.Query("BEGIN"));
	REQUIRE_NO_FAIL(joiner.Query("INSERT INTO shared_values VALUES (2)"));
	REQUIRE_FAIL(joiner.Query("JOIN TRANSACTION '" + transaction_id + "'"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	auto result = setup.Query("SELECT value FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("Shared transaction capabilities survive database aliases", "[api][join_transaction]") {
	DuckDB database(nullptr);
	Connection setup(database);
	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(setup.Query("ATTACH ':memory:' AS original_name"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE original_name.main.shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO original_name.main.shared_values VALUES (1)"));
	auto result = owner.Query("SELECT duckdb_share_transaction('original_name')");
	REQUIRE_NO_FAIL(*result);
	auto transaction_id = result->GetValue(0, 0).GetValue<string>();
	REQUIRE_NO_FAIL(setup.Query("ALTER DATABASE original_name SET ALIAS TO renamed_database"));
	JoinTransaction(joiner, transaction_id);
	REQUIRE_NO_FAIL(joiner.Query("INSERT INTO renamed_database.main.shared_values VALUES (2)"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	REQUIRE_NO_FAIL(joiner.Query("COMMIT"));
	result = setup.Query("SELECT value FROM renamed_database.main.shared_values ORDER BY value");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
}

TEST_CASE("Shared transaction capabilities remain bound across detach and reattach", "[api][join_transaction]") {
	DuckDB database(nullptr);
	Connection setup(database);
	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(setup.Query("ATTACH ':memory:' AS shared_database"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_database.main.shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_database.main.shared_values VALUES (1)"));
	auto result = owner.Query("SELECT duckdb_share_transaction('shared_database')");
	REQUIRE_NO_FAIL(*result);
	auto transaction_id = result->GetValue(0, 0).GetValue<string>();
	REQUIRE_NO_FAIL(setup.Query("DETACH shared_database"));
	REQUIRE_NO_FAIL(setup.Query("ATTACH ':memory:' AS shared_database"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_database.main.shared_values (value INTEGER)"));
	JoinTransaction(joiner, transaction_id);
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	REQUIRE_NO_FAIL(joiner.Query("COMMIT"));
	result = setup.Query("SELECT count(*) FROM shared_database.main.shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
}

TEST_CASE("The exporting statement owns the shared statement lock before publishing", "[api][join_transaction]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);
	auto capture = make_shared_ptr<CaptureTransactionState>();
	RegisterCaptureTransactionFunction(owner, capture);
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(joiner.Query("BEGIN"));
	unique_ptr<QueryResult> owner_result;
	std::thread owner_thread(
	    [&]() { owner_result = owner.Query("SELECT capture_shared_transaction(duckdb_share_transaction())"); });
	{
		unique_lock<mutex> guard(capture->lock);
		REQUIRE(capture->signal.wait_for(guard, std::chrono::seconds(2), [&]() { return capture->captured; }));
	}
	atomic<bool> join_finished {false};
	unique_ptr<QueryResult> join_result;
	std::thread join_thread([&]() {
		join_result = joiner.Query("JOIN TRANSACTION '" + capture->token + "'");
		join_finished = true;
	});
	{
		unique_lock<mutex> guard(capture->lock);
		REQUIRE(
		    !capture->signal.wait_for(guard, std::chrono::milliseconds(50), [&]() { return join_finished.load(); }));
		capture->release = true;
		capture->signal.notify_all();
	}
	owner_thread.join();
	join_thread.join();
	REQUIRE_NO_FAIL(*owner_result);
	REQUIRE_NO_FAIL(*join_result);
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	REQUIRE_NO_FAIL(joiner.Query("COMMIT"));
}

TEST_CASE("Waiting for a shared statement lock is interruptible", "[api][join_transaction]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	JoinTransaction(joiner, ShareTransaction(owner));
	auto stream = owner.SendQuery("SELECT i FROM range(10000000) t(i)");
	REQUIRE(stream->GetResultType() == QueryResultType::STREAM_RESULT);
	atomic<bool> query_finished {false};
	unique_ptr<QueryResult> blocked_result;
	std::thread blocked_thread([&]() {
		blocked_result = joiner.Query("SELECT 42");
		query_finished = true;
	});
	for (idx_t i = 0; i < 100 && !query_finished.load(); i++) {
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	REQUIRE(!query_finished.load());
	joiner.Interrupt();
	blocked_thread.join();
	REQUIRE_FAIL(blocked_result);
	stream->Cast<StreamQueryResult>().Close();
	stream.reset();
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_FAIL(owner.Query("COMMIT"));
}

TEST_CASE("Shared transaction locks can be released by another thread", "[api][join_transaction]") {
	auto statement_lock = make_shared_ptr<SharedTransactionLock>();
	atomic<bool> acquired {false};
	std::thread worker([&]() { acquired = statement_lock->TryLockFor(std::chrono::seconds(1)); });
	worker.join();
	REQUIRE(acquired.load());
	statement_lock->Unlock();
	REQUIRE(statement_lock->TryLockFor(std::chrono::seconds(1)));
	statement_lock->Unlock();
}

TEST_CASE("Non-final commit hooks wait for the shared outcome", "[api][join_transaction]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);
	auto hook_state = make_shared_ptr<SharedTransactionHookState>();
	owner.context->registered_state->Insert("shared_transaction_hooks", hook_state);
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	JoinTransaction(joiner, ShareTransaction(owner));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	REQUIRE(hook_state->commits == 0);
	REQUIRE(hook_state->rollbacks == 0);
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	owner.context->transaction.FinalizePendingTransactions();
	REQUIRE(hook_state->commits == 0);
	REQUIRE(hook_state->rollbacks == 1);
}

TEST_CASE("Non-final commit hooks receive the eventual commit", "[api][join_transaction]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);
	auto hook_state = make_shared_ptr<SharedTransactionHookState>();
	owner.context->registered_state->Insert("shared_transaction_hooks", hook_state);
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	JoinTransaction(joiner, ShareTransaction(owner));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	REQUIRE(hook_state->commits == 0);
	REQUIRE_NO_FAIL(joiner.Query("COMMIT"));
	owner.context->transaction.FinalizePendingTransactions();
	REQUIRE(hook_state->commits == 1);
	REQUIRE(hook_state->rollbacks == 0);
}

TEST_CASE("A later connection secret survives an earlier shared rollback", "[api][join_transaction]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(owner.Query("CREATE SECRET shared_secret IN connection (TYPE http, SCOPE 'http://original')"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(
	    owner.Query("CREATE OR REPLACE SECRET shared_secret IN connection (TYPE http, SCOPE 'http://shared')"));
	JoinTransaction(joiner, ShareTransaction(owner));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(
	    owner.Query("CREATE OR REPLACE SECRET shared_secret IN connection (TYPE http, SCOPE 'http://later')"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	auto result = owner.Query("SELECT scope[1] FROM duckdb_secrets() WHERE name = 'shared_secret'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value("http://later")}));
}

TEST_CASE("Connection secret undo folds an earlier shared rollback", "[api][join_transaction]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(owner.Query("CREATE SECRET shared_secret IN connection (TYPE http, SCOPE 'http://original')"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(
	    owner.Query("CREATE OR REPLACE SECRET shared_secret IN connection (TYPE http, SCOPE 'http://shared')"));
	JoinTransaction(joiner, ShareTransaction(owner));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(
	    owner.Query("CREATE OR REPLACE SECRET shared_secret IN connection (TYPE http, SCOPE 'http://later')"));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("ROLLBACK"));
	auto result = owner.Query("SELECT scope[1] FROM duckdb_secrets() WHERE name = 'shared_secret'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value("http://original")}));
}
