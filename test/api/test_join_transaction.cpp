#include "catch.hpp"
#include "duckdb/main/connection.hpp"
#include "test_helpers.hpp"

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
	auto transaction_id = ShareTransaction(owner);
	REQUIRE(ShareTransaction(owner) == transaction_id);
	JoinTransaction(joiner, transaction_id);

	auto result = joiner.Query("SELECT value FROM \"catalog/with/slash\".main.values_table");
	REQUIRE(CHECK_COLUMN(result, 0, {7}));
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

TEST_CASE("JOIN TRANSACTION validates its context and id", "[api][join_transaction]") {
	DuckDB database(nullptr);
	Connection connection(database);
	REQUIRE_FAIL(connection.Query("SELECT duckdb_share_transaction()"));
	REQUIRE_FAIL(connection.Query("JOIN TRANSACTION '1/memory'"));
	REQUIRE_NO_FAIL(connection.Query("BEGIN"));
	REQUIRE_FAIL(connection.Query("JOIN TRANSACTION ''"));
	REQUIRE_FAIL(connection.Query("JOIN TRANSACTION 'invalid'"));
	REQUIRE_FAIL(connection.Query("JOIN TRANSACTION 'abc/memory'"));
	REQUIRE_NO_FAIL(connection.Query("ROLLBACK"));
}

TEST_CASE("Shared transactions use stable lock ordering across databases", "[api][join_transaction]") {
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
	REQUIRE_NO_FAIL(joiner_a.Query("JOIN TRANSACTION '" + transaction_b + "'"));
	Connection joiner_b(database);
	JoinTransaction(joiner_b, transaction_b);
	REQUIRE_NO_FAIL(joiner_b.Query("JOIN TRANSACTION '" + transaction_a + "'"));

	atomic<bool> success {true};
	auto query_both = [&success](Connection &connection) {
		for (idx_t i = 0; i < 20; i++) {
			auto result = connection.Query("SELECT (SELECT count(*) FROM database_a.main.values_table) + "
			                               "(SELECT count(*) FROM database_b.main.values_table)");
			if (result->HasError() || result->GetValue(0, 0).GetValue<int64_t>() != 2) {
				success = false;
				return;
			}
		}
	};
	std::thread thread_a(query_both, std::ref(joiner_a));
	std::thread thread_b(query_both, std::ref(joiner_b));
	thread_a.join();
	thread_b.join();
	REQUIRE(success);

	REQUIRE_NO_FAIL(joiner_a.Query("COMMIT"));
	REQUIRE_NO_FAIL(joiner_b.Query("COMMIT"));
	REQUIRE_NO_FAIL(owner_a.Query("COMMIT"));
	REQUIRE_NO_FAIL(owner_b.Query("COMMIT"));
}
