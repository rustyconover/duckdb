#include "catch.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/transaction/shared_transaction_lock.hpp"
#include "test_helpers.hpp"

#include <condition_variable>
#include <thread>

using namespace duckdb;

static string ExportSnapshot(Connection &connection) {
	auto result = connection.Query("SELECT duckdb_export_snapshot()");
	REQUIRE_NO_FAIL(*result);
	return result->GetValue(0, 0).GetValue<string>();
}

static void SetTransactionSnapshot(Connection &connection, const string &transaction_id) {
	REQUIRE_NO_FAIL(connection.Query("BEGIN"));
	REQUIRE_NO_FAIL(connection.Query("SET TRANSACTION SNAPSHOT '" + transaction_id + "'"));
}

struct CaptureTransactionState {
	mutex lock;
	std::condition_variable signal;
	string token;
	bool captured = false;
	bool release = false;
};

//! A scalar function that publishes its argument and then blocks until released, to hold a statement open.
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

static void ReleaseCapture(const shared_ptr<CaptureTransactionState> &capture) {
	lock_guard<mutex> guard(capture->lock);
	capture->release = true;
	capture->signal.notify_all();
}

static bool WaitForCapture(const shared_ptr<CaptureTransactionState> &capture) {
	unique_lock<mutex> guard(capture->lock);
	return capture->signal.wait_for(guard, std::chrono::seconds(5), [&]() { return capture->captured; });
}

//! A barrier that only releases once `target` statements are inside the shared gate at the same time.
struct ConcurrencyProbe {
	mutex lock;
	std::condition_variable signal;
	idx_t active = 0;
	idx_t peak = 0;
	idx_t target = 0;
	//! Bumped when a full set of statements meets at the barrier, releasing everyone waiting on that round.
	idx_t generation = 0;
	bool timed_out = false;
};

static void RegisterConcurrencyProbe(Connection &connection, const shared_ptr<ConcurrencyProbe> &probe) {
	ScalarFunction function("concurrency_probe", {LogicalType::BIGINT}, LogicalType::BIGINT,
	                        [probe](DataChunk &input, ExpressionState &, Vector &result) {
		                        {
			                        unique_lock<mutex> guard(probe->lock);
			                        probe->active++;
			                        probe->peak = MaxValue<idx_t>(probe->peak, probe->active);
			                        if (probe->active >= probe->target) {
				                        // The last arrival releases the whole round.
				                        probe->generation++;
				                        probe->signal.notify_all();
			                        } else {
				                        // Wait for this round to fill; if the gate serialized us this times out.
				                        auto round = probe->generation;
				                        if (!probe->signal.wait_for(guard, std::chrono::seconds(5),
				                                                    [&]() { return probe->generation != round; })) {
					                        probe->timed_out = true;
				                        }
			                        }
			                        probe->active--;
		                        }
		                        result.Reference(input.data[0]);
	                        });
	CreateScalarFunctionInfo info(function);
	connection.context->RunFunctionInTransaction(
	    [&]() { Catalog::GetSystemCatalog(*connection.context).CreateFunction(*connection.context, info); });
}

TEST_CASE("Transactions can be shared between connections", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);
	Connection observer(database);

	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (1)"));
	auto transaction_id = ExportSnapshot(owner);
	SetTransactionSnapshot(joiner, transaction_id);

	// The joiner sees the exporter's uncommitted rows; other connections do not.
	auto result = joiner.Query("SELECT value FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = observer.Query("SELECT count(*) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (2)"));
	result = joiner.Query("SELECT value FROM shared_values ORDER BY value");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));

	// Participants only read.
	auto write = joiner.Query("INSERT INTO shared_values VALUES (3)");
	REQUIRE_FAIL(write);
	REQUIRE(write->GetError().find("only the exporting connection can modify") != string::npos);

	// Only the exporter commits.
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	result = observer.Query("SELECT value FROM shared_values ORDER BY value");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));

	// The joiner is told the transaction has ended and detaches with ROLLBACK.
	auto ended = joiner.Query("SELECT count(*) FROM shared_values");
	REQUIRE_FAIL(ended);
	REQUIRE(ended->GetError().find("Shared transaction has ended") != string::npos);
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	result = joiner.Query("SELECT value FROM shared_values ORDER BY value");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
}

TEST_CASE("A participant's COMMIT only detaches", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);
	Connection observer(database);

	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (1)"));
	SetTransactionSnapshot(joiner, ExportSnapshot(owner));
	// The participant's own temporary changes commit; the shared transaction stays with the exporter.
	REQUIRE_NO_FAIL(joiner.Query("CREATE TEMP TABLE staged AS SELECT value FROM shared_values"));
	REQUIRE_NO_FAIL(joiner.Query("COMMIT"));
	auto result = joiner.Query("SELECT value FROM staged");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = observer.Query("SELECT count(*) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	result = joiner.Query("SELECT count(*) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));

	result = owner.Query("SELECT value FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	result = observer.Query("SELECT value FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("Exporter rollback ends the joiner's view", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection setup(database);
	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value INTEGER)"));

	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (42)"));
	SetTransactionSnapshot(joiner, ExportSnapshot(owner));
	auto result = joiner.Query("SELECT value FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE_NO_FAIL(owner.Query("ROLLBACK"));

	REQUIRE_FAIL(joiner.Query("SELECT value FROM shared_values"));
	// COMMIT and ROLLBACK both detach once the exporter has ended the transaction.
	REQUIRE_NO_FAIL(joiner.Query("COMMIT"));
	result = setup.Query("SELECT count(*) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
}

TEST_CASE("Closing the exporter rolls back a shared transaction", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection setup(database);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value INTEGER)"));

	Connection joiner(database);
	{
		Connection owner(database);
		REQUIRE_NO_FAIL(owner.Query("BEGIN"));
		REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (42)"));
		SetTransactionSnapshot(joiner, ExportSnapshot(owner));
		auto result = joiner.Query("SELECT value FROM shared_values");
		REQUIRE(CHECK_COLUMN(result, 0, {42}));
	}
	REQUIRE_FAIL(joiner.Query("SELECT 42"));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));

	auto result = setup.Query("SELECT count(*) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
}

TEST_CASE("Closing a joiner leaves the shared transaction intact", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection setup(database);
	Connection owner(database);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value INTEGER)"));

	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (1)"));
	auto transaction_id = ExportSnapshot(owner);
	{
		Connection joiner(database);
		SetTransactionSnapshot(joiner, transaction_id);
		auto result = joiner.Query("SELECT value FROM shared_values");
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
	}
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (2)"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	auto result = setup.Query("SELECT value FROM shared_values ORDER BY value");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
}

TEST_CASE("Closing the exporter waits for an in-flight joiner statement", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection setup(database);
	Connection joiner(database);
	auto capture = make_shared_ptr<CaptureTransactionState>();
	RegisterCaptureTransactionFunction(setup, capture);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("INSERT INTO shared_values VALUES (1)"));

	auto owner = make_uniq<Connection>(database);
	REQUIRE_NO_FAIL(owner->Query("BEGIN"));
	SetTransactionSnapshot(joiner, ExportSnapshot(*owner));
	unique_ptr<QueryResult> joiner_result;
	std::thread joiner_thread([&]() {
		joiner_result = joiner.Query("SELECT capture_shared_transaction(CAST(value AS VARCHAR)) FROM shared_values");
	});
	REQUIRE(WaitForCapture(capture));

	// Closing the exporter blocks until the joiner's statement releases the statement lock.
	atomic<bool> owner_closed {false};
	std::thread close_thread([&]() {
		owner.reset();
		owner_closed = true;
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(100));
	REQUIRE(!owner_closed.load());
	ReleaseCapture(capture);
	joiner_thread.join();
	close_thread.join();
	REQUIRE_NO_FAIL(*joiner_result);
	REQUIRE_FAIL(joiner.Query("SELECT 42"));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	auto result = setup.Query("SELECT value FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("Participant reads run concurrently and exclude the exporter", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection setup(database);
	Connection owner(database);
	Connection reader_a(database);
	Connection reader_b(database);
	auto capture = make_shared_ptr<CaptureTransactionState>();
	RegisterCaptureTransactionFunction(setup, capture);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value INTEGER)"));

	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values SELECT * FROM range(1000)"));
	auto transaction_id = ExportSnapshot(owner);
	SetTransactionSnapshot(reader_a, transaction_id);
	SetTransactionSnapshot(reader_b, transaction_id);

	// reader_a holds the statement lock shared for as long as its statement is blocked.
	unique_ptr<QueryResult> blocked_result;
	std::thread blocked_thread([&]() {
		blocked_result = reader_a.Query("SELECT capture_shared_transaction('token') FROM shared_values LIMIT 1");
	});
	REQUIRE(WaitForCapture(capture));

	// Another participant reads concurrently.
	auto result = reader_b.Query("SELECT count(*) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {1000}));

	// The exporter's write waits for the reader to finish.
	atomic<bool> write_finished {false};
	unique_ptr<QueryResult> write_result;
	std::thread write_thread([&]() {
		write_result = owner.Query("INSERT INTO shared_values VALUES (1000)");
		write_finished = true;
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(100));
	REQUIRE(!write_finished.load());
	ReleaseCapture(capture);
	blocked_thread.join();
	write_thread.join();
	REQUIRE_NO_FAIL(*blocked_result);
	REQUIRE_NO_FAIL(*write_result);

	result = reader_b.Query("SELECT count(*) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {1001}));
	REQUIRE_NO_FAIL(reader_a.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(reader_b.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	result = setup.Query("SELECT count(*) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {1001}));
}

TEST_CASE("Appender on a joiner is rejected", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	SetTransactionSnapshot(joiner, ExportSnapshot(owner));

	bool rejected = false;
	try {
		Appender appender(joiner, "shared_values");
		appender.AppendRow(int32_t(1));
		appender.Close();
	} catch (std::exception &ex) {
		rejected = string(ex.what()).find("only the exporting connection can modify") != string::npos;
	}
	REQUIRE(rejected);
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	auto result = owner.Query("SELECT count(*) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
}

TEST_CASE("Shared transaction ids are stable and preserve catalog names", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);

	REQUIRE_NO_FAIL(owner.Query("ATTACH ':memory:' AS \"catalog/with/slash\""));
	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE \"catalog/with/slash\".main.values_table (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO \"catalog/with/slash\".main.values_table VALUES (7)"));
	auto result = owner.Query("SELECT duckdb_export_snapshot('catalog/with/slash')");
	REQUIRE_NO_FAIL(*result);
	auto transaction_id = result->GetValue(0, 0).GetValue<string>();
	REQUIRE(ExportSnapshot(owner) == transaction_id);
	SetTransactionSnapshot(joiner, transaction_id);
	REQUIRE(ExportSnapshot(joiner) == transaction_id);

	result = joiner.Query("SELECT value FROM \"catalog/with/slash\".main.values_table");
	REQUIRE(CHECK_COLUMN(result, 0, {7}));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
}

TEST_CASE("Shared transactions use an explicit database boundary", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection setup(database);
	REQUIRE_NO_FAIL(setup.Query("ATTACH ':memory:' AS database_a"));
	REQUIRE_NO_FAIL(setup.Query("ATTACH ':memory:' AS database_b"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE database_a.main.values_table (value INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE database_b.main.values_table (value INTEGER)"));

	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO database_a.main.values_table VALUES (1)"));
	REQUIRE_NO_FAIL(owner.Query("SELECT * FROM database_b.main.values_table"));
	auto result = owner.Query("SELECT duckdb_export_snapshot('database_a')");
	REQUIRE_NO_FAIL(*result);
	auto transaction_id = result->GetValue(0, 0).GetValue<string>();
	SetTransactionSnapshot(joiner, transaction_id);
	result = joiner.Query("SELECT value FROM database_a.main.values_table");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	// The joiner's other databases follow the usual rules; the shared database is read-only for it.
	REQUIRE_NO_FAIL(joiner.Query("INSERT INTO database_b.main.values_table VALUES (84)"));
	REQUIRE_FAIL(joiner.Query("INSERT INTO database_a.main.values_table VALUES (42)"));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("ROLLBACK"));
	result = setup.Query("SELECT count(*) FROM database_b.main.values_table");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));

	// Sharing a database the exporter only reads is allowed.
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO database_b.main.values_table VALUES (84)"));
	REQUIRE_NO_FAIL(owner.Query("SELECT duckdb_export_snapshot('database_a')"));
	REQUIRE_NO_FAIL(owner.Query("ROLLBACK"));

	Connection ambiguous(database);
	REQUIRE_NO_FAIL(ambiguous.Query("BEGIN"));
	REQUIRE_NO_FAIL(ambiguous.Query("SELECT * FROM database_a.main.values_table"));
	REQUIRE_NO_FAIL(ambiguous.Query("SELECT * FROM database_b.main.values_table"));
	REQUIRE_FAIL(ambiguous.Query("SELECT duckdb_export_snapshot()"));
	REQUIRE_NO_FAIL(ambiguous.Query("ROLLBACK"));
}

TEST_CASE("Temporary and system databases cannot be shared", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection owner(database);
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("CREATE TEMP TABLE temp_values (value INTEGER)"));
	REQUIRE_FAIL(owner.Query("SELECT duckdb_export_snapshot('temp')"));
	REQUIRE_FAIL(owner.Query("SELECT duckdb_export_snapshot('system')"));
	REQUIRE_NO_FAIL(owner.Query("ROLLBACK"));
}

TEST_CASE("Sharing occurs when the function executes", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);

	// Binding and explaining the function must not require or export a transaction.
	auto prepared = owner.Prepare("SELECT duckdb_export_snapshot() FROM range(4097)");
	REQUIRE(!prepared->HasError());
	REQUIRE_NO_FAIL(owner.Query("EXPLAIN SELECT duckdb_export_snapshot()"));

	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	auto result = prepared->Execute();
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetResultType() == QueryResultType::MATERIALIZED_RESULT);
	auto chunk = result->Fetch();
	REQUIRE(chunk);
	auto first_id = chunk->GetValue(0, 0).GetValue<string>();
	result.reset();
	SetTransactionSnapshot(joiner, first_id);
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));

	// A new execution must export the current transaction instead of returning a cached capability.
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	result = prepared->Execute();
	REQUIRE_NO_FAIL(*result);
	chunk = result->Fetch();
	REQUIRE(chunk);
	auto second_id = chunk->GetValue(0, 0).GetValue<string>();
	result.reset();
	REQUIRE(second_id != first_id);
	SetTransactionSnapshot(joiner, second_id);
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
}

TEST_CASE("Shared transaction ids are exact capabilities and expire", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);

	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (1)"));
	auto transaction_id = ExportSnapshot(owner);
	auto tampered_id = transaction_id;
	tampered_id[0] = tampered_id[0] == '0' ? '1' : '0';
	REQUIRE_NO_FAIL(joiner.Query("BEGIN"));
	REQUIRE_FAIL(joiner.Query("SET TRANSACTION SNAPSHOT '" + tampered_id + "'"));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));

	// Starting another transaction against the same database must not revive the old capability.
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("SELECT count(*) FROM shared_values"));
	REQUIRE_NO_FAIL(joiner.Query("BEGIN"));
	REQUIRE_FAIL(joiner.Query("SET TRANSACTION SNAPSHOT '" + transaction_id + "'"));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("ROLLBACK"));
}

TEST_CASE("SET TRANSACTION SNAPSHOT must precede any use of the database", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);

	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (42)"));
	auto transaction_id = ExportSnapshot(owner);

	REQUIRE_NO_FAIL(joiner.Query("BEGIN"));
	REQUIRE_NO_FAIL(joiner.Query("SELECT count(*) FROM shared_values"));
	auto late = joiner.Query("SET TRANSACTION SNAPSHOT '" + transaction_id + "'");
	REQUIRE_FAIL(late);
	REQUIRE(late->GetError().find("must be executed before any statement") != string::npos);
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));

	// Statements that only touch other databases do not count.
	REQUIRE_NO_FAIL(joiner.Query("BEGIN"));
	REQUIRE_NO_FAIL(joiner.Query("CREATE TEMP TABLE staged (value INTEGER)"));
	REQUIRE_NO_FAIL(joiner.Query("SET TRANSACTION SNAPSHOT '" + transaction_id + "'"));
	auto result = joiner.Query("SELECT value FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
}

TEST_CASE("SET TRANSACTION SNAPSHOT validates local state before taking part", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection setup(database);
	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (1)"));
	auto transaction_id = ExportSnapshot(owner);

	// Local changes block the join.
	REQUIRE_NO_FAIL(joiner.Query("BEGIN"));
	REQUIRE_NO_FAIL(joiner.Query("INSERT INTO shared_values VALUES (2)"));
	REQUIRE_FAIL(joiner.Query("SET TRANSACTION SNAPSHOT '" + transaction_id + "'"));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));

	// An invalidated local transaction blocks the join.
	REQUIRE_NO_FAIL(joiner.Query("BEGIN"));
	REQUIRE_FAIL(joiner.Query("SELECT CAST('not an integer' AS INTEGER)"));
	REQUIRE_FAIL(joiner.Query("SET TRANSACTION SNAPSHOT '" + transaction_id + "'"));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));

	// Joining twice is rejected.
	SetTransactionSnapshot(joiner, transaction_id);
	REQUIRE_FAIL(joiner.Query("SET TRANSACTION SNAPSHOT '" + transaction_id + "'"));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));

	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	auto result = setup.Query("SELECT value FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("A meta transaction can only take part in one shared database", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection setup(database);
	REQUIRE_NO_FAIL(setup.Query("ATTACH ':memory:' AS database_a"));
	REQUIRE_NO_FAIL(setup.Query("ATTACH ':memory:' AS database_b"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE database_a.main.values_table (value INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE database_b.main.values_table (value INTEGER)"));

	Connection owner_a(database);
	REQUIRE_NO_FAIL(owner_a.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner_a.Query("INSERT INTO database_a.main.values_table VALUES (1)"));
	auto transaction_a = ExportSnapshot(owner_a);

	Connection owner_b(database);
	REQUIRE_NO_FAIL(owner_b.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner_b.Query("INSERT INTO database_b.main.values_table VALUES (2)"));
	auto transaction_b = ExportSnapshot(owner_b);

	Connection joiner_a(database);
	SetTransactionSnapshot(joiner_a, transaction_a);
	REQUIRE_FAIL(joiner_a.Query("SET TRANSACTION SNAPSHOT '" + transaction_b + "'"));
	REQUIRE_NO_FAIL(joiner_a.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner_a.Query("COMMIT"));
	REQUIRE_NO_FAIL(owner_b.Query("COMMIT"));
}

TEST_CASE("Participants cannot write regardless of the exporter's mode", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection setup(database);
	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN TRANSACTION READ ONLY"));
	SetTransactionSnapshot(joiner, ExportSnapshot(owner));
	auto insert_result = joiner.Query("INSERT INTO shared_values VALUES (1)");
	REQUIRE_FAIL(insert_result);
	REQUIRE(insert_result->GetError().find("only the exporting connection can modify") != string::npos);
	REQUIRE(insert_result->GetError().find("\"\"memory\"\"") == string::npos);
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	auto result = setup.Query("SELECT count(*) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
}

TEST_CASE("Shared transaction capabilities survive database aliases", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection setup(database);
	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(setup.Query("ATTACH ':memory:' AS original_name"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE original_name.main.shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO original_name.main.shared_values VALUES (1)"));
	auto result = owner.Query("SELECT duckdb_export_snapshot('original_name')");
	REQUIRE_NO_FAIL(*result);
	auto transaction_id = result->GetValue(0, 0).GetValue<string>();
	REQUIRE_NO_FAIL(setup.Query("ALTER DATABASE original_name SET ALIAS TO renamed_database"));
	SetTransactionSnapshot(joiner, transaction_id);
	result = joiner.Query("SELECT value FROM renamed_database.main.shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	result = setup.Query("SELECT value FROM renamed_database.main.shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("Shared transaction capabilities stay bound across detach and reattach", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection setup(database);
	Connection owner(database);
	Connection joiner(database);
	Connection late_joiner(database);
	REQUIRE_NO_FAIL(setup.Query("ATTACH ':memory:' AS shared_database"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_database.main.shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_database.main.shared_values VALUES (1)"));
	auto result = owner.Query("SELECT duckdb_export_snapshot('shared_database')");
	REQUIRE_NO_FAIL(*result);
	auto transaction_id = result->GetValue(0, 0).GetValue<string>();
	REQUIRE_NO_FAIL(setup.Query("DETACH shared_database"));
	REQUIRE_NO_FAIL(setup.Query("ATTACH ':memory:' AS shared_database"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_database.main.shared_values (value INTEGER)"));
	SetTransactionSnapshot(joiner, transaction_id);
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));

	// A connection that already resolved the new database under that name cannot adopt the old one.
	REQUIRE_NO_FAIL(late_joiner.Query("BEGIN"));
	REQUIRE_NO_FAIL(late_joiner.Query("SELECT count(*) FROM shared_database.main.shared_values"));
	auto join_result = late_joiner.Query("SET TRANSACTION SNAPSHOT '" + transaction_id + "'");
	REQUIRE_FAIL(join_result);
	REQUIRE(join_result->GetError().find("different attached database") != string::npos);
	REQUIRE_NO_FAIL(late_joiner.Query("ROLLBACK"));

	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	result = setup.Query("SELECT count(*) FROM shared_database.main.shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	REQUIRE_NO_FAIL(setup.Query("SELECT 42"));
}

TEST_CASE("The exporting statement owns the shared statement lock before publishing", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);
	auto capture = make_shared_ptr<CaptureTransactionState>();
	RegisterCaptureTransactionFunction(owner, capture);
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(joiner.Query("BEGIN"));
	unique_ptr<QueryResult> owner_result;
	std::thread owner_thread(
	    [&]() { owner_result = owner.Query("SELECT capture_shared_transaction(duckdb_export_snapshot())"); });
	REQUIRE(WaitForCapture(capture));
	atomic<bool> join_finished {false};
	unique_ptr<QueryResult> join_result;
	std::thread join_thread([&]() {
		join_result = joiner.Query("SET TRANSACTION SNAPSHOT '" + capture->token + "'");
		join_finished = true;
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	REQUIRE(!join_finished.load());
	ReleaseCapture(capture);
	owner_thread.join();
	join_thread.join();
	REQUIRE_NO_FAIL(*owner_result);
	REQUIRE_NO_FAIL(*join_result);
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
}

TEST_CASE("Waiting for a shared statement lock is interruptible", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	SetTransactionSnapshot(joiner, ExportSnapshot(owner));
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
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
}

TEST_CASE("Waiting for a shared statement lock honours max_execution_time", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	SetTransactionSnapshot(joiner, ExportSnapshot(owner));
	REQUIRE_NO_FAIL(joiner.Query("SET max_execution_time = 200"));
	auto stream = owner.SendQuery("SELECT i FROM range(10000000) t(i)");
	REQUIRE(stream->GetResultType() == QueryResultType::STREAM_RESULT);
	auto start = std::chrono::steady_clock::now();
	auto blocked_result = joiner.Query("SELECT 42");
	auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
	REQUIRE_FAIL(blocked_result);
	// The deadline is checked on every poll of the lock, not on the throttled interrupt path.
	REQUIRE(elapsed.count() < 1500);
	stream->Cast<StreamQueryResult>().Close();
	stream.reset();
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
}

TEST_CASE("A waiting exporter takes precedence over new readers", "[api][transaction_snapshot]") {
	SharedTransactionLock statement_lock;
	statement_lock.LockShared();
	atomic<bool> writer_acquired {false};
	std::thread writer([&]() {
		statement_lock.LockExclusive();
		writer_acquired = true;
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	REQUIRE(!writer_acquired.load());
	// A new reader must wait behind the writer even though a reader currently holds the lock.
	REQUIRE(!statement_lock.TryLockSharedFor(std::chrono::milliseconds(50)));
	statement_lock.UnlockShared();
	writer.join();
	REQUIRE(writer_acquired.load());
	statement_lock.UnlockExclusive();
	REQUIRE(statement_lock.TryLockSharedFor(std::chrono::milliseconds(50)));
	statement_lock.UnlockShared();
}

TEST_CASE("Shared transaction locks can be released by another thread", "[api][transaction_snapshot]") {
	auto statement_lock = make_shared_ptr<SharedTransactionLock>();
	atomic<bool> acquired {false};
	std::thread worker([&]() { acquired = statement_lock->TryLockExclusiveFor(std::chrono::seconds(1)); });
	worker.join();
	REQUIRE(acquired.load());
	statement_lock->UnlockExclusive();
	REQUIRE(statement_lock->TryLockSharedFor(std::chrono::seconds(1)));
	REQUIRE(statement_lock->TryLockSharedFor(std::chrono::seconds(1)));
	REQUIRE(!statement_lock->TryLockExclusiveFor(std::chrono::milliseconds(10)));
	statement_lock->UnlockShared();
	statement_lock->UnlockShared();
	statement_lock->LockExclusive();
	statement_lock->UnlockExclusive();
}

TEST_CASE("Participant lookups fail once the exporter has ended", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	SetTransactionSnapshot(joiner, ExportSnapshot(owner));
	auto &db = *DatabaseManager::Get(*joiner.context).GetDatabase(*joiner.context, Identifier("memory"));
	REQUIRE(Transaction::TryGet(*joiner.context, db));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	REQUIRE(!Transaction::TryGet(*joiner.context, db));
	REQUIRE_THROWS(Transaction::Get(*joiner.context, db));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
}

TEST_CASE("Destroying the exporter during unwinding waits for participant statements", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection setup(database);
	Connection joiner(database);
	auto capture = make_shared_ptr<CaptureTransactionState>();
	RegisterCaptureTransactionFunction(setup, capture);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("INSERT INTO shared_values VALUES (1)"));

	unique_ptr<QueryResult> joiner_result;
	std::thread joiner_thread;
	atomic<bool> released {false};
	std::thread release_thread;
	try {
		Connection owner(database);
		REQUIRE_NO_FAIL(owner.Query("BEGIN"));
		REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (2)"));
		SetTransactionSnapshot(joiner, ExportSnapshot(owner));
		joiner_thread = std::thread([&]() {
			joiner_result =
			    joiner.Query("SELECT capture_shared_transaction(CAST(value AS VARCHAR)) FROM shared_values");
		});
		REQUIRE(WaitForCapture(capture));
		release_thread = std::thread([&]() {
			std::this_thread::sleep_for(std::chrono::milliseconds(200));
			released = true;
			ReleaseCapture(capture);
		});
		// Unwinding destroys the owner while the joiner's statement is still running.
		throw std::runtime_error("unwind");
	} catch (std::runtime_error &) {
	}
	// The owner's destruction had to wait for the release.
	REQUIRE(released.load());
	joiner_thread.join();
	release_thread.join();
	REQUIRE_NO_FAIL(*joiner_result);
	REQUIRE_FAIL(joiner.Query("SELECT 42"));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	auto result = setup.Query("SELECT value FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("Joiner temporary changes roll back on detach", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (42)"));
	SetTransactionSnapshot(joiner, ExportSnapshot(owner));
	REQUIRE_NO_FAIL(joiner.Query("CREATE TEMP TABLE staged AS SELECT value FROM shared_values"));
	auto result = joiner.Query("SELECT value FROM staged");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_FAIL(joiner.Query("SELECT * FROM staged"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	result = owner.Query("SELECT value FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
}

TEST_CASE("Participants read the snapshot concurrently", "[api][transaction_snapshot]") {
	constexpr idx_t PARTICIPANT_COUNT = 4;
	DuckDB database(nullptr);
	Connection setup(database);
	Connection owner(database);
	auto probe = make_shared_ptr<ConcurrencyProbe>();
	probe->target = PARTICIPANT_COUNT;
	RegisterConcurrencyProbe(setup, probe);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value BIGINT)"));

	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	// Uncommitted: only the exporter and its participants can see these rows.
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values SELECT * FROM range(1000)"));
	auto transaction_id = ExportSnapshot(owner);

	vector<unique_ptr<Connection>> participants;
	for (idx_t i = 0; i < PARTICIPANT_COUNT; i++) {
		participants.push_back(make_uniq<Connection>(database));
		SetTransactionSnapshot(*participants.back(), transaction_id);
	}

	// Every participant must be inside its statement at the same time, otherwise the probe times out.
	vector<unique_ptr<QueryResult>> results(PARTICIPANT_COUNT);
	vector<std::thread> threads;
	for (idx_t i = 0; i < PARTICIPANT_COUNT; i++) {
		threads.emplace_back([&, i]() {
			results[i] = participants[i]->Query("SELECT count(concurrency_probe(value)) FROM shared_values");
		});
	}
	for (auto &thread : threads) {
		thread.join();
	}
	for (idx_t i = 0; i < PARTICIPANT_COUNT; i++) {
		REQUIRE_NO_FAIL(*results[i]);
		REQUIRE(CHECK_COLUMN(results[i], 0, {1000}));
	}
	REQUIRE(!probe->timed_out);
	REQUIRE(probe->peak == PARTICIPANT_COUNT);

	for (auto &participant : participants) {
		REQUIRE_NO_FAIL(participant->Query("ROLLBACK"));
	}
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
}

TEST_CASE("A participant statement still uses intra-query parallelism", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection setup(database);
	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value BIGINT)"));

	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values SELECT * FROM range(500000)"));
	SetTransactionSnapshot(joiner, ExportSnapshot(owner));

	REQUIRE_NO_FAIL(joiner.Query("SET threads = 4"));
	auto result = joiner.Query("SELECT count(*), sum(value) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {500000}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(124999750000LL)}));
	// The gate is held once per statement, so the scan is free to fan out across the thread pool.
	result = joiner.Query("SELECT current_setting('threads')");
	REQUIRE(CHECK_COLUMN(result, 0, {Value("4")}));

	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
}
