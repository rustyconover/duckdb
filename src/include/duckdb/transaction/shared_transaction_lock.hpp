//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/shared_transaction_lock.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/condition_variable.hpp"

namespace duckdb {

//! A statement gate that can be released by a different executor thread than the one that acquired it.
class SharedTransactionLock {
public:
	bool TryLockFor(const std::chrono::milliseconds &timeout) {
		unique_lock<mutex> guard(lock);
		if (!condition.wait_for(guard, timeout, [&]() { return !locked; })) {
			return false;
		}
		locked = true;
		return true;
	}

	void Unlock() {
		{
			lock_guard<mutex> guard(lock);
			D_ASSERT(locked);
			locked = false;
		}
		condition.notify_one();
	}

private:
	mutex lock;
	condition_variable condition;
	bool locked = false;
};

} // namespace duckdb
