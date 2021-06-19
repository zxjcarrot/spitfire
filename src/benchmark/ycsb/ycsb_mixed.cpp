//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb_mixed.cpp
//
// Identification: src/main/ycsb/ycsb_mixed.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <ctime>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "benchmark/ycsb/ycsb_configuration.h"
#include "benchmark/ycsb/ycsb_loader.h"
#include "benchmark/ycsb/ycsb_workload.h"


namespace spitfire {
namespace benchmark {
namespace ycsb {

bool RunMixed(ConcurrentBufferManager *buf_mgr, const size_t thread_id, ZipfDistribution &zipf, FastRandom &rng, const std::vector<uint64_t>& keys) {
    auto txn_manager = MVTOTransactionManager::GetInstance(buf_mgr);

    TransactionContext *txn =
            txn_manager->BeginTransaction(thread_id);

    for (int i = 0; i < state.operation_count; i++) {
        auto rng_val = rng.NextUniform();

        auto lookup_key_idx = zipf.GetNextNumber();
        auto lookup_key = keys[lookup_key_idx -1];
        if (rng_val < state.update_ratio) {
            /////////////////////////////////////////////////////////
            // PERFORM UPDATE
            /////////////////////////////////////////////////////////

            PointUpdateExecutor<uint64_t, YCSBTuple> update_executor(*user_table, lookup_key,
                                                                     [lookup_key](const YCSBTuple &t) {
                                                                         return t.key == lookup_key;
                                                                     }, [](YCSBTuple &t) {
                        for (int c = 0; c < COLUMN_COUNT; ++c) {
                            memset(t.cols[c], 1, sizeof(t.cols[c]));
                        }
                    }, txn, buf_mgr);

            auto res = update_executor.Execute();
            if (!res) {
                assert(txn->GetResult() != ResultType::SUCCESS);
                txn_manager->AbortTransaction(txn);
                return false;
            }
        } else {
            /////////////////////////////////////////////////////////
            // PERFORM READ
            /////////////////////////////////////////////////////////


            bool point_lookup = true;
            bool acquire_owner = false;
            char str[100];
            auto field_idx = rng.next() % COLUMN_COUNT;
            IndexScanExecutor<uint64_t, YCSBTuple> lookup_executor(*user_table, lookup_key, point_lookup,
                                                                   [lookup_key, &str, field_idx](const YCSBTuple &t, bool & should_end_scan) {
                                                                       if (t.key == lookup_key) {
                                                                           should_end_scan = true;
                                                                           memcpy(str, t.cols[field_idx],
                                                                                  sizeof(t.cols[field_idx]));
                                                                           return true;
                                                                       }
                                                                       return false;
                                                                   }, acquire_owner, txn, buf_mgr);

            auto res = lookup_executor.Execute();
            if (!res) {
                assert(txn->GetResult() != ResultType::SUCCESS);
                txn_manager->AbortTransaction(txn);
                return false;
            }
        }
    }

    // transaction passed execution.
    assert(txn->GetResult() == ResultType::SUCCESS);

    auto result = txn_manager->CommitTransaction(txn);

    if (result == ResultType::SUCCESS) {
        return true;

    } else {
        // transaction failed commitment.
        assert(result == ResultType::ABORTED ||
               result == ResultType::FAILURE);
        return false;
    }
}
}
}
}
