//
// Created by zxjcarrot on 2019-12-22.
//

#include <iostream>
#include <unordered_map>
#include <vector>
#include <fstream>
#include <map>
#include <limits>
#include "util/random.h"
#include "util/crc32c.h"
#include "util/tools.h"
#include "util/sync.h"
#include "libpm/libpm.h"
#include "buf/buf_mgr.h"
#include "engine/btreeolc.h"
#include "engine/txn.h"
#include "engine/table.h"
#include "engine/executor.h"

#include "testing_transaction_util.h"

static int IntRand() {
    static thread_local std::mt19937 generator;
    std::uniform_int_distribution<int> distribution(0, std::numeric_limits<int>::max());
    return distribution(generator);
}

void StressTBBHashmap(spitfire::ThreadPool & tp, int n_workers = 10) {
    tbb::concurrent_hash_map<uint32_t, uint32_t> m;
    int n = 131702;
    for (int i = 0; i < n; ++i) {
        m.insert(std::make_pair(i , i));
    }
    spitfire::CountDownLatch latch(n_workers);

    size_t total_ops = 10000000;
    auto worker = [&](int thread_id) {
        size_t ops_per_thread = total_ops / n_workers;
        int start = thread_id * ops_per_thread;
        int end = std::min(start + ops_per_thread, total_ops);
        if (thread_id == n_workers - 1) {
            end = total_ops;
        }
        for (int i = start; i < end; ++i) {
            tbb::concurrent_hash_map<uint32_t, uint32_t>::accessor accessor;
            uint32_t key = IntRand() % n;
            bool found = m.find(accessor, key);
            assert(found);
            assert(accessor->second == key);
        }
        latch.CountDown();
    };

    spitfire::TimedThroughputReporter reporter(n);
    for (size_t i = 0; i < n_workers; ++i) {
        tp.enqueue(worker, i);
    }
    latch.Await();

    std::cout << "Concurrent random test finished, " << total_ops << " reads " << n_workers
              << " workers." << std::endl;
}


void StressCuckooHashmap(spitfire::ThreadPool & tp, int n_workers = 10) {
    spitfire::CuckooMap<uint32_t, uint32_t> m;
    int n = 131702;
    for (int i = 0; i < n; ++i) {
        m.Insert(i , i);
    }
    spitfire::CountDownLatch latch(n_workers);

    size_t total_ops = 10000000;
    auto worker = [&](int thread_id) {
        size_t ops_per_thread = total_ops / n_workers;
        int start = thread_id * ops_per_thread;
        int end = std::min(start + ops_per_thread, total_ops);
        if (thread_id == n_workers - 1) {
            end = total_ops;
        }
        for (int i = start; i < end; ++i) {
            uint32_t key = IntRand() % n;
            uint32_t value = 0;
            bool found = m.Find(key, value);
            assert(found);
            assert(value == key);
        }
        latch.CountDown();
    };

    spitfire::TimedThroughputReporter reporter(n);
    for (size_t i = 0; i < n_workers; ++i) {
        tp.enqueue(worker, i);
    }
    latch.Await();

    std::cout << "Concurrent random test finished, " << total_ops << " reads " << n_workers
              << " workers." << std::endl;
}

int main(int argc, char **argv) {
    size_t n_threads = 16;
    spitfire::ThreadPool tp(n_threads);

    StressCuckooHashmap(tp, 1);
    StressCuckooHashmap(tp, 16);
    StressTBBHashmap(tp, 1);
    StressTBBHashmap(tp, 16);
    return 0;
}