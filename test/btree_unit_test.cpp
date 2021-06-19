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
#include "buf/buf_mgr.h"
#include "engine/btreeolc.h"
#include "engine/table.h"
#include "engine/txn.h"

static int IntRand() {
    static thread_local std::mt19937 generator;
    std::uniform_int_distribution<int> distribution(0, std::numeric_limits<int>::max());
    return distribution(generator);
}

template<class T>
void ReportStatsAboutWorkload(const std::vector<T> & workload, int first_n_most_freq) {
    std::unordered_map<T, int > cnt;

    int max_cnt = 0;
    for (int i = 0; i < workload.size(); ++i) {
        cnt[workload[i]]++;
    }
    std::vector<std::pair<int,T>> pairs;
    for (auto kv : cnt) {
        pairs.push_back(std::make_pair(kv.second, kv.first));
        max_cnt = std::max(max_cnt, kv.second);
    }
    std::sort(pairs.begin(), pairs.end(), [](const std::pair<int, T> & p1, const std::pair<int, T> & p2) {
        return p1.first > p2.first;
    });

    std::cout << "1 -> " << cnt[1] << std::endl;
    std::cout << "Max count " <<  max_cnt << std::endl;
    int current_count = 0;
    for (int i = 0; i < std::min(first_n_most_freq, (int)pairs.size()); ++i) {
        current_count += pairs[i].first;
    }
    std::cout << "Top "<< std::min(first_n_most_freq, (int)pairs.size()) << " items take up " << current_count / (workload.size() + 0.0) * 100 << "% of the entire workload" << std::endl;
}

void TestBTree(spitfire::BufferPoolConfig config, const std::string &db_path,
               spitfire::PageMigrationPolicy policy) {
    using namespace spitfire;
    Status s = SSDPageManager::DestroyDB(db_path);
    assert(s.ok());
    spitfire::pid_t root_pid = kInvalidPID;
    {
        SSDPageManager ssd_page_manager(db_path, config.enable_direct_io);
        s = ssd_page_manager.Init();
        assert(s.ok());
        ConcurrentBufferManager buf_mgr(&ssd_page_manager, policy, config);
        s = buf_mgr.Init();
        assert(s.ok());
        BTree<uint64_t, uint64_t> btree(&buf_mgr);
        s = btree.Init(root_pid);
        assert(s.ok());

        btree.Insert(1, 1222);

        bool res = false;
        uint64_t val = 0;
        res = btree.Lookup(1, val);
        assert(res);
        assert(val == 1222);

        res = btree.Lookup(2, val);
        assert(res == false);
    }

    {
        SSDPageManager ssd_page_manager(db_path, config.enable_direct_io);
        s = ssd_page_manager.Init();
        assert(s.ok());
        ConcurrentBufferManager buf_mgr(&ssd_page_manager, policy, config);
        s = buf_mgr.Init();
        assert(s.ok());
        BTree<uint64_t, uint64_t> btree(&buf_mgr);
        s = btree.Init(root_pid);
        assert(s.ok());

        bool res = false;
        uint64_t val = 0;
        res = btree.Lookup(1, val);
        assert(res);
        assert(val == 1222);

        res = btree.Lookup(2, val);
        assert(res == false);

        const int n = 3324650;
        for (size_t i = 0; i < n; ++i) {
            if (i == 1023) {
                i = 1023;
            }
            btree.Insert(i, i);

            uint64_t val;
            res = btree.Lookup(i, val);
            assert(res);
            assert(val == i);
        }

        for (size_t i = 0; i < n; ++i) {
            uint64_t val;
            res = btree.Lookup(i, val);
            assert(res);
            assert(val == i);
        }
        assert(res);
    }

    s = SSDPageManager::DestroyDB(db_path);
    assert(s.ok());
    root_pid = kInvalidPID;
    {
        SSDPageManager ssd_page_manager(db_path, config.enable_direct_io);
        s = ssd_page_manager.Init();
        assert(s.ok());
        ConcurrentBufferManager buf_mgr(&ssd_page_manager, policy, config);
        s = buf_mgr.Init();
        assert(s.ok());
        BTree<uint64_t, uint64_t> btree(&buf_mgr);
        s = btree.Init(root_pid);
        assert(s.ok());


        const size_t n = 4024652;
        std::cout << n << std::endl;
        std::vector<uint64_t> keys;
        keys.reserve(n);
        for (int i = 0; i < n; ++i) {
            keys.push_back(i);
        }

        std::random_shuffle(keys.begin(), keys.end());
        for (size_t i = 0; i < keys.size(); ++i) {
            btree.Insert(keys[i], keys[i] + i);

            int j = i;
            uint64_t val;
            bool res = btree.Lookup(keys[j], val);
            if (res == false) {
                res = btree.Lookup(keys[j], val);
            }
            assert(res);
            assert(val == keys[j] + j);
        }

        for (size_t i = 0; i < keys.size(); ++i) {
            uint64_t val;
            bool res = btree.Lookup(keys[i], val);
            assert(res);
            assert(val == keys[i] + i);
        }
    }
}


static int GetPartition(pid_t pid, size_t n_partitions) {
    return (pid >> spitfire::kPageSizeBits) % n_partitions;
}


void
TestConcurrentBTree(spitfire::BufferPoolConfig config, const std::string &db_path,
                  spitfire::PageMigrationPolicy policy,
                  size_t n_threads, spitfire::ThreadPool *tp,
                  std::vector<uint64_t> &workload) {
    using namespace spitfire;
    using spitfire::pid_t;
    Status s = SSDPageManager::DestroyDB(db_path);
    assert(s.ok());

    {
        SSDPageManager ssd_page_manager(db_path, config.enable_direct_io);
        s = ssd_page_manager.Init();
        assert(s.ok());
        ConcurrentBufferManager buf_mgr(&ssd_page_manager, policy, config);
        s = buf_mgr.Init();
        assert(s.ok());

        pid_t root_pid = kInvalidPID;
        BTree<uint64_t, uint64_t> btree(&buf_mgr);

        s = btree.Init(root_pid);
        assert(s.ok());


        size_t total_ops = workload.size();
        //ReportStatsAboutWorkload(workload, 300);


        {
            std::vector<std::thread> threads;
            CountDownLatch latch(n_threads);
            auto thread_work = [&](int i) {
                size_t ops_per_thread = total_ops / n_threads;
                int start = i * ops_per_thread;
                int end = std::min(start + ops_per_thread, total_ops);
                if (i == n_threads - 1) {
                    end = total_ops;
                }
                // Upsert
                for (int i = start; i < end; ++i) {
                    btree.Insert(workload[i], workload[i]);
                }

                latch.CountDown();
            };

            TimedThroughputReporter reporter(total_ops);
            for (size_t i = 0; i < n_threads; ++i) {
                tp->enqueue(thread_work, i);
            }
            latch.Await();

            std::cout << "Concurrent random test finished, " << total_ops << " btree writes " << n_threads
                      << " workers." << std::endl;
            std::cout << "Stats: \n" << buf_mgr.GetStatsString() << std::endl;
            std::cout << "Replacer Stats: \n" << buf_mgr.ReplacerStats() << std::endl;
        }

        buf_mgr.ClearStats();
        sleep(1);

        std::cout << "Stats before read: \n" << buf_mgr.GetStatsString() << std::endl;
        std::cout << "Replacer Stats: \n" << buf_mgr.ReplacerStats() << std::endl;

        {
            std::vector<std::thread> threads;
            CountDownLatch latch(n_threads);
            auto thread_work = [&](int i) {
                size_t ops_per_thread = total_ops / n_threads;
                int start = i * ops_per_thread;
                int end = std::min(start + ops_per_thread, total_ops);
                if (i == n_threads - 1) {
                    end = total_ops;
                }
                // Search
                for (int i = start; i < end; ++i) {
                    uint64_t val = 0;
                    bool res = btree.Lookup(workload[i], val);
                    if (res == false) {
                        res = btree.Lookup(workload[i], val);
                    }
                    assert(res);
                    assert(val == workload[i]);
                }

                latch.CountDown();
            };

            TimedThroughputReporter reporter(total_ops);
            for (size_t i = 0; i < n_threads; ++i) {
                tp->enqueue(thread_work, i);
            }
            latch.Await();

            std::cout << "Concurrent random test finished, " << total_ops << " btree reads " << n_threads
                      << " workers." << std::endl;
            std::cout << "Stats: \n" << buf_mgr.GetStatsString() << std::endl;
            std::cout << "Replacer Stats: \n" << buf_mgr.ReplacerStats() << std::endl;
        }


        buf_mgr.ClearStats();


        sort(workload.begin(), workload.end());
        workload.erase(std::unique(workload.begin(), workload.end()), workload.end());
        std::random_shuffle(workload.begin(), workload.end());
        std::vector<uint64_t> delete_workload;
        delete_workload.reserve(workload.size() / 2);
        for (int i = 0; i < workload.size(); i++) {
            delete_workload.push_back(workload[i]);
        }

        {
            std::vector<std::thread> threads;
            CountDownLatch latch(n_threads);
            total_ops = delete_workload.size();
            auto thread_work = [&](int i) {
                size_t ops_per_thread = total_ops / n_threads;
                int start = i * ops_per_thread;
                int end = std::min(start + ops_per_thread, total_ops);
                if (i == n_threads - 1) {
                    end = total_ops;
                }
                // delete
                for (int i = start; i < end; ++i) {
                    bool res = btree.Erase(delete_workload[i]);
                    if (res == false) {
                        res = btree.Erase(delete_workload[i]);
                    }
                    assert(res);
                }

                latch.CountDown();
            };

            TimedThroughputReporter reporter(total_ops);
            for (size_t i = 0; i < n_threads; ++i) {
                tp->enqueue(thread_work, i);
            }
            latch.Await();

            std::cout << "Concurrent random test finished, " << total_ops << " btree deletes " << n_threads
                      << " workers." << std::endl;
            std::cout << "Stats: \n" << buf_mgr.GetStatsString() << std::endl;
            std::cout << "Replacer Stats: \n" << buf_mgr.ReplacerStats() << std::endl;
        }

        buf_mgr.ClearStats();
        sleep(1);
        {
            std::vector<std::thread> threads;
            CountDownLatch latch(n_threads);
            total_ops = workload.size();
            auto thread_work = [&](int i) {
                size_t ops_per_thread = total_ops / n_threads;
                int start = i * ops_per_thread;
                int end = std::min(start + ops_per_thread, total_ops);
                if (i == n_threads - 1) {
                    end = total_ops;
                }
                // Search
                for (int i = start; i < end; ++i) {
                    uint64_t val = 0;
                    bool res = btree.Lookup(workload[i], val);
                    if (i % 1 == 0) {
                        assert(res == false);
                    } else {
                        if (res == false) {
                            res = btree.Lookup(workload[i], val);
                        }
                        assert(res);
                        assert(val == workload[i]);
                    }
                }

                latch.CountDown();
            };

            TimedThroughputReporter reporter(total_ops);
            for (size_t i = 0; i < n_threads; ++i) {
                tp->enqueue(thread_work, i);
            }
            latch.Await();

            std::cout << "Concurrent random test finished, " << total_ops << " btree ops " << n_threads
                      << " workers." << std::endl;
            std::cout << "Stats: \n" << buf_mgr.GetStatsString() << std::endl;
            std::cout << "Replacer Stats: \n" << buf_mgr.ReplacerStats() << std::endl;
        }
    }
}


std::vector<uint64_t> GetWorkload(int n_ops, int n_kvs, const std::string &workload_filepath) {
    std::vector<uint64_t> workload;
    workload.reserve(n_ops);
    if (workload_filepath.empty()) {
        for (int i = 0; i < n_ops; ++i) {
            workload.push_back(IntRand() % n_kvs);
        }
    } else {
        std::ifstream infile(workload_filepath);
        while (!infile.eof()) {
            uint64_t idx;
            infile >> idx;
            workload.push_back(idx);
        }
    }
    return workload;
}

void TestBTreeCorrectness(spitfire::BufferPoolConfig config, const std::string &db_path,
                          spitfire::PageMigrationPolicy policy,
                          size_t n_threads, spitfire::ThreadPool *tp,
                          size_t total_ops) {
    using namespace spitfire;
    using spitfire::pid_t;
    Status s = SSDPageManager::DestroyDB(db_path);
    assert(s.ok());

    {
        SSDPageManager ssd_page_manager(db_path, config.enable_direct_io);
        s = ssd_page_manager.Init();
        assert(s.ok());
        ConcurrentBufferManager buf_mgr(&ssd_page_manager, policy, config);
        s = buf_mgr.Init();
        assert(s.ok());

        pid_t root_pid = kInvalidPID;
        BTree<uint64_t, uint64_t> btree(&buf_mgr);

        s = btree.Init(root_pid);
        assert(s.ok());
        const int partitions = 61;

        std::vector<std::mutex> part_mutexes(partitions);
        std::vector<std::unordered_map<uint64_t, uint64_t>> part_map(partitions);

        std::vector<std::thread> threads;
        CountDownLatch latch(n_threads);
        auto thread_work = [&](int i) {
            size_t ops_per_thread = total_ops / n_threads;
            int start = i * ops_per_thread;
            int end = std::min(start + ops_per_thread, total_ops);

            for (int i = start; i < end; ++i) {
                int op_code = IntRand() % 3;
                uint64_t key = IntRand() % (1024 * 1024 + 1);
                int part = key % partitions;
                std::lock_guard<std::mutex> g(part_mutexes[part]);
                if (op_code == 0) { // Lookup
                    uint64_t val = 0;
                    bool res = btree.Lookup(key, val);
                    uint64_t ans_val = 0;
                    bool ans_res = false;
                    auto it = part_map[part].find(key);
                    if (it != part_map[part].end()) {
                        ans_res = true;
                        ans_val = it->second;
                    }
                    assert(res == ans_res);
                    assert(val == ans_val);
                } else if (op_code == 1) { // Upsert
                    btree.Insert(key, key);
                    part_map[part][key] = key;
                } else { // Delete
                    btree.Erase(key);
                    part_map[part].erase(key);
                }
            }

            latch.CountDown();
        };

        TimedThroughputReporter reporter(total_ops);
        for (size_t i = 0; i < n_threads; ++i) {
            tp->enqueue(thread_work, i);
        }
        latch.Await();

    }
}
namespace spitfire {
std::vector<BaseDataTable*> database_tables;
}
int main(int argc, char **argv) {
    std::string db_path = "/mnt/optane";
    std::string workload_filepath = ""; // Empty as default uniform workload
    std::string nvm_path = "/mnt/pmem0/nvm_buf_pool";
    bool enable_mini_page = true;
    if (argc > 1) {
        db_path = std::string(argv[1]);
    }
    if (argc > 2) {
        nvm_path = argv[2];
    }
    if (argc > 3) {
        enable_mini_page = std::stoi(argv[3]);
    }
    if (argc > 4) {
        workload_filepath = argv[4];
    }


    spitfire::PosixEnv::CreateDir(db_path);
    spitfire::PosixEnv::DeleteDir(nvm_path);
    spitfire::PosixEnv::CreateDir(nvm_path);
    std::string nvm_heap_filepath = nvm_path + "/nvm/heapfile";
    std::string wal_file_path = nvm_path + "/nvm/wal";
    size_t n_threads = 16;
    spitfire::ThreadPool tp(n_threads);
    {
        spitfire::PageMigrationPolicy policy;
        policy.Dr = policy.Dw = 0.5;
        policy.Nr = policy.Nw = 0.5;

        const int n_ops = 1024 * 1024 * 25;
        const int n_kvs = n_ops;
        const int n_pages = n_kvs * sizeof(uint64_t) * 2 / spitfire::kPageSize;
        spitfire::BufferPoolConfig config;
        config.enable_nvm_buf_pool = true;
        config.enable_mini_page = enable_mini_page;
        config.enable_direct_io = true;
        //config.nvm_admission_set_size_limit = 100;
        //config.enable_ttb = true;
        config.dram_buf_pool_cap_in_bytes = n_pages / 10 * spitfire::kPageSize;
        config.nvm_buf_pool_cap_in_bytes = n_pages / 5 * spitfire::kPageSize;
        config.enable_direct_io = true;
        config.nvm_heap_file_path = nvm_heap_filepath;
        config.wal_file_path = wal_file_path;

        TestBTreeCorrectness(config, db_path, policy, n_threads, &tp, n_ops);
        auto workload = GetWorkload(n_ops, n_kvs, workload_filepath);
        for (int i = n_threads; i <= n_threads; ++i)
            TestConcurrentBTree(config, db_path, policy, i, &tp, workload);
//
//        for (size_t t = n_threads; t <= n_threads; ++t) {
//            TestConcurrentBufferPoolNVMBlockGrainedAccess(config, db_path, policy, t, &tp, n_pages, n_ops, workload);
//        }
//
//        for (double Dp = 0.1; Dp <= 1; Dp += 0.1) {
//            policy.Dr = Dp;
//            policy.Dw = Dp;
//            for (double Np = 0.1; Np <= 1; Np += 0.1) {
//                policy.Nr = Np;
//                policy.Nw = Np;
//                config.enable_nvm_buf_pool = false;
//                TestConcurrentBufferPoolNVMBlockGrainedAccess(config, db_path, policy, n_threads, &tp, n_pages, n_ops, workload);
//                config.enable_nvm_buf_pool = true;
//                TestConcurrentBufferPoolNVMBlockGrainedAccess(config, db_path, policy, n_threads, &tp, n_pages, n_ops,
//                                                              workload);
//                std::cout << Dp << " " << Np << std::endl;
//            }
//        }
    }

    return 0;
}