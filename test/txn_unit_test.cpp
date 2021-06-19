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

struct Tuple : spitfire::BaseTuple {
    uint64_t key;
    uint64_t values[10];

    uint64_t Key() const { return key; }
};

void BasicTransactionTest(spitfire::BufferPoolConfig config, const std::string &db_path,
                          spitfire::PageMigrationPolicy policy) {
    using namespace spitfire;
    Status s = SSDPageManager::DestroyDB(db_path);
    assert(s.ok());
    SSDPageManager ssd_page_manager(db_path, config.enable_direct_io);
    s = ssd_page_manager.Init();
    assert(s.ok());
    ConcurrentBufferManager buf_mgr(&ssd_page_manager, policy, config);
    s = buf_mgr.Init();
    assert(s.ok());

    uint64_t primary_index_root_pid = kInvalidPID;

    auto meta_page_pid = kInvalidPID;

    ClusteredIndex<uint64_t, Tuple> primary_index(&buf_mgr);

    s = primary_index.Init(primary_index_root_pid);
    assert(s.ok());

    PartitionedHeapTable<uint64_t, Tuple> version_table;
    s = version_table.Init(meta_page_pid, &buf_mgr, true);
    assert(s.ok());
    meta_page_pid = version_table.GetMetaPagePid();
    assert(meta_page_pid != kInvalidPID);

    DataTable<uint64_t, Tuple> data_table(primary_index, version_table);
    const int n_txns = 10;

    assert(s.ok());

    {
        int aborted_txns = 0;
        int committed_txns = 0;
        for (int i = 0; i < n_txns; ++i) {
            auto transaction_manager = MVTOTransactionManager::GetInstance(&buf_mgr);
            auto txn_ctx = transaction_manager->BeginTransaction();
            Tuple tuple;
            tuple.key = i;
            for (int j = 0; j < 10; ++j)
                tuple.values[j] = i;

            InsertExecutor<uint64_t, Tuple> executor(data_table, tuple, txn_ctx, &buf_mgr);

            if (executor.Execute() == false) {
                assert(txn_ctx->GetResult() == ResultType::FAILURE);
                transaction_manager->AbortTransaction(txn_ctx);
                aborted_txns++;
            } else {
                transaction_manager->AbortTransaction(txn_ctx);
                aborted_txns++;
            }
        }
        std::cout << "[Insert-Abort Transactions] Committed " << committed_txns << ", Aborted " << aborted_txns
                  << std::endl;
    }

    auto lookup_empty_check = [&]() {
        int aborted_txns = 0;
        int committed_txns = 0;
        for (int i = 0; i < n_txns; ++i) {
            auto transaction_manager = MVTOTransactionManager::GetInstance(&buf_mgr);
            auto txn_ctx = transaction_manager->BeginTransaction();
            uint64_t key = i;
            bool point_lookup = true;
            bool acquire_owner = false;
            IndexScanExecutor<uint64_t, Tuple> executor(data_table, i, point_lookup,
                                                        [key](const Tuple &t, bool &should_end_scan) {
                                                            should_end_scan = true;
                                                            return t.key == key;
                                                        }, acquire_owner,
                                                        txn_ctx, &buf_mgr);

            if (executor.Execute() == false) {
                assert(txn_ctx->GetResult() == ResultType::FAILURE);
                transaction_manager->AbortTransaction(txn_ctx);
                aborted_txns++;
            } else {
                auto res = executor.GetResults();
                assert(res.size() == 0);
                transaction_manager->CommitTransaction(txn_ctx);
                committed_txns++;
            }
        }
        std::cout << "[Empty-Lookup Transactions] Committed " << committed_txns << ", Aborted " << aborted_txns
                  << std::endl;
    };


    auto scan_check = [&]() {
        int aborted_txns = 0;
        int committed_txns = 0;
        for (int i = 0; i < n_txns; ++i) {
            auto transaction_manager = MVTOTransactionManager::GetInstance(&buf_mgr);
            auto txn_ctx = transaction_manager->BeginTransaction();
            auto scan_processor = [](const Tuple & t, bool & should_end) -> void {
                std::cout << t.key << std::endl;
            };
            TableScanExecutor<uint64_t, Tuple> executor(data_table, scan_processor,
                                                        txn_ctx, &buf_mgr);

            if (executor.Execute() == false) {
                assert(txn_ctx->GetResult() == ResultType::FAILURE);
                transaction_manager->AbortTransaction(txn_ctx);
                aborted_txns++;
            } else {
                transaction_manager->CommitTransaction(txn_ctx);
                committed_txns++;
            }
        }
        std::cout << "[Scan-Table Transactions] Committed " << committed_txns << ", Aborted " << aborted_txns
                  << std::endl;
    };

    lookup_empty_check();

    {
        int aborted_txns = 0;
        int committed_txns = 0;
        for (int i = 0; i < n_txns; ++i) {
            auto transaction_manager = MVTOTransactionManager::GetInstance(&buf_mgr);
            auto txn_ctx = transaction_manager->BeginTransaction();
            Tuple tuple;
            tuple.key = i;
            for (int j = 0; j < 10; ++j)
                tuple.values[j] = i;

            InsertExecutor<uint64_t, Tuple> executor(data_table, tuple, txn_ctx, &buf_mgr);

            if (executor.Execute() == false) {
                assert(txn_ctx->GetResult() == ResultType::FAILURE);
                transaction_manager->AbortTransaction(txn_ctx);
                aborted_txns++;
            } else {
                transaction_manager->CommitTransaction(txn_ctx);
                committed_txns++;
            }
        }
        std::cout << "[Insert Transactions] Committed " << committed_txns << ", Aborted " << aborted_txns << std::endl;
    }

    scan_check();

    auto lookup_check = [&](int inc) {
        int aborted_txns = 0;
        int committed_txns = 0;
        for (int i = 0; i < n_txns; ++i) {
            auto transaction_manager = MVTOTransactionManager::GetInstance(&buf_mgr);
            auto txn_ctx = transaction_manager->BeginTransaction();
            uint64_t key = i;
            Tuple tuple;
            tuple.key = i;
            for (int j = 0; j < 10; ++j)
                tuple.values[j] = i + inc;

            bool point_lookup = true;
            bool acquire_owner = false;
            IndexScanExecutor<uint64_t, Tuple> executor(data_table, i, point_lookup,
                                                        [key](const Tuple &t, bool &should_end_scan) {
                                                            should_end_scan = true;
                                                            return t.key == key;
                                                        }, acquire_owner,
                                                        txn_ctx, &buf_mgr);

            if (executor.Execute() == false) {
                assert(txn_ctx->GetResult() == ResultType::FAILURE);
                transaction_manager->AbortTransaction(txn_ctx);
                aborted_txns++;
            } else {
                auto res = executor.GetResults();
                assert(res.size() == 1);
                assert(std::memcmp(res[0].values, tuple.values, sizeof(tuple.values)) == 0);
                transaction_manager->CommitTransaction(txn_ctx);
                committed_txns++;
            }
        }
        std::cout << "[Lookup Transactions] Committed " << committed_txns << ", Aborted " << aborted_txns << std::endl;
    };


    lookup_check(0);
    cid_t snapshot_tid = MVTOTransactionManager::GetInstance(&buf_mgr)->GetCurrentTidCounter() - 1;
    {
        int aborted_txns = 0;
        int committed_txns = 0;
        for (int i = 0; i < n_txns; ++i) {
            auto transaction_manager = MVTOTransactionManager::GetInstance(&buf_mgr);
            auto txn_ctx = transaction_manager->BeginTransaction();
            Tuple tuple;
            tuple.key = i;
            for (int j = 0; j < 10; ++j)
                tuple.values[j] = i;

            uint64_t key = i;
            auto predicate = [key](const Tuple &t) -> bool {
                return t.key == key;
            };
            auto updater = [](Tuple &t) {
                for (int j = 0; j < 10; ++j)
                    t.values[j]++;
            };
            PointUpdateExecutor<uint64_t, Tuple> executor(data_table, key, predicate,
                                                          updater, txn_ctx, &buf_mgr);

            if (executor.Execute() == false) {
                assert(txn_ctx->GetResult() == ResultType::FAILURE);
                transaction_manager->AbortTransaction(txn_ctx);
                aborted_txns++;
            } else {
                transaction_manager->CommitTransaction(txn_ctx);
                committed_txns++;
            }
        }
        std::cout << "[Update Transactions] Committed " << committed_txns << ", Aborted " << aborted_txns << std::endl;
    }

    lookup_check(1);


    {
        int aborted_txns = 0;
        int committed_txns = 0;
        for (int i = 0; i < n_txns; ++i) {
            auto transaction_manager = MVTOTransactionManager::GetInstance(&buf_mgr);
            auto txn_ctx = transaction_manager->BeginTransaction();
            // read the versions before the updates
            txn_ctx->SetCommitId(snapshot_tid);
            txn_ctx->SetTxnId(snapshot_tid);
            txn_ctx->SetReadId(snapshot_tid);
            uint64_t key = i;
            Tuple tuple;
            tuple.key = i;
            for (int j = 0; j < 10; ++j)
                tuple.values[j] = i;

            bool point_lookup = true;
            bool acquire_owner = false;
            IndexScanExecutor<uint64_t, Tuple> executor(data_table, i, point_lookup,
                                                        [key](const Tuple &t, bool &should_end_scan) {
                                                            should_end_scan = true;
                                                            return t.key == key;
                                                        }, acquire_owner,
                                                        txn_ctx, &buf_mgr);

            if (executor.Execute() == false) {
                assert(txn_ctx->GetResult() == ResultType::FAILURE);
                transaction_manager->AbortTransaction(txn_ctx);
                aborted_txns++;
            } else {
                auto res = executor.GetResults();
                assert(res.size() == 1);
                assert(std::memcmp(res[0].values, tuple.values, sizeof(tuple.values)) == 0);
                transaction_manager->CommitTransaction(txn_ctx);
                committed_txns++;
            }
        }
        std::cout << "[Lookup-Old Transactions] Committed " << committed_txns << ", Aborted " << aborted_txns
                  << std::endl;
    }


    {
        int aborted_txns = 0;
        int committed_txns = 0;
        for (int i = 0; i < n_txns; ++i) {
            auto transaction_manager = MVTOTransactionManager::GetInstance(&buf_mgr);
            auto txn_ctx = transaction_manager->BeginTransaction();
            Tuple tuple;
            tuple.key = i;
            for (int j = 0; j < 10; ++j)
                tuple.values[j] = i;

            uint64_t key = i;
            auto predicate = [key](const Tuple &t) -> bool {
                return t.key == key;
            };
            auto updater = [](Tuple &t) {
                for (int j = 0; j < 10; ++j)
                    t.values[j]++;
            };
            PointUpdateExecutor<uint64_t, Tuple> executor(data_table, key, predicate,
                                                          updater, txn_ctx, &buf_mgr);

            if (executor.Execute() == false) {
                assert(txn_ctx->GetResult() == ResultType::FAILURE);
                transaction_manager->AbortTransaction(txn_ctx);
                aborted_txns++;
            } else {
                transaction_manager->AbortTransaction(txn_ctx);
                aborted_txns++;
            }
        }
        std::cout << "[Update Transactions Abort] Committed " << committed_txns << ", Aborted " << aborted_txns
                  << std::endl;
    }

    lookup_check(1);


    {
        int aborted_txns = 0;
        int committed_txns = 0;
        for (int i = 0; i < n_txns; ++i) {
            auto transaction_manager = MVTOTransactionManager::GetInstance(&buf_mgr);
            auto txn_ctx = transaction_manager->BeginTransaction();
            uint64_t key = i;
            auto predicate = [key](const Tuple &t) -> bool {
                return t.key == key;
            };
            PointDeleteExecutor<uint64_t, Tuple> executor(data_table, key, predicate,
                                                          txn_ctx, &buf_mgr);

            if (executor.Execute() == false) {
                assert(txn_ctx->GetResult() == ResultType::FAILURE);
                transaction_manager->AbortTransaction(txn_ctx);
                aborted_txns++;
            } else {
                transaction_manager->CommitTransaction(txn_ctx);
                committed_txns++;
            }
        }
        std::cout << "[Delete Transactions] Committed " << committed_txns << ", Aborted " << aborted_txns << std::endl;
    }

    lookup_empty_check();
}


void AbortVersionChainTest(spitfire::BufferPoolConfig config, const std::string &db_path,
                           spitfire::PageMigrationPolicy policy) {
    using namespace spitfire;
    using namespace spitfire::test;
    Status s = SSDPageManager::DestroyDB(db_path);
    assert(s.ok());
    SSDPageManager ssd_page_manager(db_path, config.enable_direct_io);
    s = ssd_page_manager.Init();
    assert(s.ok());
    ConcurrentBufferManager buf_mgr(&ssd_page_manager, policy, config);
    s = buf_mgr.Init();
    assert(s.ok());
    std::unordered_map<TuplePointer, Tuple, TuplePointerHasher> m;
    auto meta_page_pid = kInvalidPID;

    MVTOTransactionManager::ClearInstance();

    TestDataTable *table = TestingTransactionUtil::CreateTable(10, &buf_mgr);
    auto txn_manager = MVTOTransactionManager::GetInstance(&buf_mgr);

    {
        TransactionScheduler scheduler(2, table, txn_manager);
        scheduler.Txn(0).Update(0, 100);
        scheduler.Txn(0).Abort();
        scheduler.Txn(1).Read(0);
        scheduler.Txn(1).Commit();

        scheduler.Run();
        assert(scheduler.schedules[1].results[0] == 0);

    }

    {
        TransactionScheduler scheduler(2, table, txn_manager);
        scheduler.Txn(0).Insert(100, 0);
        scheduler.Txn(0).Abort();
        scheduler.Txn(1).Read(100);
        scheduler.Txn(1).Commit();

        scheduler.Run();
        assert(scheduler.schedules[1].results[0] == -1);
    }


    delete &table->GetPrimaryIndex();
    delete &table->GetVersionTable();
    delete table;
}

void SingleTransactionTest(spitfire::BufferPoolConfig config, const std::string &db_path,
                           spitfire::PageMigrationPolicy policy) {
    using namespace spitfire;
    using namespace spitfire::test;
    Status s = SSDPageManager::DestroyDB(db_path);
    assert(s.ok());
    SSDPageManager ssd_page_manager(db_path, config.enable_direct_io);
    s = ssd_page_manager.Init();
    assert(s.ok());
    ConcurrentBufferManager buf_mgr(&ssd_page_manager, policy, config);
    s = buf_mgr.Init();
    assert(s.ok());
    std::unordered_map<TuplePointer, Tuple, TuplePointerHasher> m;
    auto meta_page_pid = kInvalidPID;

    MVTOTransactionManager::ClearInstance();


    TestDataTable *table = TestingTransactionUtil::CreateTable(10, &buf_mgr);
    auto txn_manager = MVTOTransactionManager::GetInstance(&buf_mgr);

    // update, update, update, update, read
    {
        TransactionScheduler scheduler(1, table, txn_manager);
        scheduler.Txn(0).Update(0, 1, true);
        scheduler.Txn(0).Update(0, 2, true);
        scheduler.Txn(0).Update(0, 3, true);
        scheduler.Txn(0).Update(0, 4, true);
        scheduler.Txn(0).Read(0, true);
        scheduler.Txn(0).Commit();

        scheduler.Run();

        assert(ResultType::SUCCESS == scheduler.schedules[0].txn_result);
        assert(4 == scheduler.schedules[0].results[0]);
    }

    // delete not exist, delete exist, read deleted, update deleted,
    // read deleted, insert back, update inserted, read newly updated,
    // delete inserted, read deleted
    {
        TransactionScheduler scheduler(1, table, txn_manager);
        scheduler.Txn(0).Delete(100, true);
        scheduler.Txn(0).Delete(0, true);
        scheduler.Txn(0).Read(0, true);
        scheduler.Txn(0).Update(0, 1, true);
        scheduler.Txn(0).Read(0, true);
        scheduler.Txn(0).Insert(0, 2);
        scheduler.Txn(0).Update(0, 3, true);
        scheduler.Txn(0).Read(0, true);
        scheduler.Txn(0).Delete(0, true);
        scheduler.Txn(0).Read(0, true);
        scheduler.Txn(0).Commit();

        scheduler.Run();

        assert(ResultType::SUCCESS == scheduler.schedules[0].txn_result);
        assert(-1 == scheduler.schedules[0].results[0]);
        assert(-1 == scheduler.schedules[0].results[1]);
        assert(3 == scheduler.schedules[0].results[2]);
        assert(-1 == scheduler.schedules[0].results[3]);
        LOG_INFO("FINISH THIS");
    }

    // insert, delete inserted, read deleted, insert again, delete again
    // read deleted, insert again, read inserted, update inserted, read
    // updated
    {
        TransactionScheduler scheduler(1, table, txn_manager);

        scheduler.Txn(0).Insert(1000, 0);
        scheduler.Txn(0).Delete(1000, true);
        scheduler.Txn(0).Read(1000, true);
        scheduler.Txn(0).Insert(1000, 1);
        scheduler.Txn(0).Delete(1000, true);
        scheduler.Txn(0).Read(1000, true);
        scheduler.Txn(0).Insert(1000, 2);
        scheduler.Txn(0).Read(1000, true);
        scheduler.Txn(0).Update(1000, 3, true);
        scheduler.Txn(0).Read(1000, true);
        scheduler.Txn(0).Commit();

        scheduler.Run();

        assert(ResultType::SUCCESS == scheduler.schedules[0].txn_result);
        assert(-1 == scheduler.schedules[0].results[0]);
        assert(-1 == scheduler.schedules[0].results[1]);
        assert(2 == scheduler.schedules[0].results[2]);
        assert(3 == scheduler.schedules[0].results[3]);
    }

    delete &table->GetPrimaryIndex();
    delete &table->GetVersionTable();
    delete table;
}

void SingleTransactionTest2(spitfire::BufferPoolConfig config, const std::string &db_path,
                            spitfire::PageMigrationPolicy policy) {
    using namespace spitfire;
    using namespace spitfire::test;
    Status s = SSDPageManager::DestroyDB(db_path);
    assert(s.ok());
    SSDPageManager ssd_page_manager(db_path, config.enable_direct_io);
    s = ssd_page_manager.Init();
    assert(s.ok());
    ConcurrentBufferManager buf_mgr(&ssd_page_manager, policy, config);
    s = buf_mgr.Init();
    assert(s.ok());


    // Just scan the table
    {

        MVTOTransactionManager::ClearInstance();
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, &buf_mgr);
        auto txn_manager = MVTOTransactionManager::GetInstance(&buf_mgr);

        TransactionScheduler scheduler(1, table, txn_manager);
        scheduler.Txn(0).Scan(0);
        scheduler.Txn(0).Commit();

        scheduler.Run();

        EXPECT_EQ(10, scheduler.schedules[0].results.size());
        delete &table->GetPrimaryIndex();
        delete &table->GetVersionTable();
        delete table;
    }

    // read, read, read, read, update, read, read not exist
    {
        MVTOTransactionManager::ClearInstance();
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, &buf_mgr);
        auto txn_manager = MVTOTransactionManager::GetInstance(&buf_mgr);

        TransactionScheduler scheduler(1, table, txn_manager);
        scheduler.Txn(0).Read(0);
        scheduler.Txn(0).Read(0);
        scheduler.Txn(0).Read(0);
        scheduler.Txn(0).Read(0);
        scheduler.Txn(0).Update(0, 1);
        scheduler.Txn(0).Read(0);
        scheduler.Txn(0).Read(100);
        scheduler.Txn(0).Commit();

        scheduler.Run();

        EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
        EXPECT_EQ(0, scheduler.schedules[0].results[0]);
        EXPECT_EQ(0, scheduler.schedules[0].results[1]);
        EXPECT_EQ(0, scheduler.schedules[0].results[2]);
        EXPECT_EQ(0, scheduler.schedules[0].results[3]);
        EXPECT_EQ(1, scheduler.schedules[0].results[4]);
        EXPECT_EQ(-1, scheduler.schedules[0].results[5]);

        delete &table->GetPrimaryIndex();
        delete &table->GetVersionTable();
        delete table;
    }

    // update, update, update, update, read
    {
        MVTOTransactionManager::ClearInstance();
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, &buf_mgr);
        auto txn_manager = MVTOTransactionManager::GetInstance(&buf_mgr);

        TransactionScheduler scheduler(1, table, txn_manager);
        scheduler.Txn(0).Update(0, 1);
        scheduler.Txn(0).Read(0);
        scheduler.Txn(0).Update(0, 2);
        scheduler.Txn(0).Read(0);
        scheduler.Txn(0).Update(0, 3);
        scheduler.Txn(0).Read(0);
        scheduler.Txn(0).Update(0, 4);
        scheduler.Txn(0).Read(0);
        scheduler.Txn(0).Commit();

        scheduler.Run();

        EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
        EXPECT_EQ(1, scheduler.schedules[0].results[0]);
        EXPECT_EQ(2, scheduler.schedules[0].results[1]);
        EXPECT_EQ(3, scheduler.schedules[0].results[2]);
        EXPECT_EQ(4, scheduler.schedules[0].results[3]);

        delete &table->GetPrimaryIndex();
        delete &table->GetVersionTable();
        delete table;
    }

    // delete not exist, delete exist, read deleted, update deleted,
    // read deleted, insert back, update inserted, read newly updated,
    // delete inserted, read deleted
    {
        MVTOTransactionManager::ClearInstance();
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, &buf_mgr);
        auto txn_manager = MVTOTransactionManager::GetInstance(&buf_mgr);

        TransactionScheduler scheduler(1, table, txn_manager);
        scheduler.Txn(0).Delete(100);
        scheduler.Txn(0).Delete(0);
        scheduler.Txn(0).Read(0);
        scheduler.Txn(0).Update(0, 1);
        scheduler.Txn(0).Read(0);
        scheduler.Txn(0).Insert(0, 2);
        scheduler.Txn(0).Update(0, 3);
        scheduler.Txn(0).Read(0);
        scheduler.Txn(0).Delete(0);
        scheduler.Txn(0).Read(0);
        scheduler.Txn(0).Commit();

        scheduler.Run();

        EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
        EXPECT_EQ(-1, scheduler.schedules[0].results[0]);
        EXPECT_EQ(-1, scheduler.schedules[0].results[1]);
        EXPECT_EQ(3, scheduler.schedules[0].results[2]);
        EXPECT_EQ(-1, scheduler.schedules[0].results[3]);

        delete &table->GetPrimaryIndex();
        delete &table->GetVersionTable();
        delete table;
    }

    // insert, delete inserted, read deleted, insert again, delete again
    // read deleted, insert again, read inserted, update inserted, read updated
    {
        MVTOTransactionManager::ClearInstance();
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, &buf_mgr);
        auto txn_manager = MVTOTransactionManager::GetInstance(&buf_mgr);

        TransactionScheduler scheduler(1, table, txn_manager);

        scheduler.Txn(0).Insert(1000, 0);
        scheduler.Txn(0).Read(1000);
        scheduler.Txn(0).Delete(1000);
        scheduler.Txn(0).Read(1000);
        scheduler.Txn(0).Insert(1000, 1);
        scheduler.Txn(0).Delete(1000);
        scheduler.Txn(0).Read(1000);
        scheduler.Txn(0).Insert(1000, 2);
        scheduler.Txn(0).Read(1000);
        scheduler.Txn(0).Update(1000, 3);
        scheduler.Txn(0).Read(1000);
        scheduler.Txn(0).Commit();

        scheduler.Run();

        EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
        EXPECT_EQ(0, scheduler.schedules[0].results[0]);
        EXPECT_EQ(-1, scheduler.schedules[0].results[1]);
        EXPECT_EQ(-1, scheduler.schedules[0].results[2]);
        EXPECT_EQ(2, scheduler.schedules[0].results[3]);
        EXPECT_EQ(3, scheduler.schedules[0].results[4]);

        delete &table->GetPrimaryIndex();
        delete &table->GetVersionTable();
        delete table;
    }

    LOG_INFO("Done");
}


void ConcurrentTransactionTest(spitfire::BufferPoolConfig config, const std::string &db_path,
                               spitfire::PageMigrationPolicy policy) {
    using namespace spitfire;
    using namespace spitfire::test;
    Status s = SSDPageManager::DestroyDB(db_path);
    assert(s.ok());
    SSDPageManager ssd_page_manager(db_path, config.enable_direct_io);
    s = ssd_page_manager.Init();
    assert(s.ok());
    ConcurrentBufferManager buf_mgr(&ssd_page_manager, policy, config);
    s = buf_mgr.Init();
    assert(s.ok());

    {

        MVTOTransactionManager::ClearInstance();
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, &buf_mgr);
        auto txn_manager = MVTOTransactionManager::GetInstance(&buf_mgr);

        TransactionScheduler scheduler(2, table, txn_manager);
        scheduler.Txn(0).Insert(100, 1);
        scheduler.Txn(1).Read(100);
        scheduler.Txn(0).Read(100);
        scheduler.Txn(0).Commit();
        scheduler.Txn(1).Read(100);
        scheduler.Txn(1).Commit();

        scheduler.Run();

        EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
        EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[1].txn_result);

        EXPECT_EQ(1, scheduler.schedules[0].results[0]);
        EXPECT_EQ(-1, scheduler.schedules[1].results[0]);
        // TODO: phantom problem.
        // In fact, txn 1 should not see the inserted tuple.
        EXPECT_EQ(1, scheduler.schedules[1].results[1]);


        delete &table->GetPrimaryIndex();
        delete &table->GetVersionTable();
        delete table;
    }

    {
        MVTOTransactionManager::ClearInstance();
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, &buf_mgr);
        auto txn_manager = MVTOTransactionManager::GetInstance(&buf_mgr);

        TransactionScheduler scheduler(2, table, txn_manager);
        scheduler.Txn(0).Update(0, 1);
        scheduler.Txn(1).Read(0);
        scheduler.Txn(0).Read(0);
        scheduler.Txn(0).Commit();
        scheduler.Txn(1).Read(0);
        scheduler.Txn(1).Commit();

        scheduler.Run();

        EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
        EXPECT_EQ(ResultType::ABORTED, scheduler.schedules[1].txn_result);

        EXPECT_EQ(1, scheduler.schedules[0].results[0]);

        delete &table->GetPrimaryIndex();
        delete &table->GetVersionTable();
        delete table;
    }
    LOG_INFO("Done");
}


void MultiTransactionTest(spitfire::BufferPoolConfig config, const std::string &db_path,
                          spitfire::PageMigrationPolicy policy) {
    using namespace spitfire;
    using namespace spitfire::test;
    Status s = SSDPageManager::DestroyDB(db_path);
    assert(s.ok());
    SSDPageManager ssd_page_manager(db_path, config.enable_direct_io);
    s = ssd_page_manager.Init();
    assert(s.ok());
    ConcurrentBufferManager buf_mgr(&ssd_page_manager, policy, config);
    s = buf_mgr.Init();
    assert(s.ok());
    std::unordered_map<TuplePointer, Tuple, TuplePointerHasher> m;
    auto meta_page_pid = kInvalidPID;

    MVTOTransactionManager::ClearInstance();


    TestDataTable *table = TestingTransactionUtil::CreateTable(10, &buf_mgr);
    auto txn_manager = MVTOTransactionManager::GetInstance(&buf_mgr);

    // Txn 0: scan + select for update
    // Txn 1: scan
    // Txn 1: commit (should failed for timestamp ordering cc)
    // Txn 2: Scan + select for update
    // Txn 2: commit (should failed)
    // Txn 0: commit (success)
    {
        TransactionScheduler scheduler(3, table, txn_manager);
        scheduler.Txn(0).Scan(0, true);
        scheduler.Txn(1).Scan(0);
        scheduler.Txn(1).Commit();
        scheduler.Txn(2).Scan(0, true);
        scheduler.Txn(0).Commit();
        scheduler.Txn(2).Commit();

        scheduler.Run();
        assert(ResultType::SUCCESS == scheduler.schedules[0].txn_result);
        assert(ResultType::ABORTED == scheduler.schedules[1].txn_result);
        assert(ResultType::ABORTED == scheduler.schedules[2].txn_result);

        assert(10 == scheduler.schedules[0].results.size());
    }

    // Txn 0: scan + select for update
    // Txn 0: abort
    // Txn 1: Scan + select for update
    // Txn 1: commit (should success)
    {
        TransactionScheduler scheduler(2, table, txn_manager);
        scheduler.Txn(0).Scan(0, true);
        scheduler.Txn(0).Abort();
        scheduler.Txn(1).Scan(0, true);
        scheduler.Txn(1).Commit();

        scheduler.Run();
        assert(ResultType::ABORTED == scheduler.schedules[0].txn_result);
        assert(ResultType::SUCCESS == scheduler.schedules[1].txn_result);

        assert(10 == scheduler.schedules[1].results.size());
    }

    // Txn 0: read + select for update
    // Txn 0: abort
    // Txn 1: read + select for update
    // Txn 1: commit (should success)
    {
        TransactionScheduler scheduler(2, table, txn_manager);
        scheduler.Txn(0).Read(0, true);
        scheduler.Txn(0).Abort();
        scheduler.Txn(1).Read(0, true);
        scheduler.Txn(1).Commit();

        scheduler.Run();
        assert(ResultType::ABORTED == scheduler.schedules[0].txn_result);
        assert(ResultType::SUCCESS == scheduler.schedules[1].txn_result);

        assert(1 == scheduler.schedules[1].results.size());
    }

    // read, read, read, read, update, read, read not exist
    // another txn read
    {
        TransactionScheduler scheduler(2, table, txn_manager);
        scheduler.Txn(0).Read(0);
        scheduler.Txn(0).Read(0, true);
        scheduler.Txn(0).Read(0);
        scheduler.Txn(0).Read(0);
        scheduler.Txn(0).Update(0, 1);
        scheduler.Txn(0).Read(0);
        scheduler.Txn(0).Read(100, true);
        scheduler.Txn(0).Commit();
        scheduler.Txn(1).Read(0, true);
        scheduler.Txn(1).Commit();

        scheduler.Run();

        assert(ResultType::SUCCESS == scheduler.schedules[0].txn_result);
        assert(ResultType::SUCCESS == scheduler.schedules[1].txn_result);
        assert(0 == scheduler.schedules[0].results[0]);
        assert(0 == scheduler.schedules[0].results[1]);
        assert(0 == scheduler.schedules[0].results[2]);
        assert(0 == scheduler.schedules[0].results[3]);
        assert(1 == scheduler.schedules[0].results[4]);
        assert(-1 == scheduler.schedules[0].results[5]);
        assert(1 == scheduler.schedules[1].results[0]);
    }


    {
        // Test commit/abort protocol when part of the read-own tuples get updated.
        TransactionScheduler scheduler(3, table, txn_manager);
        scheduler.Txn(0).Read(3, true);
        scheduler.Txn(0).Read(4, true);
        scheduler.Txn(0).Update(3, 1);
        scheduler.Txn(0).Abort();
        scheduler.Txn(1).Read(3, true);
        scheduler.Txn(1).Read(4, true);
        scheduler.Txn(1).Update(3, 2);
        scheduler.Txn(1).Commit();
        scheduler.Txn(2).Read(3);
        scheduler.Txn(2).Read(4);
        scheduler.Txn(2).Commit();

        scheduler.Run();
        assert(ResultType::ABORTED == scheduler.schedules[0].txn_result);
        assert(ResultType::SUCCESS == scheduler.schedules[1].txn_result);
        assert(ResultType::SUCCESS == scheduler.schedules[2].txn_result);

        assert(0 == scheduler.schedules[1].results[0]);
        assert(0 == scheduler.schedules[1].results[1]);
        assert(2 == scheduler.schedules[2].results[0]);
        assert(0 == scheduler.schedules[2].results[1]);
    }

    delete &table->GetPrimaryIndex();
    delete &table->GetVersionTable();
    delete table;
}


void StressTest(spitfire::BufferPoolConfig config, const std::string &db_path,
                spitfire::PageMigrationPolicy policy) {
    using namespace spitfire;
    using namespace spitfire::test;
    Status s = SSDPageManager::DestroyDB(db_path);
    assert(s.ok());
    SSDPageManager ssd_page_manager(db_path, config.enable_direct_io);
    s = ssd_page_manager.Init();
    assert(s.ok());
    ConcurrentBufferManager buf_mgr(&ssd_page_manager, policy, config);
    s = buf_mgr.Init();
    assert(s.ok());

    MVTOTransactionManager::ClearInstance();

    const int num_txn = 10;  // 5
    const int scale = 4;    // 20
    const int num_key = 256;  // 256
    srand(15721);

    TestDataTable *table = TestingTransactionUtil::CreateTable(num_key, &buf_mgr);
    auto txn_manager = MVTOTransactionManager::GetInstance(&buf_mgr);

    TransactionScheduler scheduler(num_txn, table, txn_manager);
    scheduler.SetConcurrent(true);
    for (int i = 0; i < num_txn; i++) {
        for (int j = 0; j < scale; j++) {
            // randomly select two unique keys
            int key1 = rand() % num_key;
            int key2 = rand() % num_key;
            int delta = rand() % 1000;
            // Store subtracted value
            scheduler.Txn(i).ReadStore(key1, -delta);
            scheduler.Txn(i).Update(key1, TXN_STORED_VALUE);
            LOG_INFO("Txn %d deducts %d from %d", i, delta, key1);
            // Store increased value
            scheduler.Txn(i).ReadStore(key2, delta);
            scheduler.Txn(i).Update(key2, TXN_STORED_VALUE);
            LOG_INFO("Txn %d adds %d to %d", i, delta, key2);
        }
        scheduler.Txn(i).Commit();
    }
    scheduler.Run();

    // Read all values
    TransactionScheduler scheduler2(1, table, txn_manager);
    for (int i = 0; i < num_key; i++) {
        scheduler2.Txn(0).Read(i);
    }
    scheduler2.Txn(0).Commit();
    scheduler2.Run();

    EXPECT_EQ(ResultType::SUCCESS,
              scheduler2.schedules[0].txn_result);
    // The sum should be zero
    int sum = 0;
    for (auto result : scheduler2.schedules[0].results) {
        LOG_INFO("Table has tuple value: %d", result);
        sum += result;
    }

    EXPECT_EQ(0, sum);

    // stats
    int nabort = 0;
    for (auto &schedule : scheduler.schedules) {
        if (schedule.txn_result == ResultType::ABORTED) nabort += 1;
    }
    LOG_INFO("Abort: %d out of %d", nabort, num_txn);

    delete &table->GetPrimaryIndex();
    delete &table->GetVersionTable();
    delete table;
}

void DirtyWriteTest(spitfire::BufferPoolConfig config, const std::string &db_path,
                    spitfire::PageMigrationPolicy policy) {
    using namespace spitfire;
    using namespace spitfire::test;
    Status s = SSDPageManager::DestroyDB(db_path);
    assert(s.ok());
    SSDPageManager ssd_page_manager(db_path, config.enable_direct_io);
    s = ssd_page_manager.Init();
    assert(s.ok());
    ConcurrentBufferManager buf_mgr(&ssd_page_manager, policy, config);
    s = buf_mgr.Init();
    assert(s.ok());

    {
        MVTOTransactionManager::ClearInstance();
        auto txn_manager = MVTOTransactionManager::GetInstance(&buf_mgr);
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, &buf_mgr);
        TransactionScheduler scheduler(3, table, txn_manager);
        // T0 updates (0, ?) to (0, 1)
        // T1 updates (0, ?) to (0, 2)
        // T0 commits
        // T1 commits
        scheduler.Txn(0).Update(0, 1);
        scheduler.Txn(1).Update(0, 2);
        scheduler.Txn(0).Commit();
        scheduler.Txn(1).Commit();

        // observer
        scheduler.Txn(2).Read(0);
        scheduler.Txn(2).Commit();

        scheduler.Run();
        auto &schedules = scheduler.schedules;


        assert(schedules[0].txn_result == ResultType::SUCCESS);
        assert(schedules[1].txn_result == ResultType::ABORTED);

        //if (isolation == IsolationLevelType::SNAPSHOT) {
        //    assert(0 == scheduler.schedules[2].results[0]);
        //} else {
        assert(1 == scheduler.schedules[2].results[0]);
        //}


        schedules.clear();

        delete &table->GetPrimaryIndex();
        delete &table->GetVersionTable();
        delete table;
    }

    {
        MVTOTransactionManager::ClearInstance();
        auto txn_manager = MVTOTransactionManager::GetInstance(&buf_mgr);
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, &buf_mgr);
        TransactionScheduler scheduler(3, table, txn_manager);
        // T0 updates (0, ?) to (0, 1)
        // T1 updates (0, ?) to (0, 2)
        // T1 commits
        // T0 commits
        scheduler.Txn(0).Update(0, 1);
        scheduler.Txn(1).Update(0, 2);
        scheduler.Txn(1).Commit();
        scheduler.Txn(0).Commit();

        // observer
        scheduler.Txn(2).Read(0);
        scheduler.Txn(2).Commit();

        scheduler.Run();
        auto &schedules = scheduler.schedules;

        assert(schedules[0].txn_result == ResultType::SUCCESS);
        assert(schedules[1].txn_result == ResultType::ABORTED);

//            if (isolation == IsolationLevelType::SNAPSHOT) {
//                EXPECT_EQ(0, scheduler.schedules[2].results[0]);
//            } else {
        assert(1 == scheduler.schedules[2].results[0]);
//            }

        schedules.clear();
        delete &table->GetPrimaryIndex();
        delete &table->GetVersionTable();
        delete table;
    }

    {
        MVTOTransactionManager::ClearInstance();
        auto txn_manager = MVTOTransactionManager::GetInstance(&buf_mgr);
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, &buf_mgr);
        TransactionScheduler scheduler(3, table, txn_manager);
        // T0 updates (0, ?) to (0, 1)
        // T1 updates (0, ?) to (0, 2)
        // T0 aborts
        // T1 commits
        scheduler.Txn(0).Update(0, 1);
        scheduler.Txn(1).Update(0, 2);
        scheduler.Txn(0).Abort();
        scheduler.Txn(1).Commit();

        // observer
        scheduler.Txn(2).Read(0);
        scheduler.Txn(2).Commit();

        scheduler.Run();
        auto &schedules = scheduler.schedules;

        //if (conflict == ConflictAvoidanceType::ABORT) {
        assert(schedules[0].txn_result == ResultType::ABORTED);
        assert(schedules[1].txn_result == ResultType::ABORTED);

        EXPECT_EQ(0, scheduler.schedules[2].results[0]);
        //}

        schedules.clear();
        delete &table->GetPrimaryIndex();
        delete &table->GetVersionTable();
        delete table;
    }

    {
        MVTOTransactionManager::ClearInstance();
        auto txn_manager = MVTOTransactionManager::GetInstance(&buf_mgr);
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, &buf_mgr);
        // T0 updates (0, ?) to (0, 1)
        // T1 updates (0, ?) to (0, 2)
        // T1 commits
        // T0 aborts
        TransactionScheduler scheduler(3, table, txn_manager);
        scheduler.Txn(0).Update(0, 1);
        scheduler.Txn(1).Update(0, 2);
        scheduler.Txn(1).Commit();
        scheduler.Txn(0).Abort();

        // observer
        scheduler.Txn(2).Read(0);
        scheduler.Txn(2).Commit();

        scheduler.Run();
        auto &schedules = scheduler.schedules;

        EXPECT_TRUE(schedules[0].txn_result == ResultType::ABORTED);
        EXPECT_TRUE(schedules[1].txn_result == ResultType::ABORTED);

        EXPECT_EQ(0, scheduler.schedules[2].results[0]);

        schedules.clear();
        delete &table->GetPrimaryIndex();
        delete &table->GetVersionTable();
        delete table;
    }


    {
        MVTOTransactionManager::ClearInstance();
        auto txn_manager = MVTOTransactionManager::GetInstance(&buf_mgr);
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, &buf_mgr);
        TransactionScheduler scheduler(3, table, txn_manager);
        // T0 updates (0, ?) to (0, 1)
        // T1 updates (0, ?) to (0, 2)
        // T0 aborts
        // T1 aborts
        scheduler.Txn(0).Update(0, 1);
        scheduler.Txn(1).Update(0, 2);
        scheduler.Txn(0).Abort();
        scheduler.Txn(1).Abort();

        // observer
        scheduler.Txn(2).Read(0);
        scheduler.Txn(2).Commit();

        scheduler.Run();
        auto &schedules = scheduler.schedules;


        EXPECT_TRUE(schedules[0].txn_result == ResultType::ABORTED);
        EXPECT_TRUE(schedules[1].txn_result == ResultType::ABORTED);

        EXPECT_EQ(0, scheduler.schedules[2].results[0]);

        schedules.clear();
        delete &table->GetPrimaryIndex();
        delete &table->GetVersionTable();
        delete table;
    }


    {
        MVTOTransactionManager::ClearInstance();
        auto txn_manager = MVTOTransactionManager::GetInstance(&buf_mgr);
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, &buf_mgr);
        TransactionScheduler scheduler(3, table, txn_manager);
        // T0 updates (0, ?) to (0, 1)
        // T1 updates (0, ?) to (0, 2)
        // T1 aborts
        // T0 aborts
        scheduler.Txn(0).Update(0, 1);
        scheduler.Txn(1).Update(0, 2);
        scheduler.Txn(1).Abort();
        scheduler.Txn(0).Abort();

        // observer
        scheduler.Txn(2).Read(0);
        scheduler.Txn(2).Commit();

        scheduler.Run();
        auto &schedules = scheduler.schedules;


        EXPECT_TRUE(schedules[0].txn_result == ResultType::ABORTED);
        EXPECT_TRUE(schedules[1].txn_result == ResultType::ABORTED);

        EXPECT_EQ(0, scheduler.schedules[2].results[0]);


        schedules.clear();
        delete &table->GetPrimaryIndex();
        delete &table->GetVersionTable();
        delete table;
    }
}

void DirtyReadTest(spitfire::BufferPoolConfig config, const std::string &db_path,
                   spitfire::PageMigrationPolicy policy) {
    using namespace spitfire;
    using namespace spitfire::test;
    Status s = SSDPageManager::DestroyDB(db_path);
    assert(s.ok());
    SSDPageManager ssd_page_manager(db_path, config.enable_direct_io);
    s = ssd_page_manager.Init();
    assert(s.ok());
    ConcurrentBufferManager buf_mgr(&ssd_page_manager, policy, config);
    s = buf_mgr.Init();
    assert(s.ok());

    {
        MVTOTransactionManager::ClearInstance();
        auto txn_manager = MVTOTransactionManager::GetInstance(&buf_mgr);
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, &buf_mgr);
        TransactionScheduler scheduler(3, table, txn_manager);
        // T0 updates (0, ?) to (0, 1)
        // T1 reads (0, ?)
        // T0 commits
        // T1 commits
        scheduler.Txn(0).Update(0, 1);
        scheduler.Txn(1).Read(0);
        scheduler.Txn(0).Commit();
        scheduler.Txn(1).Commit();

        // observer
        scheduler.Txn(2).Read(0);
        scheduler.Txn(2).Commit();

        scheduler.Run();
        auto &schedules = scheduler.schedules;

        EXPECT_TRUE(schedules[0].txn_result == ResultType::SUCCESS);
        EXPECT_TRUE(schedules[1].txn_result == ResultType::ABORTED);
        EXPECT_EQ(1, scheduler.schedules[2].results[0]);


        delete &table->GetPrimaryIndex();
        delete &table->GetVersionTable();
        delete table;
    }

    {
        MVTOTransactionManager::ClearInstance();
        auto txn_manager = MVTOTransactionManager::GetInstance(&buf_mgr);
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, &buf_mgr);
        TransactionScheduler scheduler(3, table, txn_manager);
        // T0 updates (0, ?) to (0, 1)
        // T1 reads (0, ?)
        // T0 aborts
        // T1 commits
        scheduler.Txn(0).Update(0, 1);
        scheduler.Txn(1).Read(0);
        scheduler.Txn(0).Abort();
        scheduler.Txn(1).Commit();

        // observer
        scheduler.Txn(2).Read(0);
        scheduler.Txn(2).Commit();

        scheduler.Run();
        auto &schedules = scheduler.schedules;


        EXPECT_TRUE(schedules[0].txn_result == ResultType::ABORTED);
        EXPECT_TRUE(schedules[1].txn_result == ResultType::ABORTED);
        EXPECT_EQ(0, scheduler.schedules[2].results[0]);

        schedules.clear();
        delete &table->GetPrimaryIndex();
        delete &table->GetVersionTable();
        delete table;
    }
}

void FuzzyReadTest(spitfire::BufferPoolConfig config, const std::string &db_path,
                   spitfire::PageMigrationPolicy policy) {
    using namespace spitfire;
    using namespace spitfire::test;
    Status s = SSDPageManager::DestroyDB(db_path);
    assert(s.ok());
    SSDPageManager ssd_page_manager(db_path, config.enable_direct_io);
    s = ssd_page_manager.Init();
    assert(s.ok());
    ConcurrentBufferManager buf_mgr(&ssd_page_manager, policy, config);
    s = buf_mgr.Init();
    assert(s.ok());

    {
        MVTOTransactionManager::ClearInstance();
        auto txn_manager = MVTOTransactionManager::GetInstance(&buf_mgr);
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, &buf_mgr);
        TransactionScheduler scheduler(3, table, txn_manager);
        // T0 obtains a smaller timestamp.
        // T0 reads (0, ?)
        // T1 updates (0, ?) to (0, 1)
        // T1 commits
        // T0 reads (0, ?)
        // T0 commits
        scheduler.Txn(0).Read(0);
        scheduler.Txn(1).Update(0, 1);
        scheduler.Txn(1).Commit();
        scheduler.Txn(0).Read(0); // Should read old version
        scheduler.Txn(0).Commit();

        // observer
        scheduler.Txn(2).Read(0);
        scheduler.Txn(2).Commit();

        scheduler.Run();
        auto &schedules = scheduler.schedules;

        EXPECT_TRUE(schedules[0].txn_result == ResultType::SUCCESS);
        EXPECT_TRUE(schedules[1].txn_result == ResultType::SUCCESS);

        EXPECT_EQ(0, scheduler.schedules[0].results[0]);
        EXPECT_EQ(0, scheduler.schedules[0].results[1]);

        EXPECT_EQ(1, scheduler.schedules[2].results[0]);


        delete &table->GetPrimaryIndex();
        delete &table->GetVersionTable();
        delete table;
    }

    {
        MVTOTransactionManager::ClearInstance();
        auto txn_manager = MVTOTransactionManager::GetInstance(&buf_mgr);
        TestDataTable *table = TestingTransactionUtil::CreateTable(10, &buf_mgr);

        TransactionScheduler scheduler(3, table, txn_manager);
        // T1 obtains a smaller timestamp.
        // T1 reads (0, ?)
        // T0 reads (0, ?)
        // T1 updates (0, ?) to (0, 1)
        // T1 commits
        // T0 reads (0, ?)
        // T0 commits
        scheduler.Txn(1).Read(0);
        scheduler.Txn(0).Read(0);
        scheduler.Txn(1).Update(0, 1);
        scheduler.Txn(1).Commit();
        scheduler.Txn(0).Read(0);
        scheduler.Txn(0).Commit();

        // observer
        scheduler.Txn(2).Read(0);
        scheduler.Txn(2).Commit();

        scheduler.Run();
        auto &schedules = scheduler.schedules;

        EXPECT_TRUE(schedules[0].txn_result == ResultType::SUCCESS);
        EXPECT_TRUE(schedules[1].txn_result == ResultType::ABORTED);

        EXPECT_EQ(0, scheduler.schedules[0].results[0]);
        EXPECT_EQ(0, scheduler.schedules[0].results[1]);
        EXPECT_EQ(0, scheduler.schedules[1].results[0]);

        EXPECT_EQ(0, scheduler.schedules[2].results[0]);

        schedules.clear();
        delete &table->GetPrimaryIndex();
        delete &table->GetVersionTable();
        delete table;
    }
}


void MVCCTest(spitfire::BufferPoolConfig config, const std::string &db_path,
              spitfire::PageMigrationPolicy policy) {
    using namespace spitfire;
    using namespace spitfire::test;
    Status s = SSDPageManager::DestroyDB(db_path);
    assert(s.ok());
    SSDPageManager ssd_page_manager(db_path, config.enable_direct_io);
    s = ssd_page_manager.Init();
    assert(s.ok());
    ConcurrentBufferManager buf_mgr(&ssd_page_manager, policy, config);
    s = buf_mgr.Init();
    assert(s.ok());

    MVTOTransactionManager::ClearInstance();
    auto txn_manager = MVTOTransactionManager::GetInstance(&buf_mgr);
    TestDataTable *table = TestingTransactionUtil::CreateTable(10, &buf_mgr);
    // read, read, read, read, update, read, read not exist
    // another txn read
    {
        TransactionScheduler scheduler(2, table, txn_manager);
        scheduler.Txn(0).Read(0);
        scheduler.Txn(0).Read(0);
        scheduler.Txn(0).Read(0);
        scheduler.Txn(0).Read(0);
        scheduler.Txn(0).Update(0, 1);
        scheduler.Txn(0).Read(0);
        scheduler.Txn(0).Read(100);
        scheduler.Txn(0).Commit();
        scheduler.Txn(1).Read(0);
        scheduler.Txn(1).Commit();

        scheduler.Run();
        assert(scheduler.schedules[0].results[0] == 0);
        assert(scheduler.schedules[0].results[1] == 0);
        assert(scheduler.schedules[0].results[2] == 0);
        assert(scheduler.schedules[0].results[3] == 0);
        assert(scheduler.schedules[0].results[4] == 1);
        assert(scheduler.schedules[0].results[5] == -1);
        assert(scheduler.schedules[1].results[0] == 1);
    }

    // update, update, update, update, read
    {
        TransactionScheduler scheduler(1, table, txn_manager);
        scheduler.Txn(0).Update(0, 1);
        scheduler.Txn(0).Update(0, 2);
        scheduler.Txn(0).Update(0, 3);
        scheduler.Txn(0).Update(0, 4);
        scheduler.Txn(0).Read(0);
        scheduler.Txn(0).Commit();
        scheduler.Run();
        assert(scheduler.schedules[0].results[0] == 4);
    }

    // insert, delete inserted, read deleted, insert again, delete again
    // read deleted, insert again, read inserted, update inserted, read updated
    {
        TransactionScheduler scheduler(1, table, txn_manager);

        scheduler.Txn(0).Insert(1000, 0);
        scheduler.Txn(0).Delete(1000);
        scheduler.Txn(0).Read(1000);
        scheduler.Txn(0).Insert(1000, 1);
        scheduler.Txn(0).Delete(1000);
        scheduler.Txn(0).Read(1000);
        scheduler.Txn(0).Insert(1000, 2);
        scheduler.Txn(0).Read(1000);
        scheduler.Txn(0).Update(1000, 3);
        scheduler.Txn(0).Read(1000);
        scheduler.Txn(0).Commit();

        scheduler.Run();

        assert(scheduler.schedules[0].results[0] == -1);
        assert(scheduler.schedules[0].results[1] == -1);
        assert(scheduler.schedules[0].results[2] == 2);
        assert(scheduler.schedules[0].results[3] == 3);
    }

    delete &table->GetPrimaryIndex();
    delete &table->GetVersionTable();
    delete table;
}

void BasicTableTest(spitfire::BufferPoolConfig config, const std::string &db_path,
                    spitfire::PageMigrationPolicy policy) {
    using namespace spitfire;
    Status s = SSDPageManager::DestroyDB(db_path);
    assert(s.ok());
    SSDPageManager ssd_page_manager(db_path, config.enable_direct_io);
    s = ssd_page_manager.Init();
    assert(s.ok());
    ConcurrentBufferManager buf_mgr(&ssd_page_manager, policy, config);
    s = buf_mgr.Init();
    assert(s.ok());
    std::unordered_map<TuplePointer, Tuple, TuplePointerHasher> m;
    auto meta_page_pid = kInvalidPID;
    const int n_tuples = 100000;

    auto correctness_check = [&m, &buf_mgr, n_tuples](PartitionedHeapTable<uint64_t, Tuple> &heap_table) {
        int cnt = 0;
        heap_table.Scan([&cnt, &m](TuplePointer ptr, const Tuple &tuple) {
            cnt++;
            auto it = m.find(ptr);
            assert(it != m.end());
            assert(it->first == ptr);
            assert(memcmp(&tuple, &it->second, sizeof(Tuple)) == 0);
            return false;
        });
        assert(cnt == n_tuples);

        for (auto &kv : m) {
            auto &ptr = kv.first;
            auto &t = kv.second;
            auto pid = ptr.pid;
            auto offset = ptr.off;

            ConcurrentBufferManager::PageAccessor accessor;
            Status s = buf_mgr.Get(pid, accessor);
            assert(s.ok());
            auto page_desc = accessor.GetPageDesc();
            auto slice = accessor.PrepareForRead(offset, sizeof(Tuple));
            const Tuple *tuple = reinterpret_cast<const Tuple *>(slice.data());
            assert(memcmp(&t, tuple, sizeof(Tuple)) == 0);
            accessor.FinishAccess();
            buf_mgr.Put(page_desc);
        }
    };

    {
        PartitionedHeapTable<uint64_t, Tuple> heap_table;
        s = heap_table.Init(meta_page_pid, &buf_mgr, true);
        assert(s.ok());
        meta_page_pid = heap_table.GetMetaPagePid();
        assert(meta_page_pid != kInvalidPID);


        for (int i = 0; i < n_tuples; ++i) {
            Tuple t;
            t.key = i;
            for (int j = 0; j < 10; ++j) {
                t.values[j] = i;
            }
            TuplePointer ptr;
            heap_table.Insert(t, ptr);
            m[ptr] = t;
        }

        correctness_check(heap_table);
    }

    // Reopen the table
    {
        assert(meta_page_pid != kInvalidPID);
        PartitionedHeapTable<uint64_t, Tuple> heap_table;
        s = heap_table.Init(meta_page_pid, &buf_mgr, false);
        assert(s.ok());
        assert(meta_page_pid != kInvalidPID);
        assert(meta_page_pid == heap_table.GetMetaPagePid());
        correctness_check(heap_table);
    }
}


void StressTBBHashmap(spitfire::ThreadPool &tp, int n_workers = 10) {
    tbb::concurrent_hash_map<uint32_t, uint32_t> m;
    int n = 131702 * 3;
    for (int i = 0; i < n; ++i) {
        m.insert(std::make_pair(i, i));
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


void StressCuckooHashmap(spitfire::ThreadPool &tp, int n_workers = 10) {
    spitfire::CuckooMap<uint32_t, uint32_t> m;
    int n = 131702 * 3;
    for (int i = 0; i < n; ++i) {
        m.Insert(i, i);
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

namespace spitfire {
std::vector<BaseDataTable*> database_tables;
}
int main(int argc, char **argv) {
    std::string db_path = "/mnt/900p/buf";
    std::string workload_filepath = ""; // Empty as default uniform workload
    std::string nvm_path = "/mnt/900p";
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
//    const size_t nvm_heap_size = 16UL * 1024 * 1024 * 1024; // 16GB;
//    spitfire::libpm_init(nvm_heap_filepath, nvm_heap_size);

    {
        spitfire::PageMigrationPolicy policy;
        policy.Dr = policy.Dw = 0.5;
        policy.Nr = policy.Nw = 0.5;

        const int n_ops = 1024 * 1024 * 50;
        const int n_kvs = n_ops;
        const int n_pages = n_kvs * sizeof(uint64_t) * 2 / spitfire::kPageSize;
        spitfire::BufferPoolConfig config;
        config.enable_nvm_buf_pool = true;
        config.enable_mini_page = enable_mini_page;
        config.enable_direct_io = true;
        //config.nvm_admission_set_size_limit = 100;
        //config.enable_hymem = true;
        config.dram_buf_pool_cap_in_bytes = n_pages / 10 * spitfire::kPageSize;
        config.nvm_buf_pool_cap_in_bytes = n_pages / 5 * spitfire::kPageSize;
        config.nvm_heap_file_path = nvm_heap_filepath;
        config.wal_file_path = wal_file_path;

        BasicTableTest(config, db_path, policy);
        BasicTransactionTest(config, db_path, policy);
        MVCCTest(config, db_path, policy);
        AbortVersionChainTest(config, db_path, policy);
        SingleTransactionTest(config, db_path, policy);
        SingleTransactionTest2(config, db_path, policy);
        ConcurrentTransactionTest(config, db_path, policy);
        MultiTransactionTest(config, db_path, policy);
        DirtyWriteTest(config, db_path, policy);
        DirtyReadTest(config, db_path, policy);
        FuzzyReadTest(config, db_path, policy);
        StressTest(config, db_path, policy);
    }

//    size_t n_threads = 16;
//    spitfire::ThreadPool tp(n_threads);
//
//    StressCuckooHashmap(tp, 1);
//    StressCuckooHashmap(tp, 16);
//    StressTBBHashmap(tp, 1);
//    StressTBBHashmap(tp, 16);
    return 0;
}