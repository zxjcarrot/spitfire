//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb_loader.cpp
//
// Identification: src/main/ycsb/ycsb_loader.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <iostream>
#include <ctime>

#include "util/logger.h"
#include "util/sync.h"
#include "engine/executor.h"
#include "benchmark/ycsb/ycsb_loader.h"
#include "benchmark/ycsb/ycsb_configuration.h"

namespace spitfire {
std::vector<BaseDataTable*> database_tables;
namespace benchmark {
namespace ycsb {

#define THREAD_POOL_SIZE 48
ThreadPool the_tp(THREAD_POOL_SIZE);

YCSBTable *user_table = nullptr;


struct YCSBDatabaseMetaPage {
    static constexpr pid_t meta_page_pid = 0; // Assume page 0 stores the metadata of YCSB Database
    struct MetaData {
        lsn_t lsn;
        pid_t primary_index_root_page_pid;
        pid_t version_table_meta_page_pid;
        txn_id_t current_tid_counter;
        rid_t current_row_id_counter;
    };
    union {
        MetaData m;
        Page p;
    };
};

void CreateYCSBDatabase(ConcurrentBufferManager *buf_mgr) {
    pid_t ycsb_database_meta_page_pid = kInvalidPID;
    Status s = buf_mgr->NewPage(ycsb_database_meta_page_pid);
    assert(s.ok());
    assert(ycsb_database_meta_page_pid == YCSBDatabaseMetaPage::meta_page_pid);
    auto primary_index = new ClusteredIndex<uint64_t, YCSBTuple>(buf_mgr);
    pid_t primary_index_root_page_pid = kInvalidPID;
    s = primary_index->Init(primary_index_root_page_pid);
    assert(s.ok());
    auto version_table = new PartitionedHeapTable<uint64_t, YCSBTuple>;
    pid_t version_table_meta_page_pid = kInvalidPID;
    s = version_table->Init(version_table_meta_page_pid, buf_mgr, true);
    assert(s.ok());
    user_table = new YCSBTable(*primary_index, *version_table);

    // Record the pids of the primary index root and version table meta page
    ConcurrentBufferManager::PageAccessor accessor;
    s = buf_mgr->Get(ycsb_database_meta_page_pid, accessor, ConcurrentBufferManager::INTENT_WRITE_FULL);
    assert(s.ok());
    auto slice = accessor.PrepareForWrite(0, kPageSize);
    auto ycsb_database_meta_page_desc = accessor.GetPageDesc();
    auto ycsb_database_meta_page = reinterpret_cast<YCSBDatabaseMetaPage *>(slice.data());
    ycsb_database_meta_page->m.primary_index_root_page_pid = primary_index_root_page_pid;
    ycsb_database_meta_page->m.version_table_meta_page_pid = version_table_meta_page_pid;
    accessor.FinishAccess();
    buf_mgr->Put(ycsb_database_meta_page_desc);
    database_tables.push_back(user_table);
}

void CreateYCSBDatabaseFromPersistentStorage(ConcurrentBufferManager *buf_mgr) {
    pid_t ycsb_database_meta_page_pid = YCSBDatabaseMetaPage::meta_page_pid;
    // Restore the pids of the primary index root and version table meta page
    ConcurrentBufferManager::PageAccessor accessor;
    Status s = buf_mgr->Get(ycsb_database_meta_page_pid, accessor, ConcurrentBufferManager::INTENT_READ_FULL);
    assert(s.ok());
    auto slice = accessor.PrepareForRead(0, kPageSize);
    auto ycsb_database_meta_page_desc = accessor.GetPageDesc();
    const auto ycsb_database_meta_page = *reinterpret_cast<YCSBDatabaseMetaPage *>(slice.data());

    pid_t primary_index_root_page_pid = ycsb_database_meta_page.m.primary_index_root_page_pid;
    pid_t version_table_meta_page_pid = ycsb_database_meta_page.m.version_table_meta_page_pid;
    accessor.FinishAccess();
    buf_mgr->Put(ycsb_database_meta_page_desc);

    auto primary_index = new ClusteredIndex<uint64_t, YCSBTuple>(buf_mgr);
    s = primary_index->Init(primary_index_root_page_pid);
    assert(s.ok());
    auto version_table = new PartitionedHeapTable<uint64_t, YCSBTuple>;
    s = version_table->Init(version_table_meta_page_pid, buf_mgr, true); // Forget about old versions
    assert(s.ok());
    user_table = new YCSBTable(*primary_index, *version_table);

    MVTOTransactionManager::GetInstance(buf_mgr)->SetCurrentRowId(ycsb_database_meta_page.m.current_row_id_counter);
    MVTOTransactionManager::GetInstance(buf_mgr)->SetCurrentTid(ycsb_database_meta_page.m.current_tid_counter);

    database_tables.push_back(user_table);
}

void DestroyYCSBDatabase(ConcurrentBufferManager *buf_mgr) {
    pid_t ycsb_database_meta_page_pid = YCSBDatabaseMetaPage::meta_page_pid;
    // Records the pids of the primary index roots
    ConcurrentBufferManager::PageAccessor accessor;
    Status s = buf_mgr->Get(ycsb_database_meta_page_pid, accessor, ConcurrentBufferManager::INTENT_WRITE_FULL);
    assert(s.ok());
    auto slice = accessor.PrepareForWrite(0, kPageSize);
    auto ycsb_database_meta_page_desc = accessor.GetPageDesc();
    auto ycsb_database_meta_page = reinterpret_cast<YCSBDatabaseMetaPage *>(slice.data());
    ycsb_database_meta_page->m.primary_index_root_page_pid = user_table->GetPrimaryIndex().GetRootPid();
    ycsb_database_meta_page->m.current_tid_counter = MVTOTransactionManager::GetInstance(buf_mgr)->GetCurrentTid();
    ycsb_database_meta_page->m.current_row_id_counter = MVTOTransactionManager::GetInstance(buf_mgr)->GetCurrentRowId();
    accessor.FinishAccess();
    buf_mgr->Put(ycsb_database_meta_page_desc);

    buf_mgr->EndPurging();
    delete &user_table->GetPrimaryIndex();
    delete &user_table->GetVersionTable();
    delete user_table;
    user_table = nullptr;
}

void LoadYCSBRows(ConcurrentBufferManager *buf_mgr, const int begin_rowid, const int end_rowid) {
    /////////////////////////////////////////////////////////
    // Load in the data
    /////////////////////////////////////////////////////////

    constexpr int kBatchSize = 1024;

    // Insert kBatchSize Tuples at a time

    // Insert tuples into the data table.
    auto txn_manager = MVTOTransactionManager::GetInstance(buf_mgr);
    for (int rowid = begin_rowid; rowid < end_rowid; ) {
        auto txn = txn_manager->BeginTransaction();
        int next_batch_rowid = std::min(end_rowid, rowid + kBatchSize);
        for (; rowid < next_batch_rowid; rowid++) {
            YCSBTuple tuple;
            tuple.key = rowid;
            for (int i = 0; i < COLUMN_COUNT; ++i)
                memset(tuple.cols[i], 0, sizeof(tuple.cols[i]));

            InsertExecutor<uint64_t, YCSBTuple> executor(*user_table, tuple, txn, buf_mgr);
            auto res = executor.Execute();
            assert(res == true);
        }
        auto res = txn_manager->CommitTransaction(txn);
        assert(res == ResultType::SUCCESS);
    }
}

void LoadYCSBDatabase(ConcurrentBufferManager *buf_mgr) {

    std::chrono::steady_clock::time_point start_time;
    start_time = std::chrono::steady_clock::now();

    const int tuple_count = state.scale_factor * 1000;
    int row_per_thread = tuple_count / state.loader_count;

    CountDownLatch latch(state.loader_count);

    for (int thread_id = 0; thread_id < state.loader_count - 1; ++thread_id) {
        int begin_rowid = row_per_thread * thread_id;
        int end_rowid = row_per_thread * (thread_id + 1);
        the_tp.enqueue([&latch, buf_mgr, begin_rowid, end_rowid]() {
            LoadYCSBRows(buf_mgr, begin_rowid, end_rowid);
            latch.CountDown();
        });
    }

    int thread_id = state.loader_count - 1;
    int begin_rowid = row_per_thread * thread_id;
    int end_rowid = tuple_count;
    the_tp.enqueue([&latch, buf_mgr, begin_rowid, end_rowid]() {
        LoadYCSBRows(buf_mgr, begin_rowid, end_rowid);
        latch.CountDown();
    });

    latch.Await();

    std::chrono::steady_clock::time_point end_time = std::chrono::steady_clock::now();
    double diff = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    LOG_INFO("database table loading time = %lf ms", diff);

    //LOG_INFO("%sTABLE SIZES%s", peloton::GETINFO_HALF_THICK_LINE.c_str(), peloton::GETINFO_HALF_THICK_LINE.c_str());
    LOG_INFO("user count = %lu", tuple_count);

}

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
