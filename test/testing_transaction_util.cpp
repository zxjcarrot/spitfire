//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_tests_util.cpp
//
// Identification: test/concurrency/transaction_tests_util.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "testing_transaction_util.h"

#include "engine/txn.h"
#include "engine/executor.h"

namespace spitfire {

namespace test {

ThreadPool TransactionScheduler::tp(16);

TestDataTable *TestingTransactionUtil::CreateTable(
        int num_key, ConcurrentBufferManager *buf_mgr) {
    assert(buf_mgr != nullptr);
    auto primary_index = new ClusteredIndex<uint64_t, TestTuple>(buf_mgr);
    pid_t root_page_pid = kInvalidPID;
    Status s = primary_index->Init(root_page_pid);
    if (!s.ok())
        return nullptr;

    auto version_table = new PartitionedHeapTable<uint64_t, TestTuple>();
    pid_t meta_head_pid = kInvalidPID;
    s = version_table->Init(meta_head_pid, buf_mgr, true);
    if (!s.ok()) {
        delete primary_index;
        return nullptr;
    }
    meta_head_pid = version_table->GetMetaPagePid();
    assert(meta_head_pid != kInvalidPID);

    auto table = new TestDataTable(*primary_index, *version_table);

    auto txn_manager = MVTOTransactionManager::GetInstance(buf_mgr);
    assert(txn_manager != nullptr);

    auto txn_ctx = txn_manager->BeginTransaction();
    for (int i = 0; i < num_key; ++i) {
        ExecuteInsert(txn_ctx, table, i, 0, buf_mgr);
    }
    ResultType res = txn_manager->CommitTransaction(txn_ctx);
    assert(res == ResultType::SUCCESS);
    return table;
}

bool TestingTransactionUtil::ExecuteInsert(
        TransactionContext *transaction, TestDataTable *table,
        int id, int value, ConcurrentBufferManager *buf_mgr) {

    TestTuple t;
    t.key = id;
    t.value = value;
    InsertExecutor<uint64_t, TestTuple> executor(*table, t, transaction, buf_mgr);
    return executor.Execute();
}


bool TestingTransactionUtil::ExecuteRead(
        TransactionContext *transaction, TestDataTable *table,
        int id, int &result, bool select_for_update, ConcurrentBufferManager *buf_mgr) {

    bool point_lookup = true;
    IndexScanExecutor<uint64_t, TestTuple> read_executor(*table, (uint64_t) id, point_lookup,
                                                         [id](const TestTuple &t, bool & end_scan) {
                                                             end_scan = true;
                                                             return t.key == id;
                                                         },
                                                         select_for_update, transaction, buf_mgr);
    if (read_executor.Execute() == false)
        return false;
    auto &res = read_executor.GetResults();
    if (res.empty()) {
        result = -1;
    } else {
        assert(res.size() == 1);
        result = res[0].value;
    }
    return true;
}

bool TestingTransactionUtil::ExecuteDelete(
        TransactionContext *transaction, TestDataTable *table,
        int id, bool select_for_update, ConcurrentBufferManager *buf_mgr) {
    PointDeleteExecutor<uint64_t, TestTuple> delete_executor(*table, (uint64_t) id,
                                                             [id](const TestTuple &t) { return t.key == id; },
                                                             transaction,
                                                             buf_mgr);

    return delete_executor.Execute();
}

bool TestingTransactionUtil::ExecuteUpdate(
        TransactionContext *transaction, TestDataTable *table,
        int id, int value, bool select_for_update, ConcurrentBufferManager *buf_mgr) {
    bool point_update = true;
    PointUpdateExecutor<uint64_t, TestTuple> update_executor(*table, (uint64_t) id,
                                                             [id](const TestTuple &t) { return t.key == id; },
                                                             [value](TestTuple &t) { t.value = value; }, transaction,
                                                             buf_mgr);

    return update_executor.Execute();
}

bool TestingTransactionUtil::ExecuteUpdateByValue(
        TransactionContext *transaction, TestDataTable *table,
        int old_value, int new_value, bool select_for_update, ConcurrentBufferManager *buf_mgr) {
    bool point_update = false;
    ScanUpdateExecutor<uint64_t, TestTuple> update_executor(*table, (uint64_t) 0,
                                                            [old_value](const TestTuple &t) {
                                                                return t.value == old_value;
                                                            },
                                                            [new_value](TestTuple &t) { t.value = new_value; },
                                                            transaction,
                                                            buf_mgr);

    return update_executor.Execute();
}

bool TestingTransactionUtil::ExecuteScan(
        TransactionContext *transaction, std::vector<int> &results,
        TestDataTable *table, int id, bool select_for_update, ConcurrentBufferManager *buf_mgr) {
    bool point_lookup = false;
    // where t.id >= id
    IndexScanExecutor<uint64_t, TestTuple> scan_executor(*table, id, point_lookup,
                                                         [id](const TestTuple &t, bool & end_scan) { return t.key >= id; },
                                                         select_for_update, transaction, buf_mgr);

    if (scan_executor.Execute() == false)
        return false;

    auto res = scan_executor.GetResults();

    for (int i = 0; i < res.size(); ++i) {
        results.push_back(res[i].value);
    }
    return true;
}
}
}
