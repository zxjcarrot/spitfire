//
// Created by zxjcarrot on 2020-02-20.
//

#ifndef SPITFIRE_EXECUTOR_H
#define SPITFIRE_EXECUTOR_H

#include "util/logger.h"
#include "engine/txn.h"

namespace spitfire {

class Executor {
public:
    virtual bool Execute() = 0;
};


template<class Key, class T>
class InsertExecutor : public Executor {
public:
    InsertExecutor(DataTable <Key, T> &data_table, const T &tuple, TransactionContext *txn,
                   ConcurrentBufferManager *buf_mgr) : data_table(data_table), tuple(tuple), txn_ctx(txn),
                                                       buf_mgr(buf_mgr) {}

    virtual bool Execute() override {
        auto transaction_manager = MVTOTransactionManager::GetInstance(buf_mgr);
        auto predicate = std::bind(&MVTOTransactionManager::IsOccupied,
                                   transaction_manager, txn_ctx, std::placeholders::_1);

        TupleHeaderHard header(transaction_manager->NextRowId());
        auto header_ptr = transaction_manager->InsertHardHeader(header);
        tuple.SetRowId(header.GetRowId());
        tuple.SetHeaderPointer(header_ptr);

        bool upsert = false;
        auto insert_res = data_table.GetPrimaryIndex().Insert(tuple, predicate, upsert);
        if (insert_res == BTreeOPResult::SUCCESS) {
            transaction_manager->PerformInsert(txn_ctx, header_ptr);
            auto key = tuple.Key();
            auto &pindex = data_table.GetPrimaryIndex();
            txn_ctx->RecordRollbackFunc(header_ptr, [key, &pindex]() {
                // undo the insertion by deleting the key
                pindex.Delete(key);
            });
            return true;
        } else if (insert_res == BTreeOPResult::PRED_TRUE || insert_res == BTreeOPResult::PRED_FALSE) {
            bool failed = false;
            auto tuple_processor = [&, this](T &existing_tuple) {
                auto hard_header_ptr = existing_tuple.GetHeaderPointer();
                auto soft_header_ptr = existing_tuple.GetRowId();
                auto visibility = transaction_manager->IsVisible(txn_ctx, hard_header_ptr);
                if (visibility != VisibilityType::DELETED) {
                    failed = true;
                    return true;
                }
                TupleHeaderHardAccessor hard_header_accessor;

                transaction_manager->AcquireTupleHeaderHard(hard_header_ptr, hard_header_accessor);

                DeferCode c([&, this]() {
                    transaction_manager->ReleaseTupleHeaderHard(hard_header_accessor);
                });


                bool is_owner = transaction_manager->IsOwner(txn_ctx, hard_header_accessor);
                bool is_written = transaction_manager->IsWritten(txn_ctx, hard_header_accessor);

                assert(visibility == VisibilityType::DELETED);

                if (is_owner == false) { // There is a concurrent insertion and this transaction failed.
                    failed = true; // Abort the transaction.
                    return true;
                }
                assert(is_written);
                // Release the accessors before performing real updates
                transaction_manager->ReleaseTupleHeaderHard(hard_header_accessor);

                auto tuple_copy = existing_tuple;
                existing_tuple = tuple;
                TupleHeaderHard new_header(transaction_manager->NextRowId());
                TuplePointer new_header_ptr = transaction_manager->InsertHardHeader(new_header);
                existing_tuple.SetRowId(new_header.GetRowId());
                existing_tuple.SetHeaderPointer(new_header_ptr);

                TuplePointer tuple_copy_hdr_ptr = tuple_copy.GetHeaderPointer();

                transaction_manager->PerformInsert(txn_ctx, new_header_ptr);

                auto &pindex = data_table.GetPrimaryIndex();
                auto rollback_func = [&pindex, tuple_copy]() {
                    pindex.LookupForUpdate(tuple_copy.Key(), [&tuple_copy](T &tuple) {
                        // restore to the original tuple
                        tuple = tuple_copy;
                        return true;
                    });
                };
                txn_ctx->RecordRollbackFunc(new_header_ptr, rollback_func);
                txn_ctx->RecordRollbackFunc(tuple_copy_hdr_ptr, []() {});

                return true;

            };
            data_table.GetPrimaryIndex().LookupForUpdate(tuple.Key(), tuple_processor);
            if (failed) {
                transaction_manager->SetTransactionResult(txn_ctx, ResultType::FAILURE);
            }
            return failed == false;
        } else {
            transaction_manager->SetTransactionResult(txn_ctx, ResultType::FAILURE);
            return false;
        }
    }

private:
    DataTable <Key, T> &data_table;
    T tuple;
    TransactionContext *txn_ctx;
    ConcurrentBufferManager *buf_mgr;
};


template<class Key, class T>
class PointDeleteExecutor : public Executor {
public:
    PointDeleteExecutor(DataTable <Key, T> &data_table, const Key &key,
                        std::function<bool(const T &)> predicate,
                        TransactionContext *txn, ConcurrentBufferManager *buf_mgr)
            : data_table(data_table),
              key(key), predicate(predicate),
              txn_ctx(txn), buf_mgr(buf_mgr) {}

    virtual bool Execute() override {
        auto transaction_manager = MVTOTransactionManager::GetInstance(buf_mgr);
        bool failed = false;
        auto tuple_processor = [&, this](T &tuple) {
            auto hard_header_ptr = tuple.GetHeaderPointer();
            auto soft_header_ptr = tuple.GetRowId();
            auto visibility = transaction_manager->IsVisible(txn_ctx, hard_header_ptr);
            assert(visibility != VisibilityType::INVALID);
            if (visibility == VisibilityType::OK) {
                bool eval = predicate(tuple);
                if (eval) {
                    auto txn_read_res = transaction_manager->PerformRead(
                            txn_ctx, hard_header_ptr, true);
                    if (!txn_read_res) {
                        failed = true;
                        return true; // Abort the execution if we see a transactional conflict
                    }
                } else {
                    return true; // The predicate evaluation result indicates the end of the lookup.
                }
            } else if (visibility == VisibilityType::DELETED) {
                return true; // Deleted, end of the delete
            } else {
                failed = true; // Fail the transaction if the tuple is invisible => concurrent updates
                return true;
            }


            TupleHeaderHardAccessor hard_header_accessor;

            transaction_manager->AcquireTupleHeaderHard(hard_header_ptr, hard_header_accessor);

            DeferCode c([&, this]() {
                transaction_manager->ReleaseTupleHeaderHard(hard_header_accessor);
            });

            bool is_owner = transaction_manager->IsOwner(txn_ctx, hard_header_accessor);
            bool is_written = transaction_manager->IsWritten(txn_ctx, hard_header_accessor);

            if (is_owner == true && is_written == true) {
                // Release the accessors before performing real deletes
                transaction_manager->ReleaseTupleHeaderHard(hard_header_accessor);
                // Already owned the latest version
                transaction_manager->PerformDelete(txn_ctx, hard_header_ptr);
            } else {
                // Skip the IsOwnable and AcquireOwnership if we have already got the
                // ownership
                bool is_ownable = is_owner ||
                                  transaction_manager->IsOwnable(
                                          txn_ctx, hard_header_accessor);
                if (is_ownable == true) {
                    // if the tuple is not owned by any transaction and is visible to
                    // current transaction.

                    bool acquire_ownership_success =
                            is_owner ||
                            transaction_manager->AcquireOwnership(txn_ctx, hard_header_accessor);

                    if (acquire_ownership_success == false) {
                        LOG_TRACE("Fail to insert new tuple. Set txn failure.");
                        failed = false;
                        return true;
                    }

                    // Release the accessors before performing real updates
                    transaction_manager->ReleaseTupleHeaderHard(hard_header_accessor);

                    T tuple_copy = tuple;
                    TupleHeaderHard new_header(transaction_manager->NextRowId());
                    TuplePointer new_header_ptr = transaction_manager->InsertHardHeader(new_header);
                    TuplePointer tuple_copy_hdr_ptr = tuple_copy.GetHeaderPointer();
                    TuplePointer tuple_copy_ptr = kInvalidTuplePointer;
                    data_table.GetVersionTable().Insert(tuple_copy, tuple_copy_ptr);
                    tuple.SetNextTuplePointer(tuple_copy_ptr);
                    tuple.SetHeaderPointer(new_header_ptr);
                    tuple.SetRowId(new_header.GetRowId());

                    transaction_manager->PerformDelete(txn_ctx, tuple_copy_hdr_ptr, new_header_ptr);

                    auto &pindex = data_table.GetPrimaryIndex();
                    auto rollback_func = [&pindex, tuple_copy]() {
                        pindex.LookupForUpdate(tuple_copy.Key(), [&tuple_copy](T &tuple) {
                            // restore to the original tuple
                            tuple = tuple_copy;
                            return true;
                        });
                    };
                    txn_ctx->RecordRollbackFunc(tuple_copy_hdr_ptr, rollback_func);
                    return true;
                } else {
                    failed = true;
                    return true;
                }
            }
            return true;
        };
        data_table.GetPrimaryIndex().LookupForUpdate(key, tuple_processor);
        if (failed == true) {
            transaction_manager->SetTransactionResult(txn_ctx, ResultType::FAILURE);
        }
        return failed == false;
    }

private:
    DataTable <Key, T> &data_table;
    Key key;
    std::function<bool(const T &)> predicate;
    TransactionContext *txn_ctx;
    ConcurrentBufferManager *buf_mgr;
};


template<class Key, class T>
class ScanDeleteExecutor : public Executor {
public:
    ScanDeleteExecutor(DataTable <Key, T> &data_table, const Key &start_key,
                       std::function<bool(const T &)> predicate,
                       TransactionContext *txn, ConcurrentBufferManager *buf_mgr)
            : data_table(data_table),
              start_key(start_key), predicate(predicate),
              txn_ctx(txn), buf_mgr(buf_mgr) {}

    virtual bool Execute() override {
        auto transaction_manager = MVTOTransactionManager::GetInstance(buf_mgr);
        bool failed = false;
        auto tuple_processor = [&, this](T &tuple, bool &updated, bool &end_scan) -> void {
            auto hard_header_ptr = tuple.GetHeaderPointer();
            auto soft_header_ptr = tuple.GetRowId();
            auto visibility = transaction_manager->IsVisible(txn_ctx, hard_header_ptr);
            assert(visibility != VisibilityType::INVALID);
            if (visibility == VisibilityType::OK) {
                bool eval = predicate(tuple);
                if (eval) {
                    auto txn_read_res = transaction_manager->PerformRead(
                            txn_ctx, hard_header_ptr, true);
                    if (!txn_read_res) {
                        failed = true;
                        end_scan = true;
                        return; // Abort the execution if we see a transactional conflict
                    }
                } else {
                    end_scan = true;
                    return; // Continue searching for qualifying tuples to update
                }
            } else if (visibility == VisibilityType::DELETED) {
                return; // Deleted, end of the delete
            } else {
                end_scan = true;
                failed = true; // Fail the transaction if the tuple is invisible => concurrent updates
                return;
            }

            TupleHeaderHardAccessor hard_header_accessor;

            transaction_manager->AcquireTupleHeaderHard(hard_header_ptr, hard_header_accessor);

            DeferCode c([&, this]() {
                transaction_manager->ReleaseTupleHeaderHard(hard_header_accessor);
            });

            bool is_owner = transaction_manager->IsOwner(txn_ctx, hard_header_accessor);
            bool is_written = transaction_manager->IsWritten(txn_ctx, hard_header_accessor);

            if (is_owner == true && is_written == true) {
                // Release the accessors before performing real deletes
                transaction_manager->ReleaseTupleHeaderHard(hard_header_accessor);
                // Already owned the latest version
                transaction_manager->PerformDelete(txn_ctx, hard_header_ptr);
            } else {
                // Skip the IsOwnable and AcquireOwnership if we have already got the
                // ownership
                bool is_ownable = is_owner ||
                                  transaction_manager->IsOwnable(
                                          txn_ctx, hard_header_accessor);
                if (is_ownable == true) {
                    // if the tuple is not owned by any transaction and is visible to
                    // current transaction.

                    bool acquire_ownership_success =
                            is_owner ||
                            transaction_manager->AcquireOwnership(txn_ctx, hard_header_accessor);

                    if (acquire_ownership_success == false) {
                        LOG_TRACE("Fail to insert new tuple. Set txn failure.");
                        failed = false;
                        end_scan = true;
                        return;
                    }

                    // Release the accessors before performing real updates
                    transaction_manager->ReleaseTupleHeaderHard(hard_header_accessor);

                    T tuple_copy = tuple;
                    TupleHeaderHard new_header(transaction_manager->NextRowId());
                    TuplePointer new_header_ptr = transaction_manager->InsertHardHeader(new_header);
                    TuplePointer tuple_copy_hdr_ptr = tuple_copy.GetHeaderPointer();
                    TuplePointer tuple_copy_ptr;
                    data_table.GetVersionTable().Insert(tuple_copy, tuple_copy_ptr);
                    tuple.SetNextTuplePointer(tuple_copy_ptr);
                    tuple.SetHeaderPointer(new_header_ptr);
                    tuple.SetRowId(new_header.GetRowId());


                    transaction_manager->PerformDelete(txn_ctx, tuple_copy_hdr_ptr, new_header_ptr);

                    auto &pindex = data_table.GetPrimaryIndex();
                    auto rollback_func = [&pindex, tuple_copy]() {
                        pindex.LookupForUpdate(tuple_copy.Key(), [&tuple_copy](T &tuple) {
                            // restore to the original tuple
                            tuple = tuple_copy;
                            return true;
                        });
                    };
                    txn_ctx->RecordRollbackFunc(tuple_copy_hdr_ptr, rollback_func);
                    return;
                } else {
                    failed = true;
                    end_scan = true;
                    return;
                }
            }
            return;
        };
        data_table.GetPrimaryIndex().ScanForUpdate(start_key, tuple_processor);
        if (failed == true) {
            transaction_manager->SetTransactionResult(txn_ctx, ResultType::FAILURE);
        }
        return failed == false;
    }

private:
    DataTable <Key, T> &data_table;
    Key start_key;
    std::function<bool(const T &)> predicate;
    std::function<void(T &)> user_updater;
    TransactionContext *txn_ctx;
    ConcurrentBufferManager *buf_mgr;
};


template<class Key, class T>
class PointUpdateExecutor : public Executor {
public:
    PointUpdateExecutor(DataTable <Key, T> &data_table, const Key &key,
                        std::function<bool(const T &)> predicate, std::function<void(T &)> user_updater,
                        TransactionContext *txn, ConcurrentBufferManager *buf_mgr)
            : data_table(data_table),
              key(key), predicate(predicate), user_updater(user_updater),
              txn_ctx(txn), buf_mgr(buf_mgr) {}

    virtual bool Execute() override {
        auto transaction_manager = MVTOTransactionManager::GetInstance(buf_mgr);
        bool failed = false;
        auto tuple_processor = [&, this](T &tuple) {
            auto hard_header_ptr = tuple.GetHeaderPointer();
            auto soft_header_ptr = tuple.GetRowId();
            auto visibility = transaction_manager->IsVisible(txn_ctx, hard_header_ptr);
            assert(visibility != VisibilityType::INVALID);
            if (visibility == VisibilityType::OK) {
                bool eval = predicate(tuple);
                if (eval) {
                    auto txn_read_res = transaction_manager->PerformRead(
                            txn_ctx, hard_header_ptr, true);
                    if (!txn_read_res) {
                        failed = true;
                        return true; // Abort the execution if we see a transactional conflict
                    }
                } else {
                    return true; // The predicate evaluation result indicates the end of the lookup.
                }
            } else if (visibility == VisibilityType::DELETED) {
                return true; // Deleted, end of the update
            } else {
                failed = true; // Fail the transaction if the tuple is invisible => concurrent updates
                return true;
            }

            TupleHeaderHardAccessor hard_header_accessor;

            transaction_manager->AcquireTupleHeaderHard(hard_header_ptr, hard_header_accessor);

            DeferCode c([&, this]() {
                transaction_manager->ReleaseTupleHeaderHard(hard_header_accessor);
            });

            bool is_owner = transaction_manager->IsOwner(txn_ctx, hard_header_accessor);
            bool is_written = transaction_manager->IsWritten(txn_ctx, hard_header_accessor);

            if (is_owner == true && is_written == true) {
                // Release the accessors before performing real updates
                transaction_manager->ReleaseTupleHeaderHard(hard_header_accessor);
                user_updater(tuple);
                // Already owned the latest version
                transaction_manager->PerformUpdate(txn_ctx, hard_header_ptr);
            } else {
                // Skip the IsOwnable and AcquireOwnership if we have already got the
                // ownership
                bool is_ownable = is_owner ||
                                  transaction_manager->IsOwnable(
                                          txn_ctx, hard_header_accessor);
                if (is_ownable == true) {
                    // if the tuple is not owned by any transaction and is visible to
                    // current transaction.

                    bool acquire_ownership_success =
                            is_owner ||
                            transaction_manager->AcquireOwnership(txn_ctx, hard_header_accessor);

                    if (acquire_ownership_success == false) {
                        LOG_TRACE("Fail to insert new tuple. Set txn failure.");
                        failed = false;
                        return true;
                    }

                    // Release the accessors before performing real updates
                    transaction_manager->ReleaseTupleHeaderHard(hard_header_accessor);

                    T tuple_copy = tuple;
                    user_updater(tuple);
                    TupleHeaderHard new_header(transaction_manager->NextRowId());
                    TuplePointer new_header_ptr = transaction_manager->InsertHardHeader(new_header);
                    TuplePointer tuple_copy_hdr_ptr = tuple_copy.GetHeaderPointer();
                    TuplePointer tuple_copy_ptr = kInvalidTuplePointer;
                    data_table.GetVersionTable().Insert(tuple_copy, tuple_copy_ptr);
                    tuple.SetNextTuplePointer(tuple_copy_ptr);
                    tuple.SetHeaderPointer(new_header_ptr);
                    tuple.SetRowId(new_header.GetRowId());

                    transaction_manager->PerformUpdate(txn_ctx, tuple_copy_hdr_ptr, new_header_ptr);

                    auto &pindex = data_table.GetPrimaryIndex();
                    auto rollback_func = [&pindex, tuple_copy]() {
                        pindex.LookupForUpdate(tuple_copy.Key(), [&tuple_copy](T &tuple) {
                            // restore to the original tuple
                            tuple = tuple_copy;
                            return true;
                        });
                    };
                    txn_ctx->RecordRollbackFunc(tuple_copy_hdr_ptr, rollback_func);
                    return true;
                } else {
                    failed = true;
                    return true;
                }
            }
            return true;
        };
        data_table.GetPrimaryIndex().LookupForUpdate(key, tuple_processor);
        if (failed == true) {
            transaction_manager->SetTransactionResult(txn_ctx,
                                                      ResultType::FAILURE);
        }
        return failed == false;
    }

private:
    DataTable <Key, T> &data_table;
    Key key;
    std::function<bool(const T &)> predicate;
    std::function<void(T &)> user_updater;
    TransactionContext *txn_ctx;
    ConcurrentBufferManager *buf_mgr;
};


template<class Key, class T>
class ScanUpdateExecutor : public Executor {
public:
    ScanUpdateExecutor(DataTable <Key, T> &data_table, const Key &start_key,
                       std::function<bool(const T &)> predicate, std::function<void(T &)> user_updater,
                       TransactionContext *txn, ConcurrentBufferManager *buf_mgr)
            : data_table(data_table),
              start_key(start_key), predicate(predicate), user_updater(user_updater),
              txn_ctx(txn), buf_mgr(buf_mgr) {}

    virtual bool Execute() override {
        auto transaction_manager = MVTOTransactionManager::GetInstance(buf_mgr);
        bool failed = false;
        auto tuple_processor = [&, this](T &tuple, bool &updated, bool &end_scan) -> void {
            auto hard_header_ptr = tuple.GetHeaderPointer();
            auto soft_header_ptr = tuple.GetRowId();
            auto visibility = transaction_manager->IsVisible(txn_ctx, hard_header_ptr);
            assert(visibility != VisibilityType::INVALID);
            if (visibility == VisibilityType::OK) {
                bool eval = predicate(tuple);
                if (eval) {
                    // Acquire ownership
                    auto txn_read_res = transaction_manager->PerformRead(
                            txn_ctx, hard_header_ptr, true);
                    if (!txn_read_res) {
                        failed = true;
                        end_scan = true;
                        return; // Abort the execution if we see a transactional conflict
                    }
                } else {
                    end_scan = true;
                    return; // Continue searching for qualifying tuples to update
                }
            } else if (visibility == VisibilityType::DELETED) {
                return; // Deleted, end of the update
            } else {
                end_scan = true;
                failed = true; // Fail the transaction if the tuple is invisible => concurrent updates
                return;
            }

            TupleHeaderHardAccessor hard_header_accessor;

            transaction_manager->AcquireTupleHeaderHard(hard_header_ptr, hard_header_accessor);

            DeferCode c([&, this]() {
                transaction_manager->ReleaseTupleHeaderHard(hard_header_accessor);
            });

            bool is_owner = transaction_manager->IsOwner(txn_ctx, hard_header_accessor);
            bool is_written = transaction_manager->IsWritten(txn_ctx, hard_header_accessor);


            if (is_owner == true && is_written == true) {
                // Release the accessors before performing real updates
                transaction_manager->ReleaseTupleHeaderHard(hard_header_accessor);
                user_updater(tuple);
                updated = true;
                // Already owned the latest version
                transaction_manager->PerformUpdate(txn_ctx, hard_header_ptr);
            } else {
                // Skip the IsOwnable and AcquireOwnership if we have already got the
                // ownership
                bool is_ownable = is_owner ||
                                  transaction_manager->IsOwnable(
                                          txn_ctx, hard_header_accessor);
                if (is_ownable == true) {
                    // if the tuple is not owned by any transaction and is visible to
                    // current transaction.

                    bool acquire_ownership_success =
                            is_owner ||
                            transaction_manager->AcquireOwnership(txn_ctx, hard_header_accessor);

                    if (acquire_ownership_success == false) {
                        LOG_TRACE("Fail to update new tuple. Set txn failure.");
                        failed = false;
                        end_scan = true;
                        return;
                    }

                    // Release the accessors before performing real updates
                    transaction_manager->ReleaseTupleHeaderHard(hard_header_accessor);

                    T tuple_copy = tuple;
                    user_updater(tuple);
                    updated = true;
                    TupleHeaderHard new_header(transaction_manager->NextRowId());
                    TuplePointer new_header_ptr = transaction_manager->InsertHardHeader(new_header);
                    TuplePointer tuple_copy_hdr_ptr = tuple_copy.GetHeaderPointer();
                    TuplePointer tuple_copy_ptr = kInvalidTuplePointer;
                    data_table.GetVersionTable().Insert(tuple_copy, tuple_copy_ptr);
                    tuple.SetNextTuplePointer(tuple_copy_ptr);
                    tuple.SetHeaderPointer(new_header_ptr);
                    tuple.SetRowId(new_header.GetRowId());

                    transaction_manager->PerformUpdate(txn_ctx, tuple_copy_hdr_ptr, new_header_ptr);

                    auto &pindex = data_table.GetPrimaryIndex();
                    auto rollback_func = [&pindex, tuple_copy]() {
                        pindex.LookupForUpdate(tuple_copy.Key(), [&tuple_copy](T &tuple) {
                            // restore to the original tuple
                            tuple = tuple_copy;
                            return true;
                        });
                    };
                    txn_ctx->RecordRollbackFunc(tuple_copy_hdr_ptr, rollback_func);
                    return;
                } else {
                    failed = true;
                    end_scan = true;
                    return;
                }
            }
            return;
        };
        data_table.GetPrimaryIndex().ScanForUpdate(start_key, tuple_processor);
        if (failed == true) {
            transaction_manager->SetTransactionResult(txn_ctx, ResultType::FAILURE);
        }
        return failed == false;
    }

private:
    DataTable <Key, T> &data_table;
    Key start_key;
    std::function<bool(const T &)> predicate;
    std::function<void(T &)> user_updater;
    TransactionContext *txn_ctx;
    ConcurrentBufferManager *buf_mgr;
};


template<class Key, class T>
class IndexScanExecutor : public Executor {
public:

    // predicate:
    //  A function returns whether a tuple satisfies the predicate.
    //  If predicate returns true, the tuple is added to the result set.
    //  If end_scan is set after the predicate evaluation, the scan will be terminated.
    // point_lookup:
    //  A boolean indicating whether this is a point lookup.
    //  If so, only one tuple will be read, ignoring whether end_scan is set or not.
    IndexScanExecutor(DataTable <Key, T> &data_table, const Key &start_key, bool point_lookup,
                      std::function<bool(const T &, bool &end_scan)> predicate, bool acquire_owner,
                      TransactionContext *txn, ConcurrentBufferManager *buf_mgr) :
            data_table(data_table), start_key(start_key),
            point_lookup(point_lookup), predicate(predicate),
            acquire_owner(acquire_owner), txn(txn),
            buf_mgr(buf_mgr) {}

    virtual bool Execute() override {
        auto transaction_manager = MVTOTransactionManager::GetInstance(buf_mgr);
        bool failed = false;
        int retried_times = 0;
        auto tuple_processor = [&, this](const T &tuple) -> BTreeOPResult {
            const T *tuple_ptr = &tuple;
            PageDesc *pd = nullptr;
            DeferCode c([&]() {
                if (pd != nullptr)
                    buf_mgr->Put(pd);
                pd = nullptr;
            });
            int chain_length = 0;
            while (true) {
                ++chain_length;
                auto header_ptr = tuple_ptr->GetHeaderPointer();
                auto visibility = transaction_manager->IsVisible(txn, header_ptr);
                assert(visibility != VisibilityType::INVALID);
                if (visibility == VisibilityType::DELETED) {
                    if (point_lookup) {
                        return BTreeOPResult::END_SCAN;
                    }
                    return BTreeOPResult::CONTINUE_SCAN;; // skip the entire chain if we see a delete version
                } else if (visibility == VisibilityType::OK) {
                    bool end_scan = false;
                    bool eval = predicate(*tuple_ptr, end_scan);

                    if (eval) {
                        auto txn_read_res = transaction_manager->PerformRead(
                                txn, header_ptr, acquire_owner);
                        if (!txn_read_res) {
                            failed = true;
                            if (point_lookup) {
                                return BTreeOPResult::END_SCAN;;
                            }
                            return BTreeOPResult::END_SCAN; // Abort the execution if we see a transactional conflict
                        }
                        results.push_back(*tuple_ptr);
                    }

                    if (end_scan) {
                        return BTreeOPResult::END_SCAN; // The predicate evaluation result indicates the end of the scan.
                    }

                    if (point_lookup) {
                        return BTreeOPResult::CONTINUE_SCAN;
                    }
                    return BTreeOPResult::CONTINUE_SCAN; // Go to the next tuple
                } else {
                    assert(visibility == VisibilityType::INVISIBLE);
                    {
                        ConstTupleHeaderHardAccessor hard_header_accessor;
                        transaction_manager->AcquireTupleHeaderHard(header_ptr, hard_header_accessor);
                        auto tuple_header_hard = hard_header_accessor.GetTupleHeaderHard();


                        bool is_acquired = tuple_header_hard->GetTransactionId() == INITIAL_TXN_ID;
                        bool is_alive =
                                (tuple_header_hard->GetEndCommitId() <=
                                 txn->GetReadId());
                        transaction_manager->ReleaseTupleHeaderHard(hard_header_accessor);

                        if (is_acquired && is_alive) {
                            results.clear();
                            if (++retried_times > 5) { // Hack: fail this transaction if there are too many retries on a version chain
                                failed = true;
                                return BTreeOPResult::END_SCAN;
                            }
                            return BTreeOPResult::RETRY_SCAN;
                        }
                    }
                    // Go to next version
                    TuplePointer next_tuple_ptr = tuple_ptr->GetNextTuplePointer();
                    if (next_tuple_ptr == kInvalidTuplePointer) {
                        // End of the chain and we didn't find a valid version, abort the transaction.
                        failed = true;
                        return BTreeOPResult::END_SCAN;
                    }
                    if (pd != nullptr) {
                        buf_mgr->Put(pd);
                        pd = nullptr;
                    }
                    ConcurrentBufferManager::PageAccessor accessor;
                    Status s = buf_mgr->Get(next_tuple_ptr.pid, accessor);
                    assert(s.ok());
                    pd = accessor.GetPageDesc();
                    auto slice = accessor.PrepareForRead(next_tuple_ptr.off, sizeof(T));
                    tuple_ptr = reinterpret_cast<const T *>(slice.data());
                    // Go to the next version of this tuple
                }
            }
        };
        if (point_lookup) {
            data_table.GetPrimaryIndex().Lookup(start_key, tuple_processor);
        } else {
            data_table.GetPrimaryIndex().Scan(start_key, tuple_processor);
        }

        if (failed) {
            transaction_manager->SetTransactionResult(txn,
                                                      ResultType::FAILURE);
        }

        return failed == false;
    }

    const std::vector<T> &GetResults() {
        return results;
    }

private:
    DataTable <Key, T> &data_table;
    const Key start_key;
    bool point_lookup;
    std::function<bool(const T &, bool &should_end_scan)> predicate;
    bool acquire_owner; // Read for update
    TransactionContext *txn;
    ConcurrentBufferManager *buf_mgr;
    std::vector<T> results;
};


template<class Key, class T>
class TableScanExecutor : public Executor {
public:

    // The processor will be called on every visible tuple in the table
    // processor
    //  end_scan : whether the scan should be ended
    TableScanExecutor(DataTable <Key, T> &data_table,
                      std::function<void(const T &, bool &end_scan)> processor,
                      TransactionContext *txn, ConcurrentBufferManager *buf_mgr) :
            data_table(data_table), processor(processor), txn(txn),
            buf_mgr(buf_mgr) {}

    virtual bool Execute() override {
        auto transaction_manager = MVTOTransactionManager::GetInstance(buf_mgr);
        bool failed = false;
        int retried_times = 0;
        auto tuple_processor = [&, this](const T &tuple) -> BTreeOPResult {
            const T *tuple_ptr = &tuple;
            PageDesc *pd = nullptr;
            DeferCode c([&]() {
                if (pd != nullptr)
                    buf_mgr->Put(pd);
                pd = nullptr;
            });
            int chain_length = 0;
            while (true) {
                ++chain_length;
                auto header_ptr = tuple_ptr->GetHeaderPointer();
                auto visibility = transaction_manager->IsVisible(txn, header_ptr);
                assert(visibility != VisibilityType::INVALID);
                if (visibility == VisibilityType::DELETED) {
                    return BTreeOPResult::CONTINUE_SCAN;; // skip the entire chain if we see a delete version
                } else if (visibility == VisibilityType::OK) {
                    bool end_scan = false;

                    auto txn_read_res = transaction_manager->PerformRead(
                            txn, header_ptr, false);
                    if (!txn_read_res) {
                        failed = true;
                        return BTreeOPResult::END_SCAN; // Abort the execution if we see a transactional conflict
                    }

                    processor(*tuple_ptr, end_scan);

                    if (end_scan) {
                        return BTreeOPResult::END_SCAN; // The predicate evaluation result indicates the end of the scan.
                    }

                    return BTreeOPResult::CONTINUE_SCAN; // Go to the next tuple
                } else {
                    assert(visibility == VisibilityType::INVISIBLE);
                    {
                        ConstTupleHeaderHardAccessor hard_header_accessor;
                        transaction_manager->AcquireTupleHeaderHard(header_ptr, hard_header_accessor);
                        auto tuple_header_hard = hard_header_accessor.GetTupleHeaderHard();

                        bool is_acquired = tuple_header_hard->GetTransactionId() == INITIAL_TXN_ID;
                        bool is_alive =
                                (tuple_header_hard->GetEndCommitId() <=
                                 txn->GetReadId());
                        transaction_manager->ReleaseTupleHeaderHard(hard_header_accessor);

                        if (is_acquired && is_alive) {
                            if (++retried_times >
                                5) { // Hack: fail this transaction if there are too many retries on a version chain
                                failed = true;
                                return BTreeOPResult::END_SCAN;
                            }
                            return BTreeOPResult::RETRY_SCAN;
                        }
                    }
                    // Go to next version
                    TuplePointer next_tuple_ptr = tuple_ptr->GetNextTuplePointer();
                    if (next_tuple_ptr == kInvalidTuplePointer) {
                        // End of the chain and we didn't find a valid version, abort the transaction.
                        failed = true;
                        return BTreeOPResult::END_SCAN;
                    }
                    if (pd != nullptr) {
                        buf_mgr->Put(pd);
                        pd = nullptr;
                    }
                    ConcurrentBufferManager::PageAccessor accessor;
                    Status s = buf_mgr->Get(next_tuple_ptr.pid, accessor);
                    assert(s.ok());
                    pd = accessor.GetPageDesc();
                    auto slice = accessor.PrepareForRead(next_tuple_ptr.off, sizeof(T));
                    tuple_ptr = reinterpret_cast<const T *>(slice.data());
                    // Go to the next version of this tuple
                }
            }
        };

        data_table.GetPrimaryIndex().Scan(std::numeric_limits<Key>::min(), tuple_processor);

        if (failed) {
            transaction_manager->SetTransactionResult(txn,
                                                      ResultType::FAILURE);
        }

        return failed == false;
    }

private:
    DataTable <Key, T> &data_table;
    std::function<void(const T &, bool &should_end_scan)> processor;
    TransactionContext *txn;
    ConcurrentBufferManager *buf_mgr;
};

}
#endif //SPITFIRE_EXECUTOR_H
