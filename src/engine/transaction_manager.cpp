//
// Created by zxjcarrot on 2020-02-13.
//

#include <time.h>
#include <atomic>
#include "util/logger.h"
#include "engine/txn.h"
#include "util/sync.h"

namespace spitfire {

thread_local txn_id_t current_txn_id = INVALID_TXN_ID;
static std::atomic<txn_id_t> tid_counter(INITIAL_TXN_ID);
static std::atomic<rid_t> row_id_counter(INVALID_RID);

static concurrent_bytell_hash_map<txn_id_t, int, std::hash<txn_id_t>> active_tids;

IsolationLevelType MVTOTransactionManager::isolation_level_ =
        IsolationLevelType::SERIALIZABLE;

txn_id_t MVTOTransactionManager::GetCurrentTidCounter() {
    return tid_counter.load();
}

txn_id_t MVTOTransactionManager::MinActiveTID() {
    txn_id_t min_tid = tid_counter.load();
    active_tids.Iterate([&](const txn_id_t tid, int) {
        min_tid = std::min(min_tid, tid);
    });

    return min_tid;
}

TransactionContext *MVTOTransactionManager::BeginTransaction(
        const size_t thread_id, const IsolationLevelType type) {
    TransactionContext *txn = nullptr;

    if (type == IsolationLevelType::READ_ONLY) {
        cid_t read_id = tid_counter.fetch_add(1);
        txn = new TransactionContext(thread_id, type, read_id);

    } else if (type == IsolationLevelType::SNAPSHOT) {
        cid_t read_id, commit_id;
        read_id = commit_id = tid_counter.fetch_add(1);
        txn = new TransactionContext(thread_id, type, read_id, commit_id);
    } else {
        // if the isolation level is set to:
        // - SERIALIZABLE, or
        // - REPEATABLE_READS, or
        // - READ_COMMITTED.
        // transaction processing with decentralized thread_ref manager
        cid_t read_id = tid_counter.fetch_add(1);
        txn = new TransactionContext(thread_id, type, read_id);
    }
    if (buf_mgr_->GetLogManager()) {
        SetLastLogRecordLSN(kInvalidLSN);
        //lsn_t lsn = buf_mgr_->GetLogManager()->LogBeginTxn(txn->GetTransactionId(), GetLastLogRecordLSN());
        //SetLastLogRecordLSN(lsn);
    }

    current_txn_id = txn->GetTransactionId();
    active_tids.Insert(current_txn_id, 1);
    return txn;
}

void MVTOTransactionManager::EndTransaction(TransactionContext *current_txn) {
    // fire all on commit triggers
//    if (current_txn->GetResult() == ResultType::SUCCESS) {
//        current_txn->ExecOnCommitTriggers();
//    }
    auto tid = current_txn->GetTransactionId();
    if (buf_mgr_->GetLogManager()) {
        //auto last_lsn = GetLastLogRecordLSN();
        //lsn_t lsn = buf_mgr_->GetLogManager()->LogEOL(tid, last_lsn);
        //SetLastLogRecordLSN(lsn);
    }
    delete current_txn;
    current_txn = nullptr;
    active_tids.Erase(tid);
}


void MVTOTransactionManager::AcquireTupleHeaderHard(TuplePointer ptr, TupleHeaderHardAccessor &accessor) {
    if (accessor.released == true) {
        pid_t pid = ptr.pid;
        Status s = buf_mgr_->Get(pid, accessor.accessor, ConcurrentBufferManager::PageOPIntent::INTENT_READ);
        assert(s.ok());
    }
    auto slice = accessor.accessor.PrepareForWrite(ptr.off, sizeof(TupleHeaderHard));
    accessor.raw_data = slice.data();
    accessor.released = false;
}

void MVTOTransactionManager::AcquireTupleHeaderHard(TuplePointer ptr, ConstTupleHeaderHardAccessor &accessor) {
    if (accessor.released == true) {
        pid_t pid = ptr.pid;
        Status s = buf_mgr_->Get(pid, accessor.accessor, ConcurrentBufferManager::PageOPIntent::INTENT_READ);
        assert(s.ok());
    }
    auto slice = accessor.accessor.PrepareForRead(ptr.off, sizeof(TupleHeaderHard));
    accessor.raw_data = slice.data();
    accessor.released = false;
}

void MVTOTransactionManager::ReleaseTupleHeaderHard(TupleHeaderHardAccessor &accessor) {
    if (accessor.released == false) {
        auto page_desc = accessor.accessor.GetPageDesc();
        accessor.FinishAccess();
        if (page_desc)
            buf_mgr_->Put(page_desc);
        accessor.released = true;
    }
}

void MVTOTransactionManager::ReleaseTupleHeaderHard(ConstTupleHeaderHardAccessor &accessor) {
    if (accessor.released == false) {
        auto page_desc = accessor.accessor.GetPageDesc();
        accessor.FinishAccess();
        if (page_desc)
            buf_mgr_->Put(page_desc);
        accessor.released = true;
    }
}

bool MVTOTransactionManager::IsTupleHeaderHardPurgable(TuplePointer ptr, pid_t min_active_tid) {
    ConstTupleHeaderHardAccessor accessor;
    AcquireTupleHeaderHard(ptr, accessor);
    DeferCode c([this, &accessor]() {
        ReleaseTupleHeaderHard(accessor);
    });
    auto header_hard = accessor.GetTupleHeaderHard();
    if (header_hard->GetEndCommitId() < min_active_tid)
        return true;
    return false;
}

// this function checks whether a concurrent transaction is inserting the same
// tuple
// that is to-be-inserted by the current transaction.
bool MVTOTransactionManager::IsOccupied(TransactionContext *const current_txn,
                                        const void *base_tuple_ptr) {
    auto base_tuple = reinterpret_cast<const BaseTuple *>(base_tuple_ptr);
    auto soft_hdr_ptr = base_tuple->GetRowId();
    auto hard_hdr_ptr = base_tuple->GetHeaderPointer();


    ConstTupleHeaderHardAccessor hard_header_accessor;
    AcquireTupleHeaderHard(hard_hdr_ptr, hard_header_accessor);
    auto tuple_header_hard = hard_header_accessor.GetTupleHeaderHard();

    txn_id_t tuple_txn_id = tuple_header_hard->txn_id;

    cid_t tuple_begin_cid = tuple_header_hard->GetBeginCommitId();
    cid_t tuple_end_cid = tuple_header_hard->GetEndCommitId();

    ReleaseTupleHeaderHard(hard_header_accessor);


    if (tuple_txn_id == INVALID_TXN_ID) {
        // the tuple is not available.
        return false;
    }

    // the tuple has already been owned by the current transaction.
    bool own = (current_txn->GetTransactionId() == tuple_txn_id);
    // the tuple has already been committed.
    bool activated = (current_txn->GetReadId() >= tuple_begin_cid);
    // the tuple is not visible.
    bool invalidated = (current_txn->GetReadId() >= tuple_end_cid);

    // there are exactly two versions that can be owned by a transaction.
    // unless it is an insertion/select for update.
    if (own == true) {
        if (tuple_begin_cid == MAX_CID && tuple_end_cid != INVALID_CID) {
            assert(tuple_end_cid == MAX_CID);
            // the only version that is visible is the newly inserted one.
            return true;
        } else if (current_txn->GetRWType(hard_hdr_ptr) == RWType::READ_OWN) {
            // the ownership is from a select-for-update read operation
            return true;
        } else {
            // the older version is not visible.
            return false;
        }
    } else {
        if (tuple_txn_id != INITIAL_TXN_ID) {
            // if the tuple is owned by other transactions.
            if (tuple_begin_cid == MAX_CID) {
                // uncommitted version.
                if (tuple_end_cid == INVALID_CID) {
                    // dirty delete is invisible
                    return false;
                } else {
                    // dirty update or insert is visible
                    return true;
                }
            } else {
                // the older version may be visible.
                if (activated && !invalidated) {
                    return true;
                } else {
                    return false;
                }
            }
        } else {
            // if the tuple is not owned by any transaction.
            if (activated && !invalidated) {
                return true;
            } else {
                return false;
            }
        }
    }
}

// this function checks whether a version is visible to current transaction.
VisibilityType MVTOTransactionManager::IsVisible(
        TransactionContext *const current_txn,
        const TuplePointer &hard_hdr_ptr, const VisibilityIdType type) {


    ConstTupleHeaderHardAccessor hard_header_accessor;
    AcquireTupleHeaderHard(hard_hdr_ptr, hard_header_accessor);
    auto tuple_header_hard = hard_header_accessor.GetTupleHeaderHard();

    txn_id_t tuple_txn_id = tuple_header_hard->txn_id;
    cid_t tuple_begin_cid = tuple_header_hard->GetBeginCommitId();
    cid_t tuple_end_cid = tuple_header_hard->GetEndCommitId();

    ReleaseTupleHeaderHard(hard_header_accessor);


    // the tuple has already been owned by the current transaction.
    bool own = (current_txn->GetTransactionId() == tuple_txn_id);

    cid_t txn_vis_id;

    if (type == VisibilityIdType::READ_ID) {
        txn_vis_id = current_txn->GetReadId();
    } else {
        assert(type == VisibilityIdType::COMMIT_ID);
        txn_vis_id = current_txn->GetCommitId();
    }

    // the tuple has already been committed.
    bool activated = (txn_vis_id >= tuple_begin_cid);
    // the tuple is not visible.
    bool invalidated = (txn_vis_id >= tuple_end_cid);

    if (tuple_txn_id == INVALID_TXN_ID) {
        // the tuple is not available.
        if (activated && !invalidated) {
            // deleted tuple
            return VisibilityType::DELETED;
        } else {
            // aborted tuple
            return VisibilityType::INVISIBLE;
        }
    }

    // there are exactly two versions that can be owned by a transaction,
    // unless it is an insertion/select-for-update
    if (own == true) {
        if (tuple_begin_cid == MAX_CID && tuple_end_cid != INVALID_CID) {
            assert(tuple_end_cid == MAX_CID);
            // the only version that is visible is the newly inserted/updated one.
            return VisibilityType::OK;
        } else if (current_txn->GetRWType(hard_hdr_ptr) ==
                   RWType::READ_OWN) {
            // the ownership is from a select-for-update read operation
            return VisibilityType::OK;
        } else if (tuple_end_cid == INVALID_CID) {
            // tuple being deleted by current txn
            return VisibilityType::DELETED;
        } else {
            // old version of the tuple that is being updated by current txn
            return VisibilityType::INVISIBLE;
        }
    } else {
        if (tuple_txn_id != INITIAL_TXN_ID) {
            // if the tuple is owned by other transactions.
            if (tuple_begin_cid == MAX_CID) {
                // in this protocol, we do not allow cascading abort. so never read an
                // uncommitted version.
                return VisibilityType::INVISIBLE;
            } else {
                // the older version may be visible.
                if (activated && !invalidated) {
                    return VisibilityType::OK;
                } else {
                    return VisibilityType::INVISIBLE;
                }
            }
        } else {
            // if the tuple is not owned by any transaction.
            if (activated && !invalidated) {
                return VisibilityType::OK;
            } else {
                return VisibilityType::INVISIBLE;
            }
        }
    }
}


cid_t MVTOTransactionManager::GetLastReaderCommitId(
        ConstTupleHeaderHardAccessor &tuple_header_accessor) {
    auto header = tuple_header_accessor.GetTupleHeaderHard();
    return header->read_ts;
}

bool MVTOTransactionManager::SetLastReaderCommitId(
        TupleHeaderHardAccessor &accessor, const cid_t &current_cid, const bool is_owner) {
    // get the pointer to the last_reader_cid field.
    auto header = accessor.GetTupleHeaderHard();
    cid_t *ts_ptr = &header->read_ts;

    //GetSpinLatchField(tile_group_header, tuple_id)->Lock();

    txn_id_t tuple_txn_id = header->txn_id;

    if (is_owner == false && tuple_txn_id != INITIAL_TXN_ID) {
        // if the write lock has already been acquired by some concurrent
        // transactions,
        // then return without setting the last_reader_cid.
        //GetSpinLatchField(tile_group_header, tuple_id)->Unlock();
        return false;
    } else {
        // if current_cid is larger than the current value of last_reader_cid field,
        // then set last_reader_cid to current_cid.
        if (*ts_ptr < current_cid) {
            *ts_ptr = current_cid;
        }

        //GetSpinLatchField(tile_group_header, tuple_id)->Unlock();
        return true;
    }
}

TuplePointer MVTOTransactionManager::InsertHardHeader(const TupleHeaderHard &header) {
    TuplePointer ptr = kInvalidTuplePointer;
    hard_header_table.Insert(header, ptr);
    return ptr;
}

void MVTOTransactionManager::InitTupleReserved(TupleHeaderHardAccessor &accessor) {
    //auto reserved_area = tile_group_header->GetReservedFieldRef(tuple_id);
    auto header = accessor.GetTupleHeaderHard();
    header->read_ts = 0;
    //*(cid_t *)(reserved_area + LAST_READER_OFFSET) = 0;
}

std::mutex tnx_manager_lock;
static std::atomic<MVTOTransactionManager *> txn_manager(nullptr);

MVTOTransactionManager *
MVTOTransactionManager::GetInstance(ConcurrentBufferManager *buf_mgr) {
    if (txn_manager.load() == nullptr) {
        std::lock_guard<std::mutex> g(tnx_manager_lock);
        if (txn_manager.load() == nullptr) {
            MVTOTransactionManager *new_txn_manager = new MVTOTransactionManager;
            auto header_table_meta_pid = kInvalidPID;
            Status s = new_txn_manager->Init(header_table_meta_pid, buf_mgr);
            assert(s.ok());
            txn_manager.store(new_txn_manager);
        }
    }
    return txn_manager;
}

void MVTOTransactionManager::ClearInstance() {
    txn_manager = nullptr;
}

bool MVTOTransactionManager::IsOwner(
        TransactionContext *const current_txn,
        ConstTupleHeaderHardAccessor &accessor) {
    auto header = accessor.GetTupleHeaderHard();
    auto tuple_txn_id = header->txn_id;

    return tuple_txn_id == current_txn->GetTransactionId();
}

bool MVTOTransactionManager::IsOwned(
        TransactionContext *const current_txn,
        ConstTupleHeaderHardAccessor &accessor) {
    auto header = accessor.GetTupleHeaderHard();
    auto tuple_txn_id = header->txn_id;

    return tuple_txn_id != current_txn->GetTransactionId() &&
           tuple_txn_id != INITIAL_TXN_ID;
}

bool MVTOTransactionManager::IsWritten(
        TransactionContext *const current_txn,
        ConstTupleHeaderHardAccessor &header_hard_accessor) {
    auto header_hard = header_hard_accessor.GetTupleHeaderHard();
    auto tuple_begin_cid = header_hard->GetBeginCommitId();

    return tuple_begin_cid == MAX_CID;
}

bool MVTOTransactionManager::IsOwnable(
        TransactionContext *const current_txn,
        ConstTupleHeaderHardAccessor &header_hard_accessor) {
    auto header_hard = header_hard_accessor.GetTupleHeaderHard();
    auto tuple_txn_id = header_hard->txn_id;
    auto tuple_end_cid = header_hard->GetEndCommitId();
    return tuple_txn_id == INITIAL_TXN_ID && tuple_end_cid == MAX_CID;
}

bool MVTOTransactionManager::AcquireOwnership(
        TransactionContext *const current_txn,
        TupleHeaderHardAccessor &header_accessor) {
    auto header = header_accessor.GetTupleHeaderHard();
    auto txn_id = current_txn->GetTransactionId();

    // to acquire the ownership,
    // we must guarantee that no transaction that has read
    // the tuple has a larger timestamp than the current transaction.

    //GetSpinLatchField(tile_group_header, tuple_id)->Lock();

    // change timestamp
    cid_t last_reader_cid = GetLastReaderCommitId(header_accessor);

    // must compare last_reader_cid with a transaction's commit_id
    // (rather than read_id).
    // consider a transaction that is executed under snapshot isolation.
    // in this case, commit_id is not equal to read_id.
    if (last_reader_cid > current_txn->GetCommitId()) {
        //GetSpinLatchField(tile_group_header, tuple_id)->Unlock();

        return false;
    } else {
        if (header->SetAtomicTransactionId(txn_id) == false) {
            //GetSpinLatchField(tile_group_header, tuple_id)->Unlock();

            return false;
        } else {
            //GetSpinLatchField(tile_group_header, tuple_id)->Unlock();

            return true;
        }
    }
}

void MVTOTransactionManager::YieldOwnership(
        TransactionContext *const current_txn, TupleHeaderHardAccessor &header_accessor) {
    assert(IsOwner(current_txn, header_accessor));
    auto header = header_accessor.GetTupleHeaderHard();
    header->SetTransactionId(INITIAL_TXN_ID);
}

bool MVTOTransactionManager::PerformRead(
        TransactionContext *const current_txn, const TuplePointer &hard_hdr_ptr,
        bool acquire_ownership) {
    TuplePointer header_location = hard_hdr_ptr;

    assert(current_txn->GetIsolationLevel() ==
           IsolationLevelType::SERIALIZABLE ||
           current_txn->GetIsolationLevel() ==
           IsolationLevelType::REPEATABLE_READS);

    TupleHeaderHardAccessor hard_header_accessor;
    AcquireTupleHeaderHard(header_location, hard_header_accessor);
    auto hard_header = hard_header_accessor.GetTupleHeaderHard();

    DeferCode c([&, this]() {
        ReleaseTupleHeaderHard(hard_header_accessor);
    });
    //LOG_TRACE("PerformRead (%u, %u)\n", location.pid, location.off);

    // Check if it's select for update before we check the ownership
    // and modify the last reader cid.
    if (acquire_ownership == true) {
        // acquire ownership.
        if (IsOwner(current_txn, hard_header_accessor) == false) {
            // Acquire ownership if we haven't
            if (IsOwnable(current_txn, hard_header_accessor) == false) {
                // Cannot own
                return false;
            }
            if (AcquireOwnership(current_txn, hard_header_accessor) ==
                false) {
                // Cannot acquire ownership
                return false;
            }

            // Record RWType::READ_OWN
            current_txn->RecordReadOwn(header_location);

            // now we have already obtained the ownership.
            // then attempt to set last reader cid.
            bool ret = SetLastReaderCommitId(
                    hard_header_accessor, current_txn->GetCommitId(), true);

            assert(ret == true);
            // there's no need to maintain read set for timestamp ordering protocol.
            // T/O does not check the read set during commit phase.
        }

        // if we have already owned the version.
        assert(IsOwner(current_txn, hard_header_accessor) == true);
        assert(GetLastReaderCommitId(hard_header_accessor) ==
               current_txn->GetCommitId() ||
               GetLastReaderCommitId(hard_header_accessor) == 0);
        // Increment table read op stats
//        if (static_cast<StatsType>(settings::SettingsManager::GetInt(
//                settings::SettingId::stats_mode)) != StatsType::INVALID) {
//            stats::BackendStatsContext::GetInstance()->IncrementTableReads(
//                    location.block);
//        }
        return true;

    } else {
        if (IsOwner(current_txn, hard_header_accessor) == false) {
            // if the current transaction does not own this tuple,
            // then attempt to set last reader cid.
            if (SetLastReaderCommitId(hard_header_accessor,
                                      current_txn->GetCommitId(), false) == true) {
                // update read set.
                current_txn->RecordRead(header_location);

                // Increment table read op stats
//                if (static_cast<StatsType>(settings::SettingsManager::GetInt(
//                        settings::SettingId::stats_mode)) != StatsType::INVALID) {
//                    stats::BackendStatsContext::GetInstance()->IncrementTableReads(
//                            location.block);
//                }
                return true;
            } else {
                // if the tuple has been owned by some concurrent transactions,
                // then read fails.
                //LOG_TRACE("Transaction read failed");
                return false;
            }

        } else {
            // if the current transaction has already owned this tuple,
            // then perform read directly.
            assert(GetLastReaderCommitId(hard_header_accessor) ==
                   current_txn->GetCommitId() ||
                   GetLastReaderCommitId(hard_header_accessor) == 0);

            // this version must already be in the read/write set.
            // so no need to update read set.
            // current_txn->RecordRead(location);

            // Increment table read op stats
//            if (static_cast<StatsType>(settings::SettingsManager::GetInt(
//                    settings::SettingId::stats_mode)) != StatsType::INVALID) {
//                stats::BackendStatsContext::GetInstance()->IncrementTableReads(
//                        location.block);
//            }
            return true;
        }
    }
    // end SERIALIZABLE || REPEATABLE_READS
}

void MVTOTransactionManager::PerformInsert(
        TransactionContext *const current_txn, const TuplePointer &tuple_hard_hdr_location,
        TuplePointer *index_entry_ptr) {
    assert(current_txn->GetIsolationLevel() !=
           IsolationLevelType::READ_ONLY);


    TupleHeaderHardAccessor hard_hdr_accessor;
    AcquireTupleHeaderHard(tuple_hard_hdr_location, hard_hdr_accessor);
    auto header_hard = hard_hdr_accessor.GetTupleHeaderHard();

    DeferCode c([&, this]() {
        ReleaseTupleHeaderHard(hard_hdr_accessor);
    });
    auto transaction_id = current_txn->GetTransactionId();

    // check MVCC info
    // the tuple slot must be empty.
    assert(header_hard->GetTransactionId() ==
           INVALID_TXN_ID);
    assert(header_hard->GetBeginCommitId() == MAX_CID);
    assert(header_hard->GetEndCommitId() == MAX_CID);

    header_hard->SetTransactionId(transaction_id);

    // no need to set next item pointer.

    // Add the new tuple into the insert set
    current_txn->RecordInsert(tuple_hard_hdr_location);

    InitTupleReserved(hard_hdr_accessor);

    // Write down the head pointer's address in tile group soft_header
    //tile_group_header->SetIndirection(tuple_id, index_entry_ptr);

    // Increment table insert op stats
//    if (static_cast<StatsType>(settings::SettingsManager::GetInt(
//            settings::SettingId::stats_mode)) != StatsType::INVALID) {
//        stats::BackendStatsContext::GetInstance()->IncrementTableInserts(
//                location.block);
//    }
}


void MVTOTransactionManager::PerformUpdate(
        TransactionContext *const current_txn, const TuplePointer &old_tuple_hard_hdr_location,
        const TuplePointer &new_tuple_hard_hdr_location) {
    assert(current_txn->GetIsolationLevel() !=
           IsolationLevelType::READ_ONLY);

    TupleHeaderHardAccessor hard_header_accessor;
    TupleHeaderHardAccessor new_hard_header_accessor;

    LOG_TRACE("Performing Update old tuple %u %u", old_tuple_hard_hdr_location.pid,
              old_tuple_hard_hdr_location.off);
    LOG_TRACE("Performing Update new tuple %u %u", new_tuple_hard_hdr_location.pid,
              new_tuple_hard_hdr_location.off);

    auto transaction_id = current_txn->GetTransactionId();

    AcquireTupleHeaderHard(old_tuple_hard_hdr_location, hard_header_accessor);
    auto old_hard_header = hard_header_accessor.GetTupleHeaderHard();

    // if we can perform update, then we must have already locked the older
    // version.
    assert(old_hard_header->GetTransactionId() == transaction_id);
    //assert(base_tuple->GetPrevTuplePointer().IsNull() == true);
    // if the executor doesn't call PerformUpdate after AcquireOwnership,
    // no one will possibly release the write lock acquired by this txn.


    // Set double linked list
    old_hard_header->SetPrevHeaderPointer(new_tuple_hard_hdr_location);

    ReleaseTupleHeaderHard(hard_header_accessor);

    AcquireTupleHeaderHard(new_tuple_hard_hdr_location, new_hard_header_accessor);
    auto new_hard_header = new_hard_header_accessor.GetTupleHeaderHard();

    // check whether the new version is empty.
    assert(new_hard_header->GetTransactionId() == INVALID_TXN_ID);
    assert(new_hard_header->GetBeginCommitId() == MAX_CID);
    assert(new_hard_header->GetEndCommitId() == MAX_CID);

    new_hard_header->SetNextHeaderPointer(old_tuple_hard_hdr_location);

    new_hard_header->SetTransactionId(transaction_id);

    // we should guarantee that the newer version is all set before linking the
    // newer version to older version.
    COMPILER_MEMORY_FENCE;

    InitTupleReserved(new_hard_header_accessor);

    // we must be updating the latest version.
    // Set the soft_header information for the new version
//    TuplePointer *index_entry_ptr =
//            tile_group_header->GetIndirection(old_location.offset);

    // if there's no primary index on a table, then index_entry_ptr == nullptr.
//    if (index_entry_ptr != nullptr) {
//        new_tile_group_header->SetIndirection(new_location.offset, index_entry_ptr);
//
//        // Set the index soft_header in an atomic way.
//        // We do it atomically because we don't want any one to see a half-done
//        // pointer.
//        // In case of contention, no one can update this pointer when we are
//        // updating it
//        // because we are holding the write lock. This update should success in
//        // its first trial.
//        UNUSED_ATTRIBUTE auto res =
//                AtomicUpdateTuplePointer(index_entry_ptr, new_location);
//        assert(res == true);
//    }

    // Add the old tuple into the update set
    current_txn->RecordUpdate(old_tuple_hard_hdr_location);

    // Increment table update op stats
//    if (static_cast<StatsType>(settings::SettingsManager::GetInt(
//            settings::SettingId::stats_mode)) != StatsxxType::INVALID) {
//        stats::BackendStatsContext::GetInstance()->IncrementTableUpdates(
//                new_location.block);
//    }

    ReleaseTupleHeaderHard(new_hard_header_accessor);
}

void MVTOTransactionManager::PerformUpdate(
        TransactionContext *const current_txn,
        const TuplePointer &tuple_hard_hdr_location) {
    assert(current_txn->GetIsolationLevel() !=
           IsolationLevelType::READ_ONLY);

    ConstTupleHeaderHardAccessor hard_hdr_accessor;
    AcquireTupleHeaderHard(tuple_hard_hdr_location, hard_hdr_accessor);
    auto hard_header = hard_hdr_accessor.GetTupleHeaderHard();

    DeferCode c([&, this]() {
        ReleaseTupleHeaderHard(hard_hdr_accessor);
    });

    assert(hard_header->GetTransactionId() ==
           current_txn->GetTransactionId());
    assert(hard_header->GetBeginCommitId() == MAX_CID);
    assert(hard_header->GetEndCommitId() == MAX_CID);

    // no need to add the older version into the update set.
    // if there exists older version, then the older version must already
    // been added to the update set.
    // if there does not exist an older version, then it means that the
    // transaction
    // is updating a version that is installed by itself.
    // in this case, nothing needs to be performed.

    // Increment table update op stats
//    if (static_cast<StatsType>(settings::SettingsManager::GetInt(
//            settings::SettingId::stats_mode)) != StatsType::INVALID) {
//        stats::BackendStatsContext::GetInstance()->IncrementTableUpdates(
//                location.block);
//    }
}

void MVTOTransactionManager::PerformDelete(
        TransactionContext *const current_txn, const TuplePointer &old_tuple_hard_hdr_location,
        const TuplePointer &new_tuple_hard_hdr_location) {
    assert(current_txn->GetIsolationLevel() !=
           IsolationLevelType::READ_ONLY);

    TupleHeaderHardAccessor hard_header_accessor;
    TupleHeaderHardAccessor new_hard_header_accessor;


    LOG_TRACE("Performing Delete old tuple %u %u", old_location.pid,
              old_location.pos);
    LOG_TRACE("Performing Delete new tuple %u %u", new_location.pid,
              new_location.pos);

    auto transaction_id = current_txn->GetTransactionId();

    AcquireTupleHeaderHard(old_tuple_hard_hdr_location, hard_header_accessor);
    auto old_hard_header = hard_header_accessor.GetTupleHeaderHard();

    assert(GetLastReaderCommitId(hard_header_accessor) ==
           current_txn->GetCommitId());

    // if we can perform delete, then we must have already locked the older
    // version.
    assert(old_hard_header->GetTransactionId() == transaction_id);


    // Set up double linked list
    // we must be deleting the latest version.
    assert(old_hard_header->GetPrevHeaderPointer().IsNull() ==
           true);
    old_hard_header->SetPrevHeaderPointer(new_tuple_hard_hdr_location);

    ReleaseTupleHeaderHard(hard_header_accessor);

    AcquireTupleHeaderHard(new_tuple_hard_hdr_location, new_hard_header_accessor);
    auto new_hard_header = new_hard_header_accessor.GetTupleHeaderHard();

    // check whether the new version is empty.
    assert(new_hard_header->GetTransactionId() ==
           INVALID_TXN_ID);
    assert(new_hard_header->GetBeginCommitId() ==
           MAX_CID);
    assert(new_hard_header->GetEndCommitId() ==
           MAX_CID);


    new_hard_header->SetNextHeaderPointer(old_tuple_hard_hdr_location);

    new_hard_header->SetTransactionId(transaction_id);

    new_hard_header->SetEndCommitId(INVALID_CID);

    // we should guarantee that the newer version is all set before linking the
    // newer version to older version.
    COMPILER_MEMORY_FENCE;

    InitTupleReserved(new_hard_header_accessor);

//    // we must be deleting the latest version.
//    // Set the header information for the new version
//    TuplePointer *index_entry_ptr =
//            tile_group_header->GetIndirection(old_location.offset);
//
//    // if there's no primary index on a table, then index_entry_ptr == nullptr.
//    if (index_entry_ptr != nullptr) {
//        new_tile_group_header->SetIndirection(new_location.offset, index_entry_ptr);
//
//        // Set the index header in an atomic way.
//        // We do it atomically because we don't want any one to see a half-down
//        // pointer
//        // In case of contention, no one can update this pointer when we are
//        // updating it
//        // because we are holding the write lock. This update should success in
//        // its first trial.
//        UNUSED_ATTRIBUTE auto res =
//                AtomicUpdateTuplePointer(index_entry_ptr, new_location);
//        assert(res == true);
//    }

    current_txn->RecordDelete(old_tuple_hard_hdr_location);

    // Increment table delete op stats
//    if (static_cast<StatsType>(settings::SettingsManager::GetInt(
//            settings::SettingId::stats_mode)) != StatsType::INVALID) {
//        stats::BackendStatsContext::GetInstance()->IncrementTableDeletes(
//                old_location.block);
//    }
    ReleaseTupleHeaderHard(new_hard_header_accessor);
}

void MVTOTransactionManager::PerformDelete(
        TransactionContext *const current_txn, const TuplePointer &tuple_header_hard_location) {
    assert(current_txn->GetIsolationLevel() !=
           IsolationLevelType::READ_ONLY);

    TupleHeaderHardAccessor hard_hdr_accessor;
    AcquireTupleHeaderHard(tuple_header_hard_location, hard_hdr_accessor);
    auto hard_header = hard_hdr_accessor.GetTupleHeaderHard();

    DeferCode c([&, this]() {
        ReleaseTupleHeaderHard(hard_hdr_accessor);
    });


//    oid_t tile_group_id = location.block;
//    oid_t tuple_id = location.offset;
//
//    auto &manager = catalog::Manager::GetInstance();
//    auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

    assert(hard_header->GetTransactionId() ==
           current_txn->GetTransactionId());
    assert(hard_header->GetBeginCommitId() == MAX_CID);

    hard_header->SetEndCommitId(INVALID_CID);

    // Add the old tuple into the delete set
    auto old_location = hard_header->GetNextHeaderPointer();
    if (old_location.IsNull() == false) {
        // if this version is not newly inserted.
        current_txn->RecordDelete(old_location);
    } else {
        // if this version is newly inserted.
        current_txn->RecordDelete(tuple_header_hard_location);
    }

    // Increment table delete op stats
//    if (static_cast<StatsType>(settings::SettingsManager::GetInt(
//            settings::SettingId::stats_mode)) != StatsType::INVALID) {
//        stats::BackendStatsContext::GetInstance()->IncrementTableDeletes(
//                location.block);
//    }
}

ResultType MVTOTransactionManager::CommitTransaction(
        TransactionContext *const current_txn) {
//    LOG_TRACE("Committing peloton txn : %" PRId64,
//            current_txn->GetTransactionId());

    //////////////////////////////////////////////////////////
    //// handle READ_ONLY
    //////////////////////////////////////////////////////////
    if (current_txn->GetIsolationLevel() == IsolationLevelType::READ_ONLY) {
        EndTransaction(current_txn);
        return ResultType::SUCCESS;
    }

    //////////////////////////////////////////////////////////
    //// handle other isolation levels
    //////////////////////////////////////////////////////////

    //auto &manager = catalog::Manager::GetInstance();
    //auto &log_manager = logging::LogManager::GetInstance();

    //log_manager.StartLogging();

    // generate transaction id.
    cid_t end_commit_id = current_txn->GetCommitId();

    auto &rw_set = current_txn->GetReadWriteSet();
//    auto &rw_object_set = current_txn->GetCreateDropSet();
//
//    auto gc_set = current_txn->GetGCSetPtr();
//    auto gc_object_set = current_txn->GetGCObjectSetPtr();

//    for (auto &obj : rw_object_set) {
//        auto ddl_type = std::get<3>(obj);
//        if (ddl_type == DDLType::CREATE) continue;
//        oid_t database_oid = std::get<0>(obj);
//        oid_t table_oid = std::get<1>(obj);
//        oid_t index_oid = std::get<2>(obj);
//        gc_object_set->emplace_back(database_oid, table_oid, index_oid);
//    }
//
//    oid_t database_id = 0;
//    if (static_cast<StatsType>(settings::SettingsManager::GetInt(
//            settings::SettingId::stats_mode)) != StatsType::INVALID) {
//        for (const auto &tuple_entry : rw_set.GetConstIterator()) {
//            // Call the GetConstIterator() function to explicitly lock the cuckoohash
//            // and initilaize the iterator
//            const auto tile_group_id = tuple_entry.first.block;
//            database_id = manager.GetTileGroup(tile_group_id)->GetDatabaseId();
//            if (database_id != CATALOG_DATABASE_OID) {
//                break;
//            }
//        }
//    }

    // install everything.
    // 1. install a new version for update operations;
    // 2. install an empty version for delete operations;
    // 3. install a new tuple for insert operations.
    // Iterate through each item pointer in the read write set

    // TODO (Pooja): This might be inefficient since we will have to get the
    // tile_group_header for each entry. Check if this needs to be consolidated
    for (const auto &tuple_entry : rw_set) {
        TuplePointer item_ptr = tuple_entry.first;

//        oid_t tile_group_id = item_ptr.block;
//        oid_t tuple_slot = item_ptr.offset;
//
//        auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

        if (tuple_entry.second == RWType::READ_OWN) {
            TupleHeaderHardAccessor hard_header_accessor;

            AcquireTupleHeaderHard(item_ptr, hard_header_accessor);

            DeferCode c([&, this]() {
                ReleaseTupleHeaderHard(hard_header_accessor);
            });

            // A read operation has acquired ownership but hasn't done any further
            // update/delete yet
            // Yield the ownership
            YieldOwnership(current_txn, hard_header_accessor);
        } else if (tuple_entry.second == RWType::UPDATE) {
            TupleHeaderHardAccessor hard_header_accessor;

            AcquireTupleHeaderHard(item_ptr, hard_header_accessor);
            auto hard_header = hard_header_accessor.GetTupleHeaderHard();

            DeferCode c([&, this]() {
                ReleaseTupleHeaderHard(hard_header_accessor);
            });

            TupleHeaderHardAccessor new_hard_header_accessor;

            DeferCode c2([&, this]() {
                ReleaseTupleHeaderHard(new_hard_header_accessor);
            });

            // we must guarantee that, at any time point, only one version is
            // visible.
            TuplePointer new_version_hard_header_ptr = hard_header->GetPrevHeaderPointer();

            assert(new_version_hard_header_ptr.IsNull() == false);

            auto cid = hard_header->GetEndCommitId();
            assert(cid > end_commit_id);

            ReleaseTupleHeaderHard(hard_header_accessor);

            AcquireTupleHeaderHard(new_version_hard_header_ptr, new_hard_header_accessor);
            auto new_hard_header = new_hard_header_accessor.GetTupleHeaderHard();

            new_hard_header->SetBeginCommitId(end_commit_id);
            new_hard_header->SetEndCommitId(cid);

            COMPILER_MEMORY_FENCE;

            ReleaseTupleHeaderHard(new_hard_header_accessor);

            // Reacquire hard header
            AcquireTupleHeaderHard(item_ptr, hard_header_accessor);
            hard_header = hard_header_accessor.GetTupleHeaderHard();

            hard_header->SetEndCommitId(end_commit_id);

            hard_header->SetTransactionId(INITIAL_TXN_ID);
            ReleaseTupleHeaderHard(hard_header_accessor);

            // we should set the version before releasing the lock.
            COMPILER_MEMORY_FENCE;

            AcquireTupleHeaderHard(new_version_hard_header_ptr, new_hard_header_accessor);
            new_hard_header = new_hard_header_accessor.GetTupleHeaderHard();

            new_hard_header->SetTransactionId(INITIAL_TXN_ID);

            ReleaseTupleHeaderHard(new_hard_header_accessor);
            // add old version into gc set.
            // may need to delete versions from secondary indexes.
//            gc_set->operator[](tile_group_id)[tuple_slot] =
//                    GCVersionType::COMMIT_UPDATE;

            //log_manager.LogUpdate(new_version);

        } else if (tuple_entry.second == RWType::DELETE) {
            TupleHeaderHardAccessor hard_header_accessor;

            AcquireTupleHeaderHard(item_ptr, hard_header_accessor);
            auto hard_header = hard_header_accessor.GetTupleHeaderHard();

            DeferCode c([&, this]() {
                ReleaseTupleHeaderHard(hard_header_accessor);
            });


            TupleHeaderHardAccessor new_hard_header_accessor;

            DeferCode c2([&, this]() {
                ReleaseTupleHeaderHard(new_hard_header_accessor);
            });


            AcquireTupleHeaderHard(item_ptr, hard_header_accessor);
            hard_header = hard_header_accessor.GetTupleHeaderHard();

            // we must guarantee that, at any time point, only one version is
            // visible.
            TuplePointer new_version_hard_header_ptr = hard_header->GetPrevHeaderPointer();

            auto cid = hard_header->GetEndCommitId();
            assert(cid > end_commit_id);

            ReleaseTupleHeaderHard(hard_header_accessor);

            AcquireTupleHeaderHard(new_version_hard_header_ptr, new_hard_header_accessor);
            auto new_hard_header = new_hard_header_accessor.GetTupleHeaderHard();

            new_hard_header->SetBeginCommitId(end_commit_id);
            new_hard_header->SetEndCommitId(cid);

            COMPILER_MEMORY_FENCE;

            ReleaseTupleHeaderHard(new_hard_header_accessor);

            // Reaquire the base tuple
            AcquireTupleHeaderHard(item_ptr, hard_header_accessor);
            hard_header = hard_header_accessor.GetTupleHeaderHard();

            hard_header->SetEndCommitId(end_commit_id);

            hard_header->SetTransactionId(INITIAL_TXN_ID);
            ReleaseTupleHeaderHard(hard_header_accessor);

            // we should set the version before releasing the lock.
            COMPILER_MEMORY_FENCE;

            AcquireTupleHeaderHard(new_version_hard_header_ptr, new_hard_header_accessor);
            new_hard_header = new_hard_header_accessor.GetTupleHeaderHard();

            new_hard_header->SetTransactionId(INVALID_TXN_ID);
            ReleaseTupleHeaderHard(new_hard_header_accessor);

            // add to gc set.
            // we need to recycle both old and new versions.
            // we require the GC to delete tuple from index only once.
            // recycle old version, delete from index
            // the gc should be responsible for recycling the newer empty version.
//            gc_set->operator[](tile_group_id)[tuple_slot] =
//                    GCVersionType::COMMIT_DELETE;

            //log_manager.LogDelete(TuplePointer(tile_group_id, tuple_slot));

        } else if (tuple_entry.second == RWType::INSERT) {
            TupleHeaderHardAccessor hard_header_accessor;

            AcquireTupleHeaderHard(item_ptr, hard_header_accessor);
            auto hard_header = hard_header_accessor.GetTupleHeaderHard();

            DeferCode c([&, this]() {
                ReleaseTupleHeaderHard(hard_header_accessor);
            });

            assert(hard_header->GetTransactionId() ==
                   current_txn->GetTransactionId());
            // set the begin commit id to persist insert
            hard_header->SetBeginCommitId(end_commit_id);
            hard_header->SetEndCommitId(MAX_CID);

            // we should set the version before releasing the lock.
            COMPILER_MEMORY_FENCE;

            hard_header->SetTransactionId(INITIAL_TXN_ID);

            // nothing to be added to gc set.

            //log_manager.LogInsert(TuplePointer(tile_group_id, tuple_slot));

        } else if (tuple_entry.second == RWType::INS_DEL) {
            TupleHeaderHardAccessor hard_header_accessor;

            AcquireTupleHeaderHard(item_ptr, hard_header_accessor);
            auto hard_header = hard_header_accessor.GetTupleHeaderHard();

            DeferCode c([&, this]() {
                ReleaseTupleHeaderHard(hard_header_accessor);
            });


            assert(hard_header->GetTransactionId() ==
                   current_txn->GetTransactionId());

            hard_header->SetBeginCommitId(MAX_CID);
            hard_header->SetEndCommitId(MAX_CID);

            // we should set the version before releasing the lock.
            COMPILER_MEMORY_FENCE;

            // set the begin commit id to persist insert
            hard_header->SetTransactionId(INVALID_TXN_ID);

            // add to gc set.
//            gc_set->operator[](tile_group_id)[tuple_slot] =
//                    GCVersionType::COMMIT_INS_DEL;

            // no log is needed for this case
        }
    }

    ResultType result = current_txn->GetResult();

//    log_manager.LogEnd();

    if (buf_mgr_->GetLogManager()) {
        auto tid = current_txn->GetTransactionId();
        auto last_lsn = GetLastLogRecordLSN();
        lsn_t lsn = buf_mgr_->GetLogManager()->LogCommitTxn(tid, last_lsn);
        SetLastLogRecordLSN(lsn);
    }
    EndTransaction(current_txn);

    // Increment # txns committed metric
//    if (static_cast<StatsType>(settings::SettingsManager::GetInt(
//            settings::SettingId::stats_mode)) != StatsType::INVALID) {
//        stats::BackendStatsContext::GetInstance()->IncrementTxnCommitted(
//                database_id);
//    }

    return result;
}

ResultType MVTOTransactionManager::AbortTransaction(
        TransactionContext *const current_txn) {
    // a pre-declared read-only transaction will never abort.
    assert(current_txn->GetIsolationLevel() !=
           IsolationLevelType::READ_ONLY);

//    LOG_TRACE("Aborting peloton txn : %" PRId64, current_txn->GetTransactionId());
//    auto &manager = catalog::Manager::GetInstance();

    auto &rw_set = current_txn->GetReadWriteSet();
//    auto &rw_object_set = current_txn->GetCreateDropSet();
//
//    auto gc_set = current_txn->GetGCSetPtr();
//    auto gc_object_set = current_txn->GetGCObjectSetPtr();
//
//    for (int i = rw_object_set.size() - 1; i >= 0; i--) {
//        auto &obj = rw_object_set[i];
//        auto ddl_type = std::get<3>(obj);
//        if (ddl_type == DDLType::DROP) continue;
//        oid_t database_oid = std::get<0>(obj);
//        oid_t table_oid = std::get<1>(obj);
//        oid_t index_oid = std::get<2>(obj);
//        gc_object_set->emplace_back(database_oid, table_oid, index_oid);
//    }
//
//    oid_t database_id = 0;
//    if (static_cast<StatsType>(settings::SettingsManager::GetInt(
//            settings::SettingId::stats_mode)) != StatsType::INVALID) {
//        for (const auto &tuple_entry : rw_set.GetConstIterator()) {
//            // Call the GetConstIterator() function to explicitly lock the cuckoohash
//            // and initilaize the iterator
//            const auto tile_group_id = tuple_entry.first.block;
//            database_id = manager.GetTileGroup(tile_group_id)->GetDatabaseId();
//            if (database_id != CATALOG_DATABASE_OID) {
//                break;
//            }
//        }
//    }

    // Iterate through each item pointer in the read write set
    // TODO (Pooja): This might be inefficient since we will have to get the
    // tile_group_header for each entry. Check if this needs to be consolidated
    for (const auto &tuple_entry : rw_set) {
        TuplePointer item_ptr = tuple_entry.first;

//
//        oid_t tile_group_id = item_ptr.block;
//        oid_t tuple_slot = item_ptr.offset;
//        auto tile_group_header = manager.GetTileGroup(tile_group_id)->GetHeader();

        if (tuple_entry.second == RWType::READ_OWN) {
            TupleHeaderHardAccessor hard_header_accessor;

            DeferCode c([&, this]() {
                ReleaseTupleHeaderHard(hard_header_accessor);
            });

            AcquireTupleHeaderHard(item_ptr, hard_header_accessor);

            // A read operation has acquired ownership but hasn't done any further
            // update/delete yet
            // Yield the ownership
            YieldOwnership(current_txn, hard_header_accessor);
        } else if (tuple_entry.second == RWType::UPDATE) {
            TupleHeaderHardAccessor hard_header_accessor;

            DeferCode c([&, this]() {
                ReleaseTupleHeaderHard(hard_header_accessor);
            });
            auto rollback_logic_it = current_txn->GetRollbackFuncMap().find(item_ptr);
            assert(rollback_logic_it != current_txn->GetRollbackFuncMap().end());
            auto &rollback_logic = rollback_logic_it->second;
            // Call the user-provided custom rollback logic.
            rollback_logic();

            TupleHeaderHardAccessor new_hard_header_accessor;

            DeferCode c2([&, this]() {
                ReleaseTupleHeaderHard(new_hard_header_accessor);
            });


            AcquireTupleHeaderHard(item_ptr, hard_header_accessor);
            auto hard_header = hard_header_accessor.GetTupleHeaderHard();


            TuplePointer new_version_hard_header_ptr = hard_header->GetPrevHeaderPointer();

            ReleaseTupleHeaderHard(hard_header_accessor);

            AcquireTupleHeaderHard(new_version_hard_header_ptr, new_hard_header_accessor);
            auto new_hard_header = new_hard_header_accessor.GetTupleHeaderHard();

            //auto new_tile_group_header =
            //    manager.GetTileGroup(new_version.block)->GetHeader();
            // these two fields can be set at any time.
            new_hard_header->SetBeginCommitId(MAX_CID);
            new_hard_header->SetEndCommitId(MAX_CID);

            COMPILER_MEMORY_FENCE;

            // as the aborted version has already been placed in the version chain,
            // we need to unlink it by resetting the item pointers.

            // this must be the latest version of a version chain.
            assert(new_hard_header->GetPrevHeaderPointer()
                           .IsNull() == true);

            // if we updated the latest version.
            // We must first adjust the head pointer
            // before we unlink the aborted version from version list
//            TuplePointer *index_entry_ptr =
//                    tile_group_header->GetIndirection(tuple_slot);
//            UNUSED_ATTRIBUTE auto res = AtomicUpdateTuplePointer(
//                    index_entry_ptr, TuplePointer(tile_group_id, tuple_slot));
//            assert(res == true);
            //////////////////////////////////////////////////

            // we should set the version before releasing the lock.
            COMPILER_MEMORY_FENCE;

            new_hard_header->SetTransactionId(INVALID_TXN_ID);

            ReleaseTupleHeaderHard(new_hard_header_accessor);

            AcquireTupleHeaderHard(item_ptr, hard_header_accessor);
            hard_header = hard_header_accessor.GetTupleHeaderHard();

            hard_header->SetPrevHeaderPointer(kInvalidTuplePointer);

            // we should set the version before releasing the lock.
            COMPILER_MEMORY_FENCE;

            hard_header->SetTransactionId(INITIAL_TXN_ID);

            ReleaseTupleHeaderHard(hard_header_accessor);
            // add the version to gc set.
            // this version has already been unlinked from the version chain.
            // however, the gc should further unlink it from indexes.
//            gc_set->operator[](new_version.block)[new_version.offset] =
//                    GCVersionType::ABORT_UPDATE;

        } else if (tuple_entry.second == RWType::DELETE) {
            TupleHeaderHardAccessor hard_header_accessor;

            DeferCode c([&, this]() {
                ReleaseTupleHeaderHard(hard_header_accessor);
            });


            auto rollback_logic_it = current_txn->GetRollbackFuncMap().find(item_ptr);
            assert(rollback_logic_it != current_txn->GetRollbackFuncMap().end());
            auto &rollback_logic = rollback_logic_it->second;
            // Call the user-provided custom rollback logic.
            rollback_logic();

            TupleHeaderHardAccessor new_hard_header_accessor;

            DeferCode c2([&, this]() {
                ReleaseTupleHeaderHard(new_hard_header_accessor);
            });

            AcquireTupleHeaderHard(item_ptr, hard_header_accessor);
            auto hard_header = hard_header_accessor.GetTupleHeaderHard();

            TuplePointer new_version_hard_header_ptr = hard_header->GetPrevHeaderPointer();

            ReleaseTupleHeaderHard(hard_header_accessor);

            AcquireTupleHeaderHard(new_version_hard_header_ptr, new_hard_header_accessor);
            auto new_hard_header = new_hard_header_accessor.GetTupleHeaderHard();

            new_hard_header->SetBeginCommitId(MAX_CID);
            new_hard_header->SetEndCommitId(MAX_CID);

            COMPILER_MEMORY_FENCE;

            // as the aborted version has already been placed in the version chain,
            // we need to unlink it by resetting the item pointers.

            // this must be the latest version of a version chain.
            assert(new_hard_header->GetPrevHeaderPointer()
                           .IsNull() == true);

            // if we updated the latest version.
            // We must first adjust the head pointer
            // before we unlink the aborted version from version list
//            TuplePointer *index_entry_ptr =
//                    tile_group_header->GetIndirection(tuple_slot);
//            UNUSED_ATTRIBUTE auto res = AtomicUpdateTuplePointer(
//                    index_entry_ptr, TuplePointer(tile_group_id, tuple_slot));
//            assert(res == true);
            //////////////////////////////////////////////////

            // we should set the version before releasing the lock.
            COMPILER_MEMORY_FENCE;

            new_hard_header->SetTransactionId(INVALID_TXN_ID);

            ReleaseTupleHeaderHard(new_hard_header_accessor);

            AcquireTupleHeaderHard(item_ptr, hard_header_accessor);
            hard_header = hard_header_accessor.GetTupleHeaderHard();

            hard_header->SetPrevHeaderPointer(kInvalidTuplePointer);

            // we should set the version before releasing the lock.
            COMPILER_MEMORY_FENCE;

            hard_header->SetTransactionId(INITIAL_TXN_ID);

            // add the version to gc set.
//            gc_set->operator[](new_version.block)[new_version.offset] =
//                    GCVersionType::ABORT_DELETE;

        } else if (tuple_entry.second == RWType::INSERT) {
            TupleHeaderHardAccessor hard_header_accessor;

            DeferCode c([&, this]() {
                ReleaseTupleHeaderHard(hard_header_accessor);
            });

            auto rollback_logic_it = current_txn->GetRollbackFuncMap().find(item_ptr);
            assert(rollback_logic_it != current_txn->GetRollbackFuncMap().end());
            auto &rollback_logic = rollback_logic_it->second;
            // Call the user-provided custom rollback logic.
            rollback_logic();

            AcquireTupleHeaderHard(item_ptr, hard_header_accessor);
            auto hard_header = hard_header_accessor.GetTupleHeaderHard();
            hard_header->SetBeginCommitId(MAX_CID);
            hard_header->SetEndCommitId(MAX_CID);

            // we should set the version before releasing the lock.
            COMPILER_MEMORY_FENCE;

            hard_header->SetTransactionId(INVALID_TXN_ID);

            // add the version to gc set.
            // delete from index.
//            gc_set->operator[](tile_group_id)[tuple_slot] =
//                    GCVersionType::ABORT_INSERT;

        } else if (tuple_entry.second == RWType::INS_DEL) {
            TupleHeaderHardAccessor hard_header_accessor;

            DeferCode c2([&, this]() {
                ReleaseTupleHeaderHard(hard_header_accessor);
            });

            auto rollback_logic_it = current_txn->GetRollbackFuncMap().find(item_ptr);
            assert(rollback_logic_it != current_txn->GetRollbackFuncMap().end());
            auto &rollback_logic = rollback_logic_it->second;
            // Call the user-provided custom rollback logic.
            rollback_logic();


            AcquireTupleHeaderHard(item_ptr, hard_header_accessor);
            auto hard_header = hard_header_accessor.GetTupleHeaderHard();

            hard_header->SetBeginCommitId(MAX_CID);
            hard_header->SetEndCommitId(MAX_CID);

            // we should set the version before releasing the lock.
            COMPILER_MEMORY_FENCE;

            hard_header->SetTransactionId(INVALID_TXN_ID);

            // add to gc set.
            //gc_set->operator[](tile_group_id)[tuple_slot] =
            //        GCVersionType::ABORT_INS_DEL;
        }
    }

    if (buf_mgr_->GetLogManager()) {
        auto tid = current_txn->GetTransactionId();
        auto last_lsn = GetLastLogRecordLSN();
        lsn_t lsn = buf_mgr_->GetLogManager()->LogAbortTxn(tid, last_lsn);
        SetLastLogRecordLSN(lsn);
    }
    current_txn->SetResult(ResultType::ABORTED);
    EndTransaction(current_txn);

    // Increment # txns aborted metric
//    if (static_cast<StatsType>(settings::SettingsManager::GetInt(
//            settings::SettingId::stats_mode)) != StatsType::INVALID) {
//        stats::BackendStatsContext::GetInstance()->IncrementTxnAborted(database_id);
//    }

    return ResultType::ABORTED;
}

uint64_t MVTOTransactionManager::NextRowId() {
    return row_id_counter.fetch_add(1);
}

uint64_t MVTOTransactionManager::GetCurrentRowId() const {
    return row_id_counter.load();
}

uint64_t MVTOTransactionManager::GetCurrentTid() const {
    return tid_counter.load();
}

void MVTOTransactionManager::SetCurrentRowId(uint64_t rid) {
    row_id_counter.store(rid);
}

void MVTOTransactionManager::SetCurrentTid(uint64_t tid) {
    tid_counter.store(tid);
}


//template<class Key, class T>
//void DataTable<Key, T>::CollectPurgablePages(std::unordered_set<pid_t> &purge_set, pid_t min_active_tid,
//                                             ConcurrentBufferManager *buf_mgr) {
//    pid_t prev_page_id = kInvalidPID;
//    auto transaction_manager = MVTOTransactionManager::GetInstance(buf_mgr);
//    bool has_non_purgable_tuples = false;
//    version_table.Scan([&](TuplePointer ptr, const T &tuple) -> bool {
//        auto hard_header_ptr = tuple.GetHeaderPointer();
//        if (ptr.pid != prev_page_id) { // New Page
//            if (has_non_purgable_tuples == false && prev_page_id != kInvalidPID) {
//                purge_set.insert(prev_page_id);
//            }
//            has_non_purgable_tuples = false;
//            prev_page_id = ptr.pid;
//        }
//
//        has_non_purgable_tuples |= !transaction_manager->IsTupleHeaderHardPurgable(hard_header_ptr, min_active_tid);
//        return false;
//    });
//}
//
//template<class Key, class T>
//void DataTable<Key, T>::PurgePages(const std::unordered_set<pid_t> &purge_set)  {
//    version_table.PurgePages(purge_set);
//}

extern std::vector<BaseDataTable*> database_tables;

void MVCCPurger::PurgeProcess(MVCCPurger * purger) {
    constexpr float CYCLES_PER_SEC = 3 * 1024ULL * 1024 * 1024;
    lsn_t prev_checkpoint_lsn = kInvalidLSN;
    long long next_wait_time_ms = 100;
    while (purger->stopped.load() == false) {
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10000));
        }

        if (purger->stopped.load()) {
            break;
        }
        std::unordered_set<pid_t> purge_page_candidates;
        long long cycles = 0;
        {
            ScopedTimer timer([&cycles](unsigned long long c) {cycles += c;});

            auto transaction_manager = MVTOTransactionManager::GetInstance(purger->buf_mgr);
            auto min_active_tid = transaction_manager->MinActiveTID();
            for (auto table : database_tables) {
                table->CollectPurgablePages(purge_page_candidates, min_active_tid, purger->buf_mgr);
            }

            if (purge_page_candidates.empty() == false) {
                for (auto table : database_tables) {
                    table->PurgePages(purge_page_candidates);
                }
            }
        }
        if (purge_page_candidates.empty() == false) {
            LOG_INFO("Purged %lu pages, took %lf secs\n", purge_page_candidates.size(), cycles / (CYCLES_PER_SEC + 0.0));
        }
    }
}

void MVCCPurger::StartPurgerThread() {
    purge_thread.reset(new std::thread(PurgeProcess, this));
}

void MVCCPurger::EndPurgerThread() {
    if (stopped.load() == false) {
        this->stopped.store(true);
        purge_thread->join();
    }
}

}