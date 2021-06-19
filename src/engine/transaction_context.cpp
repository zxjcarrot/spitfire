//
// Created by zxjcarrot on 2020-02-13.
//

#include "engine/txn.h"

#include <strstream>
#include <iostream>
#include <iosfwd>
namespace spitfire {
#define INTITIAL_RW_SET_SIZE 64

/*
 * TransactionContext state transition:
 *                r           r/ro            u/r/ro
 *              +--<--+     +---<--+        +---<--+
 *           r  |     |     |      |        |      |     d
 *  (init)-->-- +-> Read  --+-> Read Own ---+--> Update ---> Delete (final)
 *                    |   ro             u  |
 *                    |                     |
 *                    +----->--------->-----+
 *                              u
 *              r/ro/u
 *            +---<---+
 *         i  |       |     d
 *  (init)-->-+---> Insert ---> Ins_Del (final)
 *
 *    r : read
 *    ro: read_own
 *    u : update
 *    d : delete
 *    i : insert
 */

TransactionContext::TransactionContext(const size_t thread_id,
                                       const IsolationLevelType isolation,
                                       const cid_t &read_id)
        : rw_set_(INTITIAL_RW_SET_SIZE) {
    Init(thread_id, isolation, read_id);
}

TransactionContext::TransactionContext(const size_t thread_id,
                                       const IsolationLevelType isolation,
                                       const cid_t &read_id,
                                       const cid_t &commit_id)
        : rw_set_(INTITIAL_RW_SET_SIZE) {
    Init(thread_id, isolation, read_id, commit_id);
}

TransactionContext::TransactionContext(const size_t thread_id,
                                       const IsolationLevelType isolation,
                                       const cid_t &read_id,
                                       const cid_t &commit_id,
                                       const size_t rw_set_size)
        : rw_set_(rw_set_size) {
    Init(thread_id, isolation, read_id, commit_id);
}

TransactionContext::~TransactionContext() {}

void TransactionContext::Init(const size_t thread_id,
                              const IsolationLevelType isolation,
                              const cid_t &read_id, const cid_t &commit_id) {
    read_id_ = read_id;

    // commit id can be set at a transaction's commit phase.
    commit_id_ = commit_id;

    // set txn_id to commit_id.
    txn_id_ = commit_id_;

    thread_id_ = thread_id;

    isolation_level_ = isolation;

    is_written_ = false;

    insert_count_ = 0;

    rw_set_.clear();
}

RWType TransactionContext::GetRWType(const TuplePointer &location) {
    RWType rw_type;
    auto it = rw_set_.find(location);
    if (it == rw_set_.end()) {
        rw_type = RWType::INVALID;
    } else {
        rw_type = it->second;
    }

    return rw_type;
}

void TransactionContext::RecordRead(const TuplePointer &location) {
    auto it = rw_set_.find(location);

    if (it != rw_set_.end()) {
        auto rw_type = it->second;
        assert(rw_type != RWType::DELETE && rw_type != RWType::INS_DEL);
        return;
    } else {
        rw_set_.insert(std::make_pair(location, RWType::READ));
    }
}

void TransactionContext::RecordReadOwn(const TuplePointer &location) {
    RWType rw_type;
    auto it = rw_set_.find(location);
    if (it != rw_set_.end()) {
        rw_type = it->second;
        assert(rw_type != RWType::DELETE && rw_type != RWType::INS_DEL);
        if (rw_type == RWType::READ) {
            it->second = RWType::READ_OWN;
        }
    } else {
        rw_set_.insert(std::make_pair(location, RWType::READ_OWN));
    }
}

void TransactionContext::RecordUpdate(const TuplePointer &location) {
    RWType rw_type;
    auto it = rw_set_.find(location);
    if (it != rw_set_.end()) {
        rw_type = it->second;
        if (rw_type == RWType::READ || rw_type == RWType::READ_OWN) {
            is_written_ = true;
            it->second = RWType::UPDATE;
            return;
        }
        if (rw_type == RWType::UPDATE) {
            return;
        }
        if (rw_type == RWType::INSERT) {
            return;
        }
        if (rw_type == RWType::DELETE) {
            assert(false);
            return;
        }
        assert(false);
    } else {
        rw_set_.insert(std::make_pair(location, RWType::UPDATE));
    }
}

void TransactionContext::RecordInsert(const TuplePointer &location) {
    if (IsInRWSet(location)) {
        assert(false);
    } else {
        rw_set_.insert(std::make_pair(location, RWType::INSERT));
        ++insert_count_;
    }
}

bool TransactionContext::RecordDelete(const TuplePointer &location) {
    RWType rw_type;
    auto it = rw_set_.find(location);
    if (it != rw_set_.end()) {
        rw_type = it->second;
        if (rw_type == RWType::READ || rw_type == RWType::READ_OWN) {
            it->second = RWType::DELETE;
            // record write
            is_written_ = true;
            return false;
        }
        if (rw_type == RWType::UPDATE) {
            it->second = RWType::DELETE;
            return false;
        }
        if (rw_type == RWType::INSERT) {
            it->second = RWType::INS_DEL;
            --insert_count_;
            return true;
        }
        if (rw_type == RWType::DELETE) {
            assert(false);
            return false;
        }
        assert(false);
    } else {
        rw_set_.insert(std::make_pair(location, RWType::DELETE));
    }
    return false;
}


void TransactionContext::RecordRollbackFunc(const TuplePointer & t, std::function<void()> func) {
    auto it = rollback_funcs.find(t);
    //assert(it == rollback_funcs.end());
    rollback_funcs.insert(std::make_pair(t, func));
}

const std::string TransactionContext::GetInfo() const {
    return "";
    //std::ostringstream os;

//    os << " Txn :: @" << this << " ID : " << std::setw(4) << txn_id_
//       << " Read ID : " << std::setw(4) << read_id_
//       << " Commit ID : " << std::setw(4) << commit_id_
//       << " Result : " << (int)result_;

    //return os.str();
}

}