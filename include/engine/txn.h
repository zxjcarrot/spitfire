//
// Created by zxjcarrot on 2020-02-12.
//

#ifndef SPITFIRE_TXN_H
#define SPITFIRE_TXN_H

#include <mutex>
#include <thread>
#include <memory>
#include <tbb/concurrent_hash_map.h>


#include "engine/table.h"
#include "util/cuckoo_map.h"

namespace spitfire {

// For transaction id

typedef uint64_t txn_id_t;

static const txn_id_t INVALID_TXN_ID = 0;

static const txn_id_t INITIAL_TXN_ID = 1;

static const txn_id_t MAX_TXN_ID = std::numeric_limits<txn_id_t>::max();

// For commit id

typedef uint64_t cid_t;

static const cid_t INVALID_CID = 0;

static const cid_t MAX_CID = std::numeric_limits<cid_t>::max();

// For row id

typedef uint64_t rid_t;

static const rid_t INVALID_RID = 0;

static const rid_t MAX_RID = std::numeric_limits<rid_t>::max();

//===--------------------------------------------------------------------===//
// Result Types
//===--------------------------------------------------------------------===//

enum class ResultType {
    INVALID = 0,  // invalid result type
    SUCCESS = 1,
    FAILURE = 2,
    ABORTED = 3,  // aborted
    NOOP = 4,     // no op
    UNKNOWN = 5,
    QUEUING = 6,
    TO_ABORT = 7,
};

//===--------------------------------------------------------------------===//
// Isolation Levels
//===--------------------------------------------------------------------===//

enum class IsolationLevelType {
    INVALID = 0,
    SERIALIZABLE = 1,      // serializable
    SNAPSHOT = 2,          // snapshot isolation
    REPEATABLE_READS = 3,  // repeatable reads
    READ_COMMITTED = 4,    // read committed
    READ_ONLY = 5          // read only
};

//===--------------------------------------------------------------------===//
// Visibility Types
//===--------------------------------------------------------------------===//

enum class VisibilityType {
    INVALID = 0,
    INVISIBLE = 1,
    DELETED = 2,
    OK = 3
};

enum class VisibilityIdType {
    INVALID = 0,
    READ_ID = 1,
    COMMIT_ID = 2
};

//===--------------------------------------------------------------------===//
// read-write set
//===--------------------------------------------------------------------===//

// this enum is to identify the operations performed by the transaction.
enum class RWType {
    INVALID,
    READ,
    READ_OWN,  // select for update
    UPDATE,
    INSERT,
    DELETE,
    INS_DEL,  // delete after insert.
};

/*
 *  TupleHeaderSoft and TupleHeaderHard consist of states related to MVCC.
 *  Soft states are stored in a in-memory hashtable.
 *  Since the clustered index may split/merge due to data growth or shrinkage
 *  which could invalidate the TuplePointer pointing to the tuples in the clustered index,
 *  we store the hard states in a separate heap table instead of inside the tuple itself
 *  to make sure the doubly-linked version list of a tuple is stable.
 *
 *  Meaning of the states(copied from Peloton codebase)
 *  ===================
 *  TxnID == INITIAL_TXN_ID, BeginTS == MAX_CID, EndTS == MAX_CID --> empty version
 *  TxnID != INITIAL_TXN_ID, BeginTS != MAX_CID --> to-be-updated old version
 *  TxnID != INITIAL_TXN_ID, BeginTS == MAX_CID, EndTS == MAX_CID --> to-be-installed new version
 *  TxnID != INITIAL_TXN_ID, BeginTS == MAX_CID, EndTS == INVALID_CID --> to-be-installed deleted version
 */
////===--------------------------------------------------------------------===//
//// TupleHeaderSoft : tuple states in memory
////===--------------------------------------------------------------------===//
//
//struct TupleHeaderSoft {
//};

//===--------------------------------------------------------------------===//
// TupleHeaderHard : persistent tuple states
//===--------------------------------------------------------------------===//

struct TupleHeaderHard {
    rid_t row_id;
    cid_t begin_ts;
    cid_t end_ts;
    TuplePointer prev_header_ptr; // Points to previous the header of next newer version
    TuplePointer next_header_ptr; // Points to previous the header of next older version

    txn_id_t txn_id;
    cid_t read_ts;

    inline bool SetAtomicTransactionId(const txn_id_t &transaction_id) {
        txn_id_t *txn_id_ptr = (&txn_id);
        return __sync_bool_compare_and_swap(txn_id_ptr, INITIAL_TXN_ID,
                                            transaction_id);
    }

    inline void SetTransactionId(const txn_id_t &transaction_id) {
        txn_id = transaction_id;
    }

    txn_id_t GetTransactionId() const {
        return txn_id;
    }

    TupleHeaderHard(rid_t row_id, cid_t begin_ts = MAX_CID, cid_t end_ts = MAX_CID) : row_id(row_id),
                                                                                      begin_ts(begin_ts),
                                                                                      end_ts(end_ts), prev_header_ptr(
                    kInvalidTuplePointer), next_header_ptr(kInvalidTuplePointer), txn_id(INVALID_TXN_ID),
                                                                                      read_ts(INVALID_CID) {}

    cid_t GetEndCommitId() const {
        return end_ts;
    }

    cid_t GetBeginCommitId() const {
        return begin_ts;
    }

    rid_t GetRowId() const {
        return row_id;
    }

    void SetEndCommitId(cid_t cid) {
        end_ts = cid;
    }

    void SetBeginCommitId(cid_t cid) {
        begin_ts = cid;
    }

    void SetRowId(rid_t rid) {
        row_id = rid;
    }

    inline void SetNextHeaderPointer(const TuplePointer &ptr) {
        next_header_ptr = ptr;
    }

    const TuplePointer &GetNextHeaderPointer() const {
        return next_header_ptr;
    }

    inline void SetPrevHeaderPointer(const TuplePointer &ptr) {
        prev_header_ptr = ptr;
    }

    const TuplePointer &GetPrevHeaderPointer() const {
        return prev_header_ptr;
    }

    rid_t Key() const {
        return row_id;
    }
};

//===--------------------------------------------------------------------===//
// BaseTuple: all tuple derive from this base tuple
//===--------------------------------------------------------------------===//

struct BaseTuple {
    rid_t row_id;
    TuplePointer next_tuple_ptr = kInvalidTuplePointer;
    TuplePointer header_ptr = kInvalidTuplePointer;

    const TuplePointer &GetNextTuplePointer() const {
        return next_tuple_ptr;
    }

    inline void SetNextTuplePointer(const TuplePointer &ptr) {
        next_tuple_ptr = ptr;
    }

    const TuplePointer &GetHeaderPointer() const {
        return header_ptr;
    }

    inline void SetHeaderPointer(const TuplePointer &ptr) {
        header_ptr = ptr;
    }

    void SetRowId(rid_t rid) {
        row_id = rid;
    }

    rid_t GetRowId() const {
        return row_id;
    }
};

//===--------------------------------------------------------------------===//
// TuplePointer Utility Functions
//===--------------------------------------------------------------------===//

class TuplePointerComparator {
public:
    bool operator()(TuplePointer *const &p1, TuplePointer *const &p2) const {
        return (p1->pid == p2->pid) && (p1->off == p2->off);
    }

    bool operator()(TuplePointer const &p1, TuplePointer const &p2) const {
        return (p1.pid == p2.pid) && (p1.off == p2.off);
    }

    TuplePointerComparator(const TuplePointerComparator &) {}

    TuplePointerComparator() {}
};

struct TuplePointerHasher {
    size_t operator()(const TuplePointer &item) const {
        // This constant is found in the CityHash code
        // [Source libcuckoo/default_hasher.hh]
        // std::hash returns the same number for unsigned int which causes
        // too many collisions in the Cuckoohash leading to too many collisions
        return (std::hash<pid_t>()(item.pid) * 0x9ddfea08eb382d69ULL) ^
               std::hash<uint64_t>()(item.off);
    }
};

class TuplePointerHashFunc {
public:
    size_t operator()(TuplePointer *const &p) const {
        return (std::hash<decltype(p->pid)>()(p->pid) * 0x9ddfea08eb382d69ULL) ^
               std::hash<decltype(p->off)>()(p->off);
    }

    TuplePointerHashFunc(const TuplePointerHashFunc &) {}

    TuplePointerHashFunc() {}
};


template<typename K>
struct TuplePointerHashCompare {
    static size_t hash(const K &key) {
        return std::hash<decltype(key.pid)>()(key.pid) ^ std::hash<decltype(key.pos)>()(key.pos);
    }

    static bool equal(const K &key1, const K &key2) { return (key1 == key2); }
};


// TuplePointer -> type
//typedef CuckooMap<TuplePointer, RWType, TuplePointerHasher, TuplePointerComparator>
//        ReadWriteSet;

typedef std::unordered_map<TuplePointer, RWType, TuplePointerHasher> ReadWriteSet;

typedef std::unordered_map<TuplePointer, std::function<void()>, TuplePointerHasher>
        RollbackMap;

//===--------------------------------------------------------------------===//
// TransactionContext
//===--------------------------------------------------------------------===//

/**
 * @brief      Class for transaction context.
 */
class TransactionContext {
    TransactionContext(TransactionContext const &) = delete;

public:
    TransactionContext(const size_t thread_id, const IsolationLevelType isolation,
                       const cid_t &read_id);

    TransactionContext(const size_t thread_id, const IsolationLevelType isolation,
                       const cid_t &read_id, const cid_t &commit_id);

    TransactionContext(const size_t thread_id, const IsolationLevelType isolation,
                       const cid_t &read_id, const cid_t &commit_id,
                       const size_t read_write_set_size);

    /**
     * @brief      Destroys the object.
     */
    ~TransactionContext();

private:
    void Init(const size_t thread_id, const IsolationLevelType isolation,
              const cid_t &read_id) {
        Init(thread_id, isolation, read_id, read_id);
    }

    void Init(const size_t thread_id, const IsolationLevelType isolation,
              const cid_t &read_id, const cid_t &commit_id);

public:
    //===--------------------------------------------------------------------===//
    // Mutators and Accessors
    //===--------------------------------------------------------------------===//

    /**
     * @brief      Gets the thread identifier.
     *
     * @return     The thread identifier.
     */
    inline size_t GetThreadId() const { return thread_id_; }

    /**
     * @brief      Gets the transaction identifier.
     *
     * @return     The transaction identifier.
     */
    inline txn_id_t GetTransactionId() const { return txn_id_; }

    /**
     * @brief      Gets the read identifier.
     *
     * @return     The read identifier.
     */
    inline cid_t GetReadId() const { return read_id_; }

    /**
     * @brief      Gets the commit identifier.
     *
     * @return     The commit identifier.
     */
    inline cid_t GetCommitId() const { return commit_id_; }

    /**
     * @brief      Gets the timestamp.
     *
     * @return     The timestamp.
     */
    inline uint64_t GetTimestamp() const { return timestamp_; }

    /**
     * @brief      Sets the commit identifier.
     *
     * @param[in]  commit_id  The commit identifier
     */
    inline void SetCommitId(const cid_t commit_id) { commit_id_ = commit_id; }

    /**
     * @brief      Sets the txn identifier.
     *
     * @param[in]  commit_id  The commit identifier
     */
    inline void SetTxnId(const txn_id_t txn_id) { txn_id_ = txn_id; }

    /**
     * @brief      Sets the read identifier.
     *
     * @param[in]  commit_id  The commit identifier
     */
    inline void SetReadId(const cid_t read_id) { read_id_ = read_id; }


    /**
     * @brief      Sets the timestamp.
     *
     * @param[in]  timestamp  The timestamp
     */
    inline void SetTimestamp(const uint64_t timestamp) { timestamp_ = timestamp; }

    void RecordRead(const TuplePointer &);

    void RecordReadOwn(const TuplePointer &);

    void RecordUpdate(const TuplePointer &);

    void RecordInsert(const TuplePointer &);

    /**
     * @brief      Delete the record.
     *
     * @param[in]  <unnamed>  The logical physical location of the record
     *
     * @return     Return true if we detect INS_DEL.
     */
    bool RecordDelete(const TuplePointer &);

    RWType GetRWType(const TuplePointer &);

    /**
     * @brief      Determines if in rw set.
     *
     * @param[in]  location  The location
     *
     * @return     True if in rw set, False otherwise.
     */
    bool IsInRWSet(const TuplePointer &location) {
        return rw_set_.find(location) != rw_set_.end();
    }

    /**
     * @brief      Gets the read write set.
     *
     * @return     The read write set.
     */
    inline const ReadWriteSet &GetReadWriteSet() { return rw_set_; }

    void RecordRollbackFunc(const TuplePointer &, std::function<void()>);

    /**
     * @brief      Gets the codes for undoing the writes
     *
     * @return     The codes
     */
    inline const RollbackMap &GetRollbackFuncMap() { return rollback_funcs; }

    /**
     * @brief      Get a string representation for debugging.
     *
     * @return     The information.
     */
    const std::string GetInfo() const;

    /**
     * Set result and status.
     *
     * @param[in]  result  The result
     */
    inline void SetResult(ResultType result) { result_ = result; }

    /**
     * Get result and status.
     *
     * @return     The result.
     */
    inline ResultType GetResult() const { return result_; }

    /**
     * @brief      Determines if read only.
     *
     * @return     True if read only, False otherwise.
     */
    inline bool IsReadOnly() const {
        return is_written_ == false && insert_count_ == 0;
    }

    /**
     * @brief      Gets the isolation level.
     *
     * @return     The isolation level.
     */
    inline IsolationLevelType GetIsolationLevel() const {
        return isolation_level_;
    }

private:
    //===--------------------------------------------------------------------===//
    // Data members
    //===--------------------------------------------------------------------===//

    /** transaction id */
    txn_id_t txn_id_;

    /** id of thread creating this transaction */
    size_t thread_id_;

    /**
     * read id
     * this id determines which tuple versions the transaction can access.
     */
    cid_t read_id_;

    /**
     * commit id
     * this id determines the id attached to the tuple version written by the
     * transaction.
     */
    cid_t commit_id_;

    /** timestamp when the transaction began */
    uint64_t timestamp_;

    ReadWriteSet rw_set_;

    /** result of the transaction */
    ResultType result_ = ResultType::SUCCESS;

    bool is_written_;
    size_t insert_count_;

    IsolationLevelType isolation_level_;
    RollbackMap rollback_funcs;
    lsn_t last_log_rec_lsn{kInvalidLSN};
};

//typedef tbb::concurrent_hash_map<rid_t, TupleHeaderSoft> soft_header_table_t;

//struct TupleHeaderSoftAccessor {
//    TupleHeaderSoftAccessor(const TupleHeaderSoftAccessor &) = delete;
//
//    TupleHeaderSoftAccessor() {}
//
//    ~TupleHeaderSoftAccessor() { FinishAccess(); }
//
//    void FinishAccess() { accessor.release(); }
//
//    TupleHeaderSoft *GetTupleHeaderSoft() { return &accessor->second; }
//
//    soft_header_table_t::accessor accessor;
//};

struct ConstBaseTupleAccessor {
    ConstBaseTupleAccessor(const ConstBaseTupleAccessor &) = delete;

    ConstBaseTupleAccessor() {}

    ~ConstBaseTupleAccessor() { FinishAccess(); }

    void FinishAccess() { accessor.FinishAccess(); }

    const BaseTuple *GetBaseTuple() { return reinterpret_cast<const BaseTuple *>(raw_data); }

    ConcurrentBufferManager::PageAccessor accessor;
    const char *raw_data;
};

struct BaseTupleAccessor : public ConstBaseTupleAccessor {
    BaseTupleAccessor(const BaseTupleAccessor &) = delete;

    BaseTupleAccessor() {}

    ~BaseTupleAccessor() {}

    BaseTuple *GetBaseTuple() { return const_cast<BaseTuple *>(ConstBaseTupleAccessor::GetBaseTuple()); }
};

struct ConstTupleHeaderHardAccessor {
    ConstTupleHeaderHardAccessor(const ConstTupleHeaderHardAccessor &) = delete;

    ConstTupleHeaderHardAccessor() {}

    ~ConstTupleHeaderHardAccessor() { FinishAccess(); }

    void FinishAccess() { accessor.FinishAccess(); }

    const TupleHeaderHard *GetTupleHeaderHard() { return reinterpret_cast<const TupleHeaderHard *>(raw_data); }

    ConcurrentBufferManager::PageAccessor accessor;
    const char *raw_data;
    bool released = true;
};

struct TupleHeaderHardAccessor : public ConstTupleHeaderHardAccessor {
    TupleHeaderHardAccessor(const TupleHeaderHardAccessor &) = delete;

    TupleHeaderHardAccessor() {}

    ~TupleHeaderHardAccessor() {}

    TupleHeaderHard *
    GetTupleHeaderHard() { return const_cast<TupleHeaderHard *>(ConstTupleHeaderHardAccessor::GetTupleHeaderHard()); }
};


/**
 * @brief      Class for MVTO transaction manager.
 */
class MVTOTransactionManager {
public:
//    void AcquireTupleHeaderSoft(rid_t rid, TupleHeaderSoftAccessor &accessor);
//
//    void ReleaseTupleHeaderSoft(TupleHeaderSoftAccessor &accessor);

    void AcquireTupleHeaderHard(TuplePointer ptr, TupleHeaderHardAccessor &accessor);

    void AcquireTupleHeaderHard(TuplePointer ptr, ConstTupleHeaderHardAccessor &accessor);

    void ReleaseTupleHeaderHard(TupleHeaderHardAccessor &accessor);

    void ReleaseTupleHeaderHard(ConstTupleHeaderHardAccessor &accessor);

    bool IsTupleHeaderHardPurgable(TuplePointer ptr, pid_t min_active_tid);

    MVTOTransactionManager() {}

    /**
     * @brief      Destroys the object.
     */
    ~MVTOTransactionManager() {}

public:

    /**
     * @brief Returns the minimum id(min_tid) of active transactions in the system.
     * This id is used in garbage collection of undo segments.
     * All tuples whose end_ts < min_tid are considered garbage and can be safely
     * purged from the system.
     * @return The minimum active transaction id in the system.
     */
    txn_id_t MinActiveTID();

    TuplePointer InsertHardHeader(const TupleHeaderHard &header);

    /**
     * @brief      Gets the instance.
     *
     * @param[in]  protocol   The protocol
     * @param[in]  isolation  The isolation
     * @param[in]  conflict   The conflict
     *
     * @return     The instance.
     */
    static MVTOTransactionManager *GetInstance(ConcurrentBufferManager *buf_mgr);

    static void ClearInstance();

    ConcurrentBufferManager *GetBufferManager() { return buf_mgr_; }

    Status Init(const pid_t meta_page_pid, ConcurrentBufferManager *mgr) {
        if (inited == false) {
            buf_mgr_ = mgr;
            hard_header_meta_page_pid = meta_page_pid;
            hard_header_table.Init(hard_header_meta_page_pid, mgr, hard_header_meta_page_pid == kInvalidPID);
            hard_header_meta_page_pid = hard_header_table.GetMetaPagePid();
        }
        inited = true;
        return Status::OK();
    }

    /**
     * Used for avoiding concurrent inserts.
     *
     * @param      current_txn        The current transaction
     * @param[in]  position_ptr  The position pointer
     *
     * @return     True if occupied, False otherwise.
     */
    bool IsOccupied(
            TransactionContext *const current_txn,
            const void *base_tuple);

    /**
     * @brief      Determines if visible.
     *
     * @param      current_txn        The current transaction
     * @param[in]  tile_group_header  The tile group header
     * @param[in]  tuple_id           The tuple identifier
     * @param[in]  type               The type
     *
     * @return     True if visible, False otherwise.
     */
    VisibilityType IsVisible(
            TransactionContext *const current_txn,
            const TuplePointer &ptr,
            const VisibilityIdType type = VisibilityIdType::READ_ID);

    /**
     * Test whether the current transaction is the owner of this tuple.
     *
     * @param      current_txn        The current transaction
     * @param[in]  tile_group_header  The tile group header
     * @param[in]  tuple_id           The tuple identifier
     *
     * @return     True if owner, False otherwise.
     */
    virtual bool IsOwner(
            TransactionContext *const current_txn,
            ConstTupleHeaderHardAccessor &accessor);

    /**
     * This method tests whether any other transaction has owned this version.
     *
     * @param      current_txn        The current transaction
     * @param[in]  tile_group_header  The tile group header
     * @param[in]  tuple_id           The tuple identifier
     *
     * @return     True if owned, False otherwise.
     */
    virtual bool IsOwned(
            TransactionContext *const current_txn,
            ConstTupleHeaderHardAccessor &accessor);

    /**
     * Test whether the current transaction has created this version of the tuple.
     *
     * @param      current_txn        The current transaction
     * @param[in]  tile_group_header  The tile group header
     * @param[in]  tuple_id           The tuple identifier
     *
     * @return     True if written, False otherwise.
     */
    virtual bool IsWritten(
            TransactionContext *const current_txn,
            ConstTupleHeaderHardAccessor &header_hard_accessor);

    /**
     * Test whether it can obtain ownership.
     *
     * @param      current_txn        The current transaction
     * @param[in]  tile_group_header  The tile group header
     * @param[in]  tuple_id           The tuple identifier
     *
     * @return     True if ownable, False otherwise.
     */
    virtual bool IsOwnable(
            TransactionContext *const current_txn,
            ConstTupleHeaderHardAccessor &header_hard_accessor);

    /**
     * Used to acquire ownership of a tuple for a transaction.
     *
     * @param      current_txn        The current transaction
     * @param[in]  tile_group_header  The tile group header
     * @param[in]  tuple_id           The tuple identifier
     *
     * @return     True if success, False otherwise.
     */
    virtual bool AcquireOwnership(
            TransactionContext *const current_txn,
            TupleHeaderHardAccessor &header_accessor);

    /**
     * Used by executor to yield ownership after the acquired it.
     *
     * @param      current_txn        The current transaction
     * @param[in]  tile_group_header  The tile group header
     * @param[in]  tuple_id           The tuple identifier
     */
    virtual void YieldOwnership(
            TransactionContext *const current_txn,
            TupleHeaderHardAccessor &header_accessor);

    /**
     * The index_entry_ptr is the address of the head node of the version chain,
     * which is directly pointed by the primary index.
     *
     * @param      current_txn        The current transaction
     * @param[in]  location         The location
     * @param      index_entry_ptr  The index entry pointer
     */
    virtual void PerformInsert(TransactionContext *const current_txn,
                               const TuplePointer &location,
                               TuplePointer *index_entry_ptr = nullptr);

    virtual bool PerformRead(TransactionContext *const current_txn,
                             const TuplePointer &location,
                             bool acquire_ownership = false);

    virtual void PerformUpdate(TransactionContext *const current_txn,
                               const TuplePointer &old_location,
                               const TuplePointer &new_location);

    virtual void PerformDelete(TransactionContext *const current_txn,
                               const TuplePointer &old_location,
                               const TuplePointer &new_location);

    virtual void PerformUpdate(TransactionContext *const current_txn,
                               const TuplePointer &location);

    virtual void PerformDelete(TransactionContext *const current_txn,
                               const TuplePointer &location);

    /**
     * @brief      Sets the transaction result.
     *
     * @param      current_txn  The current transaction
     * @param[in]  result       The result
     */
    void SetTransactionResult(TransactionContext *const current_txn, const ResultType result) {
        current_txn->SetResult(result);
    }

    TransactionContext *BeginTransaction(const IsolationLevelType type) {
        return BeginTransaction(0, type);
    }

    TransactionContext *BeginTransaction(const size_t thread_id = 0,
                                         const IsolationLevelType type = isolation_level_);

    txn_id_t GetCurrentTidCounter();

    /**
     * @brief      Ends a transaction.
     *
     * @param      current_txn  The current transaction
     */
    void EndTransaction(TransactionContext *current_txn);

    virtual ResultType CommitTransaction(TransactionContext *const current_txn);

    virtual ResultType AbortTransaction(TransactionContext *const current_txn);

    /**
     * @brief      Gets the isolation level.
     *
     * @return     The isolation level.
     */
    IsolationLevelType GetIsolationLevel() {
        return isolation_level_;
    }

    pid_t GetHeaderTableMetaPagePid() const {
        return hard_header_meta_page_pid;
    }

    uint64_t NextRowId();

    uint64_t GetCurrentRowId() const;

    uint64_t GetCurrentTid() const;

    void SetCurrentRowId(uint64_t rid);

    void SetCurrentTid(uint64_t tid);

    /**
     * @brief      Gets the last reader commit identifier.
     *
     * In timestamp ordering, the last_reader_cid records the timestamp of the last
     * transaction that reads the tuple.
     *
     * @param[in]  tile_group_header  The tile group header
     * @param[in]  tuple_id           The tuple identifier
     *
     * @return     The last reader commit identifier.
     */
    cid_t GetLastReaderCommitId(ConstTupleHeaderHardAccessor &accessor);

    /**
     * @brief      Sets the last reader commit identifier.
     *
     * @param[in]  tile_group_header  The tile group header
     * @param[in]  tuple_id           The tuple identifier
     * @param[in]  current_cid        The current cid
     * @param[in]  is_owner           Indicates if owner
     *
     * @return     True if success, False otherwise
     */
    bool SetLastReaderCommitId(TupleHeaderHardAccessor &accessor,
                               const cid_t &current_cid,
                               const bool is_owner);

private:
    /**
     * Initialize reserved area of a tuple.
     *
     * @param[in]  tile_group_header  The tile group header
     * @param[in]  tuple_id           The tuple identifier
     */
    void InitTupleReserved(TupleHeaderHardAccessor &accessor);

    static IsolationLevelType isolation_level_;
    ConcurrentBufferManager *buf_mgr_ = nullptr;
    //soft_header_table_t soft_header_table;
    PartitionedHeapTable<rid_t, TupleHeaderHard, 128> hard_header_table;
    pid_t hard_header_meta_page_pid = kInvalidPID;
    bool inited = false;
};



class BaseDataTable {
public:
    BaseDataTable() {}
    virtual void CollectPurgablePages(std::unordered_set<pid_t> &purge_set, pid_t min_active_tid,
                                      ConcurrentBufferManager *buf_mgr) = 0;
    virtual void PurgePages(const std::unordered_set<pid_t> &purge_set) = 0;
};

template<class Key, class T>
class DataTable : public BaseDataTable {
public:
    DataTable(ClusteredIndex<Key, T> &pindex, PartitionedHeapTable<Key, T> &version_table) : pindex(pindex),
                                                                                             version_table(
                                                                                                     version_table) {}

    ClusteredIndex<Key, T> &GetPrimaryIndex() { return pindex; }

    PartitionedHeapTable<Key, T> &GetVersionTable() { return version_table; }

    void CollectPurgablePages(std::unordered_set<pid_t> &purge_set, pid_t min_active_tid,
                              ConcurrentBufferManager *buf_mgr) override  {
        pid_t prev_page_id = kInvalidPID;
        auto transaction_manager = MVTOTransactionManager::GetInstance(buf_mgr);
        bool has_non_purgable_tuples = false;
        version_table.Scan([&](TuplePointer ptr, const T &tuple, bool & skip_this_page) -> bool {
            auto hard_header_ptr = tuple.GetHeaderPointer();
            if (ptr.pid != prev_page_id) { // New Page
                if (has_non_purgable_tuples == false && prev_page_id != kInvalidPID) {
                    purge_set.insert(prev_page_id);
                }
                has_non_purgable_tuples = false;
                prev_page_id = ptr.pid;
            }

            has_non_purgable_tuples |= !transaction_manager->IsTupleHeaderHardPurgable(hard_header_ptr, min_active_tid);
            if (has_non_purgable_tuples) {
                skip_this_page = true;
                has_non_purgable_tuples = false;
                prev_page_id = ptr.pid;
            }
            return false;
        });
    }

    void PurgePages(const std::unordered_set<pid_t> &purge_set) override {
        version_table.PurgePages(purge_set);
    }

private:
    ClusteredIndex<Key, T> &pindex;
    PartitionedHeapTable<Key, T> &version_table;
};

}

#endif //SPITFIRE_TXN_H
