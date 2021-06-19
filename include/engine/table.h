//
// Created by zxjcarrot on 2020-02-03.
//

#ifndef SPITFIRE_TABLE_H
#define SPITFIRE_TABLE_H

#include "buf/buf_mgr.h"
#include "engine/btreeolc.h"

namespace spitfire {

struct TuplePointer {
    pid_t pid;
    uint32_t off;

    bool IsNull() const {
        return (pid == kInvalidPID && off == std::numeric_limits<uint32_t>::max());
    }

    bool operator==(const TuplePointer &rhs) const {
        return pid == rhs.pid && off == rhs.off;
    }

    bool operator!=(const TuplePointer &rhs) const {
        return !operator==(rhs);
    }
};


static const TuplePointer kInvalidTuplePointer = {kInvalidPID, std::numeric_limits<uint32_t>::max()};


template<class Key,
        class T>
class ClusteredIndex {
public:
    ClusteredIndex(ConcurrentBufferManager *mgr) : mgr(mgr), btree(mgr) {}

    Status Init(pid_t root_page_pid) {
        return btree.Init(root_page_pid);
    }

    bool Lookup(const Key &k, T &tuple) {
        return btree.Lookup(k, tuple);
    }

    bool Lookup(const Key &k, std::function<BTreeOPResult (const T &)> processor) {
        return btree.Lookup(k, processor);
    }

    pid_t GetRootPid() {
        return btree.GetRootPageId();
    }

    BTreeOPResult
    Insert(const T &tuple, std::function<bool(const void *)> predicate = [](const void *) { return false; },
           bool upsert = true) {
        return btree.Insert(tuple.Key(), tuple, predicate, upsert);
    }

    BTreeOPResult
    Update(const T &tuple, std::function<bool(const void *)> predicate = [](const void *) { return false; }) {
        return Insert(tuple.Key(), tuple, predicate);
    }

    void Delete(const Key &k) {
        btree.Erase(k);
    }

    void Scan(const Key &start_key, std::function<BTreeOPResult(const T &tuple)> scan_processor) {
        btree.Scan(start_key, scan_processor);
    }

    void ScanForUpdate(const Key &start_key, std::function<void(T &, bool &, bool &)> scan_processor) {
        btree.ScanForUpdate(start_key, scan_processor);
    }

    BTreeOPResult LookupForUpdate(const Key &start_key, std::function<bool(T &tuple)> updater) {
        return btree.LookupForUpdate(start_key, updater);
    }

    typename BTree<Key, T>::BTreeStats GetStats() {
        return btree.GetStats();
    }

private:
    ConcurrentBufferManager *mgr;
    BTree<Key, T> btree;
};

template<class Key,
         class T>
class HeapTable {
public:
    struct HeapTablePage {
        T tuples[(kPageSize - sizeof(uint16_t) - sizeof(pid_t)) / sizeof(T)];
        pid_t next_page_pid;
        uint16_t num_tuples_in_page;
        char ___padding___[kPageSize - sizeof(tuples) - sizeof(num_tuples_in_page) - sizeof(next_page_pid)];
        constexpr static HeapTablePage *dummy = static_cast<HeapTablePage *>(nullptr);
        constexpr static size_t kMaxNumTuples = sizeof(tuples) / sizeof(T);

        static pid_t *RefNextPagePid(ConcurrentBufferManager::PageAccessor &accessor) {
            auto offset = static_cast<uint32_t>(reinterpret_cast<char *>(&dummy->next_page_pid) -
                                                reinterpret_cast<char *>(dummy));
            auto size = sizeof(pid_t);
            auto slice = accessor.PrepareForWrite(offset, size);
            return reinterpret_cast<pid_t *>(slice.data());
        }

        static pid_t GetNextPagePid(ConcurrentBufferManager::PageAccessor &accessor) {
            auto offset = static_cast<uint32_t>(reinterpret_cast<char *>(&dummy->next_page_pid) -
                                                reinterpret_cast<char *>(dummy));
            auto size = sizeof(pid_t);
            auto slice = accessor.PrepareForRead(offset, size);
            return *reinterpret_cast<pid_t *>(slice.data());
        }

        static uint16_t *RefNumTuples(ConcurrentBufferManager::PageAccessor &accessor) {
            auto offset = static_cast<uint32_t>(reinterpret_cast<char *>(&dummy->num_tuples_in_page) -
                                                reinterpret_cast<char *>(dummy));
            auto size = sizeof(uint16_t);
            auto slice = accessor.PrepareForWrite(offset, size);
            return reinterpret_cast<uint16_t *>(slice.data());
        }

        static uint16_t GetNumTuples(ConcurrentBufferManager::PageAccessor &accessor) {
            auto offset = static_cast<uint32_t>(reinterpret_cast<char *>(&dummy->num_tuples_in_page) -
                                                reinterpret_cast<char *>(dummy));
            auto size = sizeof(uint16_t);
            auto slice = accessor.PrepareForRead(offset, size);
            return *reinterpret_cast<uint16_t *>(slice.data());
        }

        static const T *GetTuples(ConcurrentBufferManager::PageAccessor &accessor, int pos, int num) {
            assert(pos + num <= kMaxNumTuples);
            auto offset = static_cast<uint32_t>(reinterpret_cast<char *>(&dummy->tuples[pos]) -
                                                reinterpret_cast<char *>(dummy));
            auto size = sizeof(T) * num;
            auto slice = accessor.PrepareForRead(offset, size);
            return reinterpret_cast<T *>(slice.data());
        }

        static T GetTuple(ConcurrentBufferManager::PageAccessor &accessor, int pos) {
            return *GetTuples(accessor, pos, 1);
        }

        static uint32_t ComputeTuplePosition(int pos) {
            return static_cast<uint32_t>(reinterpret_cast<char *>(&dummy->tuples[pos]) -
                                         reinterpret_cast<char *>(dummy));
        }

        static T *RefTuples(ConcurrentBufferManager::PageAccessor &accessor, int pos, int num) {
            assert(pos + num <= kMaxNumTuples);
            auto offset = static_cast<uint32_t>(reinterpret_cast<char *>(&dummy->tuples[pos]) -
                                                reinterpret_cast<char *>(dummy));
            auto size = sizeof(T) * num;
            auto slice = accessor.PrepareForWrite(offset, size);
            return reinterpret_cast<T *>(slice.data());
        }

        static T *RefTuple(ConcurrentBufferManager::PageAccessor &accessor, int pos) {
            return RefTuples(accessor, pos, 1);
        }
    };

    static_assert(sizeof(HeapTablePage) == kPageSize, "sizeof(HeapTablePage) != kPageSize");

    HeapTable() : mgr(nullptr), meta_page_pid(kInvalidPID) {}

    Status Init(const pid_t meta_page_pid, ConcurrentBufferManager *mgr, bool new_table) {
        this->meta_page_pid = meta_page_pid;
        this->mgr = mgr;

        PageDesc *meta_page_desc;
        if (this->meta_page_pid == kInvalidPID) {
            Status s = mgr->NewPage(this->meta_page_pid);
            if (!s.ok())
                return s;
        }
        Status s = mgr->Get(this->meta_page_pid, meta_page_desc, ConcurrentBufferManager::PageOPIntent::INTENT_READ);

        if (!s.ok())
            return s;
        DeferCode c([meta_page_desc, mgr]() { mgr->Put(meta_page_desc); });

        if (new_table) {
            pid_t head_page_pid;
            s = mgr->NewPage(head_page_pid);
            if (!s.ok())
                return s;
            PageDesc *new_page_desc;
            s = mgr->Get(head_page_pid, new_page_desc, ConcurrentBufferManager::PageOPIntent::INTENT_WRITE);
            if (!s.ok())
                return s;
            // Initialize a head page
            InitializeHeapTablePage(new_page_desc, kInvalidPID);

            // Link the meta page with the head page
            meta_page_desc->LatchExclusive();
            InitializeHeapTablePage(meta_page_desc, head_page_pid);
            meta_page_desc->UnlatchExclusive();
            mgr->Put(new_page_desc);
        }
        return Status::OK();
    }


    bool Lookup(const Key &k, T &tuple) {
        bool found = false;
        Scan([&, this](TuplePointer pointer, const T &t) -> bool {
            if (tuple.Key() == k) {
                tuple = t;
                found = true;
                return true;
            }
            return false;
        });
        return found;
    }


    void Insert(const T &tuple) {
        TuplePointer dummy;
        Insert(tuple, dummy);
    }

    void CreateNewHeadPage() {
        PageDesc *meta_page_desc = nullptr;
        Status s = mgr->Get(meta_page_pid, meta_page_desc, ConcurrentBufferManager::PageOPIntent::INTENT_READ);
        assert(s.ok());
        meta_page_desc->LatchExclusive();
        auto accessor = mgr->GetPageAccessorFromDesc(meta_page_desc);
        HeapTablePage *meta_page = nullptr;
        auto head_page_pid = meta_page->GetNextPagePid(accessor);
        if (head_page_pid == kInvalidPID) {
            pid_t head_page_pid;
            s = mgr->NewPage(head_page_pid);
            assert(s.ok());
            PageDesc *new_page_desc;
            s = mgr->Get(head_page_pid, new_page_desc, ConcurrentBufferManager::PageOPIntent::INTENT_WRITE);
            assert(s.ok());
            // Initialize the head page
            InitializeHeapTablePage(new_page_desc, kInvalidPID);

            // Link the meta page with the head page
            InitializeHeapTablePage(meta_page_desc, head_page_pid);
            mgr->Put(new_page_desc);
        }
        meta_page_desc->UnlatchExclusive();
        mgr->Put(meta_page_desc);
    }

    void Insert(const T &tuple, TuplePointer &result_holder) {
        start:
        {
            auto head_page_pid = GetHeadPagePid();
            if (head_page_pid == kInvalidPID) {
                // Empty page list.
                // Try to create a head page by grabing the latch on the meta page.
                CreateNewHeadPage();
                goto start;
            }
            PageDesc *pd = nullptr;
            Status s = mgr->Get(head_page_pid, pd);
            if (!s.ok()) {
                goto start;
            }
            pd->LatchExclusive();
            auto accessor = mgr->GetPageAccessorFromDesc(pd);
            HeapTablePage *hp = nullptr;
            int n_tuples = hp->GetNumTuples(accessor);
            if (n_tuples >= hp->kMaxNumTuples) {
                pd->UnlatchExclusive();
                mgr->Put(pd);
                PageDesc *meta_page_desc = nullptr;
                Status s = mgr->Get(meta_page_pid, meta_page_desc, ConcurrentBufferManager::PageOPIntent::INTENT_WRITE);
                assert(s.ok());
                meta_page_desc->LatchExclusive();
                auto meta_page_accessor = mgr->GetPageAccessorFromDesc(meta_page_desc);
                HeapTablePage *meta_page = nullptr;
                auto latest_head_page_pid = meta_page->GetNextPagePid(meta_page_accessor);
                if (latest_head_page_pid != head_page_pid) {
                    meta_page_desc->UnlatchExclusive();
                    mgr->Put(meta_page_desc);
                    // New head page has been installed, retry the insertion.
                    goto retry;
                }
                // Create a new head page
                pid_t new_head_page_pid = kInvalidPID;
                PageDesc *new_head_page_desc = nullptr;
                s = mgr->NewPage(new_head_page_pid);
                assert(s.ok());
                s = mgr->Get(new_head_page_pid, new_head_page_desc,
                             ConcurrentBufferManager::PageOPIntent::INTENT_WRITE);
                assert(s.ok());
                InitializeHeapTablePage(new_head_page_desc, head_page_pid);
                mgr->Put(new_head_page_desc);
                *meta_page->RefNextPagePid(meta_page_accessor) = new_head_page_pid;
                meta_page_desc->UnlatchExclusive();
                mgr->Put(meta_page_desc);
                goto retry; // Installed the new head page, retry the insertion.
            } else {
                result_holder = {head_page_pid, hp->ComputeTuplePosition(n_tuples)};
                *hp->RefTuple(accessor, n_tuples) = tuple;
                (*hp->RefNumTuples(accessor))++;
            }
            pd->UnlatchExclusive();
            mgr->Put(pd);
            return;
        }

        retry:
        Insert(tuple, result_holder);
    }

    void Update(const T &tuple) {
        auto target_key = tuple.Key();
        std::vector<TuplePointer> ptrs;
        Scan([&, this](TuplePointer pointer, const T &t) -> bool {
            if (t.Key() == target_key) {
                ptrs.push_back(pointer);
            }
            return false;
        });

        for (auto ptr : ptrs) {
            PageDesc *pd = nullptr;
            Status s = mgr->Get(ptr.pid, pd, ConcurrentBufferManager::PageOPIntent::INTENT_WRITE);
            assert(s.ok());
            pd->LatchExclusive();
            auto accessor = mgr->GetPageAccessorFromDesc(pd);
            HeapTablePage *hp = nullptr;
            *hp->RefTuple(accessor, ptr.off) = tuple;
            pd->UnlatchExclusive();
            mgr->Put(pd);
        }
    }

    void Delete(const Key &k) {
        throw std::runtime_error("Not implemented yet");
    }

    void Scan(const Key &start_key, std::function<bool(TuplePointer pointer, const T &tuple)> scan_processor) {
        Scan([&, this](TuplePointer pointer, const T &tuple) -> bool {
            if (tuple.Key() >= start_key) {
                return scan_processor(pointer, tuple);
            }
            return false;
        });
    }

    void Scan(std::function<bool(TuplePointer pointer, const T &tuple)> scan_processor) {
        // Walk the list of pages using classic latch-coupling protocol:
        //  Release the latch of the previous page only
        //  after the current page is properly latched.

        auto prev_page_pid = GetMetaPagePid();
        pid_t cur_page_pid = kInvalidPID;
        HeapTablePage *prev_page = nullptr;
        HeapTablePage *cur_page = nullptr;
        PageDesc *prev_page_desc = nullptr;
        PageDesc *cur_page_desc = nullptr;
        Status s = mgr->Get(prev_page_pid, prev_page_desc, ConcurrentBufferManager::PageOPIntent::INTENT_READ);
        assert(s.ok());
        prev_page_desc->LatchShared();
        {
            auto prev_page_accessor = mgr->GetPageAccessorFromDesc(prev_page_desc);
            cur_page_pid = prev_page->GetNextPagePid(prev_page_accessor);
        }

        bool early_exit = false;

        while (cur_page_pid != kInvalidPID) {
            s = mgr->Get(cur_page_pid, cur_page_desc, ConcurrentBufferManager::PageOPIntent::INTENT_READ);
            assert(s.ok());
            cur_page_desc->LatchShared();
            auto cur_page_accessor = mgr->GetPageAccessorFromDesc(cur_page_desc);
            pid_t next_page_pid = cur_page->GetNextPagePid(cur_page_accessor);

            // Release the latch of the previous page
            // only after the current page is latched.
            prev_page_desc->UnlatchShared();
            mgr->Put(prev_page_desc);

            HeapTablePage *hp = nullptr;
            int n_tuples = hp->GetNumTuples(cur_page_accessor);
            const T *tuples = nullptr;
            if (n_tuples) {
                tuples = hp->GetTuples(cur_page_accessor, 0, n_tuples);
            } else {
                n_tuples = 0;
            }

            for (int i = 0; i < n_tuples && early_exit == false; ++i) {
                early_exit = scan_processor(TuplePointer{cur_page_pid, hp->ComputeTuplePosition(i)}, tuples[i]);
            }

            prev_page_desc = cur_page_desc;
            cur_page_pid = next_page_pid;
            if (early_exit == true) {
                break;
            }
        }

        prev_page_desc->UnlatchShared();
        mgr->Put(prev_page_desc);
    }



    void Scan(std::function<bool(TuplePointer pointer, const T &tuple, bool & skip_this_page)> scan_processor) {
        // Walk the list of pages using classic latch-coupling protocol:
        //  Release the latch of the previous page only
        //  after the current page is properly latched.

        auto prev_page_pid = GetMetaPagePid();
        pid_t cur_page_pid = kInvalidPID;
        HeapTablePage *prev_page = nullptr;
        HeapTablePage *cur_page = nullptr;
        PageDesc *prev_page_desc = nullptr;
        PageDesc *cur_page_desc = nullptr;
        Status s = mgr->Get(prev_page_pid, prev_page_desc, ConcurrentBufferManager::PageOPIntent::INTENT_READ);
        assert(s.ok());
        prev_page_desc->LatchShared();
        {
            auto prev_page_accessor = mgr->GetPageAccessorFromDesc(prev_page_desc);
            cur_page_pid = prev_page->GetNextPagePid(prev_page_accessor);
        }

        bool early_exit = false;

        while (cur_page_pid != kInvalidPID) {
            s = mgr->Get(cur_page_pid, cur_page_desc, ConcurrentBufferManager::PageOPIntent::INTENT_READ);
            assert(s.ok());
            cur_page_desc->LatchShared();
            auto cur_page_accessor = mgr->GetPageAccessorFromDesc(cur_page_desc);
            pid_t next_page_pid = cur_page->GetNextPagePid(cur_page_accessor);

            // Release the latch of the previous page
            // only after the current page is latched.
            prev_page_desc->UnlatchShared();
            mgr->Put(prev_page_desc);

            HeapTablePage *hp = nullptr;
            int n_tuples = hp->GetNumTuples(cur_page_accessor);
            const T *tuples = nullptr;
            if (n_tuples) {
                tuples = hp->GetTuples(cur_page_accessor, 0, n_tuples);
            } else {
                n_tuples = 0;
            }

            for (int i = 0; i < n_tuples && early_exit == false; ++i) {
                bool skip_this_page = false;
                early_exit = scan_processor(TuplePointer{cur_page_pid, hp->ComputeTuplePosition(i)}, tuples[i], skip_this_page);
                if (skip_this_page == true) {
                    break;
                }
            }

            prev_page_desc = cur_page_desc;
            cur_page_pid = next_page_pid;
            if (early_exit == true) {
                break;
            }
        }

        prev_page_desc->UnlatchShared();
        mgr->Put(prev_page_desc);
    }

    pid_t GetHeadPagePid() {
        PageDesc *meta_page_desc = nullptr;
        Status s = mgr->Get(meta_page_pid, meta_page_desc, ConcurrentBufferManager::PageOPIntent::INTENT_READ);
        assert(s.ok());
        meta_page_desc->LatchShared();
        auto accessor = mgr->GetPageAccessorFromDesc(meta_page_desc);
        HeapTablePage *meta_page = nullptr;
        auto head_page_pid = meta_page->GetNextPagePid(accessor);
        meta_page_desc->UnlatchShared();
        mgr->Put(meta_page_desc);
        return head_page_pid;
    }


    pid_t GetMetaPagePid() {
        return meta_page_pid;
    }

    void PurgePages(const std::unordered_set<pid_t> & purge_set) {
        // Walk the list of pages using classic latch-coupling protocol:
        //  Release the latch of the previous page only
        //  after the current page is properly latched.

        auto prev_page_pid = GetMetaPagePid();
        pid_t cur_page_pid = kInvalidPID;
        HeapTablePage *prev_page = nullptr;
        HeapTablePage *cur_page = nullptr;
        PageDesc *prev_page_desc = nullptr;
        PageDesc *cur_page_desc = nullptr;
        Status s = mgr->Get(prev_page_pid, prev_page_desc, ConcurrentBufferManager::PageOPIntent::INTENT_READ);
        assert(s.ok());
        prev_page_desc->LatchExclusive();
        {
            auto prev_page_accessor = mgr->GetPageAccessorFromDesc(prev_page_desc);
            cur_page_pid = prev_page->GetNextPagePid(prev_page_accessor);
        }

        // Unlink the pages from the list so that future readers won't access these pages.
        std::unordered_set<pid_t> unlinked_pages;
        while (cur_page_pid != kInvalidPID) {
            s = mgr->Get(cur_page_pid, cur_page_desc, ConcurrentBufferManager::PageOPIntent::INTENT_READ);
            assert(s.ok());
            cur_page_desc->LatchExclusive();
            auto cur_page_accessor = mgr->GetPageAccessorFromDesc(cur_page_desc);
            pid_t next_page_pid = cur_page->GetNextPagePid(cur_page_accessor);
            if (purge_set.find(cur_page_pid) != purge_set.end()) {
                auto prev_page_accessor = mgr->GetPageAccessorFromDesc(prev_page_desc);
                *prev_page->RefNextPagePid(prev_page_accessor) = next_page_pid;
                unlinked_pages.insert(cur_page_pid);
                cur_page_desc->UnlatchExclusive();
                mgr->Put(cur_page_desc);
            } else {
                prev_page_desc->UnlatchExclusive();
                mgr->Put(prev_page_desc);
                prev_page_desc = cur_page_desc;
                prev_page = cur_page;
                prev_page_pid = cur_page_pid;
            }
            cur_page_pid = next_page_pid;
        }

        prev_page_desc->UnlatchExclusive();
        mgr->Put(prev_page_desc);

        mgr->EvictPurgablePages(unlinked_pages);
        // Do final purging on the unlinked pages
        for (auto pid : unlinked_pages) {
            mgr->Flush(pid, false, false);
            mgr->Flush(pid, false, false);
        }

        for (auto pid : unlinked_pages) {
            mgr->FreePage(pid);
        }
    }

private:

    void InitializeHeapTablePage(PageDesc *new_page_desc, pid_t next_page_pid) {
        auto page_accessor = mgr->GetPageAccessorFromDesc(new_page_desc);
        HeapTablePage *heap_page = nullptr;
        *heap_page->RefNextPagePid(page_accessor) = next_page_pid;
        *heap_page->RefNumTuples(page_accessor) = 0;
        page_accessor.FinishAccess();
    }

    ConcurrentBufferManager *mgr;
    pid_t meta_page_pid;
};

template<class Key,
         class T,
         int kNumParts = 16>
class PartitionedHeapTable{
public:
    struct PartitionedHeapTablePage {
        lsn_t lsn;
        pid_t part_ptrs[(kPageSize - sizeof(uint16_t) - sizeof(lsn_t)) / sizeof(pid_t)];
        uint16_t num_parts;
        char ___padding___[kPageSize - sizeof(part_ptrs) - sizeof(lsn_t) - sizeof(num_parts)];
        constexpr static PartitionedHeapTablePage *dummy = static_cast<PartitionedHeapTablePage *>(nullptr);
        constexpr static size_t kMaxNumTuples = sizeof(part_ptrs) / sizeof(pid_t);

        static uint16_t *RefNumParts(ConcurrentBufferManager::PageAccessor &accessor) {
            auto offset = static_cast<uint32_t>(reinterpret_cast<char *>(&dummy->num_parts) -
                                                reinterpret_cast<char *>(dummy));
            auto size = sizeof(uint16_t);
            auto slice = accessor.PrepareForWrite(offset, size);
            return reinterpret_cast<uint16_t *>(slice.data());
        }

        static uint16_t GetNumParts(ConcurrentBufferManager::PageAccessor &accessor) {
            auto offset = static_cast<uint32_t>(reinterpret_cast<char *>(&dummy->num_parts) -
                                                reinterpret_cast<char *>(dummy));
            auto size = sizeof(uint16_t);
            auto slice = accessor.PrepareForRead(offset, size);
            return *reinterpret_cast<uint16_t *>(slice.data());
        }

        static const pid_t *GetPartPtrs(ConcurrentBufferManager::PageAccessor &accessor, int pos, int num) {
            assert(pos + num <= kMaxNumTuples);
            auto offset = static_cast<uint32_t>(reinterpret_cast<char *>(&dummy->part_ptrs[pos]) -
                                                reinterpret_cast<char *>(dummy));
            auto size = sizeof(pid_t) * num;
            auto slice = accessor.PrepareForRead(offset, size);
            return reinterpret_cast<pid_t *>(slice.data());
        }

        static pid_t GetPartPtr(ConcurrentBufferManager::PageAccessor &accessor, int pos) {
            return *GetPartPtrs(accessor, pos, 1);
        }

        static pid_t *RefPartPtrs(ConcurrentBufferManager::PageAccessor &accessor, int pos, int num) {
            assert(pos + num <= kMaxNumTuples);
            auto offset = static_cast<uint32_t>(reinterpret_cast<char *>(&dummy->part_ptrs[pos]) -
                                                reinterpret_cast<char *>(dummy));
            auto size = sizeof(pid_t) * num;
            auto slice = accessor.PrepareForWrite(offset, size);
            return reinterpret_cast<pid_t *>(slice.data());
        }

        static pid_t *RefPartPtr(ConcurrentBufferManager::PageAccessor &accessor, int pos) {
            return RefPartPtrs(accessor, pos, 1);
        }
    };

    static_assert(sizeof(PartitionedHeapTablePage) == kPageSize, "sizeof(PartitionedHeapTablePage) != kPageSize");

    PartitionedHeapTable(int num_parts = kNumParts) : mgr(nullptr), num_parts(num_parts), parts(nullptr) {}

    ~PartitionedHeapTable() {
        delete[] parts;
    }

    Status Init(const pid_t meta_page_pid, ConcurrentBufferManager *mgr, bool new_table) {
        this->meta_page_pid = meta_page_pid;
        this->mgr = mgr;

        PageDesc *meta_page_desc;
        if (this->meta_page_pid == kInvalidPID) {
            Status s = mgr->NewPage(this->meta_page_pid);
            if (!s.ok())
                return s;
        }
        Status s = mgr->Get(this->meta_page_pid, meta_page_desc, ConcurrentBufferManager::PageOPIntent::INTENT_READ);
        if (!s.ok())
            return s;
        DeferCode c([meta_page_desc, mgr]() { mgr->Put(meta_page_desc); });

        parts = new HeapTable<Key, T>[num_parts];
        if (new_table) {
            PartitionedHeapTablePage * meta_page = nullptr;
            for (int i = 0; i < num_parts; ++i) {
                pid_t part_meta_page_pid = kInvalidPID;
                s = parts[i].Init(part_meta_page_pid, mgr, new_table);
                if (!s.ok())
                    return s;
                part_meta_page_pid = parts[i].GetMetaPagePid();
                assert(part_meta_page_pid != kInvalidPID);
                auto accessor = mgr->GetPageAccessorFromDesc(meta_page_desc);
                meta_page_desc->LatchExclusive();
                *meta_page->RefPartPtr(accessor, i) = part_meta_page_pid;
                *meta_page->RefNumParts(accessor) = num_parts;
                meta_page_desc->UnlatchExclusive();
            }
        } else {
            PartitionedHeapTablePage * meta_page = nullptr;
            auto accessor = mgr->GetPageAccessorFromDesc(meta_page_desc);
            num_parts = meta_page->GetNumParts(accessor);
            for (int i = 0; i < num_parts; ++i) {
                pid_t part_meta_page_pid = meta_page->GetPartPtr(accessor, i);
                assert(part_meta_page_pid != kInvalidPID);
                s = parts[i].Init(part_meta_page_pid, mgr, new_table);
                if (!s.ok())
                    return s;
            }
        }
        return Status::OK();
    }

    bool Lookup(const Key &k, T &tuple) {
        auto target_key = tuple.Key();
        return parts[GetPartitionNum(target_key)].Lookup(k, tuple);
    }

    void Insert(const T &tuple) {
        TuplePointer dummy;
        Insert(tuple, dummy);
    }

    void Insert(const T &tuple, TuplePointer &result_holder) {
        auto target_key = tuple.Key();
        auto part_idx = GetPartitionNum(target_key);
        parts[part_idx].Insert(tuple, result_holder);
    }

    inline int GetPartitionNum(const Key & key) {
        return std::hash<Key>()(key) % num_parts;
    }

    void Update(const T &tuple) {
        auto target_key = tuple.Key();
        parts[GetPartitionNum(target_key)].Update(tuple);
    }

    void Delete(const Key &k) {
        throw std::runtime_error("Not implemented yet");
    }

    void Scan(const Key &start_key, std::function<bool(TuplePointer pointer, const T &tuple)> scan_processor) {
        Scan([&, this](TuplePointer pointer, const T &tuple) -> bool {
            if (tuple.Key() >= start_key) {
                return scan_processor(pointer, tuple);
            }
            return false;
        });
    }

    void Scan(std::function<bool(TuplePointer pointer, const T &tuple)> scan_processor) {
        for (int i = 0; i < num_parts; ++i) {
            parts[i].Scan(scan_processor);
        }
    }

    void Scan(std::function<bool(TuplePointer pointer, const T &tuple, bool & skip_this_page)> scan_processor) {
        for (int i = 0; i < num_parts; ++i) {
            parts[i].Scan(scan_processor);
        }
    }


    pid_t GetMetaPagePid() {
        return meta_page_pid;
    }

    void PurgePages(const std::unordered_set<pid_t> &purge_set) {
        for (int i = 0; i < num_parts; ++i) {
            parts[i].PurgePages(purge_set);
        }
    }
private:
    ConcurrentBufferManager *mgr;
    pid_t meta_page_pid;
    int num_parts;
    HeapTable<Key, T> *parts;
};

}
#endif //SPITFIRE_TABLE_H
