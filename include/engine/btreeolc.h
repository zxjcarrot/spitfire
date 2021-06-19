//
// Created by zxjcarrot on 2020-01-24.
//
#ifndef SPITFIRE_BTREEOLC_H
#define SPITFIRE_BTREEOLC_H

#include "buf/buf_mgr.h"

namespace spitfire {

// Thread local container for storing page descriptors to be Put.
// This reduces the number of vector allocations.
//static thread_local std::vector<PageDesc*> global_descs;

enum class BTreeOPResult {
    FAILED,
    UPDATED,
    SUCCESS,
    NOT_FOUND,
    PRED_TRUE,
    PRED_FALSE,
    RETRY_SCAN,
    END_SCAN,
    CONTINUE_SCAN
};
thread_local extern uint64_t current_txn_id;

template<class Key,
        class Value>
class BTree {
public:
#define InnerNode 0
#define LeafNode  1

    constexpr static float kMergeThreshold = 0.34;
    typedef ConcurrentBufferManager::PageAccessor PageAccessor;

    struct NodeBase {
        uint64_t lsn;
        uint16_t type;
        uint16_t count;

        static const NodeBase GetNodeBase(PageAccessor &accessor) {
            Slice s = accessor.PrepareForRead(0, sizeof(NodeBase));
            return *reinterpret_cast<NodeBase *>(s.data());
        }

        static NodeBase *RefNodeBase(PageAccessor &accessor) {
            Slice s = accessor.PrepareForWrite(0, sizeof(NodeBase));
            return reinterpret_cast<NodeBase *>(s.data());
        }

    };

    struct BTreeInnerNode : public NodeBase {
        constexpr static uint16_t kTypeMarker = InnerNode;
        constexpr static BTreeInnerNode *dummy = static_cast<BTreeInnerNode *>(nullptr);
        typedef std::pair<Key, pid_t> KeyValueType;
        constexpr static uint64_t kMaxEntries =
                (kPageSize - sizeof(NodeBase)) / sizeof(KeyValueType);
        KeyValueType data[kMaxEntries];
        char __padding__[kPageSize - sizeof(NodeBase) - sizeof(data)];

        BTreeInnerNode() {
            this->count = 0;
            this->type = kTypeMarker;
        }

        bool isFull(const NodeBase &this_node_base) {
            return this_node_base.count >= (kMaxEntries - 1) * 0.25;
        };

        static KeyValueType *RefKeyValues(int pos, int len, ConcurrentBufferManager::PageAccessor &accessor) {
            assert(pos + len <= kMaxEntries);
            auto offset = static_cast<uint32_t>(reinterpret_cast<char *>(&dummy->data[pos]) -
                                                reinterpret_cast<char *>(dummy));
            size_t size = sizeof(KeyValueType) * len;
            Slice s = accessor.PrepareForWrite(offset, size);
            return reinterpret_cast<KeyValueType *>(s.data());
        }

        static KeyValueType *RefKeyValue(int pos, ConcurrentBufferManager::PageAccessor &accessor) {
            return RefKeyValues(pos, 1, accessor);
        }

        static const KeyValueType *GetKeyValues(int pos, int len, ConcurrentBufferManager::PageAccessor &accessor) {
            assert(pos + len <= kMaxEntries);
            auto offset = static_cast<uint32_t>(reinterpret_cast<char *>(&dummy->data[pos]) -
                                                reinterpret_cast<char *>(dummy));
            size_t size = sizeof(KeyValueType) * len;
            Slice s = accessor.PrepareForRead(offset, size);
            return reinterpret_cast<KeyValueType *>(s.data());
        }

        static const KeyValueType GetKeyValue(int pos, ConcurrentBufferManager::PageAccessor &accessor) {
            return *GetKeyValues(pos, 1, accessor);
        }

        static const Key GetKey(int pos, PageAccessor &accessor) {
            assert(pos < kMaxEntries);
            auto offset = static_cast<uint32_t>(reinterpret_cast<char *>(&dummy->data[pos]) -
                                                reinterpret_cast<char *>(dummy));
            size_t size = sizeof(Key);
            Slice s = accessor.PrepareForRead(offset, size);
            return *reinterpret_cast<Key *>(s.data());
        }


        static unsigned lowerBound(Key k, ConcurrentBufferManager::PageAccessor &accessor, const NodeBase &node_base) {
            unsigned lower = 0;
            unsigned upper = node_base.count;
            do {
                unsigned mid = ((upper - lower) / 2) + lower;
                // This is the key at the pivot position
                const Key middle_key = GetKey(mid, accessor);
                if (k < middle_key) {
                    upper = mid;
                } else if (middle_key < k) {
                    lower = mid + 1;
                } else {
                    lower = mid;
                    break;
                }
            } while (lower < upper);
            accessor.FinishAccess();
            return lower;
        }

        static PageDesc *split(Key &sep, PageAccessor &accessor, ConcurrentBufferManager *mgr) {
            NodeBase this_node_base = NodeBase::GetNodeBase(accessor);
            pid_t new_page_pid;
            Status s = mgr->NewPage(new_page_pid);
            assert(s.ok());
            PageAccessor new_page_accessor;
            s = mgr->Get(new_page_pid, new_page_accessor, ConcurrentBufferManager::PageOPIntent::INTENT_WRITE);
            assert(s.ok());
            new_page_accessor.PrepareForWrite(0, kPageSize);
            auto new_page_desc = new_page_accessor.GetPageDesc();
            BTreeInnerNode *newInner = new(reinterpret_cast<char *>(new_page_desc->page)) BTreeInnerNode();
            newInner->count = this_node_base.count - (this_node_base.count / 2);
            int count = NodeBase::RefNodeBase(accessor)->count = this_node_base.count - newInner->count - 1;
            int len = newInner->count + 1 + 1;
            const KeyValueType *data_count = GetKeyValues(count, len, accessor);
            sep = data_count[0].first;
            memcpy(newInner->data, data_count + 1,
                   sizeof(KeyValueType) * (newInner->count + 1));
            accessor.FinishAccess();
            return new_page_desc;
        }

        static void insert(Key k, pid_t child, PageAccessor &accessor) {
            NodeBase this_node_base = NodeBase::GetNodeBase(accessor);
            assert(this_node_base.count < kMaxEntries - 1);
            unsigned pos = lowerBound(k, accessor, this_node_base);
            int len = this_node_base.count - pos + 1 + 1;
            KeyValueType *data_pos = RefKeyValues(pos, len, accessor);
            memmove(data_pos + 1, data_pos, sizeof(KeyValueType) * (this_node_base.count - pos + 1));
            data_pos[0] = std::make_pair(k, child);
            std::swap(data_pos[0].second, data_pos[1].second);
            NodeBase::RefNodeBase(accessor)->count++;
            accessor.FinishAccess();
        }

        static void erase(int pos, PageAccessor &accessor) {
            auto this_node_base = NodeBase::GetNodeBase(accessor);
            KeyValueType *data_pos = RefKeyValues(pos, this_node_base.count - pos + 1, accessor);
            memmove(data_pos, data_pos + 1, sizeof(KeyValueType) * (this_node_base.count - pos));
            NodeBase::RefNodeBase(accessor)->count--;
            accessor.FinishAccess();
        }

        static bool NeedMerge(const NodeBase &node_base) {
            return node_base.count / (kMaxEntries + 0.0) < kMergeThreshold;
        }

        static bool HasEnoughSpace(const NodeBase &node_base, int need) {
            return node_base.count + need + 1 < kMaxEntries;
        }

        // Copy the content from source node to the end of this page
        static void
        Merge(PageAccessor &this_node_accessor, Key this_subtree_max_key, PageAccessor &source_node_accessor) {
            char temp_page[kPageSize];
            auto source_desc = source_node_accessor.GetPageDesc();
            auto source_inner = reinterpret_cast<BTreeInnerNode *>(source_desc->page);
            auto source_inner_base = NodeBase::GetNodeBase(source_node_accessor);
            auto source_inner_kvs = source_inner->GetKeyValues(0, source_inner_base.count + 1, source_node_accessor);
            memcpy(temp_page, (const char *) source_inner_kvs, sizeof(KeyValueType) * (source_inner_base.count + 1));
            source_node_accessor.FinishAccess();
            auto this_node_base = NodeBase::GetNodeBase(this_node_accessor);
            assert(HasEnoughSpace(this_node_base, source_inner_base.count));
            auto this_desc = this_node_accessor.GetPageDesc();
            auto this_inner = reinterpret_cast<BTreeLeafNode *>(this_desc->page);
            auto this_inner_kvs_dest = this_inner->RefKeyValues(this_node_base.count + 1, source_inner_base.count + 1,
                                                                this_node_accessor);
            memcpy(this_inner_kvs_dest, temp_page, sizeof(KeyValueType) * (source_inner_base.count + 1));
            RefKeyValue(this_node_base.count, this_node_accessor)->first = this_subtree_max_key;
            NodeBase::RefNodeBase(this_node_accessor)->count += source_inner_base.count + 1;
            this_node_accessor.FinishAccess();
        }
    };

    struct BTreeLeafNode : NodeBase {
        typedef std::pair<Key, Value> KeyValueType;
        constexpr static uint16_t kTypeMarker = LeafNode;
        constexpr static uint64_t kMaxEntries =
                (kPageSize - sizeof(NodeBase) - sizeof(pid_t)) / (sizeof(KeyValueType));
        pid_t next;
        KeyValueType data[kMaxEntries];
        char __padding__[(kPageSize - sizeof(NodeBase) - sizeof(pid_t)) % sizeof(data)];

        BTreeLeafNode() {
            this->count = 0;
            this->type = kTypeMarker;
            this->next = kInvalidPID;
        }

        static pid_t GetNextPtr(PageAccessor &accessor) {
            BTreeLeafNode *dummy = static_cast<BTreeLeafNode *>(nullptr);
            auto offset = static_cast<uint32_t>(reinterpret_cast<char *>(&dummy->next) -
                                                reinterpret_cast<char *>(dummy));
            size_t size = sizeof(pid_t);
            Slice s = accessor.PrepareForRead(offset, size);
            return *reinterpret_cast<pid_t *>(s.data());
        }

        static pid_t *RefNextPtr(PageAccessor &accessor) {
            BTreeLeafNode *dummy = static_cast<BTreeLeafNode *>(nullptr);
            auto offset = static_cast<uint32_t>(reinterpret_cast<char *>(&dummy->next) -
                                                reinterpret_cast<char *>(dummy));
            size_t size = sizeof(pid_t);
            Slice s = accessor.PrepareForWrite(offset, size);
            return reinterpret_cast<pid_t *>(s.data());
        }

        static KeyValueType *RefKeyValues(int pos, int len, PageAccessor &accessor) {
            assert(pos + len <= kMaxEntries);
            BTreeLeafNode *dummy = static_cast<BTreeLeafNode *>(nullptr);
            auto offset = static_cast<uint32_t>(reinterpret_cast<char *>(&dummy->data[pos]) -
                                                reinterpret_cast<char *>(dummy));
            size_t size = sizeof(KeyValueType) * len;
            Slice s = accessor.PrepareForWrite(offset, size);
            return reinterpret_cast<KeyValueType *>(s.data());
        }

        static void MarkKVDirty(int pos, int len, PageAccessor &accessor) {
            assert(pos + len <= kMaxEntries);
            BTreeLeafNode *dummy = static_cast<BTreeLeafNode *>(nullptr);
            auto offset = static_cast<uint32_t>(reinterpret_cast<char *>(&dummy->data[pos]) -
                                                reinterpret_cast<char *>(dummy));
            size_t size = sizeof(KeyValueType) * len;
            accessor.MarkDirty(offset, size);
        }

        static const Key GetKey(int pos, PageAccessor &accessor) {
            assert(pos < kMaxEntries);
            BTreeLeafNode *dummy = static_cast<BTreeLeafNode *>(nullptr);
            auto offset = static_cast<uint32_t>(reinterpret_cast<char *>(&dummy->data[pos]) -
                                                reinterpret_cast<char *>(dummy));
            size_t size = sizeof(Key);
            Slice s = accessor.PrepareForRead(offset, size);
            return *reinterpret_cast<Key *>(s.data());
        }

        static const KeyValueType *GetKeyValues(int pos, int len, PageAccessor &accessor) {
            assert(pos + len <= kMaxEntries);
            BTreeLeafNode *dummy = static_cast<BTreeLeafNode *>(nullptr);
            auto offset = static_cast<uint32_t>(reinterpret_cast<char *>(&dummy->data[pos]) -
                                                reinterpret_cast<char *>(dummy));
            size_t size = sizeof(KeyValueType) * len;
            Slice s = accessor.PrepareForRead(offset, size);
            return reinterpret_cast<KeyValueType *>(s.data());
        }

        static const KeyValueType GetKeyValue(int pos, PageAccessor &accessor) {
            return *GetKeyValues(pos, 1, accessor);
        }

        static KeyValueType *RefKeyValue(int pos, PageAccessor &accessor) {
            return RefKeyValues(pos, 1, accessor);
        }

        static const Key GetMaxKey(const NodeBase &node_base, PageAccessor &accessor) {
            auto res = GetKeyValues(node_base.count - 1, 1, accessor)->first;
            accessor.FinishAccess();
            return res;
        }

        static unsigned lowerBound(Key k, PageAccessor &accessor, const NodeBase &node_base) {
            unsigned lower = 0;
            unsigned upper = node_base.count;
            do {
                unsigned mid = ((upper - lower) / 2) + lower;
                // This is the key at the pivot position
                const Key &middle_key = GetKey(mid, accessor);
                accessor.FinishAccess();
                if (k < middle_key) {
                    upper = mid;
                } else if (middle_key < k) {
                    lower = mid + 1;
                } else {
                    lower = mid;
                    break;
                }
            } while (lower < upper);
            return lower;
        }

        static BTreeOPResult
        update(Key k, Value p, PageAccessor &accessor, std::function<bool(const void *)> &predicate) {
            auto this_node_base = NodeBase::GetNodeBase(accessor);
            assert(this_node_base.count < kMaxEntries);
            unsigned pos = lowerBound(k, accessor, this_node_base);
            if ((pos < this_node_base.count)) {
                auto kv = RefKeyValue(pos, accessor);
                if (kv->first == k) {
                    bool predicated_true = predicate((const void *) &kv->second) == false;
                    if (predicated_true == false) {
                        kv->second = p;
                        accessor.FinishAccess();
                        return BTreeOPResult::UPDATED;
                    }
                }
            }
            accessor.FinishAccess();
            return BTreeOPResult::FAILED;
        }

        static BTreeOPResult
        upsert(Key k, Value p, PageAccessor &accessor, std::function<bool(const void *)> &predicate) {
            auto this_node_base = NodeBase::GetNodeBase(accessor);
            assert(this_node_base.count < kMaxEntries);
            if (this_node_base.count) {
                unsigned pos = lowerBound(k, accessor, this_node_base);
                if ((pos < this_node_base.count)) {
                    auto kv = RefKeyValue(pos, accessor);
                    if (kv->first == k) {
                        bool predicated_true = predicate((const void *) &kv->second) == false;
                        if (predicated_true == false) {
                            kv->second = p;
                            accessor.FinishAccess();
                            return BTreeOPResult::UPDATED;
                        } else {
                            accessor.FinishAccess();
                            return BTreeOPResult::FAILED;
                        }
                    }
                }
                int len = this_node_base.count - pos + 1;
                KeyValueType *data_pos = RefKeyValues(pos, len, accessor);
                memmove(data_pos + 1, data_pos, sizeof(KeyValueType) * (this_node_base.count - pos));
                data_pos[0] = std::make_pair(k, p);
            } else {
                *RefKeyValue(0, accessor) = std::make_pair(k, p);
            }
            NodeBase::RefNodeBase(accessor)->count++;
            accessor.FinishAccess();
            return BTreeOPResult::SUCCESS;
        }


        static BTreeOPResult
        insert(Key k, Value p, PageAccessor &accessor, std::function<bool(const void *)> &predicate) {
            auto this_node_base = NodeBase::GetNodeBase(accessor);
            assert(this_node_base.count < kMaxEntries);
            if (this_node_base.count) {
                unsigned pos = lowerBound(k, accessor, this_node_base);
                if ((pos < this_node_base.count)) {
                    auto kv = RefKeyValue(pos, accessor);
                    if (kv->first == k) {
                        bool predicated_true = predicate((const void *) &kv->second) == false;
                        accessor.FinishAccess();
                        return predicated_true ? BTreeOPResult::PRED_TRUE : BTreeOPResult::PRED_FALSE;
                    }
                }
                int len = this_node_base.count - pos + 1;
                KeyValueType *data_pos = RefKeyValues(pos, len, accessor);
                memmove(data_pos + 1, data_pos, sizeof(KeyValueType) * (this_node_base.count - pos));
                data_pos[0] = std::make_pair(k, p);
            } else {
                *RefKeyValue(0, accessor) = std::make_pair(k, p);
            }
            NodeBase::RefNodeBase(accessor)->count++;
            accessor.FinishAccess();
            return BTreeOPResult::SUCCESS;
        }

        static PageDesc *split(Key &sep, PageAccessor &accessor, ConcurrentBufferManager *mgr) {
            auto this_node_base = NodeBase::GetNodeBase(accessor);
            pid_t new_page_pid;
            Status s = mgr->NewPage(new_page_pid);
            assert(s.ok());
            PageAccessor new_page_accessor;
            s = mgr->Get(new_page_pid, new_page_accessor, ConcurrentBufferManager::PageOPIntent::INTENT_WRITE);
            assert(s.ok());
            new_page_accessor.PrepareForWrite(0, kPageSize);
            auto new_page_desc = new_page_accessor.GetPageDesc();
            BTreeLeafNode *newLeaf = new(reinterpret_cast<char *>(new_page_desc->page)) BTreeLeafNode();
            newLeaf->count = this_node_base.count - (this_node_base.count / 2);
            int count = NodeBase::RefNodeBase(accessor)->count = this_node_base.count - newLeaf->count;
            int len = newLeaf->count;
            const pid_t next_ptr = GetNextPtr(accessor);
            const KeyValueType *data_count = GetKeyValues(count, len, accessor);
            memcpy(newLeaf->data, data_count, sizeof(KeyValueType) * newLeaf->count);
            sep = GetKeyValue(count - 1, accessor).first;
            auto this_node_next_ptr_ptr = RefNextPtr(accessor);
            newLeaf->next = *this_node_next_ptr_ptr;
            *this_node_next_ptr_ptr = new_page_pid;
            accessor.FinishAccess();
            return new_page_desc;
        }

        static bool erase(int pos, PageAccessor &accessor) {
            auto this_node_base = NodeBase::GetNodeBase(accessor);
            int len = this_node_base.count - pos;
            KeyValueType *data_pos = RefKeyValues(pos, len, accessor);
            memmove(data_pos, data_pos + 1, sizeof(KeyValueType) * (this_node_base.count - pos - 1));
            auto this_node_base_p = NodeBase::RefNodeBase(accessor);
            this_node_base_p->count--;
            bool need_merge = NeedMerge(*this_node_base_p);
            accessor.FinishAccess();
            return need_merge;
        }

        static bool NeedMerge(const NodeBase &node_base) {
            return node_base.count / (kMaxEntries + 0.0) < kMergeThreshold;
        }

        static bool HasEnoughSpace(const NodeBase &node_base, int need) {
            return kMaxEntries - node_base.count >= need;
        }

        // Copy the content from source node to the end of this page
        static void Merge(PageAccessor &this_node_accessor, PageAccessor &source_node_accessor) {
            char temp_page[kPageSize];
            auto source_desc = source_node_accessor.GetPageDesc();
            auto source_leaf = reinterpret_cast<BTreeLeafNode *>(source_desc->page);
            auto source_leaf_base = NodeBase::GetNodeBase(source_node_accessor);
            auto source_leaf_kvs = source_leaf->GetKeyValues(0, source_leaf_base.count, source_node_accessor);
            memcpy(temp_page, (const char *) source_leaf_kvs, sizeof(KeyValueType) * source_leaf_base.count);
            source_node_accessor.FinishAccess();
            auto this_node_base = NodeBase::GetNodeBase(this_node_accessor);
            assert(HasEnoughSpace(this_node_base, source_leaf_base.count));
            auto this_desc = this_node_accessor.GetPageDesc();
            auto this_leaf = reinterpret_cast<BTreeLeafNode *>(this_desc->page);
            auto this_leaf_kvs_dest = this_leaf->RefKeyValues(this_node_base.count, source_leaf_base.count,
                                                              this_node_accessor);
            memcpy(this_leaf_kvs_dest, temp_page, sizeof(KeyValueType) * source_leaf_base.count);
            NodeBase::RefNodeBase(this_node_accessor)->count += source_leaf_base.count;
            this_node_accessor.FinishAccess();
        }
    };

    static_assert(sizeof(BTreeLeafNode) == sizeof(Page), "sizeof(BTreeLeafNode) != sizeof(Page)");
    //static_assert(sizeof(BTreeInnerNode) == sizeof(Page), "sizeof(BTreeInnerNode) != sizeof(Page)");

    BTree(ConcurrentBufferManager *mgr) : mgr(mgr), root_node_desc(nullptr) {}

    Status Init(pid_t &root_pid) {
        if (root_pid == kInvalidPID) {
            // Initialize a root page
            Status s = mgr->NewPage(root_pid);
            if (!s.ok())
                return s;
            PageAccessor accessor;
            s = mgr->Get(root_pid, accessor, ConcurrentBufferManager::PageOPIntent::INTENT_WRITE_FULL);
            if (!s.ok())
                return s;
            auto node_desc = accessor.GetPageDesc();
            accessor.PrepareForWrite(0, sizeof(NodeBase));
            BTreeLeafNode *root_leaf = new(reinterpret_cast<char *>(node_desc->page)) BTreeLeafNode;
            root_node_desc.store(node_desc);
            return s;
        } else {
            PageAccessor accessor;
            Status s = mgr->Get(root_pid, accessor, ConcurrentBufferManager::PageOPIntent::INTENT_WRITE_FULL);
            if (!s.ok())
                return s;
            auto node_desc = accessor.GetPageDesc();
            root_node_desc.store(node_desc);
            return s;
        }
    }


    ~BTree() {
        mgr->Put(root_node_desc.load());
        root_node_desc.store(nullptr);
    }


private:

    void MakeRoot(Key k, pid_t leftChild, pid_t rightChild, ConcurrentBufferManager *mgr) {
        pid_t new_root_pid;
        Status s = mgr->NewPage(new_root_pid);
        assert(s.ok());
        PageAccessor new_root_accessor;
        s = mgr->Get(new_root_pid, new_root_accessor, ConcurrentBufferManager::PageOPIntent::INTENT_WRITE);
        assert(s.ok());
        PageDesc *new_root_page_desc = new_root_accessor.GetPageDesc();
        new_root_accessor.PrepareForWrite(0, kPageSize);
        auto inner = new(reinterpret_cast<char *>(new_root_page_desc->page)) BTreeInnerNode();
        inner->count = 1;
        inner->data[0] = std::make_pair(k, leftChild);
        inner->data[1].second = rightChild;
        auto old_root_node_desc = root_node_desc.load();
        root_node_desc.store(new_root_page_desc);
    }

    void Yield(int count) {
        if (count > 3)
            sched_yield();
        else
            _mm_pause();
    }

public:

    NodeBase GetNodeBase(PageAccessor &accessor) {
        Slice s = accessor.PrepareForRead(0, sizeof(NodeBase));
        return *reinterpret_cast<NodeBase *>(s.data());
    }

    BTreeOPResult
    Insert(Key k, Value v, std::function<bool(const void *)> predicate = [](const void *) { return false; },
           bool upsert = true) {
        int restartCount = 0;
        restart:
        if (restartCount++)
            Yield(restartCount);
        bool needRestart = false;
        BTreeOPResult insert_status = BTreeOPResult::FAILED;
        {
            thread_local static std::vector<PageDesc *> descs;
            descs.clear();
            DeferCode c([&descs, this]() {
                for (int i = 0; i < descs.size(); ++i) {
                    mgr->Put(descs[i]);
                }
            });

            auto root_pd = root_node_desc.load();
            uint64_t versionNode = root_pd->readLockOrRestart(needRestart);
            if (needRestart || root_pd != root_node_desc) {
                goto restart;
            }

            PageAccessor node_accessor = mgr->GetPageAccessorFromDesc(root_pd);

            PageDesc *node_desc = root_pd;
            NodeBase node_base = GetNodeBase(node_accessor);
            node_accessor.FinishAccess();

            PageDesc *parent_node_desc = nullptr;
            BTreeInnerNode *parent = nullptr;
            uint64_t versionParent = 0;
            pid_t parent_pid = kInvalidPID;
            pid_t current_pid = root_pd->pid;
            while (node_base.type == InnerNode) {
                auto inner_node_desc = node_desc;
                auto inner = reinterpret_cast<BTreeInnerNode *>(node_desc->page);
                // Split eagerly if full
                if (inner->isFull(node_base)) {
                    // Lock
                    if (parent_node_desc) {
                        parent_node_desc->upgradeToWriteLockOrRestart(versionParent, needRestart);
                        if (needRestart)
                            goto restart;
                    }
                    node_desc->upgradeToWriteLockOrRestart(versionNode, needRestart);
                    if (needRestart) {
                        if (parent_node_desc)
                            parent_node_desc->writeUnlock();
                        goto restart;
                    }
                    if (!parent_node_desc && (node_desc != root_node_desc)) { // there's a new parent
                        node_desc->writeUnlock();
                        goto restart;
                    }
                    // Split
                    Key sep;
                    PageDesc *new_inner_node_desc = inner->split(sep, node_accessor, mgr);
                    BTreeInnerNode *newInner = reinterpret_cast<BTreeInnerNode *>(new_inner_node_desc->page);
                    if (parent) {
                        assert(parent_node_desc);
                        assert(descs.empty() == false);
                        assert(descs.back() == inner_node_desc);
                        auto parent_node_accessor = mgr->GetPageAccessorFromDesc(parent_node_desc);
                        parent->insert(sep, new_inner_node_desc->pid, parent_node_accessor);
                    } else {
                        MakeRoot(sep, inner_node_desc->pid, new_inner_node_desc->pid, mgr);
                        descs.push_back(inner_node_desc);
                    }
                    descs.push_back(new_inner_node_desc);

                    if (parent_node_desc) {
                        assert(parent);
                        parent_node_desc->downgradeToReadLock(versionParent);
                    }

                    if (sep < k) {
                        inner_node_desc->writeUnlock();
                        inner_node_desc = new_inner_node_desc;
                        inner = newInner;
                        versionNode = inner_node_desc->readLockOrRestart(needRestart);
                        if (needRestart)
                            goto restart;
                        node_accessor = mgr->GetPageAccessorFromDesc(new_inner_node_desc);
                        node_base = GetNodeBase(node_accessor);
                        node_accessor.FinishAccess();
                    } else {
                        node_desc->downgradeToReadLock(versionNode);
                    }
                }

                if (parent_node_desc) {
                    assert(parent);
                    parent_node_desc->readUnlockOrRestart(versionParent, needRestart);
                    if (needRestart)
                        goto restart;
                }

                parent_node_desc = inner_node_desc;
                parent = inner;
                versionParent = versionNode;

                parent_pid = current_pid;
                auto pos = inner->lowerBound(k, node_accessor, node_base);
                pid_t current_pid = inner->GetKeyValue(pos, node_accessor).second;
                assert(current_pid != kInvalidPID);
                assert(current_pid != parent_pid);
                if (current_pid == parent_pid) {
                    std::cout << "current_pid : " << current_pid << std::endl;
                    exit(1);
                }
                node_accessor.FinishAccess();
                Status s = mgr->Get(current_pid, node_accessor, ConcurrentBufferManager::PageOPIntent::INTENT_READ);
                assert(s.ok());
                node_desc = node_accessor.GetPageDesc();
                descs.push_back(node_desc);
                inner_node_desc->checkOrRestart(versionNode, needRestart);
                if (needRestart)
                    goto restart;
                versionNode = node_desc->readLockOrRestart(needRestart);
                if (needRestart)
                    goto restart;
                node_base = GetNodeBase(node_accessor);
                node_accessor.FinishAccess();
            }

            auto leaf_node_desc = node_desc;
            auto leaf = reinterpret_cast<BTreeLeafNode *>(leaf_node_desc->page);
            // Split leaf if full
            if (node_base.count == leaf->kMaxEntries) {
                // Lock
                if (parent) {
                    assert(parent_node_desc);
                    parent_node_desc->upgradeToWriteLockOrRestart(versionParent, needRestart);
                    if (needRestart)
                        goto restart;
                }
                assert(node_desc);
                node_desc->upgradeToWriteLockOrRestart(versionNode, needRestart);
                if (needRestart) {
                    if (parent) {
                        assert(parent_node_desc);
                        parent_node_desc->writeUnlock();
                    }
                    goto restart;
                }
                if (!parent && (node_desc != root_node_desc)) { // there's a new parent
                    node_desc->writeUnlock();
                    goto restart;
                }
                // Split
                Key sep;
                PageDesc *new_leaf_desc = leaf->split(sep, node_accessor, mgr);
                BTreeLeafNode *newLeaf = reinterpret_cast<BTreeLeafNode *>(new_leaf_desc->page);
                if (sep < k) {
                    auto new_leaf_node_accessor = mgr->GetPageAccessorFromDesc(new_leaf_desc);
                    if (upsert)
                        insert_status = newLeaf->upsert(k, v, new_leaf_node_accessor, predicate);
                    else
                        insert_status = newLeaf->insert(k, v, new_leaf_node_accessor, predicate);
                } else {
                    if (upsert)
                        insert_status = leaf->upsert(k, v, node_accessor, predicate);
                    else
                        insert_status = leaf->insert(k, v, node_accessor, predicate);
                }

                if (parent) {
                    auto parent_node_accessor = mgr->GetPageAccessorFromDesc(parent_node_desc);
                    parent->insert(sep, new_leaf_desc->pid, parent_node_accessor);
                    assert(descs.empty() == false);
                    assert(descs.back() == leaf_node_desc);
                } else {
                    MakeRoot(sep, leaf_node_desc->pid, new_leaf_desc->pid, mgr);
                    descs.push_back(leaf_node_desc);
                }
                descs.push_back(new_leaf_desc);
                // Unlock and restart
                node_desc->writeUnlock();
                if (parent) {
                    assert(parent_node_desc);
                    parent_node_desc->writeUnlock();
                }
                return insert_status; // success
            } else {
                // only lock leaf node
                node_desc->upgradeToWriteLockOrRestart(versionNode, needRestart);
                if (needRestart)
                    goto restart;
                if (parent) {
                    assert(parent_node_desc);
                    parent_node_desc->readUnlockOrRestart(versionParent, needRestart);
                    if (needRestart) {
                        node_desc->writeUnlock();
                        goto restart;
                    }
                }
                if (upsert)
                    insert_status = leaf->upsert(k, v, node_accessor, predicate);
                else
                    insert_status = leaf->insert(k, v, node_accessor, predicate);
                node_desc->writeUnlock();
                return insert_status; // success
            }
        }
    }


    struct BTreeStackElement {
        PageDesc *desc;
        int pos;
        uint64_t version;
    };


    bool NodeNeedMerge(const NodeBase &node_base) {
        if (node_base.type == InnerNode) {
            auto inner = reinterpret_cast<BTreeInnerNode *>(&const_cast<NodeBase &>(node_base));
            return inner->NeedMerge(node_base);
        } else {
            auto leaf = reinterpret_cast<BTreeLeafNode *>(&const_cast<NodeBase &>(node_base));
            return leaf->NeedMerge(node_base);
        }
    }

    bool HasEnoughSpace(const NodeBase &node_base, int need) {
        if (node_base.type == InnerNode) {
            auto inner = reinterpret_cast<BTreeInnerNode *>(&const_cast<NodeBase &>(node_base));
            return inner->HasEnoughSpace(node_base, need);
        } else {
            auto leaf = reinterpret_cast<BTreeLeafNode *>(&const_cast<NodeBase &>(node_base));
            return leaf->HasEnoughSpace(node_base, need);
        }
    }

    Key SubTreeMaxKey(pid_t subtree_root_pid) {
        int restartCount = 0;
        restart:
        if (restartCount++)
            Yield(restartCount);
        bool needRestart = false;

        {
            std::vector<PageDesc *> descs;
            DeferCode c([&descs, this]() {
                for (int i = 0; i < descs.size(); ++i) {
                    mgr->Put(descs[i]);
                }
            });

            PageDesc *subtree_root_pd = nullptr;
            Status s = mgr->Get(subtree_root_pid, subtree_root_pd);
            assert(s.ok());
            descs.push_back(subtree_root_pd);
            PageDesc *node_desc = subtree_root_pd;
            uint64_t versionNode = node_desc->readLockOrRestart(needRestart);
            if (needRestart || (node_desc != root_node_desc))
                goto restart;

            PageAccessor node_accessor = mgr->GetPageAccessorFromDesc(subtree_root_pd);
            NodeBase node_base = GetNodeBase(node_accessor);
            // Parent of current node
            BTreeInnerNode *parent = nullptr;
            PageDesc *parent_node_desc = nullptr;
            uint64_t versionParent = 0;
            while (node_base.type == InnerNode) {
                auto inner_node_desc = node_desc;
                auto inner = reinterpret_cast<BTreeInnerNode *>(node_desc->page);

                if (parent) {
                    assert(parent_node_desc);
                    parent_node_desc->readUnlockOrRestart(versionParent, needRestart);
                    if (needRestart)
                        goto restart;
                }

                parent_node_desc = inner_node_desc;
                parent = inner;
                versionParent = versionNode;

                // Get the last child
                auto pos = node_base.count;
                pid_t child_pid = inner->GetKeyValue(pos, node_accessor).second;
                assert(child_pid != kInvalidPID);
                node_accessor.FinishAccess();
                Status s = mgr->Get(child_pid, node_accessor, ConcurrentBufferManager::PageOPIntent::INTENT_READ);
                assert(s.ok());
                node_desc = node_accessor.GetPageDesc();
                descs.push_back(node_desc);

                inner_node_desc->checkOrRestart(versionNode, needRestart);
                if (needRestart)
                    goto restart;
                versionNode = node_desc->readLockOrRestart(needRestart);
                if (needRestart)
                    goto restart;

                node_base = this->GetNodeBase(node_accessor);
                node_accessor.FinishAccess();
            }

            PageDesc *leaf_desc = node_desc;
            BTreeLeafNode *leaf = reinterpret_cast<BTreeLeafNode *>(node_desc->page);
            int pos = node_base.count ? node_base.count - 1 : 0;
            Key res;
            res = leaf->GetKeyValue(pos, node_accessor).first;
            if (parent) {
                assert(parent_node_desc);
                parent_node_desc->readUnlockOrRestart(versionParent, needRestart);
                if (needRestart)
                    goto restart;
            }
            node_desc->readUnlockOrRestart(versionNode, needRestart);
            if (needRestart)
                goto restart;

            return res;
        }
    }

    void EraseMerge(std::vector<BTreeStackElement> &stack) {
        // Assume the nodes in stack are all optimistically read-locked,
        // meaning the version numbers are stored in the stack.
        if (stack.size() <= 1) {
            return;
        }

        BTreeStackElement tope = stack.back();
        auto child_desc = tope.desc;
        auto child_version = tope.version;
        auto child_pos_in_parent = tope.pos;
        auto child_accessor = mgr->GetPageAccessorFromDesc(child_desc);
        auto child_base = GetNodeBase(child_accessor);

        BTreeStackElement parente = stack[stack.size() - 2];
        auto parent_desc = parente.desc;
        auto parent_version = parente.version;
        auto parent_accessor = mgr->GetPageAccessorFromDesc(parent_desc);
        auto parent_base = GetNodeBase(parent_accessor);

        uint64_t sibling_version;
        enum class MergeDirection {
            LeftToRight,
            RightToLeft,
            NoMerge,
        };

        MergeDirection direction = MergeDirection::NoMerge;
        int sibling_pos_in_parent = -1;
        auto choose_sibling = [&]() -> PageDesc * {
            BTreeInnerNode *parent = reinterpret_cast<BTreeInnerNode *>(parent_desc->page);
            auto left_sibling_pos = child_pos_in_parent - 1;
            auto right_sibling_pos = child_pos_in_parent + 1;
            auto left_sibling_pid = kInvalidPID;
            auto right_sibling_pid = kInvalidPID;
            if (left_sibling_pos >= 0) {
                auto kv = parent->GetKeyValue(left_sibling_pos, parent_accessor);
                left_sibling_pid = kv.second;
            }
            if (right_sibling_pos <= parent_base.count) {
                auto kv = parent->GetKeyValue(right_sibling_pos, parent_accessor);
                right_sibling_pid = kv.second;
            }
            if (left_sibling_pid != kInvalidPID) {
                PageAccessor left_sibling_accessor;
                Status s = mgr->Get(left_sibling_pid, left_sibling_accessor);
                assert(s.ok());
                auto left_sibling_desc = left_sibling_accessor.GetPageDesc();
                bool restart = false;
                sibling_version = left_sibling_desc->readLockOrRestart(restart);
                if (restart == false) { // If the read locking failed, try next sibling
                    auto left_sibling_base = GetNodeBase(left_sibling_accessor);
                    if (HasEnoughSpace(left_sibling_base, child_base.count)) {
                        sibling_pos_in_parent = left_sibling_pos;
                        direction = MergeDirection::RightToLeft;
                        return left_sibling_desc;
                    }
                }
                mgr->Put(left_sibling_desc);
            }

            if (right_sibling_pid != kInvalidPID) {
                PageAccessor right_sibling_accessor;
                Status s = mgr->Get(right_sibling_pid, right_sibling_accessor);
                assert(s.ok());
                auto right_sibling_desc = right_sibling_accessor.GetPageDesc();
                bool restart = false;
                sibling_version = right_sibling_desc->readLockOrRestart(restart);
                if (restart) { // If the read locking failed, do not merge.
                    mgr->Put(right_sibling_desc);
                    return nullptr;
                }
                auto right_sibling_base = GetNodeBase(right_sibling_accessor);
                if (HasEnoughSpace(right_sibling_base, child_base.count)) {
                    sibling_pos_in_parent = right_sibling_pos;
                    direction = MergeDirection::LeftToRight;
                    return right_sibling_desc;
                }
                mgr->Put(right_sibling_desc);
            }

            return nullptr;
        };

        PageDesc *sibling_desc = choose_sibling();
        DeferCode dc([this, &sibling_desc]() {
            if (sibling_desc != nullptr)
                mgr->Put(sibling_desc);
        });

        if (sibling_desc == nullptr)
            return;

        // Locking protocol:
        //  To avoid deadlock, we lock in the order of from top to bottom and from left to right.
        bool restart = false;
        parent_desc->upgradeToWriteLockOrRestart(parent_version, restart);
        if (restart)
            return;

        if (direction == MergeDirection::LeftToRight) {
            child_desc->upgradeToWriteLockOrRestart(child_version, restart);
            if (restart) {
                parent_desc->writeUnlock();
                return;
            }
            sibling_desc->upgradeToWriteLockOrRestart(sibling_version, restart);
            if (restart) {
                child_desc->writeUnlock();
                parent_desc->writeUnlock();
                return;
            }
        } else {
            sibling_desc->upgradeToWriteLockOrRestart(sibling_version, restart);
            if (restart) {
                parent_desc->writeUnlock();
                return;
            }
            child_desc->upgradeToWriteLockOrRestart(child_version, restart);
            if (restart) {
                sibling_desc->writeUnlock();
                parent_desc->writeUnlock();
                return;
            }
        }

        auto sibling_accessor = mgr->GetPageAccessorFromDesc(sibling_desc);
        auto sibling_base = GetNodeBase(sibling_accessor);
        bool parent_need_merge = false;

        auto parent = reinterpret_cast<BTreeInnerNode *>(parent_desc->page);
        // Now that we have all the nodes write-locked, do the merge.
        if (child_base.type == LeafNode) {
            auto child = reinterpret_cast<BTreeLeafNode *>(child_desc->page);
            auto sibling = reinterpret_cast<BTreeLeafNode *>(sibling_desc->page);
            if (direction == MergeDirection::LeftToRight) {
                child->Merge(child_accessor, sibling_accessor);
                auto sibling_leaf_max_key = sibling->GetMaxKey(sibling_base, sibling_accessor);
                parent->RefKeyValue(child_pos_in_parent, parent_accessor)->first = sibling_leaf_max_key;
                parent_accessor.FinishAccess();
                parent = reinterpret_cast<BTreeInnerNode *>(parent_desc->page);
                parent->erase(sibling_pos_in_parent, parent_accessor);
            } else {
                sibling->Merge(sibling_accessor, child_accessor);
                auto child_leaf_max_key = child->GetMaxKey(child_base, child_accessor);
                parent->RefKeyValue(sibling_pos_in_parent, parent_accessor)->first = child_leaf_max_key;
                parent_accessor.FinishAccess();
                parent = reinterpret_cast<BTreeInnerNode *>(parent_desc->page);
                parent->erase(child_pos_in_parent, parent_accessor);
            }
        } else {
            auto child = reinterpret_cast<BTreeInnerNode *>(child_desc->page);
            auto sibling = reinterpret_cast<BTreeInnerNode *>(sibling_desc->page);
            if (direction == MergeDirection::LeftToRight) {
                auto child_subtree_max_key = SubTreeMaxKey(child->GetKeyValue(child_base.count, child_accessor).second);
                child->Merge(child_accessor, child_subtree_max_key, sibling_accessor);
                auto sibling_subtree_max_key = SubTreeMaxKey(
                        sibling->GetKeyValue(sibling_base.count, sibling_accessor).second);
                parent->RefKeyValue(child_pos_in_parent, parent_accessor)->first = sibling_subtree_max_key;
                parent_accessor.FinishAccess();
                parent = reinterpret_cast<BTreeInnerNode *>(parent_desc->page);
                parent->erase(sibling_pos_in_parent, parent_accessor);
            } else {
                auto subtree_max_key = SubTreeMaxKey(sibling->GetKeyValue(sibling_base.count, sibling_accessor).second);
                child->Merge(child_accessor, subtree_max_key, sibling_accessor);
                subtree_max_key = SubTreeMaxKey(child_desc->pid);
                parent->RefKeyValue(sibling_pos_in_parent, parent_accessor)->first = subtree_max_key;
                parent_accessor.FinishAccess();
                parent = reinterpret_cast<BTreeInnerNode *>(parent_desc->page);
                parent->erase(child_pos_in_parent, parent_accessor);
            }
        }

        parent_base = GetNodeBase(parent_accessor);
        parent_need_merge = NodeNeedMerge(parent_base);
        // Release all the locks
        parent_desc->downgradeToReadLock(parent_version);
        child_desc->writeUnlock();
        sibling_desc->writeUnlock();

        // Check if parent need merge as well
        if (parent_need_merge) {
            // Pop the top
            stack.pop_back();
            assert(stack.back().desc == parent_desc);
            stack.back().version == parent_version;
            EraseMerge(stack);
        }
    }

    bool Erase(Key k) {
        int restartCount = 0;
        restart:
        if (restartCount++)
            Yield(restartCount);
        bool needRestart = false;
        {
            // stack stores the root to leaf path.
            // Each element stores the node and the position in the parent node
            // from which the current node is derived.
            std::vector<BTreeStackElement> stack;
            DeferCode c([&]() {
                // Skip root node
                for (int i = 1; i < stack.size(); ++i) {
                    mgr->Put(stack[i].desc);
                }
            });

            auto root_pd = root_node_desc.load();

            PageDesc *node_desc = root_pd;
            uint64_t versionNode = node_desc->readLockOrRestart(needRestart);
            if (needRestart || (node_desc != root_node_desc))
                goto restart;
            stack.push_back(BTreeStackElement{root_pd, -1, versionNode});

            PageAccessor node_accessor = mgr->GetPageAccessorFromDesc(root_pd);
            NodeBase node_base = GetNodeBase(node_accessor);
            // Parent of current node
            BTreeInnerNode *parent = nullptr;
            PageDesc *parent_node_desc = nullptr;
            uint64_t versionParent = 0;

            while (node_base.type == InnerNode) {
                auto inner_node_desc = node_desc;
                auto inner = reinterpret_cast<BTreeInnerNode *>(node_desc->page);

                if (parent) {
                    assert(parent_node_desc);
                    parent_node_desc->readUnlockOrRestart(versionParent, needRestart);
                    if (needRestart)
                        goto restart;
                }

                parent_node_desc = inner_node_desc;
                parent = inner;
                versionParent = versionNode;

                auto pos = inner->lowerBound(k, node_accessor, node_base);
                pid_t child_pid = inner->GetKeyValue(pos, node_accessor).second;
                assert(child_pid != kInvalidPID);
                node_accessor.FinishAccess();
                Status s = mgr->Get(child_pid, node_accessor, ConcurrentBufferManager::PageOPIntent::INTENT_READ);
                assert(s.ok());
                node_desc = node_accessor.GetPageDesc();
                stack.push_back(BTreeStackElement{node_desc, (int)pos, versionNode});

                inner_node_desc->checkOrRestart(versionNode, needRestart);
                if (needRestart)
                    goto restart;
                versionNode = node_desc->readLockOrRestart(needRestart);
                if (needRestart)
                    goto restart;

                node_base = this->GetNodeBase(node_accessor);
                node_accessor.FinishAccess();
            }

            PageDesc *leaf_desc = node_desc;
            BTreeLeafNode *leaf = reinterpret_cast<BTreeLeafNode *>(node_desc->page);
            unsigned pos = leaf->lowerBound(k, node_accessor, node_base);
            bool success = false;
            bool leaf_need_merge = false;
            if (pos < node_base.count) {
                auto kv = leaf->GetKeyValue(pos, node_accessor);
                if (kv.first == k) {
                    leaf_desc->upgradeToWriteLockOrRestart(versionNode, needRestart);
                    if (needRestart)
                        goto restart;
                    if (parent) {
                        assert(parent_node_desc);
                        parent_node_desc->readUnlockOrRestart(versionParent, needRestart);
                        if (needRestart) {
                            leaf_desc->writeUnlock();
                            goto restart;
                        }
                    }
                    success = true;
                    leaf_need_merge = leaf->erase(pos, node_accessor);
                    leaf_desc->downgradeToReadLock(versionNode);

                    if (leaf_need_merge) {
                        stack.back().version = versionNode;
                        auto stack_copy = stack;
                        EraseMerge(stack_copy);
                    }
                    return success;
                }
            }

            leaf_need_merge = leaf->NeedMerge(node_base);
            if (parent) {
                assert(parent_node_desc);
                parent_node_desc->readUnlockOrRestart(versionParent, needRestart);
                if (needRestart)
                    goto restart;
            }
            node_desc->readUnlockOrRestart(versionNode, needRestart);
            if (needRestart)
                goto restart;
            if (leaf_need_merge) {
                auto stack_copy = stack;
                EraseMerge(stack_copy);
            }
            return success;
        }
    }

    bool Lookup(Key k, Value &result) {
        int restartCount = 0;
        restart:
        if (restartCount++)
            Yield(restartCount);
        bool needRestart = false;

        {
            thread_local static std::vector<PageDesc *> descs;
            descs.clear();
            DeferCode c([&descs, this]() {
                for (int i = 0; i < descs.size(); ++i) {
                    mgr->Put(descs[i]);
                }
            });

            auto root_pd = root_node_desc.load();
            PageDesc *node_desc = root_pd;
            uint64_t versionNode = node_desc->readLockOrRestart(needRestart);
            if (needRestart || (node_desc != root_node_desc))
                goto restart;

            PageAccessor node_accessor = mgr->GetPageAccessorFromDesc(root_pd);
            NodeBase node_base = GetNodeBase(node_accessor);
            // Parent of current node
            BTreeInnerNode *parent = nullptr;
            PageDesc *parent_node_desc = nullptr;
            uint64_t versionParent = 0;

            while (node_base.type == InnerNode) {
                auto inner_node_desc = node_desc;
                auto inner = reinterpret_cast<BTreeInnerNode *>(node_desc->page);

                if (parent) {
                    assert(parent_node_desc);
                    parent_node_desc->readUnlockOrRestart(versionParent, needRestart);
                    if (needRestart)
                        goto restart;
                }

                parent_node_desc = inner_node_desc;
                parent = inner;
                versionParent = versionNode;


                auto pos = inner->lowerBound(k, node_accessor, node_base);
                pid_t child_pid = inner->GetKeyValue(pos, node_accessor).second;
                assert(child_pid != kInvalidPID);
                node_accessor.FinishAccess();
                Status s = mgr->Get(child_pid, node_accessor, ConcurrentBufferManager::PageOPIntent::INTENT_READ);
                assert(s.ok());
                node_desc = node_accessor.GetPageDesc();

                descs.push_back(node_desc);

                inner_node_desc->checkOrRestart(versionNode, needRestart);
                if (needRestart)
                    goto restart;
                versionNode = node_desc->readLockOrRestart(needRestart);
                if (needRestart)
                    goto restart;

                node_base = this->GetNodeBase(node_accessor);
                node_accessor.FinishAccess();
            }

            PageDesc *leaf_desc = node_desc;
            BTreeLeafNode *leaf = reinterpret_cast<BTreeLeafNode *>(node_desc->page);
            unsigned pos = leaf->lowerBound(k, node_accessor, node_base);
            bool success = false;
            if (pos < node_base.count) {
                auto kv = leaf->GetKeyValue(pos, node_accessor);
                if (kv.first == k) {
                    success = true;
                    result = kv.second;
                }
            }
            if (parent) {
                assert(parent_node_desc);
                parent_node_desc->readUnlockOrRestart(versionParent, needRestart);
                if (needRestart)
                    goto restart;
            }
            node_desc->readUnlockOrRestart(versionNode, needRestart);
            if (needRestart)
                goto restart;

            return success;
        }
    }


    bool Lookup(Key k, std::function<BTreeOPResult(const Value &)> processor) {
        int restartCount = 0;
        restart:
        if (restartCount++)
            Yield(restartCount);
        bool needRestart = false;

        {
            thread_local static std::vector<PageDesc *> descs;
            descs.clear();
            DeferCode c([&descs, this]() {
                for (int i = 0; i < descs.size(); ++i) {
                    mgr->Put(descs[i]);
                }
            });

            auto root_pd = root_node_desc.load();
            PageDesc *node_desc = root_pd;
            uint64_t versionNode = node_desc->readLockOrRestart(needRestart);
            if (needRestart || (node_desc != root_node_desc))
                goto restart;

            PageAccessor node_accessor = mgr->GetPageAccessorFromDesc(root_pd);
            NodeBase node_base = GetNodeBase(node_accessor);
            // Parent of current node
            BTreeInnerNode *parent = nullptr;
            PageDesc *parent_node_desc = nullptr;
            uint64_t versionParent = 0;

            while (node_base.type == InnerNode) {
                auto inner_node_desc = node_desc;
                auto inner = reinterpret_cast<BTreeInnerNode *>(node_desc->page);

                if (parent) {
                    assert(parent_node_desc);
                    parent_node_desc->readUnlockOrRestart(versionParent, needRestart);
                    if (needRestart)
                        goto restart;
                }

                parent_node_desc = inner_node_desc;
                parent = inner;
                versionParent = versionNode;


                auto pos = inner->lowerBound(k, node_accessor, node_base);
                pid_t child_pid = inner->GetKeyValue(pos, node_accessor).second;
                assert(child_pid != kInvalidPID);
                node_accessor.FinishAccess();
                Status s = mgr->Get(child_pid, node_accessor, ConcurrentBufferManager::PageOPIntent::INTENT_READ);
                assert(s.ok());
                node_desc = node_accessor.GetPageDesc();
                descs.push_back(node_desc);

                inner_node_desc->checkOrRestart(versionNode, needRestart);
                if (needRestart)
                    goto restart;
                versionNode = node_desc->readLockOrRestart(needRestart);
                if (needRestart)
                    goto restart;

                node_base = this->GetNodeBase(node_accessor);
                node_accessor.FinishAccess();
            }

            PageDesc *leaf_desc = node_desc;
            leaf_desc->upgradeToWriteLockOrRestart(versionNode, needRestart);
            if (needRestart)
                goto restart;
            if (parent) {
                assert(parent_node_desc);
                parent_node_desc->readUnlockOrRestart(versionParent, needRestart);
                if (needRestart) {
                    leaf_desc->writeUnlock();
                    goto restart;
                }
            }
            BTreeLeafNode *leaf = reinterpret_cast<BTreeLeafNode *>(node_desc->page);
            unsigned pos = leaf->lowerBound(k, node_accessor, node_base);
            bool success = false;
            BTreeOPResult res = BTreeOPResult::SUCCESS;
            if (pos < node_base.count) {
                auto key = leaf->GetKey(pos, node_accessor);
                if (key == k) {
                    success = true;
                    const auto kvptr = leaf->GetKeyValues(pos, 1, node_accessor);
                    res = processor(kvptr->second);
                    node_accessor.FinishAccess();
                }
            }
            leaf_desc->writeUnlock();
            if (res == BTreeOPResult::RETRY_SCAN) {
                goto restart;
            }
            return success;
        }
    }

    void Scan(Key start_key, std::function<BTreeOPResult(const Value &)> processor) {
        int restartCount = 0;
        restart:
        if (restartCount++)
            Yield(restartCount);
        bool needRestart = false;

        {
            std::vector<PageDesc *> descs;
            DeferCode c([&descs, this]() {
                for (int i = 0; i < descs.size(); ++i) {
                    mgr->Put(descs[i]);
                }
            });

            auto root_pd = root_node_desc.load();
            PageDesc *node_desc = root_pd;
            uint64_t versionNode = node_desc->readLockOrRestart(needRestart);
            if (needRestart || (node_desc != root_node_desc))
                goto restart;


            PageAccessor node_accessor = mgr->GetPageAccessorFromDesc(root_pd);

            NodeBase node_base = GetNodeBase(node_accessor);

            // Parent of current node
            BTreeInnerNode *parent = nullptr;
            PageDesc *parent_node_desc = nullptr;
            uint64_t versionParent = 0;

            while (node_base.type == InnerNode) {
                auto inner_node_desc = node_desc;
                auto inner = reinterpret_cast<BTreeInnerNode *>(node_desc->page);

                if (parent) {
                    assert(parent_node_desc);
                    parent_node_desc->readUnlockOrRestart(versionParent, needRestart);
                    if (needRestart)
                        goto restart;
                }

                parent_node_desc = inner_node_desc;
                parent = inner;
                versionParent = versionNode;

                auto pos = inner->lowerBound(start_key, node_accessor, node_base);
                pid_t child_pid = inner->GetKeyValue(pos, node_accessor).second;
                assert(child_pid != kInvalidPID);
                Status s = mgr->Get(child_pid, node_accessor, ConcurrentBufferManager::PageOPIntent::INTENT_READ);
                assert(s.ok());
                node_desc = node_accessor.GetPageDesc();
                descs.push_back(node_desc);

                inner_node_desc->checkOrRestart(versionNode, needRestart);
                if (needRestart)
                    goto restart;
                versionNode = node_desc->readLockOrRestart(needRestart);
                if (needRestart)
                    goto restart;

                node_base = GetNodeBase(node_accessor);
            }

            PageDesc *leaf_desc = node_desc;
            // We need exclusive access to the leaf during scan with external processor
            leaf_desc->upgradeToWriteLockOrRestart(versionNode, needRestart);
            if (needRestart) {
                goto restart;
            }
            if (parent) {
                assert(parent_node_desc);
                parent_node_desc->readUnlockOrRestart(versionParent, needRestart);
                if (needRestart) {
                    leaf_desc->writeUnlock();
                    goto restart;
                }
            }

            BTreeLeafNode *leaf = reinterpret_cast<BTreeLeafNode *>(node_desc->page);
            unsigned pos = leaf->lowerBound(start_key, node_accessor, node_base);
            auto data_pos = leaf->GetKeyValues(pos, node_base.count - pos, node_accessor);
            BTreeOPResult res = BTreeOPResult::CONTINUE_SCAN;
            for (unsigned i = pos; i < node_base.count; i++) {
                res = processor(data_pos[i - pos].second);
                if (res != BTreeOPResult::CONTINUE_SCAN) {
                    break;
                }
            }

            if (res == BTreeOPResult::CONTINUE_SCAN) {
                pid_t next_leaf_pid = leaf->GetNextPtr(node_accessor);
                node_desc->writeUnlock();
                for (auto pd : descs) {
                    mgr->Put(pd);
                }
                descs.clear();
                leaf_desc = nullptr;
                while (res == BTreeOPResult::CONTINUE_SCAN && next_leaf_pid != kInvalidPID) {
                    Status s = mgr->Get(next_leaf_pid, node_accessor,
                                        ConcurrentBufferManager::PageOPIntent::INTENT_READ);
                    assert(s.ok());
                    leaf_desc = node_accessor.GetPageDesc();
                    int restart_count = 0;
                    do {
                        needRestart = false;
                        leaf_desc->writeLockOrRestart(needRestart);
                        if (restart_count++)
                            Yield(restart_count);
                    } while (needRestart == true);
                    node_base = GetNodeBase(node_accessor);
                    leaf = reinterpret_cast<BTreeLeafNode *>(leaf_desc->page);
                    next_leaf_pid = leaf->GetNextPtr(node_accessor);
                    data_pos = leaf->GetKeyValues(0, node_base.count, node_accessor);
                    for (unsigned i = 0; i < node_base.count; i++) {
                        res = processor(data_pos[i].second);
                        if (res != BTreeOPResult::CONTINUE_SCAN) {
                            break;
                        }
                    }
                    leaf_desc->writeUnlock();
                    mgr->Put(leaf_desc);
                }
            } else {
                leaf_desc->writeUnlock();
            }
            if (res == BTreeOPResult::RETRY_SCAN) {
                goto restart;
            }
        }
    }

    BTreeOPResult LookupForUpdate(Key search_key, std::function<bool(Value &)> processor) {
        volatile int restartCount = 0;
        restart:
        if (restartCount++)
            Yield(restartCount);
        bool needRestart = false;
        BTreeOPResult res = BTreeOPResult::NOT_FOUND;
        {
            std::vector<PageDesc *> descs;
            DeferCode c([&descs, this]() {
                for (int i = 0; i < descs.size(); ++i) {
                    mgr->Put(descs[i]);
                }
            });

            auto root_pd = root_node_desc.load();
            PageDesc *node_desc = root_pd;
            uint64_t versionNode = node_desc->readLockOrRestart(needRestart);
            if (needRestart || (node_desc != root_node_desc))
                goto restart;


            PageAccessor node_accessor = mgr->GetPageAccessorFromDesc(root_pd);

            NodeBase node_base = GetNodeBase(node_accessor);

            // Parent of current node
            BTreeInnerNode *parent = nullptr;
            PageDesc *parent_node_desc = nullptr;
            uint64_t versionParent = 0;

            while (node_base.type == InnerNode) {
                auto inner_node_desc = node_desc;
                auto inner = reinterpret_cast<BTreeInnerNode *>(node_desc->page);

                if (parent) {
                    assert(parent_node_desc);
                    parent_node_desc->readUnlockOrRestart(versionParent, needRestart);
                    if (needRestart)
                        goto restart;
                }

                parent_node_desc = inner_node_desc;
                parent = inner;
                versionParent = versionNode;

                auto pos = inner->lowerBound(search_key, node_accessor, node_base);
                pid_t child_pid = inner->GetKeyValue(pos, node_accessor).second;
                assert(child_pid != kInvalidPID);
                Status s = mgr->Get(child_pid, node_accessor, ConcurrentBufferManager::PageOPIntent::INTENT_READ);
                assert(s.ok());
                node_desc = node_accessor.GetPageDesc();
                descs.push_back(node_desc);

                inner_node_desc->checkOrRestart(versionNode, needRestart);
                if (needRestart)
                    goto restart;
                versionNode = node_desc->readLockOrRestart(needRestart);
                if (needRestart)
                    goto restart;

                node_base = GetNodeBase(node_accessor);
            }

            PageDesc *leaf_desc = node_desc;
            // We need exclusive access to the leaf during scan with external processor
            leaf_desc->upgradeToWriteLockOrRestart(versionNode, needRestart);
            if (needRestart) {
                goto restart;
            }
            if (parent) {
                assert(parent_node_desc);
                parent_node_desc->readUnlockOrRestart(versionParent, needRestart);
                if (needRestart) {
                    leaf_desc->writeUnlock();
                    goto restart;
                }
            }

            PageAccessor leaf_accessor = mgr->GetPageAccessorFromDesc(leaf_desc);
            BTreeLeafNode *leaf = reinterpret_cast<BTreeLeafNode *>(node_desc->page);
            unsigned pos = leaf->lowerBound(search_key, leaf_accessor, node_base);
            bool success = false;
            if (pos < node_base.count) {
                auto kv = leaf->GetKeyValue(pos, leaf_accessor);
                if (kv.first == search_key) {
                    processor(leaf->RefKeyValue(pos, leaf_accessor)->second);
                    res = BTreeOPResult::SUCCESS;
                }
            }
            leaf_desc->writeUnlock();

            return res;
        }
    }


    // processor(kv, updated, exit_now);
    // updated: an output variable indicating whether the processor updated the Value
    // exit_now: an output variable indicating whether the scan should exit immediately
    void ScanForUpdate(Key start_key, std::function<void(Value &, bool &, bool &)> processor) {
        int restartCount = 0;
        restart:
        if (restartCount++)
            Yield(restartCount);
        bool needRestart = false;

        {
            std::vector<PageDesc *> descs;
            DeferCode c([&descs, this]() {
                for (int i = 0; i < descs.size(); ++i) {
                    mgr->Put(descs[i]);
                }
            });

            auto root_pd = root_node_desc.load();
            PageDesc *node_desc = root_pd;
            uint64_t versionNode = node_desc->readLockOrRestart(needRestart);
            if (needRestart || (node_desc != root_node_desc))
                goto restart;


            PageAccessor node_accessor = mgr->GetPageAccessorFromDesc(root_pd);

            NodeBase node_base = GetNodeBase(node_accessor);

            // Parent of current node
            BTreeInnerNode *parent = nullptr;
            PageDesc *parent_node_desc = nullptr;
            uint64_t versionParent = 0;

            while (node_base.type == InnerNode) {
                auto inner_node_desc = node_desc;
                auto inner = reinterpret_cast<BTreeInnerNode *>(node_desc->page);

                if (parent) {
                    assert(parent_node_desc);
                    parent_node_desc->readUnlockOrRestart(versionParent, needRestart);
                    if (needRestart)
                        goto restart;
                }

                parent_node_desc = inner_node_desc;
                parent = inner;
                versionParent = versionNode;

                auto pos = inner->lowerBound(start_key, node_accessor, node_base);
                pid_t child_pid = inner->GetKeyValue(pos, node_accessor).second;
                assert(child_pid != kInvalidPID);
                Status s = mgr->Get(child_pid, node_accessor, ConcurrentBufferManager::PageOPIntent::INTENT_READ);
                assert(s.ok());
                node_desc = node_accessor.GetPageDesc();
                descs.push_back(node_desc);

                inner_node_desc->checkOrRestart(versionNode, needRestart);
                if (needRestart)
                    goto restart;
                versionNode = node_desc->readLockOrRestart(needRestart);
                if (needRestart)
                    goto restart;

                node_base = GetNodeBase(node_accessor);
            }

            PageDesc *leaf_desc = node_desc;
            // We need exclusive access to the leaf during scan with external processor
            leaf_desc->upgradeToWriteLockOrRestart(versionNode, needRestart);
            if (needRestart) {
                goto restart;
            }
            if (parent) {
                assert(parent_node_desc);
                parent_node_desc->readUnlockOrRestart(versionParent, needRestart);
                if (needRestart) {
                    leaf_desc->writeUnlock();
                    goto restart;
                }
            }

            BTreeLeafNode *leaf = reinterpret_cast<BTreeLeafNode *>(node_desc->page);
            unsigned pos = leaf->lowerBound(start_key, node_accessor, node_base);
            auto data_pos = const_cast<typename BTreeLeafNode::KeyValueType *>(leaf->GetKeyValues(pos,
                                                                                                  node_base.count - pos,
                                                                                                  node_accessor));
            bool exit = false;

            for (unsigned i = pos; i < node_base.count; i++) {
                bool updated = false;
                typename BTreeLeafNode::KeyValueType original_kv = data_pos[i - pos];
                processor(data_pos[i - pos].second, updated, exit);
                if (updated == true) {
                    leaf->MarkKVDirty(i, 1, node_accessor);
                    typename BTreeLeafNode::KeyValueType new_kv = data_pos[i - pos];
                    if (mgr->GetLogManager()) {
                        size_t offset =
                                reinterpret_cast<uint64_t>(data_pos + i - pos) - reinterpret_cast<uint64_t>(leaf->data);
                        size_t len = sizeof(typename BTreeLeafNode::KeyValueType);
                        mgr->GetLogManager()->LogUpdate(current_txn_id, node_desc->pid, offset, len,
                                                        (char *) &new_kv, (char *) &original_kv, GetLastLogRecordLSN());
                    }
                }
                if (exit == true) {
                    break;
                }
            }

            if (exit == false) {
                pid_t next_leaf_pid = leaf->GetNextPtr(node_accessor);
                node_desc->writeUnlock();
                for (auto pd : descs) {
                    mgr->Put(pd);
                }
                descs.clear();
                leaf_desc = nullptr;
                while (exit == false && next_leaf_pid != kInvalidPID) {
                    Status s = mgr->Get(next_leaf_pid, node_accessor,
                                        ConcurrentBufferManager::PageOPIntent::INTENT_READ);
                    assert(s.ok());
                    leaf_desc = node_accessor.GetPageDesc();
                    int restart_count = 0;
                    do {
                        needRestart = false;
                        leaf_desc->writeLockOrRestart(needRestart);
                        if (restart_count++)
                            Yield(restart_count);
                    } while (needRestart == true);
                    node_base = GetNodeBase(node_accessor);
                    leaf = reinterpret_cast<BTreeLeafNode *>(leaf_desc->page);
                    next_leaf_pid = leaf->GetNextPtr(node_accessor);
                    data_pos = const_cast<typename BTreeLeafNode::KeyValueType *>(leaf->GetKeyValues(0, node_base.count,
                                                                                                     node_accessor));

                    for (unsigned i = 0; i < node_base.count; i++) {
                        bool updated = false;
                        typename BTreeLeafNode::KeyValueType original_kv = data_pos[i];
                        processor(data_pos[i].second, updated, exit);
                        if (updated == true) {
                            leaf->MarkKVDirty(i, 1, node_accessor);
                            typename BTreeLeafNode::KeyValueType new_kv = data_pos[i];
                            if (mgr->GetLogManager()) {
                                size_t offset = reinterpret_cast<uint64_t>(data_pos + i) -
                                                reinterpret_cast<uint64_t>(leaf->data);
                                size_t len = sizeof(typename BTreeLeafNode::KeyValueType);
                                mgr->GetLogManager()->LogUpdate(current_txn_id, node_desc->pid, offset, len,
                                                                (char *) &new_kv, (char *) &original_kv,
                                                                GetLastLogRecordLSN());
                            }
                        }
                        if (exit == true) {
                            break;
                        }
                    }
                    leaf_desc->writeUnlock();
                    mgr->Put(leaf_desc);
                }
            } else {
                leaf_desc->writeUnlock();
            }
        }
    }

    pid_t GetRootPageId() {
        auto root_ph = root_node_desc.load();
        if (root_ph == nullptr) {
            return kInvalidPID;
        }
        return root_ph->pid;
    }

    struct BTreeStats {
        size_t num_leaves = 0;
        size_t num_levels = 0;
        size_t num_kvs = 0;

        std::string ToString() {
            char buf[1000];
            snprintf(buf, sizeof(buf), "num_leaves    %lu\n"
                                       "num_levels    %lu\n"
                                       "num_kvs       %lu\n"
                                       "avg_fill      %.3f\n"
                                       "leaves_bytes  %lu\n",
                     num_leaves, num_levels, num_kvs,
                     num_kvs / (num_leaves * BTreeLeafNode::kMaxEntries + 0.0),
                     num_leaves * kPageSize);
            return buf;
        }
    };

    BTreeStats GetStats() {
        int restartCount = 0;
        restart:
        if (restartCount++)
            Yield(restartCount);
        bool needRestart = false;

        BTreeStats stats;
        {
            std::vector<PageDesc *> descs;
            DeferCode c([&descs, this]() {
                for (int i = 0; i < descs.size(); ++i) {
                    mgr->Put(descs[i]);
                }
            });

            auto root_pd = root_node_desc.load();
            PageDesc *node_desc = root_pd;
            uint64_t versionNode = node_desc->readLockOrRestart(needRestart);
            if (needRestart || (node_desc != root_node_desc))
                goto restart;


            PageAccessor node_accessor = mgr->GetPageAccessorFromDesc(root_pd);

            NodeBase node_base = GetNodeBase(node_accessor);

            // Parent of current node
            BTreeInnerNode *parent = nullptr;
            PageDesc *parent_node_desc = nullptr;
            uint64_t versionParent = 0;

            while (node_base.type == InnerNode) {
                ++stats.num_levels;
                auto inner_node_desc = node_desc;
                auto inner = reinterpret_cast<BTreeInnerNode *>(node_desc->page);

                if (parent) {
                    assert(parent_node_desc);
                    parent_node_desc->readUnlockOrRestart(versionParent, needRestart);
                    if (needRestart)
                        goto restart;
                }

                parent_node_desc = inner_node_desc;
                parent = inner;
                versionParent = versionNode;

                pid_t child_pid = inner->GetKeyValue(0, node_accessor).second;
                assert(child_pid != kInvalidPID);
                Status s = mgr->Get(child_pid, node_accessor, ConcurrentBufferManager::PageOPIntent::INTENT_READ);
                assert(s.ok());
                node_desc = node_accessor.GetPageDesc();
                descs.push_back(node_desc);

                inner_node_desc->checkOrRestart(versionNode, needRestart);
                if (needRestart)
                    goto restart;
                versionNode = node_desc->readLockOrRestart(needRestart);
                if (needRestart)
                    goto restart;

                node_base = GetNodeBase(node_accessor);
            }

            ++stats.num_levels;
            PageDesc *leaf_desc = node_desc;
            // We need exclusive access to the leaf during scan with external processor
            leaf_desc->upgradeToWriteLockOrRestart(versionNode, needRestart);
            if (needRestart) {
                goto restart;
            }
            if (parent) {
                assert(parent_node_desc);
                parent_node_desc->readUnlockOrRestart(versionParent, needRestart);
                if (needRestart) {
                    leaf_desc->writeUnlock();
                    goto restart;
                }
            }
            BTreeLeafNode *leaf = reinterpret_cast<BTreeLeafNode *>(node_desc->page);
            pid_t next_leaf_pid = kInvalidPID;
            int num_kvs = 0;
            do {
                stats.num_kvs += node_base.count;
                stats.num_leaves++;
                next_leaf_pid = leaf->GetNextPtr(node_accessor);
                leaf_desc->writeUnlock();
                mgr->Put(leaf_desc);
                leaf_desc = nullptr;
                leaf = nullptr;
                if (next_leaf_pid != kInvalidPID) {
                    Status s = mgr->Get(next_leaf_pid, node_accessor,
                                        ConcurrentBufferManager::PageOPIntent::INTENT_READ);
                    assert(s.ok());
                    leaf_desc = node_accessor.GetPageDesc();
                    int restart_count = 0;
                    do {
                        needRestart = false;
                        leaf_desc->writeLockOrRestart(needRestart);
                        if (restart_count++)
                            Yield(restart_count);
                    } while (needRestart == true);
                    node_base = GetNodeBase(node_accessor);
                    leaf = reinterpret_cast<BTreeLeafNode *>(leaf_desc->page);
                }
            } while (next_leaf_pid != kInvalidPID);
            return stats;
        }
    }


private:
    ConcurrentBufferManager *mgr;
    std::atomic<PageDesc *> root_node_desc;
};

}
#endif //SPITFIRE_BTREEOLC_H
