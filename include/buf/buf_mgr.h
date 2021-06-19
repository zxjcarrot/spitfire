//
// Created by zxjcarrot on 2019-12-20.
//

#ifndef SPITFIRE
#define SPITFIRE

#include <cstdio>
#include <cstdint>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <string>
#include <functional>
#include <atomic>
#include <list>
#include <thread>
#include <condition_variable>
#include <shared_mutex>
#include <iostream>
#include <random>
#include <mutex>
#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_queue.h>
#include <murmur/MurmurHash2.h>
#include "config.h"
#include "util/status.h"
#include "util/env.h"
#include "util/sync.h"
#include "util/bitmaps.h"
#include "util/concurrent_bytell_hash_map.h"

namespace spitfire {

#define DO_ALIGN(addr, alignment) ((char *)((unsigned long)(addr) & ~((alignment) - 1)))

typedef uint64_t pid_t;
constexpr size_t kInvalidPID = std::numeric_limits<size_t>::max();
constexpr size_t kPageSizeBits = 14;
constexpr size_t kPageSize = 1 << 14;
constexpr size_t kCacheLineSize = 64;
constexpr size_t kNumCachelinesPerPage = kPageSize / kCacheLineSize;

extern thread_local size_t num_rw_ops;
struct SharedPageDesc;

class LogManager;

constexpr size_t kPageResidencyBitmapSize = (kPageSize / kNVMBlockSize + 7) / 8;
static_assert(kPageSize % (kNVMBlockSize) == 0, "kPageSize must be divisible by kNVMBlockSize");

struct NVMBlock {
    char data[kNVMBlockSize];
};

constexpr size_t kNumBlocksPerPage = kPageSize / kNVMBlockSize;
constexpr size_t kFullPageMiniPageSizeRatio = 4;
constexpr size_t kMiniPageNVMBlockNum = std::max((size_t) 1, kPageSize / kFullPageMiniPageSizeRatio / sizeof(NVMBlock));
static_assert(kMiniPageNVMBlockNum <= std::numeric_limits<uint8_t>::max() + 1, "kMiniPageNVMBlockNum out of bound");
enum PageType {
    DRAM_FULL,
    DRAM_MINI,
    NVM_FULL,
    INVALID
};

// Full Page
struct Page {
    union {;
        uint64_t lsn;
        NVMBlock blocks[kNumBlocksPerPage];
    };
};

struct MiniPage {
    union {
        uint64_t lsn;
        NVMBlock blocks[kMiniPageNVMBlockNum];
    };
};


struct OptLock {
    std::atomic<uint64_t> typeVersionLockObsolete{0b100};

    bool isLocked(uint64_t version) { return ((version & 0b10) == 0b10); }

    uint64_t readLockOrRestart(bool &needRestart) {
        uint64_t version;
        version = typeVersionLockObsolete.load(std::memory_order_relaxed);
        if (isLocked(version)) {
            needRestart = true;
            _mm_pause();
        }
        return version;
    }

    void writeLockOrRestart(bool &needRestart) {
        uint64_t version;
        version = readLockOrRestart(needRestart);
        if (needRestart)
            return;

        upgradeToWriteLockOrRestart(version, needRestart);
        if (needRestart)
            return;
    }

    void upgradeToWriteLockOrRestart(uint64_t &version, bool &needRestart) {
        if (typeVersionLockObsolete.compare_exchange_strong(version,
                                                            version + 0b10)) {
            version = version + 0b10;
        } else {
            //_mm_pause();
            needRestart = true;
        }
    }

    void downgradeToReadLock(uint64_t &version) {
        version = typeVersionLockObsolete.fetch_add(0b10);
        version += 0b10;
    }

    void writeUnlock() { typeVersionLockObsolete.fetch_add(0b10); }

    bool isObsolete(uint64_t version) { return (version & 1) == 1; }

    void checkOrRestart(uint64_t startRead, bool &needRestart) const {
        readUnlockOrRestart(startRead, needRestart);
    }

    void readUnlockOrRestart(uint64_t startRead, bool &needRestart) const {
        needRestart = (startRead != typeVersionLockObsolete.load());
    }

    void writeUnlockObsolete() { typeVersionLockObsolete.fetch_add(0b11); }
};

struct PageDesc : OptLock {
    pid_t pid;
    std::atomic<PageType> type;

    bool dirty;
    std::atomic<bool> used;
    std::atomic<int> pin;

    // For replacement policy
    PageDesc *prev;
    PageDesc *next;

    // Whether a block is resident in DRAM
    BitMap<kPageResidencyBitmapSize> residency_bitmap;
    std::atomic<int> version{0};
    BitMap<kPageResidencyBitmapSize> dirty_bitmap;

    // Stores the indices of NVM blocks resident in memory, sorted by memory address.
    // Used only by MiniPage type.
    uint8_t block_pointers[kMiniPageNVMBlockNum];
    // Number of valid pointers in block_pointers;
    // Used only by MiniPage type.
    uint8_t num_blocks;

    // Protects actual payload
    std::mutex latch;
    // points to the actual page payload
    Page *page;
    SharedPageDesc *const sph_back_pointer;

    PageDesc(pid_t pid, PageType type, SharedPageDesc *const back_pointer = nullptr) : pid(pid), used(false),
                                                                                       dirty(false), pin(1),
                                                                                       prev(nullptr), next(nullptr),
                                                                                       page(nullptr), type(type),
                                                                                       num_blocks(0),
                                                                                       sph_back_pointer(back_pointer) {}

    void ForEachBitPosInRange(size_t off, size_t size, std::function<void(int)> processor) {
        int start_bit_pos = off / kNVMBlockSize;
        int end_bit_pos = (off + size - 1) / kNVMBlockSize;
        for (; start_bit_pos <= end_bit_pos; start_bit_pos++) {
            processor(start_bit_pos);
        }
    }

    void FillBitmapByPageRange(size_t off, size_t size, BitMap<kPageResidencyBitmapSize> &bitmap) {
        ForEachBitPosInRange(off, size, [&](int bit_pos) {
            bitmap.Set(bit_pos);
        });
    }

    size_t NumUnsetBitsInBitmapByRange(size_t off, size_t size, BitMap<kPageResidencyBitmapSize> &bitmap) {
        int start_bit_pos = off / kNVMBlockSize;
        int end_bit_pos = (off + size - 1) / kNVMBlockSize;
        size_t ans = bitmap.NumZeroesInRange(start_bit_pos, end_bit_pos);
        return ans;
    }

    int CountSetBitsInBitmapByRange(size_t off, size_t size, BitMap<kPageResidencyBitmapSize> &bitmap) {
        int res = 0;
        ForEachBitPosInRange(off, size, [&](int bit_pos) {
            res += bitmap.Test(bit_pos);
        });
        return res;
    }

    void ForeachBitInBitmapByPageRangeGeneral(size_t off, size_t size, BitMap<kPageResidencyBitmapSize> &bitmap,
                                              bool target_value, std::function<void(int)> processor) {
        ForEachBitPosInRange(off, size, [&](int bit_pos) {
            if (bitmap.Test(bit_pos) == target_value) {
                processor(bit_pos);
            }
        });
    }

    void ForeachSetBitInBitmapByPageRange(size_t off, size_t size, BitMap<kPageResidencyBitmapSize> &bitmap,
                                          std::function<void(int)> processor) {
        ForeachBitInBitmapByPageRangeGeneral(off, size, bitmap, true, processor);
    }

    void ForeachUnsetBitInBitmapByPageRange(size_t off, size_t size, BitMap<kPageResidencyBitmapSize> &bitmap,
                                            std::function<void(int)> processor) {
        ForeachBitInBitmapByPageRangeGeneral(off, size, bitmap, false, processor);
    }

    bool TryEvict() {
        int exp = 0;
        return pin.compare_exchange_strong(exp, -1);
    }

    void SortMiniPageBlocks(size_t original_num_blcoks = 0) {
        // An insertion sort should suffice since kMiniPageNVMBlockNum is small
        assert(type == PageType::DRAM_MINI);
        assert(num_blocks <= kMiniPageNVMBlockNum);
        for (int i = original_num_blcoks; i < num_blocks; ++i) {
            int j = i;
            while (j > 0 && block_pointers[j - 1] > block_pointers[j]) {
                std::swap(block_pointers[j - 1], block_pointers[j]);
                std::swap(page->blocks[j - 1], page->blocks[j]);
                j = j - 1;
            }
        }
    }

    size_t PageSize() {
        switch (type) {
            case PageType::DRAM_MINI :
                return sizeof(MiniPage);
            case PageType::NVM_FULL:
            case PageType::DRAM_FULL:
                return sizeof(Page);
            default:
                return 0;
        }
    }

    bool Pin() {
        int x;
        do {
            x = pin.load();
            if (x <= -1)
                return false;
        } while (!pin.compare_exchange_strong(x, x + 1));
        return true;
    }

    void Unpin() {
        pin.fetch_add(-1);
    }

    bool Evicted() {
        return pin == -1;
    }

    int PinCount() const {
        return pin;
    }

    void ClearReferenced() {
        used = false;
    }

    void Reference() {
        used = true;
    }

    bool Referenced() const {
        return used;
    }

    void LatchShared() {
        //latch.lock_shared();
        latch.lock();
    }

    void UnlatchShared() {
        //latch.unlock_shared();
        latch.unlock();
    }

    void LatchExclusive() {
        latch.lock();
    }

    void UnlatchExclusive() {
        latch.unlock();
    }
};

class ReplacementPolicy {
public:
    virtual ~ReplacementPolicy() {}

    virtual void Add(PageDesc *p) = 0;

    virtual void Touch(PageDesc *p) = 0;

    virtual PageDesc *Evict() = 0;

    virtual std::string Name() const = 0;
};


class ClockReplacer : public ReplacementPolicy {
public:
    ClockReplacer();

    ~ClockReplacer();

    void unlink(PageDesc *p);

    void Add(PageDesc *p);

    void Touch(PageDesc *p);

    PageDesc *Evict();

    std::string Name() const;

private:
    PageDesc *clock_head;
    PageDesc *clock_hand;
};

static std::string kHeapFilePrefix = "heapfile.";


struct PidHasher {
    size_t operator()(const pid_t &pid) const {
        return pid >> kPageSizeBits;
    }
};

struct PidComparator {
    size_t operator()(const pid_t &lhs, const pid_t &rhs) {
        return lhs == rhs;
    }
};

class SSDPageManager {
public:

    SSDPageManager(const std::string &db_path, bool direct_io) : db_path(db_path), max_file_no(0),
                                                                 direct_io(direct_io), last_allocated_from(0) {}

    // Destroy all related db files under `db_path` but not deleting the directory.
    static Status DestroyDB(const std::string &db_path);

    Status Init();

    Status ReadPage(pid_t pid, Page *p);

    Status WritePage(pid_t pid, const Page *p);

    static inline pid_t MakePID(const uint32_t file_no, const uint32_t off_in_file) {
        return (((uint64_t) file_no) << 32) | off_in_file;
    }

    static inline uint32_t GetFileOff(const pid_t pid) {
        return pid & 0xffffffff;
    }

    static inline uint32_t GetFileNo(const pid_t pid) {
        return pid >> 32;
    }

    // Allocates a page from SSD heap.
    // Returns Status and pid
    Status AllocateNewPage(pid_t &pid);

    // Free a page given pid
    Status FreePage(const pid_t pid);

    bool Allocated(const pid_t pid);

    size_t CountPages();

    std::vector<pid_t> GetAllocatedPids();

private:
    static constexpr size_t kFileBitMaskSizeInBytes = kPageSize;
    //static constexpr size_t kPagesPerFile = kFileBitMaskSizeInBytes * 8 - 1;
    static constexpr size_t kPagesPerFile = 4095;
    static constexpr size_t kFileSize = (kPagesPerFile + 1) * kPageSize;
    static constexpr size_t kFileEffectiveSize = kFileSize - kPageSize;

    struct HeapFile {
        // File system path of the heap file
        const std::string heapfile_path;
        // Opened heap file descriptor
        int fd;
        // Size of the heap file, should always be kFileSize
        size_t fsize;
        // Allocation status bitmap, one bit per page.
        BitMap<kFileBitMaskSizeInBytes> *bitmap;

        int file_no;

        bool direct_io;

        HeapFile(const std::string &heapfile_path, int file_no, bool direct_io = false) : heapfile_path(heapfile_path),
                                                                                          fd(-1), fsize(0),
                                                                                          bitmap(nullptr),
                                                                                          file_no(file_no),
                                                                                          direct_io(direct_io) {}

        ~HeapFile() {
            free(bitmap);
            bitmap = nullptr;
        }

        // We place the bitmap at the end of the heap file.
        static constexpr size_t kBitmapOffset = kFileSize - kPageSize;

        Status Init();

        // Allocate one free page from the heap file
        Status AllocateOnePage(uint32_t &off);

        Status SyncBitmap(uint32_t byte_off, size_t bytes);

        Status DeallocateOnePage(uint32_t off);

        bool Allocated(uint32_t off);

        Status ReadPage(uint32_t off, void *buf, size_t buf_size);

        Status WritePage(uint32_t off, const void *buf, size_t buf_size);

        size_t CountPages();

        void GetAllocatedPids(int file_no, std::vector<pid_t> &);
    };

    Status NewHeapFile(HeapFile **heap_file);

    const std::string db_path;
    int max_file_no;
    std::vector<HeapFile *> files;
    bool direct_io;
    std::mutex mtx;
    std::atomic<int> num_files{0};
    int last_allocated_from;
};


class BufferPool {
public:
    BufferPool(const size_t capacity_in_bytes, std::function<Status(pid_t, PageDesc *&)> lower_levels_page_loader,
               std::function<Status(PageDesc *)> lower_levels_page_unloader, std::function<void *(std::size_t)> alloc,
               std::function<void(void *)> dealloc);

    // Test if page is in buffer pool. If so, pin it and return the page. Otherwise, do nothing.
    // Return true if it is the case and return the page descriptor in `ph` as well.
    // Return false if it is not and do not bring the page in.
    bool ProbeBufferPool(const pid_t pid, PageDesc *&ph);

    Status Get(const pid_t pid, PageDesc *&ph);

    Status Put(PageDesc *ph, bool dirtied = false);

    Status FlushDirtyPages();

private:
    ClockReplacer replacer;
    const size_t capacity_in_pages;
    std::unordered_map<pid_t, PageDesc *> mapping_table;
    // Loads a page from lower levels(e.g., NVM, SSD).
    // Responsible for the allocation of the PageDesc and Page.
    std::function<Status(const pid_t, PageDesc *&)> lower_levels_page_loader;
    // Writes out a page from DRAM buffer pool to lower levels.
    // Responsible for the deallocation of the PageDesc and Page.
    std::function<Status(PageDesc *)> lower_levels_page_unloader;
    std::function<void *(std::size_t)> alloc;
    std::function<void(void *)> dealloc;
};

struct PageMigrationPolicy {
    double Dw = 1;
    double Dr = 1;
    double Nr = 1;
    double Nw = 1;

    PageMigrationPolicy() {}

    inline double rand01() {
        static thread_local std::mt19937 generator;
        std::uniform_int_distribution<int> distribution(0, 100000);
        return (distribution(generator) + 0.0) / 100000;
    }

    inline bool BypassNVMDuringRead() {
        auto x = rand01();
        return x > 0 ? x > Nr : true;
    }

    inline bool BypassNVMDuringWrite() {
        static auto x = rand01();
        return x > 0 ? x > Nw : true;
    }

    inline bool BypassDRAMDuringRead() {
        auto x = rand01();
        return x > 0 ? x > Dr : true;
    }

    bool BypassDRAMDuringWrite() {
        auto x = rand01();
        return x > 0 ? x > Dw : true;
    }

    std::string ToString() const;
};


static void *block_aligned_alloc(std::size_t sz) {
    void *ptr;
    auto res = posix_memalign(&ptr, 512, sz);
    if (res != 0) {
        return nullptr;
    }
    return ptr;
}

void* nvm_page_alloc(size_t sz);
void nvm_page_free(void * p);

class NVMPageAllocator {
public:
    NVMPageAllocator(const std::string &heapfile_path, size_t n_pages) : heapfile_path(heapfile_path),
                                                                         num_pages(((n_pages + 63) / 64) * 64),
                                                                         mmap_start_addr(nullptr), bitmap(num_pages),
                                                                         last_pos(0) {}

    Status Init();

    void *AllocatePage() {
        restart:
        int p = bitmap.TakeFirstNotSet(last_pos);
        if (p == -1) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            goto restart;
        }
        last_pos.store(p);
        return static_cast<void *>(static_cast<char *>(mmap_start_addr) + p * kPageSize);
    }

    void DeallocatePage(void *p) {
        size_t pointer_diff = static_cast<size_t>(static_cast<char *>(p) - static_cast<char *>(mmap_start_addr));
        assert(pointer_diff % kPageSize == 0);
        int pos = pointer_diff / kPageSize;
        bitmap.Clear(pos);
    }

private:
    std::string heapfile_path;
    size_t num_pages;
    void *mmap_start_addr;
    AtomicBitmap bitmap;
    std::atomic<int> last_pos;
};

struct BufferPoolConfig {
    static constexpr size_t default_num_buffer_pages = 500;
    size_t dram_buf_pool_cap_in_bytes = default_num_buffer_pages * kPageSize;
    size_t nvm_buf_pool_cap_in_bytes = 3 * default_num_buffer_pages * kPageSize;
    bool enable_nvm_buf_pool = true;
    bool enable_mini_page = false;
    bool enable_hymem = false;
    bool enable_direct_io = false;
    size_t nvm_admission_set_size_limit = 10;
    std::function<void *(std::size_t)> nvm_malloc = block_aligned_alloc;
    std::function<void(void *)> nvm_free = free;
    std::function<void *(std::size_t)> dram_malloc = block_aligned_alloc;
    std::function<void(void *)> dram_free = free;
    std::string nvm_heap_file_path;
    std::string wal_file_path;
};

template<class T>
class LeakyBuffer {
public:
    LeakyBuffer(size_t capacity) : buffer(capacity) {
        for (size_t i = 0; i < buffer.size(); ++i)
            buffer[i] = nullptr;
    }

    bool Put(T *ptr) {
        for (size_t i = 0; i < buffer.size(); ++i) {
            T *exp = buffer[i].load();
            if (exp == nullptr) {
                if (buffer[i].compare_exchange_strong(exp, ptr)) {
                    return true;
                }
            }
        }
        return false;
    }

    T *Get() {
        for (size_t i = 0; i < buffer.size(); ++i) {
            T *exp = buffer[i].load();
            if (exp != nullptr) {
                // Try to take this element
                if (buffer[i].compare_exchange_strong(exp, nullptr)) {
                    return exp;
                }
            }
        }
        return nullptr;
    }

private:
    std::vector<std::atomic<T *>> buffer;
};



class AdmissionSet {
private:
    struct DLinkNode {
        pid_t key;
        DLinkNode *prev, *next;
        DLinkNode(pid_t key, DLinkNode*prev = nullptr, DLinkNode *next = nullptr): key(key), prev(prev), next(next) {}
        void remove_self() {
            prev->next = next;
            next->prev = prev;
            prev = next = nullptr;
        }
    };
public:
    AdmissionSet(const int capacity) : capacity(capacity) {
        head = new DLinkNode(kInvalidPID);
        tail = new DLinkNode(kInvalidPID);
        head->prev = nullptr;
        tail->next = nullptr;
        head->next = tail;
        tail->prev = head;
    }

    bool Exist(pid_t key) {
        std::lock_guard<std::mutex> g(latch);
        auto it = m.find(key);
        if (it == m.end()) {
            return false;
        }
        return true;
    }

    void Remove(pid_t key) {
        std::lock_guard<std::mutex> g(latch);
        auto it = m.find(key);
        if (it == m.end())
            return;
        auto n = it->second;
        n->remove_self();
        m.erase(key);
        delete n;
    }

    void Put(pid_t key) {
        std::lock_guard<std::mutex> g(latch);
        auto it = m.find(key);
        if (it == m.end()) {
            if (m.size() == capacity) {
                pop_least_recently_used();
            }
            auto n = new DLinkNode(key);
            append_after_head(n);
            m.insert(std::make_pair(key, n));
        } else {
            auto n = it->second;
            touch_node(n);
        }
    }

private:
    void touch_node(DLinkNode *n) {
        n->remove_self();
        append_after_head(n);
    }

    void append_after_head(DLinkNode* n) {
        n->prev = head;
        n->next = head->next;
        head->next = n;
        n->next->prev = n;
    }

    void pop_least_recently_used() {
        if (list_empty())
            return;
        auto last_node = tail->prev;
        last_node->remove_self();
        m.erase(last_node->key);
        delete last_node;
    }

    bool list_empty() const {
        return head->next == tail;
    }

    std::unordered_map<pid_t, DLinkNode*> m;
    DLinkNode* head, *tail;
    const int capacity;
    std::mutex latch;
};


class ConcurrentAdmissionSet {
public:
    ConcurrentAdmissionSet(size_t cap) : cap(cap), slots(cap) {
        for (size_t i = 0; i < cap; ++i) {
            slots[i].store(kInvalidPID);
        }
    }

    int InitialPosition(const pid_t pid) {
        return (pid >> kPageSizeBits) % cap;
    }

    bool Contains(const pid_t pid) {
        int p = InitialPosition(pid);
        return slots[p].load() == pid;
    }

    void Add(const pid_t pid) {
        int p = InitialPosition(pid);
        slots[p].store(pid);
    }

    void Remove(const pid_t pid) {
        int p = InitialPosition(pid);
        slots[p].store(kInvalidPID);
    }

    size_t Size() {
        size_t size = 0;
        for (int i = 0; i < cap; ++i) {
            if (slots[i] != kInvalidPID) {
                size++;
            }
        }
        return size;
    }

private:
    const size_t cap;
    std::vector<std::atomic<pid_t>> slots;
};

class BufferManager {
public:
    BufferManager(SSDPageManager *ssd_page_manager, PageMigrationPolicy policy, BufferPoolConfig config);

    ~BufferManager();

    Status Init();

    Status NewPage(pid_t &pid);

    enum PageIntent {
        INTENT_READ,
        INTENT_WRITE
    };

    Status Get(const pid_t pid, PageDesc *&ph, PageIntent intent = INTENT_READ);

    Status Put(PageDesc *ph, bool dirtied);

    void SetPageMigrationPolicy(PageMigrationPolicy policy);

    SSDPageManager *ssd_page_manager;
    PageMigrationPolicy migration_policy;
    BufferPool *DRAM_buffer_pool;
    BufferPool *NVM_buffer_pool;
    BufferPoolConfig config;
};

class ConcurrentBufferManager;

typedef uint64_t lsn_t;
typedef uint64_t txn_id_t;
static constexpr lsn_t kInvalidLSN = std::numeric_limits<lsn_t>::max();

lsn_t GetLastLogRecordLSN();
void SetLastLogRecordLSN(lsn_t lsn);

enum LogRecordType {
    BEGIN_TXN = 1,
    COMMIT_TXN,
    ABORT_TXN,
    UPDATE,
    EOL, // End Of Log
    COMPENSATION,
    CHECKPOINT,
};

enum LogFileBackendType {
    kBlockStorage,
    kByteAddressableStorage
};

// This record is placed at the front of the log file
struct MainRecord {
    // The position of the latest begin checkpoint record.
    lsn_t latest_checkpoint;
    /**
     * lsn - MainRecord.start_lsn + sizeof(MainRecord) gives the offset into the current physical log file.
     */
    lsn_t start_lsn;
};

class WritableSlice {
public:
    WritableSlice(char * start = nullptr, size_t cap = 0) :start(start), cur(start), cap(cap) {}

    Status Append(const char *buf, uint64_t size) {
        if (buf == nullptr || cur == nullptr) {
            return Status::InvalidArgument("");
        }
        Status s;
        if (cur + size > start + cap) {
            return Status::NotEnoughSpace("");
        }
        memcpy(cur, buf, size);
        cur += size;
        return s;
    }

    size_t FreeSpace() { return static_cast<size_t>(start + cap - cur); }

    char * Data() { return start; }

    size_t DataSize() const { return static_cast<size_t>(cur - start); }
private:
    char * start;
    size_t cap;
    char * cur;
};

class ReadableSlice {
public:
    ReadableSlice(const char * start = nullptr, size_t cap = 0) : start(start), cur(start), cap(cap) {}

    Status Read(char * buf, uint64_t size) {
        if (buf == nullptr || cur == nullptr) {
            return Status::InvalidArgument("");
        }
        Status s;
        if (cur + size > start + cap) {
            return Status::NotEnoughSpace("");
        }
        memcpy(buf, cur, size);
        cur += size;
        return s;
    }

    size_t BytesLeft() { return static_cast<size_t>(start + cap - cur); }
    size_t Offset() { return static_cast<size_t>(cur - start); }
private:
    const char * start;
    size_t cap;
    const char * cur;
};


class NVMLogFileBackend {
public:
    Status Seek(uint64_t off);

    Status Read(char *buf, uint64_t size);

    Status Append(const char *buf, uint64_t size);

    char * AllocatePersistentBufferAtTheEnd(uint64_t size);

    size_t FreeSpace();

    Status Init();

    size_t NextWritingPosition();

    size_t CurrentCapacity();

    NVMLogFileBackend(const std::string &file_path, size_t initial_file_capacity);
private:
    Status Extend();
    Status Shrink();
    std::string file_path;
    size_t file_capacity;
    const size_t initial_file_capacity;
    char *mmap_addr;
    char *ptr;
};


/**
 * For log record type except COMPENSATION and CHECKPOINT,
 * the record is written to stable storage conforming the following format:
 *  Type: log record type, 1B.
 *  LSN: lsn of this log record, 8B.
 *  TID: id of the transaction that created this log record, 8B.
 *  Page ID: id of the page that this log record affects, 8B.
 *  Redo Info:
 *      len: size of the Redo Info Payload, 2B.
 *      data: actual payload for redo, `len`B.
 *  Undo Info:
 *      len: size of the Undo Info Payload, 2B.
 *      data: actual payload for undo, `len`B.
 *  Prev LSN: LSN of the previous log record that was created for this transaction, 8B.
 */
class LogRecord {
public:
    virtual LogRecordType GetType() const = 0;

    virtual lsn_t GetPrevLSN() const = 0;

    virtual size_t GetIdHash() const = 0;

    virtual void Redo(ConcurrentBufferManager *buf_mgr) = 0;

    virtual void Undo(ConcurrentBufferManager *buf_mgr) = 0;

    virtual void Flush(WritableSlice & slice) = 0;

    virtual Status Parse(ReadableSlice & slice) = 0;

    virtual size_t Size() = 0;

    LogRecord() {}
};

class LogRecordBeginTxn : public LogRecord {
public:
    LogRecordType GetType() const { return BEGIN_TXN; }

    lsn_t GetPrevLSN() const { return prev_lsn; }

    txn_id_t GetTID() const { return tid; }

    size_t GetIdHash() const { return tid; }

    void Redo(ConcurrentBufferManager *buf_mgr) {}

    void Undo(ConcurrentBufferManager *buf_mgr) {}

    void Flush(WritableSlice & slice);

    Status Parse(ReadableSlice & slice);

    size_t Size();

    LogRecordBeginTxn() {}

    LogRecordBeginTxn(lsn_t prev_lsn, txn_id_t tid);

private:
    lsn_t prev_lsn;
    txn_id_t tid;
};

class LogRecordCommitTxn : public LogRecord {
public:
    LogRecordType GetType() const { return COMMIT_TXN; }

    lsn_t GetPrevLSN() const { return prev_lsn; }

    txn_id_t GetTID() const { return tid; }

    size_t GetIdHash() const { return tid; }

    void Redo(ConcurrentBufferManager *buf_mgr) {}

    void Undo(ConcurrentBufferManager *buf_mgr) {}

    void Flush(WritableSlice & slice);

    Status Parse(ReadableSlice & slice);

    size_t Size();

    LogRecordCommitTxn(lsn_t prev_lsn, txn_id_t tid);

    LogRecordCommitTxn() {}
private:
    lsn_t prev_lsn;
    txn_id_t tid;
};

class LogRecordAbortTxn : public LogRecord {
public:
    LogRecordType GetType() const { return ABORT_TXN; }

    lsn_t GetPrevLSN() const { return prev_lsn; }

    txn_id_t GetTID() const { return tid; }

    size_t GetIdHash() const { return tid; }

    void Redo(ConcurrentBufferManager *buf_mgr) {}

    void Undo(ConcurrentBufferManager *buf_mgr) {}

    void Flush(WritableSlice & slice);

    Status Parse(ReadableSlice & slice);

    size_t Size();

    LogRecordAbortTxn(lsn_t prev_lsn, txn_id_t tid);

    LogRecordAbortTxn() {}
private:
    lsn_t prev_lsn;
    txn_id_t tid;
};


class LogRecordEOL : public LogRecord {
public:
    LogRecordType GetType() const { return EOL; }

    lsn_t GetPrevLSN() const { return prev_lsn; }

    txn_id_t GetTID() const { return tid; }

    size_t GetIdHash() const { return tid; }

    void Redo(ConcurrentBufferManager *buf_mgr) {}

    void Undo(ConcurrentBufferManager *buf_mgr) {}

    void Flush(WritableSlice & slice);

    Status Parse(ReadableSlice & slice);

    size_t Size();

    LogRecordEOL(lsn_t prev_lsn, txn_id_t tid);
    LogRecordEOL(){}
private:
    lsn_t prev_lsn;
    txn_id_t tid;
};

class LogRecordUpdate : public LogRecord {
public:
    LogRecordType GetType() const { return UPDATE; }

    lsn_t GetPrevLSN() const { return prev_lsn; }

    txn_id_t GetTID() const { return tid; }

    size_t GetIdHash() const { return PidHasher()(page_id); }

    void Redo(ConcurrentBufferManager *buf_mgr) {}

    void Undo(ConcurrentBufferManager *buf_mgr) {}

    void Flush(WritableSlice & slice);

    Status Parse(ReadableSlice & slice);

    size_t Size();

    LogRecordUpdate(lsn_t prev_lsn, txn_id_t tid, pid_t page_id, uint64_t offset, uint64_t len, const char *redo,
                    const char *undo);
    LogRecordUpdate() {}

    pid_t GetPageId() { return page_id; }
private:
    lsn_t prev_lsn;
    txn_id_t tid;
    pid_t page_id;
    uint64_t offset_in_page;
    uint64_t len;
    std::shared_ptr<char> redo_info;
    std::shared_ptr<char> undo_info;
};


class LogRecordCheckpoint : public LogRecord {
public:
    LogRecordType GetType() const { return CHECKPOINT; }

    lsn_t GetPrevLSN() const { return kInvalidLSN; }

    txn_id_t GetTID() const { return 0; }

    size_t GetIdHash() const { return 0; }

    void Redo(ConcurrentBufferManager *buf_mgr) {}

    void Undo(ConcurrentBufferManager *buf_mgr) {}

    void Flush(WritableSlice & slice);

    Status Parse(ReadableSlice & slice);

    size_t Size();

    LogRecordCheckpoint(lsn_t checkpoint_lsn);

    LogRecordCheckpoint() {}
private:
    lsn_t checkpoint_lsn;
};


class LogBufferManager {
public:
    LogBufferManager() {}

    virtual ~LogBufferManager() {}

    virtual lsn_t WriteRecord(LogRecord * rec) = 0;
};


class ConcurrentLogBufferManager : public LogBufferManager {
public:
    ConcurrentLogBufferManager(LogManager * log_mgr, size_t buf_size = 2 * 1024 * 1024);

    ~ConcurrentLogBufferManager();

    lsn_t WriteRecord(LogRecord * rec) override;

private:
    char * ClaimSpace(size_t sz, lsn_t & claimed_lsn);
    char * buf;
    std::atomic<size_t> log_buffer_start_lsn;
    size_t buf_capacity;
    LogManager * log_mgr;
    DistributedCounter<128> filled_bytes;
    std::atomic<size_t> free_pos;
};

class BasicLogBufferManager : public LogBufferManager {
public:
    BasicLogBufferManager(LogManager * log_mgr, size_t buf_size = 2 * 1024 * 1024);

    ~BasicLogBufferManager();

    lsn_t WriteRecord(LogRecord * rec) override;

private:
    char * ClaimSpace(size_t sz, lsn_t & claimed_lsn);
    char * buf;
    size_t log_buffer_start_lsn;
    size_t buf_capacity;
    LogManager * log_mgr;
    size_t free_pos;
};

class ThreadLocalBasicLogBufferManager : public LogBufferManager {
public:
    ThreadLocalBasicLogBufferManager(LogManager *log_mgr, size_t buf_size = 2 * 1024 * 1024) :
            log_mgr(log_mgr), buf_size(buf_size) {}

    ~ThreadLocalBasicLogBufferManager() {}

    lsn_t WriteRecord(LogRecord *rec) override;

private:
    LogManager *log_mgr;
    size_t buf_size;
};

class LogRecordParser {
public:
    LogRecordParser(char * buf, size_t size): slice(buf, size){}

    LogRecord * ParseNext(size_t & offset);
private:
    ReadableSlice slice;
};

class LogManager {
public:
    LogManager(ConcurrentBufferManager *buf_mgr, NVMLogFileBackend *backend1, NVMLogFileBackend *backend2, size_t log_buffer_size = 32 * 1024 * 1024);

    ~LogManager();

    lsn_t LogBeginTxn(txn_id_t tid, lsn_t prev_lsn = kInvalidLSN);

    lsn_t LogUpdate(txn_id_t tid, pid_t page_id, size_t page_offset, size_t len, const char *redo_info,
                    const char *undo_info, lsn_t prev_lsn = kInvalidLSN);

    lsn_t LogCommitTxn(txn_id_t tid, lsn_t prev_lsn = kInvalidLSN);

    lsn_t LogAbortTxn(txn_id_t tid, lsn_t prev_lsn = kInvalidLSN);

    lsn_t LogEOL(txn_id_t tid, lsn_t prev_lsn = kInvalidLSN);

    size_t FlushDirtyPages(lsn_t upto_lsn);

    void Checkpoint(lsn_t checkpoint_lsn);

    void DirtyPage(pid_t page_id, lsn_t lsn);

    void WakeUpPageCleaner();

    void PersistMainRecord(NVMLogFileBackend * backend);

    lsn_t GetLastLSN(txn_id_t);

    void StartPageCleanerProcess();

    void EndPageCleanerProcess();

    Status Init();

//    void RemoveFromDirtyPageTable(pid_t page_id);
//
//    void RemoveFromTransactionTable(txn_id_t tid);

    // Return the next lsn and a new persistent log buffer for writing after incorporating the log buffer into the log.
    std::pair<lsn_t, char*> PersistLogBufferAsync(char *log_buffer, size_t log_buffer_size, size_t new_log_buffer_cap);

    lsn_t NextLSN();
private:
    static void PageCleaningProcess(LogManager * mgr);

    NVMLogFileBackend * GetCurrentLogFileBackend();

    void SwitchLogFileIfTooBig();
    std::atomic_bool stopped{false};
//    /**
//     * Transaction Table : txn_id -> <first lsn, last lsn>
//     * first lsn: LSN of the earliest log record seen for this transaction,
//     *  i.e., the earliest change done by this transaction.
//     * last lsn: LSN of the most recent log record seen for this transaction,
//     *  i.e., the latest change done by this transaction.
//     */
//    std::unordered_map<txn_id_t, std::pair<lsn_t, lsn_t>> TT;

//    /**
//     * Dirty Page Table : pid_t -> recovery lsn
//     * recovery lsn: LSN of first log record that made this page dirty,
//     *  i.e., the earliest change done to this page.
//     */
//    std::unordered_map<pid_t, lsn_t> DPT;
//    concurrent_bytell_hash_map<pid_t, lsn_t, PidHasher> C_DPT;

    // Lock that protects TT and DPT.
    std::recursive_mutex lock;

    ConcurrentBufferManager *buf_mgr;
    NVMLogFileBackend *logfile_backends[2];
    LogBufferManager * log_buffer_mgr;
    int current_backend_idx;
    size_t log_buffer_size;
    MainRecord current_main_rec;

    concurrent_bytell_hash_map<pid_t, lsn_t, PidHasher> dirty_page_table;
//    /**
//     * List of dirty pages to be flushed to database.
//     * The list is ordered by the recovery lsn of the pages.
//     */
//    std::list<pid_t> flush_list;
    std::unique_ptr<std::thread> checkpoint_process;

    std::atomic<lsn_t> next_lsn{0};
    std::atomic<lsn_t> checkpoint_safe_lsn{0};
    // Log records whose lsn <= persisted_lsn are persisted
    std::atomic<lsn_t> persisted_lsn{0};

    std::mutex cv_mtx;
    std::condition_variable page_cleaner_cv;

    static ThreadPool flusher_tp;
};

template<size_t kLogSlotSize = 512>
class PersistentLogBuffer {
public:
    PersistentLogBuffer(char * nvm_buf_start, size_t buf_capacity, lsn_t start_lsn):
        nvm_buf_start(nvm_buf_start),
        buf_capacity(buf_capacity),
        num_slots(buf_capacity / kLogSlotSize),
        num_reserved_uint64s(buf_capacity / 64),
        reserved(num_reserved_uint64s),
        buf_start_lsn(start_lsn),
        filled_bytes(0)
    {
        assert(buf_capacity % kLogSlotSize == 0);
        assert(buf_capacity % 64 == 0);
        next_slot_hints.resize(num_slots);
        for (int i = 0; i < num_slots; ++i) {
            next_slot_hints[i] = i;
        }
        memset(nvm_buf_start, 0, buf_capacity);
        NVMUtilities::persist(nvm_buf_start, buf_capacity);
    }

    Status Init() { return Status::OK(); }

    /**
     * A log record consists of one or more fix-sized log slots.
     * These slots are chained using the `slot_chain` field in
     * log slot descriptor.
     */
    struct LogSlotDescriptor {
        // Position of the next slot that belongs to the same logical log record.
        uint32_t slot_chain;
    };

    struct HeadLogSlotDescriptor {
        uint32_t slot_chain;
        /**
         * Indicates which page this log record affects.
         * A value of kInvalidPID indicates a empty slot.
         */
        pid_t page_id;
    };

    /**
     * The highest two bits of `slot_chain` indicates
     * the type of the log slot.
     */
    enum class LogSlotType {
        kEmptyLogSlot = 0,
        kHeadLogSlot,
        kMiddleLogSlot,
        kTailLogSlot
    };

    static constexpr uint32_t kLogSlotTypeBitMask = 0xC0000000;
    static constexpr uint32_t kLogSlotTypeMaskShifts = 30;
    static constexpr uint32_t kReservedLogSlotChain = ((uint32_t)(LogSlotType::kTailLogSlot) << kLogSlotTypeMaskShifts)
                                                        | 0x3FFFFFFF; // -> 0xFFFFFFFF

    struct LogSlot {
        union {
            HeadLogSlotDescriptor head_slot_desc;
            LogSlotDescriptor log_slot_desc;
            char buf[kLogSlotSize];
        };

        size_t DescriptorSize() const {
            if (Type() == LogSlotType::kHeadLogSlot) {
                return sizeof(head_slot_desc);
            } else {
                return sizeof(log_slot_desc);
            }
        }

        uint32_t Next() const {
            return head_slot_desc.slot_chain & (!kLogSlotTypeBitMask);
        }

        LogSlotType Type() const {
            int type = (head_slot_desc.slot_chain & kLogSlotTypeBitMask) >> kLogSlotTypeMaskShifts;
            return (LogSlotType)(type);
        }

        uint32_t FormSlotChain(LogSlotType type, uint32_t next) const {
            return ((uint32_t)type << kLogSlotTypeMaskShifts) | next;
        }

        // Atomically reserve the slot
        inline bool Reserve() {
            // Do a dirty read to quickly skip the taken slots.
            if (head_slot_desc.slot_chain != 0) {
                return false;
            }
            uint32_t expected = 0;
            return __sync_bool_compare_and_swap(&head_slot_desc.slot_chain,
                                                  expected, kReservedLogSlotChain);
        }

        void SetSlotChainAndPersist(LogSlotType type, uint32_t next) {
            head_slot_desc.slot_chain = FormSlotChain(type, next);
            PersistSlotChain();
        }

        void ClearSlotChain() {
            SetSlotChainAndPersist(LogSlotType::kEmptyLogSlot, 0);
        }

        void SetIdHash(pid_t pid) {
            head_slot_desc.page_id = pid;
        }

        char * PayloadBuffer() const {
            return buf + DescriptorSize();
        }

        size_t PayloadBufferSize() const {
            return kLogSlotSize - DescriptorSize();
        }

        void PersistPayload() {
            NVMUtilities::persist(PayloadBuffer(), PayloadBufferSize());
        }

        void PersistDescriptor() {
            NVMUtilities::persist(buf, DescriptorSize());
        }

        void PersistSlotChain() {
            NVMUtilities::persist((char*)&head_slot_desc.slot_chain, sizeof(uint32_t));
        }

        bool EndOfChain() const {
            return head_slot_desc.slot_chain == FormSlotChain(LogSlotType::kHeadLogSlot, 0x3FFFFFFF) ||
                    head_slot_desc.slot_chain == 0xFFFFFFFF;
        }
    };

    bool ReserveSlot(int idx) {
        auto int_idx = idx / 64;
        auto bit_idx = idx % 64;
        uint64_t word = reserved[int_idx];
        if ((1ULL << bit_idx) & word) {
            return false;
        }
        return reserved[int_idx].compare_exchange_strong(word, word | (1ULL << bit_idx));
    }

    void ClearReserved(int idx) {
        auto int_idx = idx / 64;
        auto bit_idx = idx % 64;
        uint64_t word = reserved[int_idx];
        uint64_t new_word = word & (~(1ULL << bit_idx));
        while (reserved[int_idx].compare_exchange_strong(word, new_word) == false) {
            word = reserved[int_idx];
            new_word = word & (~(1ULL << bit_idx));
        }
    }

    LogSlot* At(int idx) {
        return ((LogSlot*)nvm_buf_start) + idx;
    }

    /**
     * Persist the log record.
     * Return the offset of the first log slot for the record.
     * Return -1 if there is not enough space left for the record.
     */
    int32_t PersistLogRecord(size_t id_hash, char * record_buf, size_t buf_size) {
        thread_local static std::vector<int> slot_indices;
        slot_indices.clear();
        AllocateSlots(buf_size, id_hash, slot_indices);
        if (slot_indices.empty()) {
            return -1;
        }
        char * record_buf_p = record_buf;
        size_t buf_size_left = buf_size;
        for (size_t i = 0; i < slot_indices.size(); ++i) {
            char * slot_buf_start;
            size_t slot_buf_size;
            if (i == 0) {
                slot_buf_start = At(slot_indices[i])->buf + sizeof(HeadLogSlotDescriptor);
                slot_buf_size = kLogSlotSize - sizeof(HeadLogSlotDescriptor);
            } else {
                slot_buf_start = At(slot_indices[i])->buf + sizeof(LogSlotDescriptor);
                slot_buf_size = kLogSlotSize - sizeof(LogSlotDescriptor);
            }
            size_t copy_size = std::min(buf_size_left, slot_buf_size);
            memcpy(slot_buf_start, record_buf_p, copy_size);
            buf_size_left -= copy_size;
        }

        for (int i = (int)slot_indices.size() - 1; i >= 0; --i) {
            if (i == 0) {
                At(slot_indices[i])->SetIdHash(id_hash);
                uint32_t next = 0x3FFFFFFF;
                if (slot_indices.size() > 1) {
                    next = slot_indices[i + 1];
                }
                At(slot_indices[i])->SetSlotChainAndPersist(LogSlotType::kHeadLogSlot, next);
            } else if (i == slot_indices.size() - 1) {
                At(slot_indices[i])->SetSlotChainAndPersist(LogSlotType::kTailLogSlot, 0);
            } else {
                uint32_t next = slot_indices[i + 1];
                At(slot_indices[i])->SetSlotChainAndPersist(LogSlotType::kMiddleLogSlot, 0);
            }
        }
        return (int32_t) ((char*)At(slot_indices[0]) - nvm_buf_start);
    }

    char* Data() { return nvm_buf_start; }

    lsn_t GetStartLSN() const { return buf_start_lsn; }

    ~PersistentLogBuffer() {
        //fprintf(stderr, "Avg probe length %d\n", probes / (allocs + 1));
    }

private:
    //int allocs = 0;
    //int probes = 0;
    std::vector<int> AllocateSlots(size_t size, size_t id_hash, std::vector<int> & res) {
        int start_idx = MurmurHash2(&id_hash, sizeof(size_t), 0) % num_slots;
        int steps = 0;
        int32_t left = size;
        const int kProbeLimit = this->num_slots / 5;
        //allocs++;
        {
            // Fast path utilizing the hints
            int i = start_idx;
            do {
                while (i != next_slot_hints[i] && steps < kProbeLimit) {
                    i = next_slot_hints[i];
                    ++steps;
                }
                ++steps;
                //++probes;
                if (ReserveSlot(i)) {
                    res.push_back(i);
                    if (res.size() == 1) {
                        left -= kLogSlotSize - sizeof(HeadLogSlotDescriptor);
                    } else {
                        left -= kLogSlotSize - sizeof(LogSlotDescriptor);
                    }
                }
                i = (i + 1)  == num_slots ? 0 : i + 1;
                if (steps > kProbeLimit) {
                    break;
                }
            } while (i != start_idx && left > 0);
            if (left <= 0) {
                next_slot_hints[start_idx] = i;
            }
        }
        if (left > 0 || steps > num_slots / 5) {
            // Not enough space left in this buffer to meet the size requirement,
            // clear the reserved slots.
            for (int i = 0; i < res.size(); ++i) {
                //At(res[i])->ClearSlotChain();
                ClearReserved(res[i]);
            }
            res.clear();
            return res;
        }
//        if (left > 0){
//            steps = 0;
//            left = size;
//            int i = start_idx;
//            do {
//                if (At(i)->Reserve()) {
//                    res.push_back(i);
//                    if (res.size() == 1) {
//                        left -= kLogSlotSize - sizeof(HeadLogSlotDescriptor);
//                    } else {
//                        left -= kLogSlotSize - sizeof(LogSlotDescriptor);
//                    }
//                }
//                i = (i + 1) % num_slots;
//                steps++;
//                if (steps > num_slots / 5) {
//                    break;
//                }
//            } while (i != start_idx && left > 0);
//            if (left <= 0) {
//                next_slot_hints[start_idx] = i;
//            }
//        }
//
//        if (left > 0 || steps > num_slots / 5) {
//            // Not enough space left in this buffer to meet the size requirement,
//            // clear the reserved slots.
//            for (int i = 0; i < res.size(); ++i) {
//                At(res[i])->ClearSlotChain();
//            }
//            res.clear();
//        }
        return res;
    }

    char * nvm_buf_start;
    size_t buf_capacity;
    size_t num_slots;
    size_t num_reserved_uint64s;
    std::vector<std::atomic<uint64_t>> reserved;
    std::vector<int> next_slot_hints;
    lsn_t buf_start_lsn;
    DistributedCounter<128> filled_bytes;
};


class PersistentLogBufferManager : public LogBufferManager {
public:
    PersistentLogBufferManager(LogManager * log_mgr, size_t buf_size = 2 * 1024 * 1024);

    ~PersistentLogBufferManager() {}

    lsn_t WriteRecord(LogRecord * rec) override;

private:
    char * buf;
    size_t buf_size;
    LogManager * log_mgr;
    PersistentLogBuffer<> * persistent_log_buffer;
    std::mutex log_buffer_init_mtx;
    #define BUFFER_SWITCH_STATUS_NORMAL 0
    #define BUFFER_SWITCH_STATUS_PRESWITCH 1
    #define BUFFER_SWITCH_STATUS_SWITCHING 2
    std::atomic<int> buffer_switch_status;
};

struct SharedPageDesc {
    // Protects accesses to the following fields
    std::mutex m;
    // Latches that guarantee the consistencies of pages during IO
    std::mutex dram_latch;
    std::mutex nvm_latch;
    std::mutex ssd_latch;

    PageDesc *dram_ph;
    PageDesc *nvm_ph;

    SharedPageDesc(PageDesc *dram_ph = nullptr, PageDesc *nvm_ph = nullptr) : dram_ph(dram_ph), nvm_ph(dram_ph) {}
};

class ConcurrentClockReplacer {
private:
    const int64_t cap_in_bytes;
    const size_t n_pages;
    std::vector<std::atomic<PageDesc *>> pool;
    std::atomic<int> free;
    std::atomic<int> clock_hand;
    // current_bytes_in_buffer <= cap_in_bytes
    DistributedCounter<32> current_bytes_in_buffer;
    RefManager *epoch_manager;
    static thread_local ThreadRefHolder *pd_reader_ref;
    bool evict_dirty = true;
    ConcurrentBufferManager * buf_mgr;
public:
    ConcurrentClockReplacer(const int64_t capacity_in_bytes, const size_t page_size, RefManager *epoch_manager, ConcurrentBufferManager * buf_mgr);


    void EvictPurgablePages(const std::unordered_set<pid_t> &evict_set);

    void SetEvictDirty(bool evict_dirty) { this->evict_dirty = evict_dirty; }
    void Clear();

    void AssertNotInBuffer(PageDesc *entry);

    ConcurrentClockReplacer(const ConcurrentClockReplacer &) = delete;

    ConcurrentClockReplacer(ConcurrentClockReplacer &&) = delete;

    PageDesc *Add(PageDesc *entry, int64_t page_size);

    PageDesc *Swap(PageDesc *entry, int64_t page_size);

    void AddCurrentBytesInBuf(int size) { current_bytes_in_buffer += size; }

    void EnsureSpace(std::vector<pid_t> &evicted_pids);

    void MoveClockHand(int curr, int start);

    std::string GetStats() const;

    std::unordered_set<pid_t> GetManagedPids() const;
};

class ConcurrentHashCounterArray {
public:
    ConcurrentHashCounterArray(size_t sz) : counters(sz), capacity(sz) {}

    size_t GetHashPos(size_t h) const { return h % capacity; }

    bool TryLock(size_t pos) {
        int zero = 0;
        return counters[pos].compare_exchange_strong(zero, 1);
    }

    void Unlock(size_t pos) {
        auto s = counters[pos].fetch_sub(1);
        assert(s == 1);
    }

    bool Check(size_t pos) { return counters[pos].load(); }

    std::vector<std::atomic<int>> counters;
    size_t capacity;
};


class MVCCPurger {
public:
    MVCCPurger(ConcurrentBufferManager * buf_mgr): buf_mgr(buf_mgr) {}

    void StartPurgerThread();

    void EndPurgerThread();

private:
    static void PurgeProcess(MVCCPurger * purger);

    ConcurrentBufferManager * buf_mgr;
    std::atomic<bool> stopped{false};
    std::unique_ptr<std::thread> purge_thread;
};


class ConcurrentBufferManager {
public:
    ConcurrentBufferManager(SSDPageManager *ssd_page_manager, PageMigrationPolicy policy, BufferPoolConfig config);

    ~ConcurrentBufferManager();

    enum PageOPIntent {
        INTENT_READ,
        INTENT_WRITE,
        INTENT_READ_FULL,
        INTENT_WRITE_FULL,
    };

    class PageAccessor {
    public:
        PageAccessor(ConcurrentBufferManager *mgr = nullptr, SharedPageDesc *sph = nullptr, PageDesc *ph = nullptr,
                     PageType cur_type = PageType::INVALID);

        // Make sure the page contents ranging from `off` to `off`+`size` are available for read.
        // Return the beginning of the range.
        Slice PrepareForRead(uint32_t off, size_t size);

        // Make sure the page contents from offsets `off` to `off`+`size` are available for write.
        // Return the beginning of the range.
        Slice PrepareForWrite(uint32_t off, size_t size);

        PageDesc *GetPageDesc() { return ph; }

        void MarkDirty(uint32_t off, size_t size);

        ~PageAccessor() {
            FinishAccess();
            LogWrite();
        }

        void FinishAccess();

        ConcurrentBufferManager * GetBufferManager() { return mgr; }
    private:
        void ClearLoggingStates();

        void LogWrite();

        friend class ConcurrentBufferManager;

        Slice PrepareForAccess(uint32_t off, size_t size, PageOPIntent intent);

        SharedPageDesc *shared_ph;
        PageDesc *ph;
        PageType cur_type;
        ConcurrentBufferManager *mgr;
        bool accessed = false;
        char * redo_buf = nullptr;
        char * undo_buf = nullptr;
        char * undo_origin = nullptr;
        size_t redo_undo_size = 0;
        size_t redo_undo_page_off = 0;
        bool dirtied = false;
    };

    Status Init();

    Status InitLogging();

    Status NewPage(pid_t &pid);

    Status FreePage(const pid_t &pid);

    std::vector<pid_t> GetManagedPids();

    size_t CountPages();

    std::string ReplacerStats() const;

    struct Stats {
        static constexpr int kBuckets = 128;
        DistributedCounter<kBuckets> bytes_copied_nvm_to_dram;
        DistributedCounter<kBuckets> cycles_spent_nvm_to_dram;
        DistributedCounter<kBuckets> bytes_copied_dram_to_nvm;
        DistributedCounter<kBuckets> cycles_spent_dram_to_nvm;
        DistributedCounter<kBuckets> bytes_copied_nvm_to_ssd;
        DistributedCounter<kBuckets> cycles_spent_nvm_to_ssd;
        DistributedCounter<kBuckets> bytes_copied_dram_to_ssd;
        DistributedCounter<kBuckets> cycles_spent_dram_to_ssd;
        DistributedCounter<kBuckets> bytes_copied_ssd_to_dram;
        DistributedCounter<kBuckets> cycles_spent_ssd_to_dram;
        DistributedCounter<kBuckets> bytes_copied_ssd_to_nvm;
        DistributedCounter<kBuckets> cycles_spent_ssd_to_nvm;
        DistributedCounter<kBuckets> bytes_direct_write_nvm;
        DistributedCounter<kBuckets> bytes_allocated_dram;
        DistributedCounter<kBuckets> bytes_allocated_nvm;
        DistributedCounter<kBuckets> dram_evictions;
        DistributedCounter<kBuckets> nvm_evictions;
        DistributedCounter<kBuckets> mini_page_promotions;
        DistributedCounter<kBuckets> hits_on_dram;
        DistributedCounter<kBuckets> hits_on_nvm;
        DistributedCounter<kBuckets> buf_gets;
        DistributedCounter<kBuckets> ssd_reads;
        DistributedCounter<kBuckets> ssd_writes;
        ConcurrentBufferManager *buf_mgr;

        Stats(ConcurrentBufferManager *buf_mgr);

        std::string ToString() const;

        void Clear();
    };

    void EvictPurgablePages(const std::unordered_set<pid_t> &evict_set);

    Status Get(const pid_t pid, PageDesc *&ph, PageOPIntent intent = INTENT_READ);

    Status Get(const pid_t pid, PageAccessor &page_accessor, PageOPIntent intent = INTENT_READ);

    PageAccessor GetPageAccessorFromDesc(PageDesc *ph);

    Status Flush(const pid_t pid, bool forced = false, bool keep_in_buffer = false);

    Status Put(PageDesc *ph, bool dirtied);

    Status Put(PageDesc *ph);

    void WaitForPageCleanerSignal(uint32_t timeout_in_microseconds);

    void CleanPageReady();

    void SetPageMigrationPolicy(PageMigrationPolicy policy);

    std::string GetStatsString() const;

    Stats GetStats() const { return *stat; }

    void ClearStats();

    BufferPoolConfig & GetConfig() { return config; }

    LogManager* GetLogManager() { return log_manager; }

    void SetNVMSSDMode() { this->NVM_SSD_MODE = true; }
    bool IsNVMSSDMode() { return this->NVM_SSD_MODE; }

    void EndPurging();
private:

    Status Get(const pid_t pid, SharedPageDesc *&shared_ph, PageDesc *&ph, PageOPIntent intent = INTENT_READ);

    friend class PageAccessor;

    // Fill the dram page at offset `off` with `size` bytes from lower level buffers.
    // Assume shared_ph->m is taken.
    Status FillDRAMPage(SharedPageDesc *shared_ph, PageDesc *dram_ph, size_t off, size_t size,
                        std::vector<pid_t> &evicted_pids, size_t num_blocks_to_fill);

    Status PromoteMiniPage(PageDesc *dram_ph, std::vector<pid_t> &evicted_pids);

    SSDPageManager *ssd_page_manager;
    PageMigrationPolicy migration_policy;
    BufferPoolConfig config;
    bool NVM_SSD_MODE = false;
    ConcurrentClockReplacer dram_buf_pool_replacer;
    ConcurrentClockReplacer nvm_buf_pool_replacer;

    // To reduce allocation cost, we designed concurrent leaky buffers to cache the allocated pages.
    LeakyBuffer<Page> dram_page_leaky_buffer;
    LeakyBuffer<Page> nvm_page_leaky_buffer;


    ConcurrentHashCounterArray pid_in_flush;

    template<typename K>
    struct PidHashCompare {
        static size_t hash(const K &key) { return key >> kPageSizeBits; }

        static bool equal(const K &key1, const K &key2) { return (key1 == key2); }
    };

    //tbb::concurrent_hash_map<pid_t, SharedPageDesc *, PidHashCompare<pid_t>> mapping_table;
    //CuckooMap<pid_t, SharedPageDesc*, PidHasher, PidComparator> mapping_table;
    concurrent_bytell_hash_map<pid_t, SharedPageDesc *, PidHasher> mapping_table;
    AdmissionSet admission_set;
    NVMPageAllocator * nvm_page_allocator;
    LogManager * log_manager;
    MVCCPurger * mvcc_purger;
    static RefManager page_ref_manager;
    static RefManager shared_pd_ref_manager;
    static thread_local ThreadRefHolder page_payload_ref;
    static thread_local ThreadRefHolder shared_pd_ref;
    std::condition_variable page_cleaner_signal_cv;
    std::mutex page_cleaner_signal_cv_mtx;
    Stats *stat;
};
}

#endif //DPTREE_BUF_MGR_H
