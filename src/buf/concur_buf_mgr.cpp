//
// Created by zxjcarrot on 2019-12-28.
//

#include <tbb/concurrent_hash_map.h>
#include "util/crc32c.h"
#include "util/logger.h"
#include "buf/buf_mgr.h"
#include <sstream>

namespace spitfire {

// Thread local container for storing pids to be flushed.
// This reduces the number of vector allocations which is very
// frequent in PrepareAccess method of PageAccessor.
//static thread_local std::vector<pid_t> global_evicted_pids;
RefManager ConcurrentBufferManager::page_ref_manager(1024);
RefManager ConcurrentBufferManager::shared_pd_ref_manager(1024);

static bool debug_pid_access_skew = false;
static concurrent_bytell_hash_map<pid_t, int, PidHasher> debug_pid_access_freq;

static std::string DebugPidAccessSkewToString() {
    static constexpr size_t kTop = 1000;
    size_t total_accesses = 0;
    std::vector<std::pair<pid_t, int>> freqs;
    debug_pid_access_freq.Iterate([&](const pid_t pid, const int cnt) {
        freqs.push_back(std::make_pair(pid, cnt));
        total_accesses += cnt;
    });
    std::string res = "Top " + std::to_string(kTop) + " Pid Access Frequencies, " + std::to_string(debug_pid_access_freq.Size())+"Total: " + std::to_string(total_accesses) + "\n";
    std::sort(freqs.begin(), freqs.end(),
            [](const std::pair<pid_t, int> & p1, const std::pair<pid_t, int> & p2) {
        return p1.second > p2.second || p1.second == p2.second && p1.first < p2.first;
    });
    for (size_t i = 0; i < std::min(freqs.size(), kTop); ++i) {
        res += std::to_string(freqs[i].first) + " : " + std::to_string(freqs[i].second) + "\n";
    }
    return res;
}
thread_local ThreadRefHolder ConcurrentBufferManager::page_payload_ref;
thread_local ThreadRefHolder ConcurrentBufferManager::shared_pd_ref;
thread_local ThreadRefHolder *ConcurrentClockReplacer::pd_reader_ref = new ThreadRefHolder;
thread_local size_t num_rw_ops = 0;

ConcurrentBufferManager::ConcurrentBufferManager(SSDPageManager *ssd_page_manager, PageMigrationPolicy policy,
                                                 BufferPoolConfig config)
        : ssd_page_manager(ssd_page_manager), migration_policy(policy), config(config),
          dram_buf_pool_replacer(config.dram_buf_pool_cap_in_bytes,
                                 config.enable_mini_page ? sizeof(MiniPage) : kPageSize, &page_ref_manager, this),
          nvm_buf_pool_replacer(config.nvm_buf_pool_cap_in_bytes, kPageSize, &page_ref_manager, this), stat(new Stats(this)),
          pid_in_flush(config.dram_buf_pool_cap_in_bytes / (config.enable_mini_page ? sizeof(MiniPage) : kPageSize)),
          dram_page_leaky_buffer(32), nvm_page_leaky_buffer(32),
          admission_set(config.enable_hymem ? config.nvm_admission_set_size_limit * 1.3 : 0),
          nvm_page_allocator(nullptr), log_manager(nullptr), mvcc_purger(nullptr) {}


ConcurrentBufferManager::~ConcurrentBufferManager() {
    if (log_manager) {
        log_manager->EndPageCleanerProcess();
        delete log_manager;
        log_manager = nullptr;
    }

    EndPurging();

    // Flush every dirty page
    std::vector<pid_t> pids;
    dram_buf_pool_replacer.Clear();
    nvm_buf_pool_replacer.Clear();
    mapping_table.Iterate([&](const pid_t &pid, SharedPageDesc *const &) {
        pids.push_back(pid);
    });
    for (auto pid: pids) {
        Flush(pid, true);
    }
    // Flush them again because some of the pages reached only in NVM during the first round of flush.
    for (auto pid: pids) {
        Flush(pid, true);
    }

    Page *p = nullptr;
    while ((p = nvm_page_leaky_buffer.Get()) != nullptr) {
        config.nvm_free(p);
    }

    while ((p = dram_page_leaky_buffer.Get()) != nullptr) {
        config.dram_free(p);
    }
    if (nvm_page_allocator != nullptr) {
        delete nvm_page_allocator;
        nvm_page_allocator = nullptr;
    }


}

void ConcurrentBufferManager::EndPurging() {
    if (mvcc_purger != nullptr) {
        mvcc_purger->EndPurgerThread();
        delete mvcc_purger;
        mvcc_purger = nullptr;
    }
}

std::string ConcurrentBufferManager::ReplacerStats() const {
    std::string stats = "----dram_buf_replacer----\n";
    stats += dram_buf_pool_replacer.GetStats();
    stats += "----dram_buf_replacer----\n";
    if (config.enable_nvm_buf_pool) {
        stats += "\n----nvm_buf_replacer----\n";
        stats += nvm_buf_pool_replacer.GetStats();
        stats += "----nvm_buf_replacer----\n";
    }
    return stats;
}

Status ConcurrentBufferManager::InitLogging() {
    return Status::OK();
}

Status ConcurrentBufferManager::Init() {
    fprintf(stderr, "ConcurrentBufferManager::Init nvm_heap_file_path %s\n", config.nvm_heap_file_path.c_str());
    if (!config.nvm_heap_file_path.empty()) {
        assert(nvm_page_allocator == nullptr);
        size_t required_num_pages = config.nvm_buf_pool_cap_in_bytes / kPageSize;
        
        size_t padded_num_pages = required_num_pages * 1.1; // Give 10% more overflow pages.
        nvm_page_allocator = new NVMPageAllocator(config.nvm_heap_file_path, padded_num_pages);
        Status s = nvm_page_allocator->Init();
        if (!s.ok())
            fprintf(stderr, "ConcurrentBufferManager::Init nvm_page_allocator->Init() failed: %s\n", s.ToString().c_str());
        if (!s.ok())
            return s;
        config.nvm_malloc = [&](size_t sz) -> void * {
            assert(sz == kPageSize);
            return nvm_page_allocator->AllocatePage();
        };
        config.nvm_free = [&](void *p) {
            nvm_page_allocator->DeallocatePage(p);
        };
    }

    fprintf(stderr, "ConcurrentBufferManager::Init wal_file_path %s\n", config.wal_file_path.c_str());
    if (!config.wal_file_path.empty()) {
        fprintf(stderr, "logging module not initialized yet\n");
        auto backend1 = new NVMLogFileBackend(config.wal_file_path + ".1",
                                              128 * 1024 * 1024UL);
        Status s = backend1->Init();
        if (!s.ok())
            return s;
        auto backend2 = new NVMLogFileBackend(config.wal_file_path + ".2",
                                              128 * 1024 * 1024UL);
        s = backend2->Init();
        if (!s.ok())
            return s;
        log_manager = new LogManager(this, backend1, backend2);
        s = log_manager->Init();
        if (!s.ok())
            return s;
        dram_buf_pool_replacer.SetEvictDirty(false);
        if (NVM_SSD_MODE) {
            dram_buf_pool_replacer.SetEvictDirty(true);
            assert(config.enable_nvm_buf_pool == false);
        }
        if (config.enable_nvm_buf_pool) {
            nvm_buf_pool_replacer.SetEvictDirty(true);
        }
        fprintf(stderr, "logging module initialized\n");
    }
    //mvcc_purger = new MVCCPurger(this);
    //mvcc_purger->StartPurgerThread();
    return Status::OK();
}

Status ConcurrentBufferManager::NewPage(pid_t &pid) {
    return ssd_page_manager->AllocateNewPage(pid);
}

Status ConcurrentBufferManager::FreePage(const pid_t &pid) {
    return ssd_page_manager->FreePage(pid);
}

ConcurrentBufferManager::Stats::Stats(ConcurrentBufferManager *buf_mgr) {
    this->buf_mgr = buf_mgr;
    Clear();
}

std::string ConcurrentBufferManager::Stats::ToString() const {
    char buf[100000];
    float _1MB = 1024 * 1024;
    constexpr float CYCLES_PER_SEC = 3 * 1024ULL * 1024 * 1024;
    auto num_pages_on_disk = buf_mgr->ssd_page_manager->CountPages();
    snprintf(buf, sizeof(buf),
             "nvm_to_dram              %.3fMB\n"
             "dram_to_nvm              %.3fMB\n"
             "nvm_to_ssd               %.3fMB\n"
             "dram_to_ssd              %.3fMB\n"
             "ssd_to_dram              %.3fMB\n"
             "ssd_to_nvm               %.3fMB\n"
             "direct_nvm_write         %.3fMB\n"
             "data_allocated_dram      %.3fMB\n"
             "data_allocated_nvm       %.3fMB\n"
             "nvm_to_dram_time         %.3fs\n"
             "dram_to_nvm_time         %.3fs\n"
             "nvm_to_ssd_time          %.3fs\n"
             "dram_to_ssd_time         %.3fs\n"
             "ssd_to_dram_time         %.3fs\n"
             "ssd_to_nvm_time          %.3fs\n"
             "dram_evictions           %ld\n"
             "nvm_evictions            %ld\n"
             "mini_page_promotions     %ld\n"
             "hits_on_dram             %ld\n"
             "hits_on_nvm              %ld\n"
             "buf_gets                 %ld\n"
             "ssd_reads                %ld\n"
             "ssd_writes               %ld\n"
             "num_pages_allocated      %ld\n"
             "database_size            %.3fMB\n"
             "%s",
             bytes_copied_nvm_to_dram.load() / _1MB,
             bytes_copied_dram_to_nvm.load() / _1MB,
             bytes_copied_nvm_to_ssd.load() / _1MB,
             bytes_copied_dram_to_ssd.load() / _1MB,
             bytes_copied_ssd_to_dram.load() / _1MB,
             bytes_copied_ssd_to_nvm.load() / _1MB,
             bytes_direct_write_nvm.load() / _1MB,
             bytes_allocated_dram.load() / _1MB,
             bytes_allocated_nvm.load() / _1MB,
             cycles_spent_nvm_to_dram.load() / CYCLES_PER_SEC,
             cycles_spent_dram_to_nvm.load() / CYCLES_PER_SEC,
             cycles_spent_nvm_to_ssd.load() / CYCLES_PER_SEC,
             cycles_spent_dram_to_ssd.load() / CYCLES_PER_SEC,
             cycles_spent_ssd_to_dram.load() / CYCLES_PER_SEC,
             cycles_spent_ssd_to_nvm.load() / CYCLES_PER_SEC,
             dram_evictions.load(),
             nvm_evictions.load(),
             mini_page_promotions.load(),
             hits_on_dram.load(),
             hits_on_nvm.load(),
             buf_gets.load(),
             ssd_reads.load(),
             ssd_writes.load(),
             num_pages_on_disk,
             num_pages_on_disk * kPageSize / _1MB,
             debug_pid_access_skew ? DebugPidAccessSkewToString().c_str():"");
    return buf;
}

void ConcurrentBufferManager::Stats::Clear() {
    bytes_copied_nvm_to_dram.store(0);
    cycles_spent_nvm_to_dram.store(0);
    bytes_copied_dram_to_nvm.store(0);
    cycles_spent_dram_to_nvm.store(0);
    bytes_copied_nvm_to_ssd.store(0);
    cycles_spent_nvm_to_ssd.store(0);
    bytes_copied_dram_to_ssd.store(0);
    cycles_spent_dram_to_ssd.store(0);
    bytes_copied_ssd_to_dram.store(0);
    cycles_spent_ssd_to_dram.store(0);
    bytes_copied_ssd_to_nvm.store(0);
    cycles_spent_ssd_to_nvm.store(0);
    bytes_direct_write_nvm.store(0);
    bytes_allocated_dram.store(0);
    bytes_allocated_nvm.store(0);
    dram_evictions.store(0);
    nvm_evictions.store(0);
    mini_page_promotions.store(0);
    hits_on_dram.store(0);
    hits_on_nvm.store(0);
    buf_gets.store(0);
    ssd_reads.store(0);
    ssd_writes.store(0);
}

std::vector<pid_t> ConcurrentBufferManager::GetManagedPids() {
    return ssd_page_manager->GetAllocatedPids();
}

size_t ConcurrentBufferManager::CountPages() {
    return ssd_page_manager->CountPages();
}

Status ConcurrentBufferManager::Get(const pid_t pid, SharedPageDesc *&shared_ph, PageDesc *&ph, PageOPIntent intent) {
    if (pid == kInvalidPID) {
        return Status::NotFound("");
    }
    auto pid_hash_pos = pid_in_flush.GetHashPos(PidHasher()(pid));
    static thread_local std::vector<pid_t> evicted_pids;
    evicted_pids.clear();
    DeferCode c([&, this]() {
        for (auto pid : evicted_pids) {
            this->Flush(pid);
        }
        evicted_pids.clear();
    });
    shared_pd_ref.Register(&shared_pd_ref_manager);
    restart:
    this->stat->buf_gets++;
    {
        shared_pd_ref.Leave();
        // loop goto restart until pid is not in-flush
        ThreadRefGuard guard(shared_pd_ref);
        if (pid_in_flush.Check(pid_hash_pos)) {
            shared_pd_ref.Leave();
            // pid might be in flush, retry later
            std::this_thread::sleep_for(std::chrono::microseconds(1));
            goto restart;
        }
        SharedPageDesc *sph = nullptr;
        bool found = mapping_table.Find(pid, sph);
        if (found == false) {
            sph = new SharedPageDesc;
            assert(sph != nullptr);
            shared_pd_ref.SetValue((uint64_t) sph);
            if (mapping_table.Insert(pid, sph) == false) {
                delete sph;
                sph = nullptr;
                goto restart;
            }
        } else {
            shared_pd_ref.SetValue((uint64_t) sph);
        }
        assert(sph != nullptr);
        shared_ph = sph;
        {
            if (debug_pid_access_skew){
                debug_pid_access_freq.LockOnKey(pid);
                int cnt = 0;
                debug_pid_access_freq.FindUnsafe(pid, cnt);
                debug_pid_access_freq.UpsertIfUnsafe(pid, cnt + 1, [](std::pair<const pid_t, const int>) ->bool {return true;});
                debug_pid_access_freq.UnlockOnKey(pid);
            }
            // Take the per page mutex
            //LockGuard g(&sph->m);
            //LockGuard g_dram_latch(&sph->dram_latch);
            {
                // Case 1: page in DRAM buffer pool but not in NVM buffer pool
                // Case 2: page in DRAM buffer pool and in NVM buffer pool
                // In those two cases, we simply return the DRAM page.
                auto dram_ph = sph->dram_ph;
                if (dram_ph) {
                    page_payload_ref.Enter();
                    page_payload_ref.SetValue((uint64_t) dram_ph);
                    // If the page is already in DRAM buffer pool, return it.
                    ph = dram_ph;
                    if (!ph->Pin()) { // Evicted, maybe the page is being written down to lower levels, retry later.
                        //g_dram_latch.Unlock();
                        //g.Unlock();
                        page_payload_ref.Leave();
                        std::this_thread::sleep_for(std::chrono::microseconds(1));
                        goto restart;
                    }
                    ph->Reference();
                    assert(ph->PinCount() >= 1);
                    stat->hits_on_dram++;
                    page_payload_ref.Leave();
                    return Status::OK();
                }
            }
            bool bypass_dram = false;
            bool fill_dram_from_ssd_page = false;
            if (config.enable_nvm_buf_pool) {
                LockGuard g_nvm_latch(&sph->nvm_latch);
                auto nvm_ph = sph->nvm_ph;
                bool in_NVM_buf_pool = nvm_ph != nullptr;
                // Case 3: Page not in DRAM buffer pool
                // In this case, we consult migration policy to determine
                // if we should bypass DRAM and directly return reference to the page in NVM.
                bypass_dram = !config.enable_hymem && // If ttb enabled, we never bypass DRAM.
                              ((intent == INTENT_READ || intent == INTENT_READ_FULL) &&
                               migration_policy.BypassDRAMDuringRead() ||
                               (intent == INTENT_WRITE || intent == INTENT_WRITE_FULL) &&
                               migration_policy.BypassDRAMDuringWrite());
                if (!in_NVM_buf_pool) {
                    fill_dram_from_ssd_page = 
                        ((intent == INTENT_READ || intent == INTENT_READ_FULL) 
                            && migration_policy.BypassNVMDuringRead()) ||
                        ((intent == INTENT_WRITE || intent == INTENT_WRITE_FULL) 
                            && migration_policy.BypassNVMDuringWrite());
                }
                if (in_NVM_buf_pool && bypass_dram) {
                    ph = nvm_ph;
                    if (!ph->Pin()) { // Evicted, retry
                        g_nvm_latch.Unlock();
                        //g.Unlock();
                        std::this_thread::sleep_for(std::chrono::microseconds(1));
                        goto restart;
                    }
                    stat->hits_on_nvm++;
                    ph->Reference();
                    assert(ph->PinCount() >= 1);
                    return Status::OK();
                } else if (bypass_dram && in_NVM_buf_pool == false) {
                    if (!fill_dram_from_ssd_page) {
                        // load the page into the NVM pool.
                        nvm_ph = new PageDesc(pid, PageType::NVM_FULL, sph);
                        Page *nvm_p = nullptr;
                        if ((nvm_p = nvm_page_leaky_buffer.Get()) == nullptr) {
                            // The leaky buffer is empty, do an allocation
                            nvm_p = (Page *) (config.nvm_malloc(kPageSize));
                            if (nvm_p == nullptr)
                                throw std::bad_alloc();
                        }
                        stat->bytes_allocated_nvm += kPageSize;
                        nvm_ph->page = nvm_p;
                        {
                            ScopedTimer timer([&, this](unsigned long long d){this->stat->cycles_spent_ssd_to_nvm +=d; });
                            LockGuard g_ssd_latch(&sph->ssd_latch);
                            Status s = ssd_page_manager->ReadPage(pid, nvm_p);
                            if (!s.ok())
                                return s;
                            stat->ssd_reads += 1;
                        }

                        auto evicted = nvm_buf_pool_replacer.Add(nvm_ph, kPageSize);
                        assert(nvm_ph != evicted);
                        if (evicted != nullptr) {
                            evicted_pids.push_back(evicted->pid);
                        }
                        nvm_buf_pool_replacer.EnsureSpace(evicted_pids);
                        stat->nvm_evictions += evicted_pids.size();

                        assert(config.enable_nvm_buf_pool == true);
                        stat->bytes_copied_ssd_to_nvm += kPageSize;

                        assert(sph->nvm_ph == nullptr);
                        sph->nvm_ph = nvm_ph;
                        ph = nvm_ph;
                        assert(ph->PinCount() >= 1);
                        return Status::OK();
                    }
                }
            }
            LockGuard g_dram_latch(&sph->dram_latch);
            if (sph->dram_ph != nullptr) {
                // Someone else has installed a dram page, restart
                goto restart;
            }

            assert(sph->dram_ph == nullptr);

            bool requested_full = intent == INTENT_READ_FULL || intent == INTENT_WRITE_FULL;
            PageType new_page_type =
                    config.enable_mini_page == true && requested_full == false && fill_dram_from_ssd_page == false ? PageType::DRAM_MINI
                                                                               : PageType::DRAM_FULL;
            // Case 4: Page not in DRAM buffer pool and not in NVM buffer pool or dram not bypassed
            // In this case, we should bring the page into DRAM buffer pool.
            ph = new PageDesc(pid, new_page_type, sph);
            Page *p = nullptr;
            if (new_page_type == PageType::DRAM_MINI) {
                size_t sz = sizeof(MiniPage);
                p = (Page *) (config.dram_malloc(sz));
            } else if ((p = dram_page_leaky_buffer.Get()) == nullptr) {
                // The leaky buffer is empty, do an allocation
                p = (Page *) (config.dram_malloc(kPageSize));
                if (p == nullptr)
                    throw std::bad_alloc();
            }
            stat->bytes_allocated_dram += ph->PageSize();
            ph->page = p;

            auto evicted = dram_buf_pool_replacer.Add(ph, ph->PageSize());
            assert(ph != evicted);
            if (evicted != nullptr) {
                evicted_pids.push_back(evicted->pid);
            }
            dram_buf_pool_replacer.EnsureSpace(evicted_pids);
            stat->dram_evictions += evicted_pids.size();
            assert(sph->dram_ph == nullptr);
            assert(ph->PinCount() == 1);
            if (fill_dram_from_ssd_page) {
                ScopedTimer timer([&, this](unsigned long long d){this->stat->cycles_spent_ssd_to_dram+=d; });
                assert(ph->type == PageType::DRAM_FULL);
                LockGuard g_ssd_latch(&shared_ph->ssd_latch);
                // Load the entire page from SSD instead
                Status s = ssd_page_manager->ReadPage(pid, ph->page);
                if (!s.ok())
                    return s;
                stat->bytes_copied_ssd_to_dram += kPageSize;
                stat->ssd_reads += 1;
                ph->residency_bitmap.SetAll();
            }
            sph->dram_ph = ph;
        }

        return Status::OK();
    }
}


void ConcurrentBufferManager::EvictPurgablePages(const std::unordered_set<pid_t> &evict_set) {
    dram_buf_pool_replacer.EvictPurgablePages(evict_set);
    if (config.enable_nvm_buf_pool)
        nvm_buf_pool_replacer.EvictPurgablePages(evict_set);
}

Status ConcurrentBufferManager::Get(const pid_t pid, PageDesc *&ph, PageOPIntent intent) {
    SharedPageDesc *shared_ph = nullptr;
    return Get(pid, shared_ph, ph, intent);
}
Status ConcurrentBufferManager::Get(const pid_t pid, PageAccessor &page_accessor, PageOPIntent intent) {
    PageDesc *ph = nullptr;
    SharedPageDesc *shared_ph = nullptr;
    Status s = Get(pid, shared_ph, ph, intent);
    if (!s.ok())
        return s;
    page_accessor = PageAccessor(this, shared_ph, ph, ph->type);
    return s;
}

ConcurrentBufferManager::PageAccessor ConcurrentBufferManager::GetPageAccessorFromDesc(PageDesc *ph) {
    assert(ph->PinCount() > 0);
    assert(ph->sph_back_pointer);
    return PageAccessor(this, ph->sph_back_pointer, ph, ph->type);
}

// Assume shared_ph->dram_latch is taken.
Status ConcurrentBufferManager::PromoteMiniPage(PageDesc *dram_ph, std::vector<pid_t> &evicted_pids) {
    // Must have not been evicted
    assert(dram_ph->PinCount() > 0);
    assert(dram_ph != nullptr);
    assert(dram_ph->type == PageType::DRAM_MINI);
    assert(dram_ph->num_blocks <= kMiniPageNVMBlockNum);
    Page *mp = dram_ph->page;
    Page *p = nullptr;
    if ((p = dram_page_leaky_buffer.Get()) == nullptr) {
        p = (Page *) config.dram_malloc(kPageSize);
        if (p == nullptr)
            throw std::bad_alloc();
    }

    // Copy over the blocks
    for (size_t i = 0; i < dram_ph->num_blocks; ++i) {
        uint8_t idx = dram_ph->block_pointers[i];
        p->blocks[idx] = dram_ph->page->blocks[i];
    }
    dram_ph->page = p;
    dram_ph->type = PageType::DRAM_FULL;

    stat->bytes_allocated_dram += dram_ph->PageSize();
    stat->mini_page_promotions += 1;
    dram_buf_pool_replacer.AddCurrentBytesInBuf(kPageSize - sizeof(MiniPage));
    dram_buf_pool_replacer.EnsureSpace(evicted_pids);
    stat->dram_evictions += evicted_pids.size();

    WaitUntilNoRefs(page_ref_manager, (uint64_t) mp);
    config.dram_free(mp);

    return Status::OK();
}

// Assume shared_ph->dram_latch is taken.
Status ConcurrentBufferManager::FillDRAMPage(SharedPageDesc *shared_ph, PageDesc *dram_ph, size_t off, size_t size,
                                             std::vector<pid_t> &evicted_pids, size_t num_blocks_to_fill) {
    assert(dram_ph != nullptr);
    assert(shared_ph != nullptr);
    auto pid = dram_ph->pid;
    if (dram_ph->type == DRAM_MINI && dram_ph->num_blocks + num_blocks_to_fill > kMiniPageNVMBlockNum) {
        Status s = PromoteMiniPage(dram_ph, evicted_pids);
        if (!s.ok())
            return s;
    }
    bool go_to_ssd = true;
    if (config.enable_nvm_buf_pool) {
        LockGuard g_nvm_latch(&shared_ph->nvm_latch);
        auto nvm_ph = shared_ph->nvm_ph;
        size_t resident_bits = 0;
        // If ttb enabled and page not in nvm, go to SSD for retrieval.
        if (config.enable_hymem == false || nvm_ph != nullptr) {
            // If page in nvm or dram_ph->page not fully resident, go to nvm for retrieval.
            if (nvm_ph != nullptr ||
                migration_policy.BypassNVMDuringRead() == false ||
                ((resident_bits = dram_ph->CountSetBitsInBitmapByRange(0, kPageSize, dram_ph->residency_bitmap)) &&
                 resident_bits != kNumBlocksPerPage)) {
                if (nvm_ph == nullptr) {
                    // Not in NVM buffer pool and not allowed to bypass NVM during read.
                    // Bring the page into NVM buffer pool.
                    nvm_ph = new PageDesc(pid, PageType::NVM_FULL, shared_ph);
                    Page *nvm_p = nullptr;
                    if ((nvm_p = nvm_page_leaky_buffer.Get()) == nullptr) {
                        // The leaky buffer is empty, do an allocation
                        nvm_p = (Page *) (config.nvm_malloc(kPageSize));
                        if (nvm_p == nullptr)
                            throw std::bad_alloc();
                    }
                    stat->bytes_allocated_nvm += kPageSize;
                    nvm_ph->page = nvm_p;
                    {
                        ScopedTimer timer([&, this](unsigned long long d){this->stat->cycles_spent_ssd_to_nvm +=d; });
                        LockGuard g_ssd_latch(&shared_ph->ssd_latch);
                        Status s = ssd_page_manager->ReadPage(pid, nvm_p);
                        if (!s.ok())
                            return s;
                        stat->ssd_reads += 1;
                    }

                    auto evicted = nvm_buf_pool_replacer.Add(nvm_ph, kPageSize);
                    assert(nvm_ph != evicted);
                    if (evicted != nullptr) {
                        evicted_pids.push_back(evicted->pid);
                    }
                    nvm_buf_pool_replacer.EnsureSpace(evicted_pids);
                    stat->nvm_evictions += evicted_pids.size();

                    assert(config.enable_nvm_buf_pool == true);
                    stat->bytes_copied_ssd_to_nvm += kPageSize;
                    assert(nvm_ph->PinCount() == 1);
                } else {
                    // Wait for all direct references to NVM page to drop before doing the copying.
                    // This ensures the data in DRAM page is up to date.
                    int cnt = 0;
                    while (nvm_ph->PinCount() > 0) {
                        std::this_thread::yield();
                        ++cnt;
                        if (cnt > 10000) {
                            // Waited too long, try later.
                            return Status::PageEvicted(std::to_string(pid));
                        }
                    }

                    // Try to pin the NVM page down
                    if (!nvm_ph->Pin()) { // This page is evicted.
                        return Status::PageEvicted(std::to_string(pid));
                    }
                    assert(nvm_ph->PinCount() == 1);
                    stat->hits_on_nvm++;
                }
                assert(nvm_ph != nullptr);
                assert(nvm_ph->page != nullptr);
        nvm_ph->Reference();

                ScopedTimer timer([&, this](unsigned long long d){this->stat->cycles_spent_nvm_to_dram+=d;});
                size_t bytes_copied = 0;
                auto original_num_blocks = dram_ph->num_blocks;
                // Fill every non-resident block in DRAM page using contents from the NVM page
                dram_ph->ForeachUnsetBitInBitmapByPageRange(off, size, dram_ph->residency_bitmap,
                                                            [&](int bit_pos) {
                                                                assert(dram_ph->residency_bitmap.Test(bit_pos) ==
                                                                       false);
                                                                assert(dram_ph->dirty_bitmap.Test(bit_pos) == false);

                                                                if (dram_ph->type == PageType::DRAM_MINI) {
                                                                    dram_ph->block_pointers[dram_ph->num_blocks] = bit_pos;
                                                                    dram_ph->page->blocks[dram_ph->num_blocks] = nvm_ph->page->blocks[bit_pos];
                                                                } else {
                                                                    dram_ph->page->blocks[bit_pos] = nvm_ph->page->blocks[bit_pos];
                                                                }
                                                                dram_ph->residency_bitmap.Set(bit_pos);
                                                                bytes_copied += kNVMBlockSize;
                                                                dram_ph->num_blocks++;
                                                            });
                if (dram_ph->type == PageType::DRAM_MINI && bytes_copied) {
                    dram_ph->SortMiniPageBlocks(original_num_blocks);
                    assert(dram_ph->num_blocks <= kMiniPageNVMBlockNum);
                }
                shared_ph->nvm_ph = nvm_ph;
                assert(nvm_ph == shared_ph->nvm_ph);

                // Unpin the NVM page after copying
                assert(nvm_ph->PinCount() > 0);
                Put(nvm_ph, false);

                stat->bytes_copied_nvm_to_dram += bytes_copied;
                go_to_ssd = false;
            }
        }
    }
    if (go_to_ssd) {
        if (dram_ph->type == PageType::DRAM_MINI) {
            Status s = PromoteMiniPage(dram_ph, evicted_pids);
            if (!s.ok())
                return s;
        }
        ScopedTimer timer([&, this](unsigned long long d){this->stat->cycles_spent_ssd_to_dram+=d; });
        assert(dram_ph->type == PageType::DRAM_FULL);
        //assert(dram_ph->CountSetBitsInBitmapByRange(0, kPageSize, dram_ph->dirty_bitmap) == 0);
        LockGuard g_ssd_latch(&shared_ph->ssd_latch);
        // Load the entire page from SSD instead
        Status s = ssd_page_manager->ReadPage(pid, dram_ph->page);
        if (!s.ok())
            return s;
        stat->bytes_copied_ssd_to_dram += kPageSize;
        stat->ssd_reads += 1;
        dram_ph->residency_bitmap.SetAll();
    }
    return Status::OK();
}

Status ConcurrentBufferManager::Flush(const pid_t pid, bool forced, bool keep_in_buffer) {
    auto pid_hash_pos = pid_in_flush.GetHashPos(PidHasher()(pid));
    std::vector<pid_t> local_evicted_pids;
    restart:
    {
        retry_lock_pid_in_flush:
        // Make sure there is only one flusher for every pid at any time
        if (pid_in_flush.TryLock(pid_hash_pos) == false) {
            std::this_thread::sleep_for(std::chrono::microseconds(1));
            goto retry_lock_pid_in_flush;
        }
        bool pid_in_flush_unlocked = false;
        DeferCode c([&, this, pid_hash_pos]() {
            if (pid_in_flush_unlocked == false) {
                pid_in_flush.Unlock(pid_hash_pos);
                pid_in_flush_unlocked = true;
            }
        });
        SharedPageDesc *sph = nullptr;
        bool found = mapping_table.Find(pid, sph);
        if (found == false) {
            return Status::NotFound("pid: " + std::to_string(pid));
        }

        Status s;
        assert(sph != nullptr);

        //LockGuard g(&sph->m);
        if (config.enable_nvm_buf_pool) {
            LockGuard g_nvm_latch(&sph->nvm_latch);
            // NVM flush
            auto nvm_ph1 = sph->nvm_ph;
            if (nvm_ph1 != nullptr && (nvm_ph1->Evicted() || forced)) {
                nvm_buf_pool_replacer.AssertNotInBuffer(nvm_ph1);
                dram_buf_pool_replacer.AssertNotInBuffer(nvm_ph1);
                assert(nvm_ph1->page != nullptr);
                if (forced)
                    assert(nvm_ph1->PinCount() <= 0);
                if (nvm_ph1->dirty) {
                    ScopedTimer timer([&, this](unsigned long long d){this->stat->cycles_spent_nvm_to_ssd+=d; });
                    LockGuard g_ssd_latch(&sph->ssd_latch);
                    s = ssd_page_manager->WritePage(nvm_ph1->pid, nvm_ph1->page);
                    assert(s.ok());
                    if (!s.ok()) {
                        return s;
                    }
                    stat->bytes_copied_nvm_to_ssd += kPageSize;
                    stat->ssd_writes += 1;
                }
                nvm_ph1->dirty = false;
                if (keep_in_buffer == false || nvm_ph1->Evicted()) {
                    if (nvm_page_leaky_buffer.Put(nvm_ph1->page) == false) {
                        // The leaky buffer is full, do a deallocation
                        config.nvm_free(nvm_ph1->page);
                    }
                    nvm_ph1->page = nullptr;
                    sph->nvm_ph = nullptr;
                    assert(config.enable_nvm_buf_pool == true);
                    WaitUntilNoRefs(page_ref_manager, (uint64_t) nvm_ph1);
                    delete nvm_ph1;
                }
            }
        }

        {
            LockGuard g_dram_latch(&sph->dram_latch);
            auto dram_ph = sph->dram_ph;
            if (dram_ph != nullptr && (dram_ph->Evicted() || keep_in_buffer || forced)) {
                //assert(dram_ph->BitmapSubsume(0, kPageSize, dram_ph->residency_bitmap, dram_ph->dirty_bitmap));
                if (forced) {
                    assert(dram_ph->PinCount() <= 0);
                    if (dram_ph->PinCount() > 0) {
                        std::cout << "Not ok" << std::endl;
                        exit(-1);
                    }
                }

                // If this is ttb setting, we need to consider admitting the page into NVM
                // even if it's clean.
                if (dram_ph->dirty == false && config.enable_hymem == false) {
                    goto cleanup;
                }

                if (dram_ph->type == DRAM_MINI && dram_ph->num_blocks == 0) {
                    // If the evicted page is a mini page and there are no
                    // block loadings occurred since the creation of the page,
                    // skip the flushing process entirely.
                    goto cleanup;
                }
                {
                    // We write dirty page to SSD directly if any of the followings holds:
                    //  1. The nvm_buf_pool is not enabled.
                    //  2. The page is not in NVM pool and the migration policy says to bypass NVM and all blocks are resident in DRAM.
                    int resident_bits = 0;
                    bool go_to_ssd =  config.enable_nvm_buf_pool == false ||
                                      (sph->nvm_ph == nullptr &&
                                      migration_policy.BypassNVMDuringWrite() == true &&
                                      (resident_bits = dram_ph->CountSetBitsInBitmapByRange(0, kPageSize,
                                                                                            dram_ph->residency_bitmap)) ==
                                      kNumBlocksPerPage);
                    if (config.enable_hymem == true) {
                        assert(config.enable_nvm_buf_pool);
                        if (sph->nvm_ph == nullptr) {
                            assert(fabs(migration_policy.Nr - 1) < 1e-6 && fabs(migration_policy.Nw - 1) < 1e-6);
                            go_to_ssd = false;
                            resident_bits = dram_ph->CountSetBitsInBitmapByRange(0, kPageSize,
                                                                                 dram_ph->residency_bitmap);
                            bool in_admission_set = admission_set.Exist(pid);
                            if (in_admission_set || resident_bits != kNumBlocksPerPage) {
                                // In the admission set, admit the page into NVM
                                //size_t admission_set_size =  admission_set.Size();
                                go_to_ssd = false;
                                admission_set.Remove(dram_ph->pid);
                            } else {
                                admission_set.Put(pid);
                                go_to_ssd = true;
                            }
                        } else {
                            go_to_ssd = false;
                        }
                    }

                    LockGuard g_nvm_latch(&sph->nvm_latch);
                    if (go_to_ssd) {
                        resident_bits = dram_ph->CountSetBitsInBitmapByRange(0, kPageSize,
                                                                             dram_ph->residency_bitmap);
//                        assert(dram_ph->type != PageType::DRAM_MINI);
                        g_nvm_latch.Unlock();
                        if (dram_ph->dirty && (resident_bits != 0 || forced)) {
                            assert(resident_bits == kNumBlocksPerPage);
                            ScopedTimer timer([&, this](unsigned long long d){this->stat->cycles_spent_dram_to_ssd+=d; });
                            LockGuard g_ssd_latch(&sph->ssd_latch);
                            // Directly write to ssd page
                            s = ssd_page_manager->WritePage(dram_ph->pid, dram_ph->page);
                            if (!s.ok())
                                return s;
                            stat->bytes_copied_dram_to_ssd += kPageSize;
                            stat->ssd_writes += 1;
                        } else {
                            //assert(dram_ph->CountSetBitsInBitmapByRange(0, kPageSize, dram_ph->dirty_bitmap) == 0);
                        }
                    } else {
                        size_t bytes_copied = 0;
                        auto nvm_ph = sph->nvm_ph;
                        if (nvm_ph == nullptr) {
                            // Not in NVM buffer pool and not allowed to bypass NVM during read.
                            assert(nvm_ph == nullptr);
                            nvm_ph = new PageDesc(pid, PageType::NVM_FULL, sph);
                            Page *nvm_p = nullptr;
                            if ((nvm_p = nvm_page_leaky_buffer.Get()) == nullptr) {
                                // The leaky buffer is empty, do an allocation
                                nvm_p = (Page *) (config.nvm_malloc(kPageSize));
                                if (nvm_p == nullptr)
                                    throw std::bad_alloc();
                            }
                            stat->bytes_allocated_nvm += kPageSize;
                            nvm_ph->page = nvm_p;

                            // No need to read page from SSD if the page is fully resident in DRAM
                            if (resident_bits != kNumBlocksPerPage) {
                                ScopedTimer timer([&, this](unsigned long long d){this->stat->cycles_spent_ssd_to_nvm +=d; });
                                LockGuard g_ssd_latch(&sph->ssd_latch);
                                s = ssd_page_manager->ReadPage(pid, nvm_ph->page);
                                if (!s.ok())
                                    return s;
                                stat->bytes_copied_ssd_to_nvm += kPageSize;
                                stat->ssd_reads += 1;
                            }

                            auto evicted = nvm_buf_pool_replacer.Add(nvm_ph, kPageSize);
                            assert(evicted != nvm_ph);
                            if (evicted != nullptr) {
                                assert(evicted->PinCount() == -1);
                                local_evicted_pids.push_back(evicted->pid);
                            }
                            nvm_buf_pool_replacer.EnsureSpace(local_evicted_pids);
                            stat->nvm_evictions += local_evicted_pids.size();

                            assert(config.enable_nvm_buf_pool == true);

                            // Once we reached here, it is sure that nvm_ph is not nullptr.
                            assert(nvm_ph->PinCount() == 1); // Make sure we are the only one writing the NVM page
                            assert(nvm_ph != nullptr);
                            assert(nvm_ph->page);

                            // Fully resident DRAM page => No ssd read IO involved, do an entire page copy
                            if (resident_bits == kNumBlocksPerPage) {
                                ScopedTimer timer([&, this](unsigned long long d){this->stat->cycles_spent_dram_to_nvm +=d; });
                                assert(dram_ph->type != PageType::DRAM_MINI || sizeof(MiniPage) == sizeof(Page));
                                // Copy over the entire page
                                memcpy((void *) nvm_ph->page, (const void *) dram_ph->page, kPageSize);
                                bytes_copied = kPageSize;
                            } else if (dram_ph->dirty == true) {
                                ScopedTimer timer([&, this](unsigned long long d){this->stat->cycles_spent_dram_to_nvm +=d; });
                                // If the page comes from NVM, we only copy over the dirty blocks if any.
                                // Copy the dirty blocks in DRAM page over to the NVM page
                                if (dram_ph->type == DRAM_MINI) {
                                    assert(dram_ph->num_blocks <= kMiniPageNVMBlockNum);
                                    for (int i = 0; i < dram_ph->num_blocks; ++i) {
                                        int bit_pos = dram_ph->block_pointers[i];
                                        nvm_ph->page->blocks[bit_pos] = dram_ph->page->blocks[i];
                                        bytes_copied += kNVMBlockSize;
                                    }
                                } else {
                                    dram_ph->ForeachSetBitInBitmapByPageRange(0, kPageSize, dram_ph->dirty_bitmap,
                                                                              [&](int bit_pos) {
                                                                                  assert(dram_ph->residency_bitmap.Test(
                                                                                          bit_pos));
                                                                                  nvm_ph->page->blocks[bit_pos] = dram_ph->page->blocks[bit_pos];
                                                                                  bytes_copied += kNVMBlockSize;
                                                                              });
                                }
                            }
                            nvm_ph->Reference();
                            sph->nvm_ph = nvm_ph;
                            assert(sph->nvm_ph != nullptr);

                        } else {
                            int pc = nvm_ph->PinCount();
                            assert(pc <= 0);
                            // Try to pin it down.
                            if (!nvm_ph->Pin()) {
                                // This page got evicted, retry loading it
                                g_nvm_latch.Unlock();
                                g_dram_latch.Unlock();
                                pid_in_flush.Unlock(pid_hash_pos);
                                pid_in_flush_unlocked = true;
                                //g.Unlock();
                                std::this_thread::sleep_for(std::chrono::microseconds(1));
                                goto restart;
                            }
                            assert(nvm_ph->PinCount() >= 1);
                            nvm_ph->Reference();

                            // Once we reached here, it is sure that nvm_ph is not nullptr.
                            assert(nvm_ph->PinCount() == 1); // Make sure we are the only one writing the NVM page
                            assert(sph->nvm_ph != nullptr);
                            assert(nvm_ph != nullptr);
                            assert(nvm_ph->page);

                            if (dram_ph->dirty == true) {
                                ScopedTimer timer([&, this](unsigned long long d){this->stat->cycles_spent_dram_to_nvm +=d; });
                                // If the page comes from NVM, we only copy over the dirty blocks if any.
                                // Copy the dirty blocks in DRAM page over to the NVM page
                                if (dram_ph->type == DRAM_MINI) {
                                    assert(dram_ph->num_blocks <= kMiniPageNVMBlockNum);
                                    for (int i = 0; i < dram_ph->num_blocks; ++i) {
                                        int bit_pos = dram_ph->block_pointers[i];
                                        nvm_ph->page->blocks[bit_pos] = dram_ph->page->blocks[i];
                                        bytes_copied += kNVMBlockSize;
                                    }
                                } else {
                                    dram_ph->ForeachSetBitInBitmapByPageRange(0, kPageSize, dram_ph->dirty_bitmap,
                                                                              [&](int bit_pos) {
                                                                                  assert(dram_ph->residency_bitmap.Test(
                                                                                          bit_pos));
                                                                                  nvm_ph->page->blocks[bit_pos] = dram_ph->page->blocks[bit_pos];
                                                                                  bytes_copied += kNVMBlockSize;
                                                                              });
                                }
                            }
                        }

                        assert(nvm_ph->PinCount() > 0);
                        // Unpin the NVM page after copying
                        Put(nvm_ph, dram_ph->dirty);
                        stat->bytes_copied_dram_to_nvm += bytes_copied;
                    }
                }
                cleanup:
                if (dram_ph->dirty) {
                    dram_ph->dirty = false;
                }
                dram_ph->dirty = false;
                dram_ph->dirty_bitmap.ClearAll();
                if (keep_in_buffer == false || dram_ph->Evicted()) {
                    dram_ph->residency_bitmap.ClearAll();
                    dram_ph->num_blocks = 0;
                    if (dram_ph->type == DRAM_MINI || dram_page_leaky_buffer.Put(dram_ph->page) == false) {
                        // The leaky buffer is full, do a deallocation
                        config.dram_free(dram_ph->page);
                    }

                    dram_ph->page = nullptr;
                    sph->dram_ph = nullptr;

                    WaitUntilNoRefs(page_ref_manager, (uint64_t) dram_ph);
                    delete dram_ph;
                }
            }
        }


        if (sph->dram_ph == nullptr && sph->nvm_ph == nullptr) {
            // It is possible that there is a temporary page being filled
            // and to be installed in sph->dram_ph or sph->nvm_ph.
            // We make sure there is no reference to sph and check again.
            WaitUntilNoRefs(shared_pd_ref_manager, (uint64_t) sph);
            if (sph->dram_ph == nullptr && sph->nvm_ph == nullptr) {
                bool erased = mapping_table.Erase(pid);
                assert(erased);
                delete sph;
            }
        }
    }
    for (auto local_pid : local_evicted_pids) {
        Status s = Flush(local_pid, forced);
        assert(s.ok());
        if (!s.ok()) {
            local_evicted_pids.clear();
            return s;
        }
    }
    return Status::OK();
}

Status ConcurrentBufferManager::Put(PageDesc *ph, bool dirtied) {
    ph->dirty |= dirtied;
    if (ph->type == DRAM_FULL && dirtied) {
        ph->dirty_bitmap.SetAll();
    }
    ph->Unpin();
    return Status::OK();
}

Status ConcurrentBufferManager::Put(PageDesc *ph) {
    ph->Unpin();
    return Status::OK();
}

void ConcurrentBufferManager::WaitForPageCleanerSignal(uint32_t timeout_in_microseconds) {
    std::unique_lock<std::mutex> l(page_cleaner_signal_cv_mtx);
    page_cleaner_signal_cv.wait_for(l, std::chrono::microseconds(timeout_in_microseconds));
}

void ConcurrentBufferManager::CleanPageReady() {
    page_cleaner_signal_cv.notify_all();
}

void ConcurrentBufferManager::SetPageMigrationPolicy(PageMigrationPolicy policy) {
    this->migration_policy = policy;
}

std::string ConcurrentBufferManager::GetStatsString() const {
    if (config.enable_nvm_buf_pool) {
        auto dram_pids = std::move(dram_buf_pool_replacer.GetManagedPids());
        auto nvm_pids = std::move(nvm_buf_pool_replacer.GetManagedPids());
        auto merged_pids = nvm_pids;
        for (auto &pid : dram_pids) {
            merged_pids.insert(pid);
        }
        size_t n_pages_in_dram_and_nvm = 0;
        for (auto &pid : dram_pids)
            n_pages_in_dram_and_nvm += nvm_pids.count(pid);
        double inclusivity = n_pages_in_dram_and_nvm / (merged_pids.size() + 0.0);

        char buf[2048];
        snprintf(buf, sizeof(buf),
                 "%s\n"
                 "# dram_pids    %lld\n"
                 "# nvm_pids     %lld\n"
                 "# merged_pids  %lld\n"
                 "inclusivity    %f\n"
                 "%s",
                 stat->ToString().c_str(),
                 dram_pids.size(),
                 nvm_pids.size(),
                 merged_pids.size(),
                 inclusivity,
                 ReplacerStats().c_str());
        return buf;
    } else {
        return stat->ToString();
    }
}

void ConcurrentBufferManager::ClearStats() {
    stat->Clear();
    if (debug_pid_access_skew) {
        debug_pid_access_freq.Clear();
    }
}


ConcurrentClockReplacer::ConcurrentClockReplacer(const int64_t capacity_in_bytes, const size_t page_size,
                                                 RefManager *epoch_manager, ConcurrentBufferManager *buf_mgr)
        : cap_in_bytes(capacity_in_bytes),
          n_pages(cap_in_bytes / page_size),
          pool(n_pages),
          free(n_pages),
          clock_hand(0),
          epoch_manager(epoch_manager),
          buf_mgr(buf_mgr) {
    Clear();
}

void ConcurrentClockReplacer::Clear() {
    for (int i = 0; i < n_pages; ++i)
        pool[i].store(nullptr);
    free.store(n_pages);
    clock_hand.store(0);
    current_bytes_in_buffer.store(0);
}

void ConcurrentClockReplacer::AssertNotInBuffer(PageDesc *entry) {
    //for (size_t i = 0; i < size; ++i)
    //    assert(pool[i].load() != entry);
}

std::string ConcurrentClockReplacer::GetStats() const {
    int64_t total_size = 0;
    size_t mini_pages = 0;
    size_t full_pages = 0;
    for (size_t i = 0; i < n_pages; ++i) {
        if (pool[i].load()) {
            if (pool[i].load()->type == PageType::DRAM_MINI) {
                ++mini_pages;
            } else {
                ++full_pages;
            }
            total_size += pool[i].load()->PageSize();
        }
    }
    assert(total_size == current_bytes_in_buffer.load());
    char buf[1000];
    snprintf(buf, sizeof(buf), "mini pages: %lu\n"
                               "full pages: %lu\n"
                               "page slots: %lu\n"
                               "bytes:      %lu\n"
                               "cap_bytes:  %lu\n",
             mini_pages, full_pages,
             n_pages, current_bytes_in_buffer.load(),
             cap_in_bytes);
    return buf;
}

std::unordered_set<pid_t> ConcurrentClockReplacer::GetManagedPids() const {
    std::unordered_set<pid_t> pids;
    for (size_t i = 0; i < n_pages; ++i) {
        if (pool[i].load()) {
            pids.insert(pool[i].load()->pid);
        }
    }
    return pids;
}


void ConcurrentClockReplacer::EvictPurgablePages(const std::unordered_set<pid_t> &evict_set) {
    pd_reader_ref->Register(epoch_manager);
    {

        int start = clock_hand.load() % n_pages;
        int i = start;
        do {
            PageDesc *e = pool[i].load();
            if (e == nullptr) {
                i = (i + 1) % n_pages;
                continue;
            }
            ThreadRefGuard g(*pd_reader_ref);
            e = pool[i].load();
            if (e == nullptr) {
                i = (i + 1) % n_pages;
                continue;
            }
            pd_reader_ref->SetValue((uint64_t) e);
            if (evict_set.find(e->pid) == evict_set.end()) {
                i = (i + 1) % n_pages;
                continue;
            }
            int pin_count = e->PinCount();
            assert(pin_count <= 0);
            // pin_count == 0
            if (e->TryEvict()) {
                pool[i].store(nullptr);
                MoveClockHand(i, start);
                current_bytes_in_buffer.increment(0 - e->PageSize());
                e->dirty = false;
                e->dirty_bitmap.ClearAll();
            }
            // else {} others have evicted this entry
            i = (i + 1) % n_pages;
        } while (i != start);
    }
}
PageDesc *ConcurrentClockReplacer::Add(PageDesc *entry, int64_t size) {
    AssertNotInBuffer(entry);
    assert(entry->PinCount() > 0);
    assert(entry != nullptr);
    return Swap(entry, size);
//    do {
//        int free_cnt = free.load();
//        if (free_cnt == 0)
//            return Swap(entry, size);
//        if (free.compare_exchange_strong(free_cnt, free_cnt - 1))
//            break;
//    } while (true);
//
//    // Get an empty slot
//    auto hand = clock_hand.load();
//    int idx = hand % n_pages;
//    while (true) {
//        auto null = pool[idx].load();
//        ++hand;
//        if (null == nullptr) {
//            if (pool[idx].compare_exchange_strong(null, entry)) {
//                break;
//            }
//        }
//        idx = (idx + 1) % n_pages;
//    }
//
//    clock_hand.store(hand);
//    current_bytes_in_buffer.increment(size);
    return nullptr;
}

static std::atomic<bool> replacer_cleaning(false);

void ConcurrentClockReplacer::EnsureSpace(std::vector<pid_t> &evicted_pids) {
    restart:
    if (current_bytes_in_buffer.load() <= cap_in_bytes)
        return;
    bool s = false;
    if (replacer_cleaning.load() == true || replacer_cleaning.compare_exchange_strong(s, true) == false) {
        while (current_bytes_in_buffer.load() > cap_in_bytes) {
            if (replacer_cleaning == false) {
                goto restart;
            }
            std::this_thread::yield();
        }
        return;
    }
    // Only one thread is allowed to do the space cleaning
    do {
        PageDesc *evicted = Swap(nullptr, 0);
        if (evicted != nullptr) {
            assert(evicted->Evicted());
            evicted_pids.push_back(evicted->pid);
        }
    } while (current_bytes_in_buffer.load() > cap_in_bytes);

    replacer_cleaning.store(false);
}

PageDesc *ConcurrentClockReplacer::Swap(PageDesc *entry, int64_t size) {
    pd_reader_ref->Register(epoch_manager);
    {
        int num_pinning = 0;
        int start = clock_hand.load();
        int steps = 0;
        for (int i = start % n_pages;; i = (i + 1) % n_pages) {
            PageDesc *e = pool[i].load();
            ++steps;
            if (steps % (n_pages / 5) == 0) {
                // There is still no clean page after a sweep over a long distance, notify the page cleaner.
                if (buf_mgr->GetLogManager()) {
                    buf_mgr->GetLogManager()->WakeUpPageCleaner();
                    // Wait a while for the page cleaner to produce some clean pages
                    buf_mgr->WaitForPageCleanerSignal(30);
                }
            }
            if (e == nullptr) {
                if (entry != nullptr) {
                    if (pool[i].compare_exchange_strong(e, entry)) {
                        entry->Reference();
                        MoveClockHand(i, start);
                        current_bytes_in_buffer.increment(size);
                        return e;
                    }
                }
                continue;
            }
            ThreadRefGuard g(*pd_reader_ref);
            e = pool[i].load();
            if (e == nullptr)
                continue;
            pd_reader_ref->SetValue((uint64_t) e);
            int pin_count = e->PinCount();
            if (pin_count == -1) { // evicted ?
                // Do not compete with other evictors
                continue;
            }
            if (pin_count > 0) {
                if (++num_pinning >= n_pages) // All pinned ?
                    std::this_thread::yield();
                continue;
            }
            // pin_count == 0
            if (e->Referenced() == false && (evict_dirty == true || e->dirty == false)) {
                if (e->TryEvict()) {
                    pool[i].store(entry);
                    MoveClockHand(i, start);
                    if (entry != nullptr)
                        entry->Reference();
                    current_bytes_in_buffer.increment(size - e->PageSize());
                    //                if (entry == nullptr)
                    //                    free += 1;
                    return e;
                }
                // others have evicted this entry
            }
            e->ClearReferenced();
        }
    }
}

void ConcurrentClockReplacer::MoveClockHand(int curr, int start) {
    int delta;
    if (curr < start) {
        delta = curr + (int) n_pages - start + 1;
    } else {
        delta = curr - start + 1;
    }
    clock_hand.fetch_add(delta);
}


ConcurrentBufferManager::PageAccessor::PageAccessor(ConcurrentBufferManager *mgr, SharedPageDesc *sph, PageDesc *ph,
                                                    PageType cur_type)
        : mgr(mgr), shared_ph(sph), ph(ph), cur_type(cur_type) {}


static void yield(int count) {
    if (count > 3)
        sched_yield();
    else
        _mm_pause();
}

thread_local extern uint64_t current_txn_id;

Slice ConcurrentBufferManager::PageAccessor::PrepareForAccess(uint32_t off, size_t size, PageOPIntent intent) {
//    if (cur_type == PageType::INVALID || size == 0)
//        return Slice(nullptr, 0);
    // Log pending writes if any
    LogWrite();
    thread_local static std::vector<pid_t> evicted_pids;
    auto flush_evicted_pages = [&]() {
        for (auto pid : evicted_pids) {
            mgr->Flush(pid);
        }
        evicted_pids.clear();
    };
    ++num_rw_ops;
    int restart_times = 0;
    restart:
    {
        if (restart_times++)
            yield(restart_times);
        if (cur_type == PageType::NVM_FULL) {
            //LockGuard g_nvm_latch(&shared_ph->nvm_latch);
            assert(ph->PinCount() >= 1);
            auto nvm_ph = shared_ph->nvm_ph;
            if (nvm_ph != ph) {
                exit(-1);
            }
            assert(mgr->config.enable_nvm_buf_pool == true);
            assert(nvm_ph != nullptr);
            assert(nvm_ph->PinCount() > 0);
            if (intent == PageOPIntent::INTENT_WRITE || intent == PageOPIntent::INTENT_WRITE_FULL) {
                // Mark the corresponding page as dirty
                MarkDirty(0, kPageSize);
                mgr->stat->bytes_direct_write_nvm += size;
            }
            //nvm_ph->Reference();
            //mgr->stat.hits_on_nvm++;
            return Slice(reinterpret_cast<char *>(nvm_ph->page) + off, size);
        }
        //LockGuard g(&shared_ph->m);
        //
        mgr->page_payload_ref.Leave();
        auto dram_ph = shared_ph->dram_ph;
        assert(dram_ph);
        // if (dram_ph != ph) {
        //     exit(-1);
        // }
        assert(dram_ph == ph);
        assert(dram_ph->PinCount() >= 1);
        auto v = dram_ph->version.load();
        if ((v & 1) == 1) {
            goto restart;
        }
        size_t unset_bits = dram_ph->NumUnsetBitsInBitmapByRange(off, size, dram_ph->residency_bitmap);
        if (unset_bits > 0) {
            LockGuard g_dram_latch(&shared_ph->dram_latch);
            unset_bits = dram_ph->NumUnsetBitsInBitmapByRange(off, size, dram_ph->residency_bitmap);
            if (unset_bits) {
                bool dram_mini = dram_ph->type == PageType::DRAM_MINI;
                if (dram_mini) {
                    dram_ph->version++;
                    // Wait for all concurrent readers to drop reference to the page payload
                    bool good = WaitUntilNoRefsNonBlocking(mgr->page_ref_manager, (uint64_t) dram_ph->page, 10);
                    if (good == false) {
                        g_dram_latch.Unlock();
                        dram_ph->version--;
                        goto restart;
                    }
                }
                Status s = mgr->FillDRAMPage(shared_ph, dram_ph, off, size, evicted_pids, unset_bits);
                if (dram_mini) {
                    dram_ph->version++;
                }
                if (!s.ok()) {
                    if (s.IsPageEvicted()) {
                        g_dram_latch.Unlock();
                        //g.Unlock();
                        std::this_thread::sleep_for(std::chrono::microseconds(1));
                        goto restart;
                    } else {
                        std::cout << "Fatal Error: " << s.ToString() << std::endl;
                        exit(-1);
                    }
                }
            }
        }

        if (evicted_pids.empty() == false) {
            flush_evicted_pages();
        }
        if (intent == PageOPIntent::INTENT_WRITE || intent == PageOPIntent::INTENT_WRITE_FULL) {
            MarkDirty(off, size);
        }

        mgr->page_payload_ref.Register(&page_ref_manager);
        mgr->page_payload_ref.Enter();
        mgr->page_payload_ref.SetValue((uint64_t) dram_ph->page);
        accessed = true;
        if (dram_ph->type == DRAM_MINI) {
            int block_no = off / kNVMBlockSize;
            int off_in_block = off % kNVMBlockSize;
            for (int i = 0; i < dram_ph->num_blocks; ++i) {
                if (dram_ph->block_pointers[i] == block_no) {
                    auto res = Slice(dram_ph->page->blocks[i].data + off_in_block, size);
                    auto cur_v = dram_ph->version.load();
                    if (cur_v != v) {
                        mgr->page_payload_ref.Leave();
                        goto restart;
                    }
                    return res;
                }
            }
            goto restart;
        } else {
            return Slice(reinterpret_cast<char *>(dram_ph->page) + off, size);
        }
    }
}

void ConcurrentBufferManager::PageAccessor::MarkDirty(uint32_t off, size_t size) {
    if (cur_type == PageType::NVM_FULL) {
        auto nvm_ph = ph;
        nvm_ph->dirty = true;
    } else {
        // Mark the corresponding blocks as dirty
        auto dram_ph = ph;
        dram_ph->dirty = true;
        dram_ph->FillBitmapByPageRange(off, size, dram_ph->dirty_bitmap);
    }
}

void ConcurrentBufferManager::PageAccessor::LogWrite() {
    if (mgr->GetLogManager() != nullptr) {
        if (dirtied == true) {
            redo_buf = new char[redo_undo_size];
            memcpy(redo_buf, undo_origin, redo_undo_size);
            lsn_t lsn = mgr->GetLogManager()->LogUpdate(current_txn_id, GetPageDesc()->pid, redo_undo_page_off,
                                                        redo_undo_size,
                                                        redo_buf, undo_buf,
                                                        GetLastLogRecordLSN());
            SetLastLogRecordLSN(lsn);
            ClearLoggingStates();
            if (mgr->IsNVMSSDMode() == true) {
                NVMUtilities::persist(((char*)shared_ph->dram_ph->page) + redo_undo_page_off, redo_undo_size);
            } else if (cur_type == NVM_FULL) {
                NVMUtilities::persist(((char*)shared_ph->nvm_ph->page) + redo_undo_page_off, redo_undo_size);
            }
        }
    }
}

void ConcurrentBufferManager::PageAccessor::ClearLoggingStates() {
    if (mgr->GetLogManager() != nullptr) {
        if (dirtied == true) {
            if (redo_buf)
                delete[] redo_buf;
            if (undo_buf)
                delete[] undo_buf;

            redo_buf = nullptr;
            undo_origin = nullptr;
            undo_buf = nullptr;
            redo_undo_size = 0;
            redo_undo_page_off = 0;
            dirtied = false;
        }
    }
}


void ConcurrentBufferManager::PageAccessor::FinishAccess() {
    if (accessed) {
        mgr->page_payload_ref.Leave();
        // Log pending writes if any
        LogWrite();
    }
}

Slice ConcurrentBufferManager::PageAccessor::PrepareForWrite(uint32_t off, size_t size) {
    Slice s = PrepareForAccess(off, size, PageOPIntent::INTENT_WRITE);
    assert(dirtied == false);
    if (mgr->GetLogManager() != nullptr) {
        dirtied = true;
        undo_buf = new char[size];
        memcpy(undo_buf, s.data(), size);
        redo_undo_size = size;
        redo_undo_page_off = off;
        undo_origin = s.data();
    }
    return s;
}

Slice ConcurrentBufferManager::PageAccessor::PrepareForRead(uint32_t off, size_t size) {
    return PrepareForAccess(off, size, PageOPIntent::INTENT_READ);
}

std::string PageMigrationPolicy::ToString() const {
    std::ostringstream os;
    os << "Dr=" << Dr << ", Dw=" << Dw << ", Nr=" << Nr << ", Nw=" << Nw;
    return os.str();
}

Status NVMPageAllocator::Init() {
    size_t filesize = num_pages * kPageSize;
    return PosixEnv::MMapNVMFile(heapfile_path, mmap_start_addr, filesize);
}


}