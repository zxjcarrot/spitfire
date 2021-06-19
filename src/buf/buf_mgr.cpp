//
// Created by zxjcarrot on 2019-12-30.
//

#include "buf/buf_mgr.h"

namespace spitfire {


BufferPool::BufferPool(const size_t capacity_in_bytes, std::function<Status(pid_t, PageDesc *&)> lower_levels_page_loader,
           std::function<Status(PageDesc *)> lower_levels_page_unloader, std::function<void *(std::size_t)> alloc,
           std::function<void(void *)> dealloc)
        : capacity_in_pages(capacity_in_bytes / kPageSize), lower_levels_page_loader(lower_levels_page_loader),
          lower_levels_page_unloader(lower_levels_page_unloader), alloc(alloc), dealloc(dealloc) {}

// Test if page is in buffer pool. If so, pin it and return the page. Otherwise, do nothing.
// Return true if it is the case and return the page descriptor in `ph` as well.
// Return false if it is not and do not bring the page in.
bool BufferPool::ProbeBufferPool(const pid_t pid, PageDesc *&ph) {
    auto it = mapping_table.find(pid);
    if (it == mapping_table.end())
        return false;
    ph = it->second;
    ph->Pin();
    replacer.Touch(ph);
    return true;
}

Status BufferPool::Get(const pid_t pid, PageDesc *&ph) {
    auto it = mapping_table.find(pid);
    if (it == mapping_table.end()) { // Not in buffer pool
        if (mapping_table.size() >= capacity_in_pages) {
            // Buffer pool is full.
            // Try evict one page out.
            PageDesc *evicted = replacer.Evict();
            assert(evicted->pin == 0);
            mapping_table.erase(evicted->pid);
            Status s = lower_levels_page_unloader(evicted);
            evicted->dirty = false;
            dealloc(evicted->page);
            delete evicted;
            if (!s.ok())
                return s;
        }
        assert(mapping_table.size() < capacity_in_pages);
        // Bring in the page from lower levels.
        Status s = lower_levels_page_loader(pid, ph);
        if (!s.ok())
            return s;
        mapping_table[pid] = ph;
        assert(ph->pin == 1);
        replacer.Add(ph);
        return Status::OK();
    } else {
        ph = it->second;
        ph->Pin();
        replacer.Touch(ph);
        return Status::OK();
    }
}

Status BufferPool::Put(PageDesc *ph, bool dirtied) {
    ph->dirty |= dirtied;
    ph->Unpin();
    assert(ph->pin >= 0);
    return Status::OK();
}

Status BufferPool::FlushDirtyPages() {
    for (auto kv : mapping_table) {
        auto ph = kv.second;
        if (ph->dirty) {
            Status s = lower_levels_page_unloader(ph);
            ph->dirty = false;
            if (!s.ok())
                return s;
        }
        dealloc(ph->page);
        delete ph;
    }
    return Status::OK();
}


BufferManager::BufferManager(SSDPageManager *ssd_page_manager, PageMigrationPolicy policy, BufferPoolConfig config)
: ssd_page_manager(ssd_page_manager), migration_policy(policy), DRAM_buffer_pool(nullptr),
NVM_buffer_pool(nullptr), config(config) {

}


BufferManager::~BufferManager() {
    auto s = DRAM_buffer_pool->FlushDirtyPages();
    assert(s.ok());
    if (NVM_buffer_pool != nullptr)
        s = NVM_buffer_pool->FlushDirtyPages();
    assert(s.ok());
    delete DRAM_buffer_pool;
    delete NVM_buffer_pool;
}

Status BufferManager::Init() {
    // Loads page from lower levels into DRAM buffer pool
    std::function<Status(const pid_t, PageDesc *&)> dram_buf_pool_lower_levels_page_loader;
    // Unloads page from DRAM buffer pool and into lower levels
    std::function<Status(PageDesc * )> dram_buf_pool_lower_levels_page_unloader;
    // Loads page from lower levels into NVM buffer pool
    std::function<Status(const pid_t, PageDesc *&)> nvm_buf_pool_lower_levels_page_loader;
    // Unloads page from NVM buffer pool and into lower levels
    std::function<Status(PageDesc * )> nvm_buf_pool_lower_levels_page_unloader;

    if (config.enable_nvm_buf_pool) {
        nvm_buf_pool_lower_levels_page_loader = [this](const pid_t pid, PageDesc *&ph) -> Status {
            // We consider only one SSD level below the NVM for now.
            // Load page from SSD as a NVM page.
            // Create a NVM page frame
            ph = new PageDesc(pid, PageType::NVM_FULL);
            Page *p = (Page * )(config.nvm_malloc(kPageSize));
            if (p == nullptr)
                throw std::bad_alloc();
            ph->page = p;
            return ssd_page_manager->ReadPage(pid, p);
        };
        nvm_buf_pool_lower_levels_page_unloader = [this](PageDesc *ph) -> Status {
            // We consider only one SSD level below the NVM for now.
            Status s;
            if (ph->dirty) {
                s = ssd_page_manager->WritePage(ph->pid, ph->page);
                if (!s.ok())
                    return s;
            }
            return s;
        };
    }

    dram_buf_pool_lower_levels_page_loader = [this](const pid_t pid, PageDesc *&ph) -> Status {
        ph = new PageDesc(pid, PageType::DRAM_FULL);
        Page *p = (Page * )(config.dram_malloc(kPageSize));
        if (p == nullptr)
            throw std::bad_alloc();
        ph->page = p;
        PageDesc *nvm_ph = nullptr;

        bool in_nvm_buf_pool = false;
        if (config.enable_nvm_buf_pool &&
            ((in_nvm_buf_pool = NVM_buffer_pool->ProbeBufferPool(pid, nvm_ph)) ||
             migration_policy.BypassNVMDuringRead() == false)) {
            if (in_nvm_buf_pool == false) {
                // Not in NVM buffer pool and not allowed to bypass NVM during read.
                // Bring the page into NVM buffer pool.
                Status s = NVM_buffer_pool->Get(pid, nvm_ph);
                if (!s.ok()) {
                    assert(false);
                    return s;
                }

            }

            assert(NVM_buffer_pool != nullptr);
            assert(nvm_ph != nullptr);
            assert(nvm_ph->page != nullptr);
            memcpy(p, nvm_ph->page, kPageSize);
            // Unpin the NVM page after copying
            NVM_buffer_pool->Put(nvm_ph);
            return Status::OK();
        } else {
            // Load page from SSD instead
            return ssd_page_manager->ReadPage(pid, p);
        }
    };

    dram_buf_pool_lower_levels_page_unloader = [this](PageDesc *ph) -> Status {
        pid_t pid = ph->pid;
        Status s;
        if (ph->dirty) {
            PageDesc *nvm_ph = nullptr;
            // We are not allowed to bypass NVM during write
            // if the page is in NVM buffer pool or the migration policy say so.
            bool in_nvm_buf_pool = false;
            if (config.enable_nvm_buf_pool &&
                ((in_nvm_buf_pool = NVM_buffer_pool->ProbeBufferPool(pid, nvm_ph)) ||
                 migration_policy.BypassNVMDuringWrite() == false)) {
                if (in_nvm_buf_pool == false) {
                    // Not in NVM buffer pool and not allowed to bypass NVM during read.
                    // Bring the page into NVM buffer pool.
                    Status s = NVM_buffer_pool->Get(pid, nvm_ph);
                    if (!s.ok()) {
                        assert(false);
                        return s;
                    }
                }

                // Once we reached here, it is sure that nvm_ph is not nullptr.
                assert(NVM_buffer_pool != nullptr);
                assert(nvm_ph != nullptr);
                assert(nvm_ph->page);
                memcpy(nvm_ph->page, ph->page, kPageSize);
                // Unpin the NVM page after copying
                NVM_buffer_pool->Put(nvm_ph, true);
            } else {
                // Directly write to ssd page
                s = ssd_page_manager->WritePage(ph->pid, ph->page);
                if (!s.ok())
                    return s;
            }
        }
        return s;
    };

    DRAM_buffer_pool = new BufferPool(config.dram_buf_pool_cap_in_bytes, dram_buf_pool_lower_levels_page_loader,
                                      dram_buf_pool_lower_levels_page_unloader, config.dram_malloc,
                                      config.dram_free);

    if (config.enable_nvm_buf_pool)
        NVM_buffer_pool = new BufferPool(config.nvm_buf_pool_cap_in_bytes, nvm_buf_pool_lower_levels_page_loader,
                                         nvm_buf_pool_lower_levels_page_unloader, config.nvm_malloc,
                                         config.nvm_free);

    return Status::OK();
}

Status BufferManager::NewPage(pid_t &pid) {
    return ssd_page_manager->AllocateNewPage(pid);
}

Status BufferManager::Get(const pid_t pid, PageDesc *&ph, PageIntent intent) {
    // Case 1: page in DRAM buffer pool but not in NVM buffer pool
    // Case 2: page in DRAM buffer pool and in NVM buffer pool
    // In those two cases, we simply return the DRAM page.
    if (DRAM_buffer_pool->ProbeBufferPool(pid, ph)) {
        // If the page is already in DRAM buffer pool, return it.
        return Status::OK();
    }

    if (config.enable_nvm_buf_pool) {
        bool in_NVM_buf_pool = NVM_buffer_pool->ProbeBufferPool(pid, ph);

        // Case 3: Page not in DRAM buffer pool but in NVM buffer pool
        // In this case, we consult migration policy to determine
        // if we should bypass DRAM.
        if (in_NVM_buf_pool &&
            (intent == INTENT_READ && migration_policy.BypassDRAMDuringRead() ||
             intent == INTENT_WRITE && migration_policy.BypassDRAMDuringWrite())) {
            return Status::OK();
        }

        if (in_NVM_buf_pool) {
            // Not bypassed, decrement the pin.
            NVM_buffer_pool->Put(ph);
        }
    }

    // Case 4: Page not in DRAM buffer pool and not in NVM buffer pool or not bypassed
    // In this case, we should bring the page into
    // either DRAM buffer pool solely or (NVM buffer pool first, then DRAM buffer pool).
    // These logics are in lambda function `dram_buf_pool_lower_levels_page_loader`
    // which is passed into the DRAM Buffer Pool instance as the customized page loader.
    Status s = DRAM_buffer_pool->Get(pid, ph);
    if (s.ok()) {
        return Status::OK();
    } else {
        return s;
    }
}

Status BufferManager::Put(PageDesc *ph, bool dirtied) {
    if (ph->type == PageType::NVM_FULL) {
        assert(config.enable_nvm_buf_pool);
        return NVM_buffer_pool->Put(ph, dirtied);
    } else {
        return DRAM_buffer_pool->Put(ph, dirtied);
    }
}

void BufferManager::SetPageMigrationPolicy(PageMigrationPolicy policy) {
    this->migration_policy = policy;
}


ClockReplacer::ClockReplacer() {
    clock_head = new PageDesc(kInvalidPID, PageType::INVALID);
    clock_head->prev = clock_head->next = clock_head;
    clock_hand = clock_head;
}

ClockReplacer::~ClockReplacer() {
    delete clock_head;
}

void ClockReplacer::unlink(PageDesc *p) {
    if (p->next || p->prev) {
        assert(p->next);
        assert(p->prev);
        p->prev->next = p->next;
        p->next->prev = p->prev;
        p->next = p->prev = nullptr;
    }
}

void ClockReplacer::Add(PageDesc *p) {
    unlink(p);
    // Add to before the clock hand
    auto before = clock_hand->prev;
    p->prev = before;
    p->next = clock_hand;
    before->next = p;
    clock_hand->prev = p;
    Touch(p);
}

void ClockReplacer::Touch(PageDesc *p) {
    p->Reference();
}

PageDesc * ClockReplacer::Evict() {
    while (true) {
        if (clock_hand->Referenced() == true) {
            clock_hand->ClearReferenced();
        } else if (clock_hand != clock_head && clock_hand->pin == 0) {
            PageDesc *victim = clock_hand;
            clock_hand = clock_hand->next;
            unlink(victim);
            return victim;
        }
        clock_hand = clock_hand->next;
    }
    return nullptr;
}

std::string ClockReplacer::Name() const {
    return "Clock";
}

}
