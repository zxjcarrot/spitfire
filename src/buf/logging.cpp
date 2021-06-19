//
// Created by zxjcarrot on 2020-06-25.
//

#include "util/logger.h"
#include "engine/txn.h"
#include "buf/buf_mgr.h"

namespace spitfire {

NVMLogFileBackend::NVMLogFileBackend(const std::string &file_path, size_t initial_file_capacity) :
        file_path(file_path), file_capacity(initial_file_capacity), initial_file_capacity(initial_file_capacity),
        mmap_addr(nullptr) {}

Status NVMLogFileBackend::Init() {
    // MMap the file
    void *mmap_addr_tmp;
    Status s = PosixEnv::MMapNVMFile(file_path, mmap_addr_tmp, file_capacity);
    if (!s.ok())
        return s;
    mmap_addr = (char *) mmap_addr_tmp;
    ptr = mmap_addr;
    return s;
}

Status NVMLogFileBackend::Seek(uint64_t off) {
    if (off >= file_capacity) {
        return Status::IOError("Out of range");
    }
    ptr = mmap_addr + off;
    return Status::OK();
}

Status NVMLogFileBackend::Read(char *buf, uint64_t size) {
    if (buf + size > mmap_addr + file_capacity) {
        return Status::IOError("Out of range");
    }
    memcpy(buf, ptr, size);
    ptr += size;
    return Status::OK();
}

Status NVMLogFileBackend::Extend() {
    auto pos = NextWritingPosition();
    Status s = PosixEnv::MUNMapNVMFile(mmap_addr, file_capacity);
    if (!s.ok())
        return s;
    void *mmap_addr_tmp;
    auto new_file_capacity = file_capacity * 2; // Double the capacity of the log file when extending
    s = PosixEnv::MMapNVMFile(file_path, mmap_addr_tmp, new_file_capacity);
    if (!s.ok())
        return s;
    mmap_addr = (char *) mmap_addr_tmp;
    ptr = mmap_addr;
    ptr += pos;
    file_capacity = new_file_capacity;
    return s;
}

Status NVMLogFileBackend::Shrink() {
    auto pos = NextWritingPosition();
    void *mmap_addr_tmp = (void *) mmap_addr;
    Status s = PosixEnv::MUNMapNVMFile(mmap_addr_tmp, file_capacity);
    if (!s.ok())
        return s;
    auto new_file_capacity = initial_file_capacity;
    s = PosixEnv::MMapNVMFile(file_path, mmap_addr_tmp, new_file_capacity);
    if (!s.ok())
        return s;
    mmap_addr = (char *) mmap_addr_tmp;
    ptr = mmap_addr;
    file_capacity = new_file_capacity;
    return s;
}

Status NVMLogFileBackend::Append(const char *buf, uint64_t size) {
    Status s;
    if (size > FreeSpace()) {
        s = Extend();
        if (!s.ok())
            return s;
    }
    memcpy(ptr, buf, size);
    NVMUtilities::persist(ptr, size);
    ptr += size;
    return s;
}

char *NVMLogFileBackend::AllocatePersistentBufferAtTheEnd(uint64_t size) {
    Status s;
    if (size > FreeSpace()) {
        s = Extend();
        if (!s.ok())
            return nullptr;
    }
    char *ret = ptr;
    memset(ret, size, 0);
    NVMUtilities::persist(ret, size);
    ptr += size;
    return ret;
}

size_t NVMLogFileBackend::FreeSpace() {
    return file_capacity - NextWritingPosition();
}

size_t NVMLogFileBackend::NextWritingPosition() {
    return static_cast<uint64_t>(ptr - mmap_addr);
}

size_t NVMLogFileBackend::CurrentCapacity() {
    return file_capacity;
}


LogRecordBeginTxn::LogRecordBeginTxn(lsn_t prev_lsn, txn_id_t tid) :
        prev_lsn(prev_lsn), tid(tid) {}

LogRecordCommitTxn::LogRecordCommitTxn(lsn_t prev_lsn, txn_id_t tid) :
        prev_lsn(prev_lsn), tid(tid) {}

LogRecordAbortTxn::LogRecordAbortTxn(lsn_t prev_lsn, txn_id_t tid) :
        prev_lsn(prev_lsn), tid(tid) {}

LogRecordEOL::LogRecordEOL(lsn_t prev_lsn, txn_id_t tid) :
        prev_lsn(prev_lsn), tid(tid) {}

LogManager::LogManager(ConcurrentBufferManager *buf_mgr, NVMLogFileBackend *backend1, NVMLogFileBackend *backend2,
                       size_t log_buffer_size) :
        buf_mgr(buf_mgr), current_backend_idx(0), next_lsn(0), log_buffer_size(log_buffer_size) {
    logfile_backends[0] = backend1;
    logfile_backends[1] = backend2;
    backend1->Seek(sizeof(MainRecord));
    backend2->Seek(sizeof(MainRecord));
    log_buffer_mgr = new ConcurrentLogBufferManager(this, log_buffer_size);
    //log_buffer_mgr = new PersistentLogBufferManager(this, log_buffer_size);
    //log_buffer_mgr = new ThreadLocalBasicLogBufferManager(this, log_buffer_size);
}


void LogRecordAbortTxn::Flush(WritableSlice &slice) {
    assert(slice.FreeSpace() >= this->Size());
    uint16_t type = GetType();
    slice.Append((const char *) &type, sizeof(type));
    slice.Append((const char *) &this->prev_lsn, sizeof(this->prev_lsn));
    slice.Append((const char *) &this->tid, sizeof(this->tid));
}

Status LogRecordAbortTxn::Parse(spitfire::ReadableSlice &slice) {
    Status s;
    s = slice.Read((char *) this->prev_lsn, sizeof(this->prev_lsn));
    if (!s.ok())
        return s;
    s = slice.Read((char *) this->tid, sizeof(this->tid));
    if (!s.ok())
        return s;
    return s;
}

size_t LogRecordAbortTxn::Size() {
    uint16_t type = GetType();
    return sizeof(type) + sizeof(this->prev_lsn) + sizeof(this->tid);
}

size_t LogRecordCheckpoint::Size() {
    uint16_t type = GetType();
    return sizeof(type) + sizeof(this->checkpoint_lsn);
}

void LogRecordCommitTxn::Flush(WritableSlice &slice) {
    assert(slice.FreeSpace() >= this->Size());
    uint16_t type = GetType();
    slice.Append((const char *) &type, sizeof(type));
    slice.Append((const char *) &this->prev_lsn, sizeof(this->prev_lsn));
    slice.Append((const char *) &this->tid, sizeof(this->tid));
}

Status LogRecordCommitTxn::Parse(spitfire::ReadableSlice &slice) {
    Status s;
    s = slice.Read((char *) this->prev_lsn, sizeof(this->prev_lsn));
    if (!s.ok())
        return s;
    s = slice.Read((char *) this->tid, sizeof(this->tid));
    if (!s.ok())
        return s;
    return s;
}


size_t LogRecordCommitTxn::Size() {
    uint16_t type = GetType();
    return sizeof(type) + sizeof(this->prev_lsn) + sizeof(this->tid);
}

void LogRecordBeginTxn::Flush(WritableSlice &slice) {
    assert(slice.FreeSpace() >= this->Size());
    uint16_t type = GetType();
    slice.Append((const char *) &type, sizeof(type));
    slice.Append((const char *) &this->prev_lsn, sizeof(this->prev_lsn));
    slice.Append((const char *) &this->tid, sizeof(this->tid));
}

Status LogRecordBeginTxn::Parse(spitfire::ReadableSlice &slice) {
    Status s;
    s = slice.Read((char *) this->prev_lsn, sizeof(this->prev_lsn));
    if (!s.ok())
        return s;
    s = slice.Read((char *) this->tid, sizeof(this->tid));
    if (!s.ok())
        return s;
    return s;
}


size_t LogRecordBeginTxn::Size() {
    uint16_t type = GetType();
    return sizeof(type) + sizeof(this->prev_lsn) + sizeof(this->tid);
}

void LogRecordEOL::Flush(WritableSlice &slice) {
    assert(slice.FreeSpace() >= this->Size());
    uint16_t type = GetType();
    slice.Append((const char *) &type, sizeof(type));
    slice.Append((const char *) &this->prev_lsn, sizeof(this->prev_lsn));
    slice.Append((const char *) &this->tid, sizeof(this->tid));
}

Status LogRecordEOL::Parse(spitfire::ReadableSlice &slice) {
    Status s;
    s = slice.Read((char *) this->prev_lsn, sizeof(this->prev_lsn));
    if (!s.ok())
        return s;
    s = slice.Read((char *) this->tid, sizeof(this->tid));
    if (!s.ok())
        return s;
    return s;
}


size_t LogRecordEOL::Size() {
    uint16_t type = GetType();
    return sizeof(type) + sizeof(this->prev_lsn) + sizeof(this->tid);
}

void LogRecordUpdate::Flush(WritableSlice &slice) {
    assert(slice.FreeSpace() >= this->Size());
    uint16_t type = GetType();
    slice.Append((const char *) &type, sizeof(type));
    slice.Append((const char *) &this->prev_lsn, sizeof(this->prev_lsn));
    slice.Append((const char *) &this->tid, sizeof(this->tid));
    slice.Append((const char *) &this->page_id, sizeof(this->page_id));
    slice.Append((const char *) &this->offset_in_page, sizeof(this->offset_in_page));
    slice.Append((const char *) &this->len, sizeof(this->len));
    slice.Append(this->redo_info.get(), this->len);
    slice.Append(this->undo_info.get(), this->len);
}

size_t LogRecordUpdate::Size() {
    uint16_t type = GetType();
    size_t sz = sizeof(type);
    sz += sizeof(this->prev_lsn);
    sz += sizeof(this->tid);
    sz += sizeof(this->page_id);
    sz += sizeof(this->offset_in_page);
    sz += sizeof(this->len);
    sz += this->len;
    sz += this->len;
    return sz;
}

Status LogRecordUpdate::Parse(spitfire::ReadableSlice &slice) {
    Status s;
    s = slice.Read((char *) this->prev_lsn, sizeof(this->prev_lsn));
    if (!s.ok())
        return s;
    s = slice.Read((char *) this->tid, sizeof(this->tid));
    if (!s.ok())
        return s;
    s = slice.Read((char *) this->page_id, sizeof(this->page_id));
    if (!s.ok())
        return s;
    s = slice.Read((char *) this->offset_in_page, sizeof(this->offset_in_page));
    if (!s.ok())
        return s;
    s = slice.Read((char *) this->len, sizeof(this->len));
    if (!s.ok())
        return s;
    this->redo_info = std::shared_ptr<char>(new char[len], std::default_delete<char[]>());
    this->undo_info = std::shared_ptr<char>(new char[len], std::default_delete<char[]>());
    s = slice.Read((char *) this->redo_info.get(), this->len);
    if (!s.ok())
        return s;
    s = slice.Read((char *) this->undo_info.get(), this->len);
    if (!s.ok())
        return s;
    return s;
}


LogRecordUpdate::LogRecordUpdate(lsn_t prev_lsn, txn_id_t tid, pid_t page_id, uint64_t offset, uint64_t len,
                                 const char *redo, const char *undo) {
    this->prev_lsn = prev_lsn;
    this->tid = tid;
    this->page_id = page_id;
    this->offset_in_page = offset;
    this->len = len;
    this->redo_info = std::shared_ptr<char>(new char[len], std::default_delete<char[]>());
    this->undo_info = std::shared_ptr<char>(new char[len], std::default_delete<char[]>());
    memcpy(redo_info.get(), redo, len);
    memcpy(undo_info.get(), undo, len);
}

static lsn_t MakeLSN(lsn_t main_rec_start_lsn, size_t pos) {
    return main_rec_start_lsn + sizeof(MainRecord) + pos;
}

lsn_t LogManager::LogBeginTxn(txn_id_t tid, lsn_t prev_lsn) {
    LogRecordBeginTxn rec(prev_lsn, tid);
    return this->log_buffer_mgr->WriteRecord(&rec);
}

lsn_t LogManager::LogUpdate(txn_id_t tid, pid_t page_id, size_t page_offset, size_t len, const char *redo_info,
                            const char *undo_info, lsn_t prev_lsn) {
    LogRecordUpdate rec(prev_lsn, tid, page_id, page_offset, len, redo_info, undo_info);
    this->dirty_page_table.LockOnKey(page_id);
    lsn_t lsn = this->log_buffer_mgr->WriteRecord(&rec);
    // If it's in NVM_SSD mode, we don't need to record the page as dirty for flushing
    // because the page-update is already persistent.
    if (buf_mgr->IsNVMSSDMode() == false)
        this->DirtyPage(page_id, lsn);
    this->dirty_page_table.UnlockOnKey(page_id);
    return lsn;
}

NVMLogFileBackend *LogManager::GetCurrentLogFileBackend() {
    return logfile_backends[current_backend_idx];
}

lsn_t LogManager::LogCommitTxn(txn_id_t tid, lsn_t prev_lsn) {
    LogRecordCommitTxn rec(prev_lsn, tid);
    return this->log_buffer_mgr->WriteRecord(&rec);
}

lsn_t LogManager::LogAbortTxn(txn_id_t tid, lsn_t prev_lsn) {
    LogRecordAbortTxn rec(prev_lsn, tid);
    return this->log_buffer_mgr->WriteRecord(&rec);
}

lsn_t LogManager::LogEOL(txn_id_t tid, lsn_t prev_lsn) {
    LogRecordEOL rec(prev_lsn, tid);
    return this->log_buffer_mgr->WriteRecord(&rec);
}

static thread_local lsn_t last_log_record_lsn = kInvalidLSN;

lsn_t GetLastLogRecordLSN() {
    return last_log_record_lsn;
}

void SetLastLogRecordLSN(lsn_t lsn) {
    last_log_record_lsn = lsn;
}


LogRecordCheckpoint::LogRecordCheckpoint(lsn_t checkpoint_lsn) : checkpoint_lsn(checkpoint_lsn) {}


void LogRecordCheckpoint::Flush(WritableSlice &slice) {
    assert(slice.FreeSpace() >= this->Size());
    uint16_t type = GetType();
    slice.Append((const char *) &type, sizeof(type));
    slice.Append((const char *) &this->checkpoint_lsn, sizeof(this->checkpoint_lsn));
}

Status LogRecordCheckpoint::Parse(spitfire::ReadableSlice &slice) {
    Status s;
    s = slice.Read((char *) this->checkpoint_lsn, sizeof(this->checkpoint_lsn));
    if (!s.ok())
        return s;
    return s;
}


void LogManager::PersistMainRecord(NVMLogFileBackend *backend) {
    std::lock_guard<std::recursive_mutex> g(this->lock);
    auto save = backend->NextWritingPosition();
    backend->Seek(0);
    backend->Append((const char *) &this->current_main_rec, sizeof(MainRecord));
    backend->Seek(save);
}

void LogManager::PageCleaningProcess(LogManager *mgr) {
    lsn_t prev_checkpoint_lsn = kInvalidLSN;
    long long next_wait_time_ms = 100;
    size_t num_pages_in_dram_buf = (mgr->buf_mgr->GetConfig().dram_buf_pool_cap_in_bytes / kPageSize);
    size_t flush_lsn_amount = 1024 * 1024 * 128;
    double flush_lwm = 0.5;
    int continue_times = 0;
    while (mgr->stopped.load() == false) {
        {
            std::unique_lock<std::mutex> l(mgr->cv_mtx);
            mgr->page_cleaner_cv.wait_for(l, std::chrono::microseconds(100));
        }
        if (mgr->stopped.load()) {
            break;
        }

        {
            lsn_t persisted_lsn = mgr->persisted_lsn.load();
            if (prev_checkpoint_lsn != kInvalidLSN && persisted_lsn == prev_checkpoint_lsn && continue_times++ < 100) {
                continue;
            }

            continue_times = 0;
            double dirty_ratio = mgr->dirty_page_table.Size() / (num_pages_in_dram_buf + 0.0);

            if (dirty_ratio > flush_lwm) {
                size_t flushed = 0;
                size_t factor = 8;
                lsn_t flush_lsn;
                do {
                    flush_lsn = std::min((size_t)(persisted_lsn + mgr->log_buffer_size / factor), prev_checkpoint_lsn + flush_lsn_amount);
                    flushed = mgr->FlushDirtyPages(flush_lsn);;
                    factor /= 2;
                } while(flushed < 10);
                prev_checkpoint_lsn = flush_lsn;
            }
            // Checkpoint if log file is too big
            mgr->SwitchLogFileIfTooBig();
        }

    }
}


LogManager::~LogManager() {
    EndPageCleanerProcess();
    delete log_buffer_mgr;
}

Status LogManager::Init() {
    StartPageCleanerProcess();
    return Status::OK();
}

void LogManager::StartPageCleanerProcess() {
    checkpoint_process.reset(new std::thread(PageCleaningProcess, this));
}

void LogManager::EndPageCleanerProcess() {
    if (stopped.load() == false) {
        this->stopped.store(true);
        checkpoint_process->join();
    }
}

void LogManager::WakeUpPageCleaner() {
    this->page_cleaner_cv.notify_one();
}

ThreadPool LogManager::flusher_tp(20);

size_t LogManager::FlushDirtyPages(lsn_t upto_lsn) {
    if (dirty_page_table.Size() == 0) {
        return 0;
    }
    constexpr float CYCLES_PER_SEC = 3 * 1024ULL * 1024 * 1024;
    std::vector<std::pair<lsn_t, pid_t >> dirty_pages;
    size_t cycles_spent_on_scanning = 0;
    size_t cycles_spent_on_entire_flush = 0;
    size_t pages_greater_than_upto_lsn = 0;
    {
        ScopedTimer timer1([&](unsigned long long d) { cycles_spent_on_entire_flush += d; });
        {
            ScopedTimer timer2([&](unsigned long long d) { cycles_spent_on_scanning += d; });
            dirty_page_table.Iterate([&](const pid_t &p, const lsn_t &lsn) {
                if (lsn <= upto_lsn) {
                    dirty_pages.push_back(std::make_pair(lsn, p));
                } else {
                    ++pages_greater_than_upto_lsn;
                }
            });

            if (dirty_pages.empty()) {
                //LOG_INFO("pages_greater_than_upto_lsn[%lu] %lu\n", upto_lsn, pages_greater_than_upto_lsn);
                return 0;
            }
                
            std::sort(dirty_pages.begin(), dirty_pages.end());
        }
        std::vector<std::thread> flush_workers;
        int num_workers = std::min(1, (int) dirty_pages.size());
        int num_tasks_per_worker = dirty_pages.size() / num_workers;
        CountDownLatch latch(num_workers);
        for (int i = 0; i < num_workers; ++i) {
            flusher_tp.enqueue([&, this](int no) {
                int begin = no * num_tasks_per_worker;
                int end = no == num_workers - 1 ? dirty_pages.size() : begin + num_tasks_per_worker;
                int cnt = 0;
                for (size_t j = begin; j < end; ++j) {
                    pid_t pid = dirty_pages[j].second;
                    Status s = buf_mgr->Flush(pid, false, true);
                    //s = buf_mgr->Flush(pid, false, true);
                    dirty_page_table.Erase(pid);
                    ++cnt;
                    if (cnt % 25 == 0) {
                        buf_mgr->CleanPageReady();
                    }
                }

                latch.CountDown();
            }, i);
        }

        latch.Await();
    }
    //LOG_INFO("Flushed %lu dirty pages up to lsn %lu(inclusive), scan took %fs, total %fs, %lu dirty pages left\n", dirty_pages.size(),
    //         upto_lsn, cycles_spent_on_scanning / (CYCLES_PER_SEC + 0.0),
    //         cycles_spent_on_entire_flush / (CYCLES_PER_SEC + 0.0), dirty_page_table.Size());
    return dirty_pages.size();
}

void LogManager::SwitchLogFileIfTooBig() {
    lsn_t checkpoint_lsn = 0;
    {
        std::lock_guard<std::recursive_mutex> g(this->lock);
        size_t log_file_size = GetCurrentLogFileBackend()->NextWritingPosition();
        constexpr size_t log_switch_threshold = 4 * 1024ULL * 1024 * 1024ULL;
        if (log_file_size <= log_switch_threshold) {
            return;
        }
        LOG_INFO("log_file_size %fMB exceeds threshold(%fMB), cleaning & switching", log_file_size / 1024.0 / 1024, (float)log_switch_threshold / 1024 / 1024);
        // Update the main record to incorporate the size of the previous log
        current_main_rec.start_lsn += this->GetCurrentLogFileBackend()->NextWritingPosition() - sizeof(MainRecord);
        // Switch to the other log file
        this->current_backend_idx = 1 - this->current_backend_idx;
        PersistMainRecord(this->logfile_backends[0]);
        PersistMainRecord(this->logfile_backends[1]);

        // Start writing to the position after the Main Record.
        this->GetCurrentLogFileBackend()->Seek(sizeof(MainRecord));

        checkpoint_lsn = current_main_rec.start_lsn;
    }
    FlushDirtyPages(checkpoint_lsn);

    /**
     * Phase 1: write the checkpoint begin record.
     */
//    lsn_t begin_checkpoint_lsn = kInvalidLSN;
//    size_t backend_idx = 0;
//    {
//        std::lock_guard <std::recursive_mutex> g(this->lock);
//        // Update the main record to incorporate the size of the previous log
//        current_main_rec.start_lsn += this->GetCurrentLogFileBackend()->NextWritingPosition();
//        // Switch to the other log file
//        this->current_backend_idx = 1 - this->current_backend_idx;
//        PersistMainRecord(this->GetCurrentLogFileBackend());
//        // Start writing to the position after the Main Record.
//        this->GetCurrentLogFileBackend()->Seek(sizeof(MainRecord));
//
//        size_t pos = this->GetCurrentLogFileBackend()->NextWritingPosition();
//        begin_checkpoint_lsn = MakeLSN(current_main_rec.start_lsn, pos);
//
//        LogRecordCheckpointBegin rec(kInvalidLSN);
//        std::unique_ptr<char[]> buf(new char[rec.Size()]);
//        WritableSlice slice(buf.get(), rec.Size());
//        rec.Flush(slice);
//        GetCurrentLogFileBackend()->Append(buf.get(), rec.Size());
//        backend_idx = this->current_backend_idx;
//    }
//
//    LOG_INFO("Checkpoint Phase 1: begin_checkpoint_lsn %lu, new backend idx %lu", begin_checkpoint_lsn, backend_idx);
    /**
     * Phase 2: flush all dirty pages whose recovery lsn < begin_checkpoint_lsn down to the database.
     * This phase does not block the transactions from running.
     */
//    size_t flushed_count = 0;
//    size_t skipped_count = 0;
//    lsn_t prev_lsn = kInvalidLSN;
//    this->lock.lock();
//    while (flush_list.empty() == false) {
//        pid_t page_id = (*flush_list.begin());
//        auto it = DPT.find(page_id);
//
//        if (it != DPT.end()) {
//            auto recovery_lsn = it->second;
////            if (prev_lsn != kInvalidLSN)
////                assert(recovery_lsn > prev_lsn);
//            prev_lsn = recovery_lsn;
//            if (recovery_lsn >= begin_checkpoint_lsn) {
//                break;
//            } else {
//                this->lock.unlock();
//                Status s = buf_mgr->Flush(page_id, false, true);
//                if (!s.IsNotFound() && !s.ok()) {
//                    LOG_INFO("%s", s.ToString().c_str());
//                    assert(false);
//                }
//                ++flushed_count;
//                this->lock.lock();
//                DPT.erase(page_id);
//            }
//        } else {
//            skipped_count++;
//        }
//        flush_list.pop_front();
//    }
//    this->lock.unlock();
//
//    LOG_INFO("Checkpoint Phase 2: flushing dirty pages up to lsn %lu, %lu flushed %lu skipped", begin_checkpoint_lsn, flushed_count, skipped_count);
//
//    size_t old_backend_idx = 0;
    /**
     * Phase 3: Write out the end checkpoint record containing DPT and TT as of the last begin checkpoint record.
     */
//    {
//        std::lock_guard <std::recursive_mutex> g(this->lock);
//        size_t pos = this->GetCurrentLogFileBackend()->NextWritingPosition();
//        lsn_t end_checkpoint_lsn = MakeLSN(current_main_rec.start_lsn, pos);
//        old_backend_idx = 1 - this->current_backend_idx;
//
//        LogRecordCheckpointEnd rec(kInvalidLSN, TT, DPT);
//        size_t rec_size = rec.Size();
//        std::unique_ptr<char[]> buf(new char[rec_size]);
//        WritableSlice slice(buf.get(), rec_size);
//        rec.Flush(slice);
//        GetCurrentLogFileBackend()->Append(buf.get(), rec_size);
//
//        this->current_main_rec.latest_checkpoint = begin_checkpoint_lsn;
//        this->logfile_backends[old_backend_idx]->Shrink();
//        PersistMainRecord(this->logfile_backends[0]);
//        PersistMainRecord(this->logfile_backends[1]);
//    }
//
//    LOG_INFO("Checkpoint Phase 3: TT and DPT flushed and main record updated, old_backend_idx %lu", old_backend_idx);
}


//void LogManager::RemoveFromDirtyPageTable(pid_t page_id) {
//    std::lock_guard <std::recursive_mutex> g(this->lock);
//    DPT.erase(page_id);
//}
//
//void LogManager::RemoveFromTransactionTable(txn_id_t tid) {
//    std::lock_guard <std::recursive_mutex> g(this->lock);
//    TT.erase(tid);
//}


std::pair<lsn_t, char *>
LogManager::PersistLogBufferAsync(char *log_buffer, size_t log_buffer_size, size_t new_log_buffer_cap) {
    std::lock_guard<std::recursive_mutex> g(this->lock);
    char *buf = GetCurrentLogFileBackend()->AllocatePersistentBufferAtTheEnd(new_log_buffer_cap);
    size_t offset = GetCurrentLogFileBackend()->NextWritingPosition() - sizeof(MainRecord);
    persisted_lsn += log_buffer_size;
    assert(buf);
    //assert(current_main_rec.start_lsn + offset == persisted_lsn.load());
    return std::make_pair(current_main_rec.start_lsn + offset, buf);
    // Notify the page cleaner that a log buffer is persisted.

//    std::async([&, this]() {
//        std::lock_guard<std::recursive_mutex> g(this->lock);
//        this->GetCurrentLogFileBackend()->Append(log_buffer, log_buffer_size);
//
//        // Now flush the dirty pages made by this log_buffer and make a checkpoint.
//        //this->FlushDirtyPagesInLogBuffer(next_lsn - log_buffer_size, log_buffer, log_buffer_size);
//        this->Checkpoint(checkpoint_safe_lsn + log_buffer_size);
//        // Advance checkpoint_safe_lsn
//        checkpoint_safe_lsn += log_buffer_size;
//        this->current_main_rec.start_lsn = checkpoint_safe_lsn;
//        this->current_main_rec.latest_checkpoint = checkpoint_safe_lsn;
//        PersistMainRecord(this->logfile_backends[0]);
//        PersistMainRecord(this->logfile_backends[1]);
//    });
}

lsn_t LogManager::NextLSN() {
    std::lock_guard<std::recursive_mutex> g(this->lock);
    return next_lsn;
}


LogRecord *LogRecordParser::ParseNext(size_t &offset) {
    uint16_t type;
    offset = slice.Offset();
    slice.Read((char *) &type, sizeof(type));
    LogRecord *rec;
    switch (type) {
        case LogRecordType::BEGIN_TXN:
            rec = new LogRecordBeginTxn;
            break;
        case LogRecordType::CHECKPOINT:
            rec = new LogRecordCheckpoint;
            break;
        case LogRecordType::ABORT_TXN:
            rec = new LogRecordAbortTxn;
            break;
        case LogRecordType::COMMIT_TXN:
            rec = new LogRecordCommitTxn;
            break;
        case LogRecordType::UPDATE:
            rec = new LogRecordUpdate;
            break;
        case LogRecordType::EOL:
            rec = new LogRecordEOL;
            break;
        default:
            assert(false);
    }
    Status s = rec->Parse(slice);
    assert(s.ok());
    return rec;
}

void LogManager::DirtyPage(pid_t page_id, lsn_t lsn) {
    // If page_id not in the dirty_page_table, insert <page_id, lsn>.
    // Otherwise, update the the dirty_page_table[page_id] with min(lsn, dirty_page_table[page_id])
    dirty_page_table.UpsertIfUnsafe(page_id, lsn, [&](std::pair<const pid_t, const lsn_t> kv) {
        return lsn < kv.second;
    });
}

BasicLogBufferManager::BasicLogBufferManager(LogManager *log_mgr, size_t buf_size) : log_mgr(log_mgr), buf_capacity(buf_size) {
    buf = new char[buf_capacity];
    log_buffer_start_lsn = 0;
    free_pos = 0;
}

BasicLogBufferManager::~BasicLogBufferManager() {}

lsn_t BasicLogBufferManager::WriteRecord(LogRecord *rec) {
    size_t size = rec->Size();
    lsn_t lsn;
    char *buf_start = this->ClaimSpace(size, lsn);
    assert(buf_start);
    WritableSlice slice(buf_start, size);
    rec->Flush(slice);
    NVMUtilities::persist(buf_start, size);
    return lsn;
}

char *BasicLogBufferManager::ClaimSpace(size_t sz, lsn_t &claimed_lsn) {
    size_t pos = free_pos;
    if (pos + sz > buf_capacity) {
		std::pair<lsn_t, char *> p = log_mgr->PersistLogBufferAsync(buf, buf_capacity, buf_capacity);

		log_buffer_start_lsn = p.first;
		buf = p.second;
		free_pos = 0;
    }

	// Increment the free_pos by sz bytes.
	free_pos += sz;
	claimed_lsn = log_buffer_start_lsn + pos;
	// Successfully claimed the space.
	// Return the beginning address of the claimed space.
	return buf + pos;
}

ConcurrentLogBufferManager::ConcurrentLogBufferManager(LogManager *log_mgr, size_t buf_size) : log_mgr(log_mgr), buf_capacity(buf_size) {
    buf = new char[buf_capacity];
    log_buffer_start_lsn.store(0);
    filled_bytes.store(0);
    free_pos.store(0);
}

ConcurrentLogBufferManager::~ConcurrentLogBufferManager() {}

lsn_t ConcurrentLogBufferManager::WriteRecord(LogRecord *rec) {
    size_t size = rec->Size();
    lsn_t lsn;
    char *buf_start = this->ClaimSpace(size, lsn);
    assert(buf_start);
    WritableSlice slice(buf_start, size);
    rec->Flush(slice);
    NVMUtilities::persist(buf_start, size);
    filled_bytes.increment(size);
    return lsn;
}

char *ConcurrentLogBufferManager::ClaimSpace(size_t sz, lsn_t &claimed_lsn) {
	constexpr uint64_t kStopAllocationMask = 0x8000000000000000ULL;
    retry:
    size_t pos = free_pos.load();
    if (pos & kStopAllocationMask) {
        // If the stop allocation bit is set in free_pos,
        // then someone is waiting for all the
        // holes to be filled and initiating a log buffer flush.
        // In this case, we busy loop until this bit is cleared.
        goto retry;
    }

    if (pos + sz > buf_capacity) {
        // Not enough space, try to set the Stop Allocation bit in free_pos exclusively.
        if (free_pos.compare_exchange_strong(pos, pos | kStopAllocationMask)) {
            // This is the winner thread.
            // Let's wait for all the holes to be filled.
            while (filled_bytes.load() < pos)
                std::this_thread::sleep_for(std::chrono::microseconds(1));
            assert(filled_bytes.load() == pos);

            // TODO : hand over the log buffer task to the log manager.
            std::pair<lsn_t, char *> p = log_mgr->PersistLogBufferAsync(buf, buf_capacity, buf_capacity);

            log_buffer_start_lsn = p.first;
            buf = p.second;
            // Clear the filled_bytes counter.
            filled_bytes.store(0);
            // Restart the log buffer space allocation using one atomic store.
            free_pos.store(0);
        }
        goto retry;
    } else {
        // Increment the free_pos by sz bytes.
        if (free_pos.compare_exchange_strong(pos, pos + sz) == false) {
            goto retry;
        }
        claimed_lsn = log_buffer_start_lsn.load() + pos;
        // Successfully claimed the space.
        // Return the beginning address of the claimed space.
        return buf + pos;
    }
}

lsn_t ThreadLocalBasicLogBufferManager::WriteRecord(LogRecord *rec) {
    thread_local static BasicLogBufferManager * basic_log_buf_mgr = nullptr;
    if (basic_log_buf_mgr == nullptr) {
        basic_log_buf_mgr = new BasicLogBufferManager(log_mgr, buf_size);
    }
    return basic_log_buf_mgr->WriteRecord(rec);
}

PersistentLogBufferManager::PersistentLogBufferManager(LogManager *log_mgr, size_t buf_size) : buf(nullptr),
                                                                                               buf_size(buf_size),
                                                                                               log_mgr(log_mgr),
                                                                                               persistent_log_buffer(
                                                                                                       nullptr) {}


static RefManager log_buffer_ref_manager(1024);
static thread_local ThreadRefHolder *log_buffer_ref = new ThreadRefHolder;

lsn_t PersistentLogBufferManager::WriteRecord(LogRecord *rec) {
    restart:
    struct LogRecordWorkspace {
        char *buf;
        size_t cap;
    };

    static thread_local LogRecordWorkspace *log_record_workspace = nullptr;

    size_t record_size = rec->Size();
    if (log_record_workspace == nullptr) {
        log_record_workspace = new LogRecordWorkspace;
        log_record_workspace->buf = new char[record_size];
        log_record_workspace->cap = record_size;
    } else if (log_record_workspace->cap < record_size) {
        assert(log_record_workspace->buf);
        delete[] log_record_workspace->buf;
        log_record_workspace->buf = nullptr;
        log_record_workspace->buf = new char[record_size];
        log_record_workspace->cap = record_size;
    }
    WritableSlice slice(log_record_workspace->buf, record_size);
    rec->Flush(slice);
    log_buffer_ref->Register(&log_buffer_ref_manager);
    if (persistent_log_buffer == nullptr) {
        std::lock_guard<std::mutex> g(log_buffer_init_mtx);
        if (persistent_log_buffer == nullptr) {
            std::pair<lsn_t, char *> p = log_mgr->PersistLogBufferAsync(nullptr, buf_size, buf_size);
            persistent_log_buffer = new PersistentLogBuffer<>(p.second, buf_size, p.first);
            Status s = persistent_log_buffer->Init();
            assert(s.ok());
        }
    }
    assert(slice.DataSize() == record_size);
    log_buffer_ref->Enter();
    PersistentLogBuffer<> *persistent_log_buffer_ref = persistent_log_buffer;
    log_buffer_ref->SetValue((uint64_t) persistent_log_buffer_ref);
    int32_t offset_in_buf = persistent_log_buffer_ref->PersistLogRecord(rec->GetIdHash(), slice.Data(),
                                                                        slice.DataSize());
    if (offset_in_buf == -1) {
        int status = BUFFER_SWITCH_STATUS_NORMAL;
        if (buffer_switch_status.compare_exchange_strong(status, BUFFER_SWITCH_STATUS_PRESWITCH)) {
            log_buffer_ref->Leave();
            std::pair<lsn_t, char *> p = log_mgr->PersistLogBufferAsync(persistent_log_buffer_ref->Data(), buf_size,
                                                                        buf_size);
            persistent_log_buffer = new PersistentLogBuffer<>(p.second, buf_size, p.first);
            WaitUntilNoRefs(log_buffer_ref_manager, (uint64_t) persistent_log_buffer_ref);
            buffer_switch_status.store(BUFFER_SWITCH_STATUS_NORMAL);
            delete persistent_log_buffer_ref;
        } else {
            log_buffer_ref->Leave();
            while (buffer_switch_status.load() != BUFFER_SWITCH_STATUS_NORMAL) {
                std::this_thread::sleep_for(std::chrono::microseconds(1));
            }
        }
        goto restart;
    } else {
        lsn_t res = persistent_log_buffer_ref->GetStartLSN() + offset_in_buf;
        log_buffer_ref->Leave();
        return res;
    }
}

}