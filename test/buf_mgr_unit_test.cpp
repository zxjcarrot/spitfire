//
// Created by zxjcarrot on 2019-12-22.
//

#include <iostream>
#include <unordered_map>
#include <vector>
#include <fstream>
#include <map>
#include <limits>
#include "util/random.h"
#include "buf/buf_mgr.h"
#include "util/crc32c.h"
#include "util/tools.h"

static int IntRand() {
    static thread_local std::mt19937 generator;
    std::uniform_int_distribution<int> distribution(0, std::numeric_limits<int>::max());
    return distribution(generator);
}

template<class T>
void ReportStatsAboutWorkload(const std::vector<T> & workload, int first_n_most_freq) {
    std::unordered_map<T, int > cnt;

    int max_cnt = 0;
    for (int i = 0; i < workload.size(); ++i) {
        cnt[workload[i]]++;
    }
    std::vector<std::pair<int,T>> pairs;
    for (auto kv : cnt) {
        pairs.push_back(std::make_pair(kv.second, kv.first));
        max_cnt = std::max(max_cnt, kv.second);
    }
    std::sort(pairs.begin(), pairs.end(), [](const std::pair<int, T> & p1, const std::pair<int, T> & p2) {
        return p1.first > p2.first;
    });

    std::cout << "1 -> " << cnt[1] << std::endl;
    std::cout << "Max count " <<  max_cnt << std::endl;
    int current_count = 0;
    for (int i = 0; i < std::min(first_n_most_freq, (int)pairs.size()); ++i) {
        current_count += pairs[i].first;
    }
    std::cout << "Top "<< std::min(first_n_most_freq, (int)pairs.size()) << " items take up " << current_count / (workload.size() + 0.0) * 100 << "% of the entire workload" << std::endl;
}

void TestSSDPageManager(const std::string &db_path, bool direct_io) {
    using namespace spitfire;
    Status s = SSDPageManager::DestroyDB(db_path);
    {
        assert(s.ok());
        spitfire::pid_t pid;
        {
            SSDPageManager ssd_page_manager(db_path, direct_io);
            s = ssd_page_manager.Init();
            assert(s.ok());
            pid = kInvalidPID;
            ssd_page_manager.AllocateNewPage(pid);
            assert(pid != kInvalidPID);
        }
        std::vector<char> buf1(kPageSize, 1);
        {
            SSDPageManager ssd_page_manager(db_path, direct_io);
            s = ssd_page_manager.Init();
            assert(s.ok());
            assert(ssd_page_manager.Allocated(pid));
            assert(pid != kInvalidPID);
            assert(ssd_page_manager.CountPages() == 1);
            s = ssd_page_manager.WritePage(pid, reinterpret_cast<Page *>(buf1.data()));
            assert(s.ok());
        }
        {
            SSDPageManager ssd_page_manager(db_path, direct_io);
            s = ssd_page_manager.Init();
            assert(s.ok());
            assert(ssd_page_manager.Allocated(pid));
            assert(pid != kInvalidPID);
            assert(ssd_page_manager.CountPages() == 1);
            std::vector<char> buf2(kPageSize, 0);
            s = ssd_page_manager.ReadPage(pid, reinterpret_cast<Page *>(buf2.data()));
            assert(s.ok());
            assert(buf1 == buf2);

            ssd_page_manager.Allocated(pid);
            assert(pid != kInvalidPID);
            s = ssd_page_manager.FreePage(pid);
            assert(s.ok());
            assert(ssd_page_manager.CountPages() == 0);
            assert(ssd_page_manager.Allocated(pid) == false);
        }

        {
            SSDPageManager ssd_page_manager(db_path, direct_io);
            s = ssd_page_manager.Init();
            assert(s.ok());
            assert(ssd_page_manager.Allocated(pid) == false);
            assert(ssd_page_manager.CountPages() == 0);
        }
    }

    const int n = 16 * 1024 * 2.5;
    std::vector<spitfire::pid_t> pids;

    {
        SSDPageManager ssd_page_manager(db_path, direct_io);
        s = ssd_page_manager.Init();
        assert(s.ok());
        std::vector<char> buf(kPageSize, 0);
        for (int i = 0; i < n; ++i) {
            std::fill(buf.begin(), buf.end(), i % 256);
            spitfire::pid_t pid;
            ssd_page_manager.AllocateNewPage(pid);
            pids.push_back(pid);
            assert(ssd_page_manager.Allocated(pid));
            s = ssd_page_manager.WritePage(pid, reinterpret_cast<Page *>(buf.data()));
            assert(s.ok());
        }

        assert(ssd_page_manager.CountPages() == n);
        std::vector<char> buf2(kPageSize, 0);
        for (int i = 0; i < n; ++i) {
            std::fill(buf.begin(), buf.end(), i % 256);
            spitfire::pid_t pid = pids[i];
            assert(ssd_page_manager.Allocated(pid));
            s = ssd_page_manager.ReadPage(pid, reinterpret_cast<Page *>(buf2.data()));
            assert(s.ok());
            assert(buf == buf2);
        }

    }

    {
        // Reopen and test page contents
        SSDPageManager ssd_page_manager(db_path, direct_io);
        s = ssd_page_manager.Init();
        assert(s.ok());
        assert(ssd_page_manager.CountPages() == n);
        std::vector<char> buf2(kPageSize, 0);
        std::vector<char> buf(kPageSize, 0);
        for (int i = 0; i < n; ++i) {
            std::fill(buf.begin(), buf.end(), i % 256);
            spitfire::pid_t pid = pids[i];
            assert(ssd_page_manager.Allocated(pid));
            s = ssd_page_manager.ReadPage(pid, reinterpret_cast<Page *>(buf2.data()));
            assert(s.ok());
            assert(buf == buf2);
        }
    }

    {
        s = SSDPageManager::DestroyDB(db_path);
        assert(s.ok());
        // Random read write tests
        std::unordered_map<spitfire::pid_t, std::vector<char>> mem_bufpool;
        SSDPageManager ssd_page_manager(db_path, direct_io);
        s = ssd_page_manager.Init();
        assert(s.ok());
        assert(ssd_page_manager.CountPages() == 0);
        pids.clear();

        for (int i = 0; i < n / 10; ++i) {
            spitfire::pid_t pid;
            ssd_page_manager.AllocateNewPage(pid);
            pids.push_back(pid);
            auto &buf = mem_bufpool[pid] = std::vector<char>(kPageSize, i % 256);
            assert(ssd_page_manager.Allocated(pid));
            s = ssd_page_manager.WritePage(pid, reinterpret_cast<Page *>(buf.data()));
            assert(s.ok());
        }

        for (int i = 0; i < n; ++i) {
            spitfire::pid_t pid = pids[IntRand() % pids.size()];
            int op = IntRand() % 2;
            size_t off = IntRand() % kPageSize;
            size_t size = std::min(IntRand() % kNVMBlockSize, kPageSize - off);
            auto &buf = mem_bufpool[pid];

            if (op == 0) {
                std::fill(buf.begin() + off, buf.begin() + off + size, i % 256);
                s = ssd_page_manager.WritePage(pid, reinterpret_cast<Page *>(buf.data()));
                assert(s.ok());
            } else {
                std::vector<char> buf2(kPageSize);
                assert(ssd_page_manager.Allocated(pid));
                s = ssd_page_manager.ReadPage(pid, reinterpret_cast<Page *>(buf2.data()));
                assert(s.ok());
                assert(buf == buf2);
            }
        }
    }
}

template<class BufferManagerType>
void TestBufferPool(const std::string &db_path, spitfire::PageMigrationPolicy policy,
                    bool enable_nvm_buf_pool = false) {
    using namespace spitfire;
    using spitfire::pid_t;
    Status s = SSDPageManager::DestroyDB(db_path);
    assert(s.ok());
    BufferPoolConfig config;
    config.enable_nvm_buf_pool = enable_nvm_buf_pool;
    config.dram_buf_pool_cap_in_bytes = 100 * kPageSize;
    config.nvm_buf_pool_cap_in_bytes = 300 * kPageSize;

    const int n_pages = 1000;

    std::unordered_map<pid_t, std::vector<char>> mem_buf_pool;
    std::vector<pid_t> pids;
    {
        SSDPageManager ssd_page_manager(db_path, false);
        s = ssd_page_manager.Init();
        assert(s.ok());
        BufferManagerType buf_mgr(&ssd_page_manager, policy, config);
        s = buf_mgr.Init();
        assert(s.ok());
        for (int i = 0; i < n_pages; ++i) {
            pid_t pid = kInvalidPID;
            s = buf_mgr.NewPage(pid);
            assert(s.ok());
            assert(pid != kInvalidPID);
            pids.push_back(pid);
            mem_buf_pool[pid] = std::vector<char>(kPageSize, i % 256);
            PageDesc *ph;
            s = buf_mgr.Get(pid, ph, BufferManagerType::INTENT_WRITE);
            assert(s.ok());
            memcpy((char *) ph->page, mem_buf_pool[pid].data(), kPageSize);
            buf_mgr.Put(ph, true);
        }

        for (int i = 0; i < n_pages; ++i) {
            auto pid = pids[i];
            PageDesc *ph;
            s = buf_mgr.Get(pid, ph, BufferManagerType::INTENT_READ);
            assert(s.ok());
            std::vector<char> page_content(kPageSize, 0);
            memcpy(page_content.data(), (char *) ph->page, kPageSize);
            auto correct_content = mem_buf_pool[pid];
            assert(page_content == correct_content);
            buf_mgr.Put(ph, false);
        }
    }

    // Reopen the buffer manager
    {
        SSDPageManager ssd_page_manager(db_path, false);
        s = ssd_page_manager.Init();
        assert(s.ok());
        BufferManagerType buf_mgr(&ssd_page_manager, policy, config);
        s = buf_mgr.Init();
        assert(s.ok());

        for (int i = 0; i < n_pages; ++i) {
            auto pid = pids[i];
            PageDesc *ph;
            s = buf_mgr.Get(pid, ph, BufferManagerType::INTENT_READ);
            assert(s.ok());
            std::vector<char> page_content(kPageSize, 0);
            memcpy(page_content.data(), (char *) ph->page, kPageSize);
            auto correct_content = mem_buf_pool[pid];
            assert(page_content == correct_content);
            buf_mgr.Put(ph, false);
        }
    }

    // Random test.
    {
        SSDPageManager ssd_page_manager(db_path, false);
        s = ssd_page_manager.Init();
        assert(s.ok());
        BufferManagerType buf_mgr(&ssd_page_manager, policy, config);
        s = buf_mgr.Init();
        assert(s.ok());
        for (int i = 0; i < n_pages * 10; ++i) {
            auto pid = pids[(IntRand() % pids.size())];
            mem_buf_pool[pid] = std::vector<char>(kPageSize, IntRand() % 256);
            PageDesc *ph;
            s = buf_mgr.Get(pid, ph, BufferManagerType::INTENT_WRITE);
            assert(s.ok());
            memcpy((char *) ph->page, mem_buf_pool[pid].data(), kPageSize);
            buf_mgr.Put(ph, true);
        }

        for (int i = 0; i < n_pages * 10; ++i) {
            auto pid = pids[(IntRand() % pids.size())];
            PageDesc *ph;
            s = buf_mgr.Get(pid, ph, BufferManagerType::INTENT_READ);
            assert(s.ok());
            std::vector<char> page_content(kPageSize, 0);
            memcpy(page_content.data(), (char *) ph->page, kPageSize);
            assert(page_content == mem_buf_pool[pid]);
            buf_mgr.Put(ph, false);
        }
    }
}


static int GetPartition(pid_t pid, size_t n_partitions) {
    return (pid >> spitfire::kPageSizeBits) % n_partitions;
}


void
TestConcurrentBufferPoolNVMBlockGrainedAccess(spitfire::BufferPoolConfig config, const std::string &db_path,
                                              spitfire::PageMigrationPolicy policy,
                                              size_t n_threads, spitfire::ThreadPool *tp,
                                              const int n_pages,
                                              const int n_ops,
                                              const std::vector<int> &workload) {
    using namespace spitfire;
    using spitfire::pid_t;
    Status s = SSDPageManager::DestroyDB(db_path);
    assert(s.ok());

    std::vector<std::mutex> locks(64);
    std::vector<std::unordered_map<pid_t, std::vector<char>>> mem_buf_pools(64);
    std::vector<pid_t> pids;
    {
        // Initial single threaded loading
        SSDPageManager ssd_page_manager(db_path, config.enable_direct_io);
        s = ssd_page_manager.Init();
        assert(s.ok());
        ConcurrentBufferManager buf_mgr(&ssd_page_manager, policy, config);
        s = buf_mgr.Init();
        assert(s.ok());
        for (int i = 0; i < n_pages; ++i) {
            pid_t pid = kInvalidPID;
            s = buf_mgr.NewPage(pid);
            assert(s.ok());
            assert(pid != kInvalidPID);
            pids.push_back(pid);
            auto partition = GetPartition(pid, 64);
            mem_buf_pools[partition][pid] = std::vector<char>(kPageSize, std::max(1, i % 256));
            ConcurrentBufferManager::PageAccessor accessor;
            s = buf_mgr.Get(pid, accessor, ConcurrentBufferManager::INTENT_WRITE);
            if (!s.ok()) {
                std::cout << s.ToString() << std::endl;
            }
            assert(s.ok());
            Slice slice = accessor.PrepareForWrite(0, kPageSize);
            memcpy(slice.data(), mem_buf_pools[partition][pid].data(), kPageSize);
            buf_mgr.Put(accessor.GetPageDesc());
            //buf_mgr.ReplacerStats();
        }

        for (int i = 0; i < n_pages; ++i) {
            auto pid = pids[i];
            auto partition = GetPartition(pid, 64);
            ConcurrentBufferManager::PageAccessor accessor;
            s = buf_mgr.Get(pid, accessor, ConcurrentBufferManager::INTENT_READ);
            assert(s.ok());
            std::vector<char> page_content(kPageSize, 0);
            Slice slice = accessor.PrepareForRead(0, kPageSize);
            memcpy(page_content.data(), slice.data(), kPageSize);
            auto correct_content = mem_buf_pools[partition][pid];
            assert(page_content == correct_content);
            buf_mgr.Put(accessor.GetPageDesc());
            //buf_mgr.ReplacerStats();
        }

//        auto managed_pids = buf_mgr.GetManagedPids();
//        for (int i = 0; i < managed_pids.size(); ++i) {
//            std::cout << managed_pids[i] << " ";
//        }
//        std::cout << std::endl;
    }
    {
        SSDPageManager ssd_page_manager(db_path, config.enable_direct_io);
        s = ssd_page_manager.Init();
        assert(s.ok());
        ConcurrentBufferManager buf_mgr(&ssd_page_manager, policy, config);
        s = buf_mgr.Init();
        assert(s.ok());

        std::unordered_map<pid_t, std::vector<char>> mem_buf_pools2;
        for (auto pid : pids) {
            auto partition = GetPartition(pid, 64);
            mem_buf_pools2[pid] = mem_buf_pools[partition][pid];
        }

        std::vector<std::thread> threads;
        size_t total_ops = workload.size();
        std::vector<pid_t> op_pids;
        assert(pids.size() == n_pages);
        for (size_t i = 0; i < workload.size(); ++i) {
            op_pids.push_back(pids[workload[i] % n_pages]);
        }
        std::cout << "Generated " << total_ops << " op pids." << std::endl;
//        ReportStatsAboutWorkload(workload, 100);
//        ReportStatsAboutWorkload(workload, 200);
//        ReportStatsAboutWorkload(workload, 300);
//        ReportStatsAboutWorkload(workload, 400);
//        ReportStatsAboutWorkload(workload, 500);
        CountDownLatch latch(n_threads);
        auto thread_work = [&](int i) {
            size_t ops_per_thread = total_ops / n_threads;
            int start = i * ops_per_thread;
            int end = std::min(start + ops_per_thread, total_ops);

//            // Full page reads and writes
//            for (int i = start; i < end; ++i) {
//                //int op = rand() % 2;
//                int op = 1;
//                auto pid = op_pids[i];
//                if (op == 0) {
//                    // Write
//                    ConcurrentBufferManager::PageAccessor accessor;
//                    s = buf_mgr.Get(pid, accessor, ConcurrentBufferManager::INTENT_WRITE);
//                    accessor.GetPageDesc()->LatchExclusive();
//                    assert(s.ok());
//                    Slice slice = accessor.PrepareForWrite(0, kPageSize);
//                    mem_buf_pools2[pid] = std::vector<char>(kPageSize, std::max(1, i % 256));
//                    memcpy(slice.data(), mem_buf_pools2[pid].data(), kPageSize);
//                    accessor.GetPageDesc()->UnlatchExclusive();
//                    buf_mgr.Put(accessor.GetPageDesc());
//                } else {
//                    // Read
//                    ConcurrentBufferManager::PageAccessor accessor;
//                    s = buf_mgr.Get(pid, accessor, ConcurrentBufferManager::INTENT_READ);
//                    accessor.GetPageDesc()->LatchShared();
//                    assert(s.ok());
//                    Slice slice = accessor.PrepareForRead(0, kPageSize);
//                    std::vector<char> page_content(kPageSize, 0);
//                    memcpy(page_content.data(), slice.data(), kPageSize);
//                    auto correct_content = mem_buf_pools2[pid];
//                    assert(page_content == correct_content);
//                    accessor.GetPageDesc()->UnlatchShared();
//                    buf_mgr.Put(accessor.GetPageDesc());
//                }
//            }

            size_t access_size_range = 2048;
            // Partial page reads and writes
            int cnt = 0;
            //auto target_pid = pids[]
            for (int i = start; i < end; ++i) {
                //int op = IntRand() % 2;
                int op = 1;
                auto pid = op_pids[i];
                //size_t off = IntRand() % kPageSize;
                size_t off = 0;
                //size_t size = std::min(IntRand() % access_size_range, kPageSize - off);
                size_t size = 1024;

                if (op == 0) {
                    // Write
                    ConcurrentBufferManager::PageAccessor accessor;
                    s = buf_mgr.Get(pid, accessor, ConcurrentBufferManager::INTENT_WRITE);
                    accessor.GetPageDesc()->LatchExclusive();
                    assert(s.ok());
                    Slice slice = accessor.PrepareForWrite(off, size);
                    auto &mem_page = mem_buf_pools2[pid];
                    char new_c = std::max(1, i % 256);
                    fill(mem_page.begin() + off, mem_page.begin() + off + size, new_c);
                    memset(slice.data(), new_c, size);
                    accessor.GetPageDesc()->UnlatchExclusive();
                    buf_mgr.Put(accessor.GetPageDesc());
                } else {
                    // Read
                    ConcurrentBufferManager::PageAccessor accessor;
                    s = buf_mgr.Get(pid, accessor, ConcurrentBufferManager::INTENT_READ);
                    accessor.GetPageDesc()->LatchShared();
                    const auto &mem_page = mem_buf_pools2[pid];
                    assert(s.ok());
                    Slice slice = accessor.PrepareForRead(off, size);
                    std::vector<char> page_content(slice.data(), slice.data() + size);
                    std::vector<char> correct_content(mem_page.begin() + off, mem_page.begin() + off + size);
                    assert(page_content == correct_content);
                    accessor.GetPageDesc()->UnlatchShared();
                    buf_mgr.Put(accessor.GetPageDesc());
                }
            }

            latch.CountDown();
        };

        TimedThroughputReporter reporter(total_ops);
        for (size_t i = 0; i < n_threads; ++i) {
            tp->enqueue(thread_work, i);
        }
        latch.Await();

        std::cout << "Concurrent random test finished, " << total_ops << " page read/writes " << n_threads
                  << " workers." << std::endl;
        std::cout << "Stats: \n" << buf_mgr.GetStatsString() << std::endl;
        std::cout << "Replacer Stats: \n" << buf_mgr.ReplacerStats() << std::endl;
    }
}

void TestConcurrentAdmissionSet() {
    const size_t num_pids = 200;
    std::vector<pid_t> pids;
    for (size_t i = 0; i < num_pids; ++i) {
        pid_t pid = ((uint64_t) rand()) << spitfire::kPageSizeBits;
        pids.push_back(pid);
    }

    spitfire::ConcurrentAdmissionSet admission_set(120);
    for (size_t i = 0; i < 10000; ++i) {
        int idx = rand() % pids.size();
        admission_set.Add(pids[idx]);
    }
    std::cout << "Admission set size " << admission_set.Size() << std::endl;
}

void TestZipf() {
    std::vector<int> zipf_values;
    const double alpha = 1.31;
    const int n = 100;
    spitfire::zipf(zipf_values, alpha, n, 30);
    for (int i = 0; i < zipf_values.size(); ++i) {
        std::cout << zipf_values[i] << " ";
    }
    std::cout << std::endl;
}


std::vector<int> GetWorkload(int n_pages, int n_ops, const std::string &workload_filepath) {
    std::vector<int> workload;
    workload.reserve(n_ops);
    if (workload_filepath.empty()) {
        for (int i = 0; i < n_ops; ++i) {
            workload.push_back(IntRand() % n_pages);
        }
    } else {
        std::ifstream infile(workload_filepath);
        while (!infile.eof()) {
            int idx;
            infile >> idx;
            workload.push_back(idx);
        }
    }
    return workload;
}


int main(int argc, char **argv) {
    std::string db_path = "/mnt/optane";
    std::string workload_filepath = ""; // Empty as default uniform workload
    std::string nvm_path = "/mnt/optane";
    bool enable_mini_page = false;
    if (argc > 1) {
        db_path = std::string(argv[1]);
    }
    if (argc > 2) {
        nvm_path = argv[2];
    }
    if (argc > 3) {
        workload_filepath = argv[3];
    }
    if (argc > 4) {
        enable_mini_page = std::stoi(argv[4]);
    }

    spitfire::PosixEnv::CreateDir(db_path);
    spitfire::PosixEnv::DeleteDir(nvm_path);
    spitfire::PosixEnv::CreateDir(nvm_path);
    std::string nvm_heap_filepath = nvm_path + "/nvm/heapfile";
//    const size_t nvm_heap_size = 16UL * 1024 * 1024 * 1024; // 16GB;
//    spitfire::libpm_init(nvm_heap_filepath, nvm_heap_size);

//    {
//        TestSSDPageManager(db_path, false);
//        TestSSDPageManager(db_path, true);
//    }
//
//    {
//        spitfire::PageMigrationPolicy policy;
//        TestBufferPool<spitfire::BufferManager>(db_path, policy, false);
//        TestBufferPool<spitfire::BufferManager>(db_path, policy, true);
//
//        for (double Dp = 0.1; Dp <= 1; Dp += 0.1) {
//            policy.Dr = Dp;
//            policy.Dw = Dp;
//            for (double Np = 0.1; Np <= 1; Np += 0.1){
//                policy.Nr = Np;
//                policy.Nw = Np;
//                TestBufferPool<spitfire::BufferManager>(db_path, policy, false);
//                TestBufferPool<spitfire::BufferManager>(db_path, policy, true);
//                std::cout << Dp << " " << Np << std::endl;
//            }
//        }
//    }

    const size_t n_threads = 8;
    spitfire::ThreadPool tp(n_threads);
    {
        spitfire::PageMigrationPolicy policy;
        //policy.Dr = policy.Dw = policy.Nr = policy.Nw = 0.5;
        policy.Dr = policy.Dw = 1;
        policy.Nr = policy.Nw = 1;
        //TestConcurrentBufferPoolNVMBlockGrainedAccess(db_path, policy, n_threads, &tp, false);

        const int n_pages = 1000;
        const int n_ops = n_pages * 100;
        spitfire::BufferPoolConfig config;
        config.enable_nvm_buf_pool = false;
        config.enable_mini_page = enable_mini_page;
        //config.nvm_admission_set_size_limit = 100;
        //config.enable_hymem = true;
        config.dram_buf_pool_cap_in_bytes = n_pages / 10 * spitfire::kPageSize;
        config.nvm_buf_pool_cap_in_bytes = n_pages / 5 * spitfire::kPageSize;
        config.enable_direct_io = true;
        config.nvm_heap_file_path = nvm_heap_filepath;

        auto workload = GetWorkload(n_pages, n_ops, workload_filepath);

        for (size_t t = n_threads; t <= n_threads; ++t) {
            TestConcurrentBufferPoolNVMBlockGrainedAccess(config, db_path, policy, t, &tp, n_pages, n_ops, workload);
        }

        for (double Dp = 0.1; Dp <= 1; Dp += 0.1) {
            policy.Dr = Dp;
            policy.Dw = Dp;
            for (double Np = 0.1; Np <= 1; Np += 0.1) {
                policy.Nr = Np;
                policy.Nw = Np;
                config.enable_nvm_buf_pool = false;
                TestConcurrentBufferPoolNVMBlockGrainedAccess(config, db_path, policy, n_threads, &tp, n_pages, n_ops, workload);
                config.enable_nvm_buf_pool = true;
                TestConcurrentBufferPoolNVMBlockGrainedAccess(config, db_path, policy, n_threads, &tp, n_pages, n_ops,
                                                              workload);
                std::cout << Dp << " " << Np << std::endl;
            }
        }
    }

    return 0;
}