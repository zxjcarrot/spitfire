//
// Created by zxjcarrot on 2019-12-22.
//

#include <iostream>
#include <unordered_map>
#include <vector>
#include <map>
#include <limits>
#include <thread>
#include <chrono>
#include <atomic>
#include <emmintrin.h>


using namespace std::chrono;


struct LogRequestBuffer {
    int cnt{0};
    uint8_t pad[64- sizeof(cnt)];
};

void PushBaseLogRecordAllocation(const size_t n_threads, const size_t n_request) {
    std::vector<LogRequestBuffer> buffers(n_threads);
    std::vector<std::thread> txn_workers;
    const size_t n_request_per_thread = n_request / n_threads;
    for (int i = 0; i < n_threads; ++i) {
        buffers[i].cnt = 1;
        txn_workers.emplace_back(std::thread([&](int id) {
            int n = n_request_per_thread;
            int work = 0;
            while (n) {
                int cnt = 0;
                buffers[id].cnt = 1;
                while (buffers[id].cnt == 1) {
                    if (++cnt < 5) {
                        _mm_pause();
                    } else {
                        std::this_thread::yield();
                    }
                }
//                for (int i = 0; i < 10; ++i) {
//                    work += n + i;
//                }
                n--;
            }
            printf("%d\n", work);
        }, i));
    }
    std::atomic<bool> end_distribution{false};
    auto distributor = std::thread([&]() {
        size_t lsn = 0;
        while (end_distribution.load() == false) {
            for (int i = 0; i < n_threads; ++i) {
                if (buffers[i].cnt == 1) {
                    buffers[i].cnt = 0;
                    lsn++;
                }
            }
        }
    });

    for (int i = 0; i < n_threads; ++i) {
        txn_workers[i].join();
    }
    end_distribution.store(true);
    distributor.join();
}


void PullBaseLogRecordAllocation(const size_t n_threads, const size_t n_request) {
    std::atomic<uint64_t> lsn{0};
    std::vector<std::thread> txn_workers;
    const size_t n_request_per_thread = n_request / n_threads;
    for (int i = 0; i < n_threads; ++i) {
        txn_workers.emplace_back(std::thread([&]() {
            int n = n_request_per_thread;
            int work = 0;
            while (n) {
                size_t cur_lsn = 0;
                do {
                    cur_lsn = lsn.load();
                } while (lsn.compare_exchange_strong(cur_lsn, cur_lsn + 1) == false);
//                for (int i = 0; i < 10; ++i) {
//                    work += n + i;
//                }
                n--;
            }
            printf("%d\n", work);
        }));
    }
    for (int i = 0; i < n_threads; ++i) {
        txn_workers[i].join();
    }
}

int main(int argc, char **argv) {
    size_t n_threads = 20;
    size_t n_request = 3e7;
    printf("n_threads: %llu, n_requests: %llu\n", n_threads, n_request);
    {
        long start = duration_cast<milliseconds>(
                system_clock::now().time_since_epoch()
        ).count();
        PushBaseLogRecordAllocation(n_threads, n_request);
        long end = duration_cast<milliseconds>(
                system_clock::now().time_since_epoch()
        ).count();
        auto diff_sec = ((end - start) / 1000.0);
        if (diff_sec < 0.001) {
            diff_sec = 0.001;
        }
        printf("PushBaseLogRecordAllocation Throughput: %f op/s, took %fs\n", n_request / diff_sec, diff_sec);
    }


    {
        long start = duration_cast<milliseconds>(
                system_clock::now().time_since_epoch()
        ).count();
        PullBaseLogRecordAllocation(n_threads, n_request);
        long end = duration_cast<milliseconds>(
                system_clock::now().time_since_epoch()
        ).count();
        auto diff_sec = ((end - start) / 1000.0);
        if (diff_sec < 0.001) {
            diff_sec = 0.001;
        }
        printf("PullBaseLogRecordAllocation Throughput: %f op/s, took %fs\n", n_request / diff_sec, diff_sec);
    }


    return 0;
}