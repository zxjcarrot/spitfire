//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb_workload.cpp
//
// Identification: src/main/ycsb/ycsb_workload.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <iostream>
#include <ctime>
#include <thread>
#include <algorithm>
#include <random>
#include <cstddef>
#include <limits>

#include "benchmark/ycsb/ycsb_workload.h"
#include "benchmark/ycsb/ycsb_configuration.h"
#include "benchmark/ycsb/ycsb_loader.h"
#include "benchmark/minimizer.h"

namespace spitfire {
namespace benchmark {
namespace ycsb {


/////////////////////////////////////////////////////////
// WORKLOAD
/////////////////////////////////////////////////////////

volatile bool is_running = true;

PadInt *abort_counts;
PadInt *commit_counts;

#ifndef __APPLE__

void PinToCore(size_t core) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
#else
    void PinToCore(size_t UNUSED_ATTRIBUTE core) {
    // Mac OS X does not export interfaces that identify processors or control thread placement
    // explicit thread to processor binding is not supported.
    // Reference: https://superuser.com/questions/149312/how-to-set-processor-affinity-on-os-x
#endif
}


void RunWarmupBackend(ConcurrentBufferManager *buf_mgr, const size_t thread_id, const std::vector<uint64_t> &keys) {

    //PinToCore(thread_id);

    ZipfDistribution zipf((state.scale_factor * 1000) - 1,
                          state.zipf_theta);

    FastRandom rng(rand());

    // backoff
    uint32_t backoff_shifts = 0;
    int cnt = 0;
    while (true) {
        if (is_running == false) {
            break;
        }
        ++cnt;
        while (RunMixed(buf_mgr, thread_id, zipf, rng, keys) == false) {
            if (is_running == false) {
                break;
            }
            // backoff
            if (state.exp_backoff) {
                if (backoff_shifts < 13) {
                    ++backoff_shifts;
                }
                uint64_t sleep_duration = 1UL << backoff_shifts;
                sleep_duration *= 100;
                std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
            }
        }
        backoff_shifts >>= 1;
    }
}


void RunBackend(ConcurrentBufferManager *buf_mgr, const size_t thread_id, const std::vector<uint64_t> &keys) {

    //PinToCore(thread_id);

    PadInt &execution_count_ref = abort_counts[thread_id];
    PadInt &transaction_count_ref = commit_counts[thread_id];

    ZipfDistribution zipf((state.scale_factor * 1000) - 1,
                          state.zipf_theta);

    FastRandom rng(rand());

    // backoff
    uint32_t backoff_shifts = 0;
    int cnt = 0;
    while (true) {
        if (is_running == false) {
            break;
        }
//    if (cnt % 1000 == 0)
//        LOG_INFO("%d working %d", thread_id, cnt);
        ++cnt;
        size_t num_rw_ops_snap = num_rw_ops;
        while (RunMixed(buf_mgr, thread_id, zipf, rng, keys) == false) {
            if (is_running == false) {
                break;
            }
            num_rw_ops_snap = num_rw_ops;
            execution_count_ref.data++;
            // backoff
            if (state.exp_backoff) {
                if (backoff_shifts < 13) {
                    ++backoff_shifts;
                }
                uint64_t sleep_duration = 1UL << backoff_shifts;
                sleep_duration *= 100;
                std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
            }
        }
        backoff_shifts >>= 1;
        transaction_count_ref.data += num_rw_ops - num_rw_ops_snap;
    }
    //LOG_INFO("%d Inner Done", thread_id);
}


void RunWarmupWorkload(ConcurrentBufferManager *buf_mgr, const std::vector<uint64_t> &keys) {
    size_t num_threads = 1;
    size_t warmup_duration = state.warmup_duration;

    bool use_eager_policy = state.mg_policy.Nr > 0;
    spitfire::PageMigrationPolicy policy_save = state.mg_policy;
    if (use_eager_policy) {
        state.mg_policy.Nr = state.mg_policy.Nw = 1;
        buf_mgr->SetPageMigrationPolicy(state.mg_policy);
    }
    CountDownLatch latch(num_threads);

    // Launch a group of threads
    for (size_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
        the_tp.enqueue([&latch, thread_itr, buf_mgr, &keys]() {
            //LOG_INFO("%lu Started", thread_itr);
            RunWarmupBackend(buf_mgr, thread_itr, keys);
            //LOG_INFO("%lu Done", thread_itr);
            latch.CountDown();
        });
    }

    for (size_t round_id = 0; round_id < warmup_duration; ++round_id) {
        std::this_thread::sleep_for(
                std::chrono::milliseconds(int(1000)));
    }

    is_running = false;

    // Join the threads with the main thread
    latch.Await();


    //Restore the states
    is_running = true;
    state.mg_policy = policy_save;
    buf_mgr->SetPageMigrationPolicy(policy_save);
    LOG_INFO("Warmed up");
    buf_mgr->ClearStats();
}

void SimulatedAnnealingDriver(PadInt *commit_counts, ConcurrentBufferManager *buf_mgr) {
    size_t num_threads = state.backend_count;
    auto initial_config = state.mg_policy;
    //initial_config.Dr = initial_config.Dw = initial_config.Nw = initial_config.Nr = 0.7;
    std::chrono::time_point<std::chrono::system_clock> last_time;

    // Update the page migration policy.
    auto set_config = [&](const PageMigrationPolicy &new_config) -> void {
        buf_mgr->SetPageMigrationPolicy(new_config);
        last_time = std::chrono::system_clock::now();
    };
    PadInt *prev_commit_counts = new PadInt[num_threads];
    DistributedCounter<32> prev_nvm_writes(0), prev_ssd_writes(0);
    for (int i = 0; i < num_threads; ++i)
        prev_commit_counts[i].data = 0;
    // Get the cost for current configuration
    auto get_current_config_cost = [&]() -> double {
        constexpr double lambda = 0.5; // [0,1]
        constexpr double KB = 1e3;
        auto now = std::chrono::system_clock::now();;
        auto time_diff_in_secs = std::chrono::duration_cast<std::chrono::seconds>(now - last_time).count();
        if (time_diff_in_secs < 1)
            time_diff_in_secs = 1;
        long long commits = 0;
        for (int i = 0; i < num_threads; ++i) {
            commits += commit_counts[i].data - prev_commit_counts[i].data;
            prev_commit_counts[i] = commit_counts[i];
        }
        auto stat = buf_mgr->GetStats();
        auto current_nvm_writes = stat.bytes_direct_write_nvm.load() + stat.bytes_copied_dram_to_nvm.load() +
                                  stat.bytes_copied_ssd_to_nvm.load();
        auto current_ssd_writes = stat.bytes_copied_dram_to_ssd.load() + stat.bytes_copied_nvm_to_ssd.load();

        last_time = now;
        double throughput = commits / 1000 / time_diff_in_secs;
        double nvm_writes_per_sec = (current_nvm_writes - prev_nvm_writes.load()) / KB / time_diff_in_secs;
        double ssd_writes_per_sec = (current_ssd_writes - prev_ssd_writes.load()) / KB / time_diff_in_secs;
        prev_nvm_writes.store(current_nvm_writes);
        prev_ssd_writes.store(current_ssd_writes);
        LOG_INFO("throughput: %f, nvm_writes(KB/s): %f, ssd_writes(KB/s): %f", throughput, nvm_writes_per_sec,
                 ssd_writes_per_sec);
        //return (lambda * nvm_writes_per_sec + (1 - lambda) * ssd_writes_per_sec) / throughput;
        return (1000) / throughput;
    };


    std::random_device rd;
    std::mt19937 generator(rd());
    std::uniform_real_distribution<> distribution(0.0, 0.1);

    auto cap_prob = [](double p) -> double {
        if (p > 1) return 1;
        else if (p <= 0) return 0.01;
        return p;
    };
    // Get neighboring configurations randomly with step size +-0.05
    auto get_neighboring_config = [&](const PageMigrationPolicy &p) -> PageMigrationPolicy {
        PageMigrationPolicy new_config = p;
        
        double factor = (rand() % 100 / 100.0);
        double inc = (rand() & 1) ? 0.3 * factor : -0.3 * factor;
        int d = rand() % 4;
        if (d == 0) {
            new_config.Dr = cap_prob(p.Dr + inc);
        } else if (d == 1) {
            new_config.Dw = cap_prob(p.Dw + inc);
        } else if (d == 2) {
            new_config.Nr = cap_prob(p.Nr + inc);
        } else {
            new_config.Nw = cap_prob(p.Nw + inc);
        }

        return new_config;
    };

    benchmark::SimulatedAnnealing(state.duration, initial_config, set_config, get_current_config_cost,                              get_neighboring_config);
    //benchmark::GradientDescent(state.duration, initial_config, set_config, get_current_config_cost);
    delete[] prev_commit_counts;
}

void RunWorkload(ConcurrentBufferManager *buf_mgr, const std::vector<uint64_t> &keys) {
    // Execute the workload to build the log
    std::vector<std::thread> thread_group;
    size_t num_threads = state.backend_count;

    abort_counts = new PadInt[num_threads];
    memset(abort_counts, 0, sizeof(PadInt) * num_threads);

    commit_counts = new PadInt[num_threads];
    memset(commit_counts, 0, sizeof(PadInt) * num_threads);

    size_t profile_round = (size_t) (state.duration / state.profile_duration);

    PadInt **abort_counts_profiles = new PadInt *[profile_round];
    for (size_t round_id = 0; round_id < profile_round; ++round_id) {
        abort_counts_profiles[round_id] = new PadInt[num_threads];
    }

    PadInt **commit_counts_profiles = new PadInt *[profile_round];
    for (size_t round_id = 0; round_id < profile_round; ++round_id) {
        commit_counts_profiles[round_id] = new PadInt[num_threads];
    }

    CountDownLatch latch(num_threads + state.enable_annealing);
    srand(time(0));
    // Launch a group of threads
    for (size_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
        the_tp.enqueue([&latch, thread_itr, buf_mgr, &keys]() {
            //LOG_INFO("%lu Started", thread_itr);
            RunBackend(buf_mgr, thread_itr, keys);
            //LOG_INFO("%lu Done", thread_itr);
            latch.CountDown();
        });
    }

    if (state.enable_annealing) {
        the_tp.enqueue([&]() {
            SimulatedAnnealingDriver(commit_counts, buf_mgr);
            latch.CountDown();
        });
    }

    //////////////////////////////////////
    for (size_t round_id = 0; round_id < profile_round; ++round_id) {
        std::this_thread::sleep_for(
                std::chrono::milliseconds(int(state.profile_duration * 1000)));
        memcpy(abort_counts_profiles[round_id], abort_counts,
               sizeof(PadInt) * num_threads);
        memcpy(commit_counts_profiles[round_id], commit_counts,
               sizeof(PadInt) * num_threads);

        if (round_id != 0) {
            state.profile_memory.push_back(0);
        }
    }

    state.profile_memory.push_back(state.profile_memory.at(state.profile_memory.size() - 1));

    is_running = false;

    // Join the threads with the main thread
    latch.Await();

    // calculate the throughput and abort rate for the first round.
    uint64_t total_commit_count = 0;
    for (size_t i = 0; i < num_threads; ++i) {
        total_commit_count += commit_counts_profiles[0][i].data;
    }

    uint64_t total_abort_count = 0;
    for (size_t i = 0; i < num_threads; ++i) {
        total_abort_count += abort_counts_profiles[0][i].data;
    }

    state.profile_throughput.push_back(total_commit_count * 1.0 /
                                       state.profile_duration);
    state.profile_abort_rate.push_back(total_abort_count * 1.0 /
                                       total_commit_count);

    // calculate the throughput and abort rate for the remaining rounds.
    for (size_t round_id = 0; round_id < profile_round - 1; ++round_id) {
        total_commit_count = 0;
        for (size_t i = 0; i < num_threads; ++i) {
            total_commit_count += commit_counts_profiles[round_id + 1][i].data -
                                  commit_counts_profiles[round_id][i].data;
        }

        total_abort_count = 0;
        for (size_t i = 0; i < num_threads; ++i) {
            total_abort_count += abort_counts_profiles[round_id + 1][i].data -
                                 abort_counts_profiles[round_id][i].data;
        }
//        if (state.enable_annealing)
        LOG_INFO("%f", total_commit_count * 1.0 / state.profile_duration);
        state.profile_throughput.push_back(total_commit_count * 1.0 /
                                           state.profile_duration);
        state.profile_abort_rate.push_back(total_abort_count * 1.0 /
                                           total_commit_count);
    }

    //////////////////////////////////////////////////
    // calculate the aggregated throughput and abort rate.
    uint64_t warmup_period_commit_count = 0;
    for (size_t i = 0; i < num_threads; ++i) {
        warmup_period_commit_count += commit_counts_profiles[(profile_round - 1) / 2][i].data;
    }

    uint64_t warmup_period_abort_count = 0;
    for (size_t i = 0; i < num_threads; ++i) {
        warmup_period_abort_count += abort_counts_profiles[(profile_round - 1) / 2][i].data;
    }

    state.warmup_throughput = warmup_period_commit_count * 1.0 / (state.duration / 2);

    total_commit_count = 0;
    for (size_t i = 0; i < num_threads; ++i) {
        total_commit_count += commit_counts_profiles[profile_round - 1][i].data;
    }

    total_abort_count = 0;
    for (size_t i = 0; i < num_threads; ++i) {
        total_abort_count += abort_counts_profiles[profile_round - 1][i].data;
    }

    state.throughput = (total_commit_count) * 1.0 / (state.duration);
    state.abort_rate = (total_abort_count) * 1.0 / (total_commit_count + total_abort_count);

    LOG_INFO("Buffer Manager Stats:\n%s\n", buf_mgr->GetStatsString().c_str());
    //LOG_INFO("Clustered Index Stats:\n%s\n", user_table->GetPrimaryIndex().GetStatsString().ToString().c_str());
    //////////////////////////////////////////////////

    // cleanup everything.
    for (size_t round_id = 0; round_id < profile_round; ++round_id) {
        delete[] abort_counts_profiles[round_id];
        abort_counts_profiles[round_id] = nullptr;
    }

    for (size_t round_id = 0; round_id < profile_round; ++round_id) {
        delete[] commit_counts_profiles[round_id];
        commit_counts_profiles[round_id] = nullptr;
    }

    delete[] abort_counts_profiles;
    abort_counts_profiles = nullptr;
    delete[] commit_counts_profiles;
    commit_counts_profiles = nullptr;

    delete[] abort_counts;
    abort_counts = nullptr;
    delete[] commit_counts;
    commit_counts = nullptr;

}


}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
