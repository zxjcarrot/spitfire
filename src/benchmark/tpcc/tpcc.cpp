//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpcc.cpp
//
// Identification: src/main/tpcc/tpcc.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <fstream>
#include <iomanip>

#include "util/logger.h"
#include "../../../include/config.h"
#include "benchmark/tpcc/tpcc_configuration.h"
#include "benchmark/tpcc/tpcc_loader.h"
#include "benchmark/tpcc/tpcc_workload.h"
#include "../../../include/benchmark/tpcc/tpcc_configuration.h"


namespace spitfire {
namespace benchmark {
namespace tpcc {

configuration state;

// Main Entry Point
void RunBenchmark() {
    LOG_INFO("%s : %d", "Run exponential backoff", state.exp_backoff);
    if (state.db_path.empty()) {
        state.db_path = "/mnt/optane/spitfire/tpcc";
    }
    spitfire::PosixEnv::CreateDir(state.db_path);

    if (state.bp_mode == BufferPoolMode::DRAM_DRAM_SSD) {
        if (!state.nvm_path.empty()) {
            LOG_INFO("Emptied nvm_path[%s]", state.nvm_path.c_str());
            state.nvm_path.clear();
        }
    } else if (state.bp_mode == BufferPoolMode::DRAM_NVM_SSD) {
        if (state.nvm_path.empty()) {
            LOG_INFO("nvm_path must not be empty for bp_mode %s", ToString(state.bp_mode).c_str());
            exit(-1);
        }
        state.bp_config.enable_nvm_buf_pool = true;
    } else if (state.bp_mode == BufferPoolMode::DRAM_SSD) {
        if (!state.nvm_path.empty()) {
            LOG_INFO("Emptied nvm_path[%s]", state.nvm_path.c_str());
            state.nvm_path.clear();
        }
        state.bp_config.enable_nvm_buf_pool = false;
        state.bp_config.enable_mini_page = false;
        LOG_INFO("Disabled NVM buffer pool");
        LOG_INFO("Disabled mini_page optimization");
    } else { //state.bp_mode == BufferPoolMode::NVM_SSD
        if (state.nvm_path.empty()) {
            LOG_INFO("nvm_path must not be empty for bp_mode %s", ToString(state.bp_mode).c_str());
            exit(-1);
        }
        // We disable the mid tier and use only the upper tier as our buffer pool
        state.bp_config.enable_nvm_buf_pool = false;
        state.bp_config.enable_mini_page = false;
        LOG_INFO("Disabled DRAM buffer pool");
        LOG_INFO("Disabled mini_page optimization");
    }

    if (!state.nvm_path.empty()) {
        spitfire::PosixEnv::DeleteDir(state.nvm_path);
        spitfire::PosixEnv::CreateDir(state.nvm_path);
        std::string nvm_heap_filepath = state.nvm_path + "/nvm/heapfile";
        const size_t nvm_heap_size = state.bp_config.nvm_buf_pool_cap_in_bytes * 1.5;
        spitfire::PosixEnv::DeleteFile(nvm_heap_filepath);
        state.bp_config.nvm_heap_file_path = nvm_heap_filepath;
        if (state.bp_mode == BufferPoolMode::NVM_SSD) {
            state.bp_config.dram_buf_pool_cap_in_bytes = state.bp_config.nvm_buf_pool_cap_in_bytes;
        }
    } else {
        LOG_INFO("Running with emulated NVM(DRAM)");
    }

    state.bp_config.enable_hymem = state.enable_hymem;

    if (state.bp_config.enable_nvm_buf_pool && state.bp_config.enable_hymem) {
        state.bp_config.nvm_admission_set_size_limit = (state.bp_config.nvm_buf_pool_cap_in_bytes / kPageSize) * state.admission_set_size;
    }

    LOG_INFO("%s : %f", "scale_factor", state.scale_factor);
    LOG_INFO("%s : %lf", "profile_duration", state.profile_duration);
    LOG_INFO("%s : %lf", "duration", state.duration);
    LOG_INFO("%s : %lf", "warmup duration", state.warmup_duration);
    LOG_INFO("%s : %d", "lodaer_count", state.loader_count);
    LOG_INFO("%s : %d", "backend_count", state.backend_count);
    LOG_INFO("%s : %d", "warehouse_count", state.warehouse_count);
    LOG_INFO("%s : %s", "db_path", state.db_path.c_str());
    LOG_INFO("%s : %s", "nvm_path", state.nvm_path.c_str());
    LOG_INFO("%s : %s", "wal_path", state.wal_path.c_str());
    LOG_INFO("%s : %d", "enable_mini_page", state.bp_config.enable_mini_page);
    LOG_INFO("%s : %.3fMBs", "DRAM buffer size", state.bp_config.dram_buf_pool_cap_in_bytes / 1000000.0);
    LOG_INFO("%s : %.3fMBs", "NVM buffer size", state.bp_config.nvm_buf_pool_cap_in_bytes / 1000000.0);
    LOG_INFO("%s : %lf", "dram_read_prob", state.mg_policy.Dr);
    LOG_INFO("%s : %lf", "dram_write_prob", state.mg_policy.Dw);
    LOG_INFO("%s : %lf", "nvm_read_prob", state.mg_policy.Nr);
    LOG_INFO("%s : %lf", "nvm_write_prob", state.mg_policy.Nw);
    LOG_INFO("%s : %d", "enable_nvm_buf_pool", state.bp_config.enable_nvm_buf_pool);
    LOG_INFO("%s : %s", "bp_mode", ToString(state.bp_mode).c_str());
    LOG_INFO("%s : %d", "enable_direct_io", state.bp_config.enable_direct_io);
    LOG_INFO("%s : %d", "enable_annealing", state.enable_annealing);
    LOG_INFO("%s : %d", "enable_hymem", state.enable_hymem);
    LOG_INFO("%s : %d", "nvm_block_size", spitfire::kNVMBlockSize);

    if (state.enable_hymem) {
        LOG_INFO("%s : %f", "admission_set_size", state.admission_set_size);
    }

    bool skip_workload = state.bp_config.dram_buf_pool_cap_in_bytes == 0 && state.bp_config.nvm_buf_pool_cap_in_bytes == 0;

    if (state.bp_config.dram_buf_pool_cap_in_bytes == 0) {
        state.bp_config.dram_buf_pool_cap_in_bytes = 100000 * kPageSize;
    }

    if (state.bp_config.nvm_buf_pool_cap_in_bytes == 0) {
        state.bp_config.nvm_buf_pool_cap_in_bytes = 1000000 * kPageSize;
    }


    if (!state.wal_path.empty()) {
        std::string wal_file_path = state.wal_path + "/wal";
        state.bp_config.wal_file_path = wal_file_path;
    }


    using namespace spitfire;
    Status s;
    if (state.load_existing_db == false) {
        s = SSDPageManager::DestroyDB(state.db_path);
        assert(s.ok());
    }
    SSDPageManager ssd_page_manager(state.db_path, state.bp_config.enable_direct_io);
    s = ssd_page_manager.Init();
    assert(s.ok());
    ConcurrentBufferManager buf_mgr(&ssd_page_manager, state.mg_policy, state.bp_config);
    if (state.bp_mode == BufferPoolMode::NVM_SSD) {
        buf_mgr.SetNVMSSDMode();
    }
    s = buf_mgr.Init();
    assert(s.ok());

    if (!state.nvm_path.empty()) {
        if (state.bp_mode == BufferPoolMode::NVM_SSD) {
            assert(state.bp_config.enable_mini_page == false);
            assert(state.bp_config.enable_nvm_buf_pool == false);
            // Use nvm-memory allocator in the dram allocator hooks.
            buf_mgr.GetConfig().dram_malloc = buf_mgr.GetConfig().nvm_malloc;
            buf_mgr.GetConfig().dram_free = buf_mgr.GetConfig().nvm_free;
        }
    }


    if (state.load_existing_db == false) {
        LOG_INFO("Loading database from scratch");
        // Create the database
        CreateTPCCDatabase(&buf_mgr);

        // Load the databases
        LoadTPCCDatabase(&buf_mgr);
    } else {
        // Load database from existing db_path
        LOG_INFO("Loaded database from %s", state.db_path.c_str());
        CreateTPCCDatabaseFromPersistentStorage(&buf_mgr);
    }

    s = buf_mgr.InitLogging();
    assert(s.ok());
    
    LOG_INFO("Buffer Manager Stats:\n%s\n", buf_mgr.GetStatsString().c_str());
    if (!skip_workload) {
        if (state.warmup_duration != 0) {
            RunWarmupWorkload(&buf_mgr);
        }

        // Run the workload
        RunWorkload(&buf_mgr);
    }
    // Emit throughput
    WriteOutput();

    DestroyTPCCDatabase(&buf_mgr);
}

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton

int main(int argc, char **argv) {
    spitfire::benchmark::tpcc::ParseArguments(argc, argv,
                                                        spitfire::benchmark::tpcc::state);

    spitfire::benchmark::tpcc::RunBenchmark();

    return 0;
}
