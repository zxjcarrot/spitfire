//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb_configuration.h
//
// Identification: src/include/benchmark/ycsb/ycsb_configuration.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <string>
#include <cstring>
#include <getopt.h>
#include <vector>
#include <sys/time.h>
#include <iostream>

#include "engine/txn.h"

namespace spitfire {
namespace benchmark {
namespace ycsb {

#define COLUMN_COUNT 10

struct YCSBTuple : BaseTuple {
    uint32_t key;
    char cols[COLUMN_COUNT][100];
    uint32_t Key() const {
        return key;
    }
};

enum class BufferPoolMode {
    DRAM_DRAM_SSD = 0,
    DRAM_NVM_SSD  = 1,
    DRAM_SSD      = 2,
    NVM_SSD       = 3,
};

static std::string ToString(const BufferPoolMode & mode) {
    if (mode == BufferPoolMode::DRAM_DRAM_SSD) {
        return "DRAM_DRAM_SSD";
    } else if (mode == BufferPoolMode::DRAM_NVM_SSD) {
        return "DRAM_NVM_SSD";
    } else if (mode == BufferPoolMode::DRAM_SSD) {
        return "DRAM_SSD";
    } else {
        return "NVM_SSD";
    }
}

class configuration {
public:
    // size of the table
    int scale_factor;

    // execution duration (in s)
    double duration;

    // profile duration (in s)
    double profile_duration;

    // number of backends
    int backend_count;

    // operation count in a transaction
    int operation_count;

    // update ratio
    double update_ratio;

    // contention level
    double zipf_theta;

    // exponential backoff
    bool exp_backoff;

    // store strings
    bool string_mode;

    // number of loaders
    int loader_count;

    // throughput
    double warmup_throughput = 0;

    // throughput
    double throughput = 0;

    // abort rate
    double abort_rate = 0;

    std::vector<double> profile_throughput;

    std::vector<double> profile_abort_rate;

    std::vector<int> profile_memory;

    // warmup duration (in s)
    double warmup_duration = 0;

    bool shuffle_keys = false;

    bool load_existing_db = false;

    std::string db_path;

    std::string nvm_path;
    
    std::string wal_path;

    BufferPoolConfig bp_config;

    PageMigrationPolicy mg_policy;

    BufferPoolMode bp_mode = BufferPoolMode::DRAM_DRAM_SSD;

    bool enable_annealing = false;

    // Whether enable the settings in three-tier-buffer by Renen et al.
    bool enable_hymem = false;

    double admission_set_size = 0.1;
};

extern configuration state;

void Usage(FILE *out);

void ParseArguments(int argc, char *argv[], configuration &state);

void ValidateScaleFactor(const configuration &state);

void ValidateDuration(const configuration &state);

void ValidateProfileDuration(const configuration &state);

void ValidateBackendCount(const configuration &state);

void ValidateOperationCount(const configuration &state);

void ValidateUpdateRatio(const configuration &state);

void ValidateZipfTheta(const configuration &state);

void ValidateMigrationProbs(const configuration &state);

void WriteOutput();

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
