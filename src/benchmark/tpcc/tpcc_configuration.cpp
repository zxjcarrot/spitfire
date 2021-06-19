//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpcc_configuration.cpp
//
// Identification: src/main/tpcc/tpcc_configuration.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <iomanip>
#include <algorithm>
#include <fstream>

#include "benchmark/tpcc/tpcc_configuration.h"
#include "util/logger.h"

namespace spitfire {
namespace benchmark {
namespace tpcc {

void Usage(FILE *out) {
    fprintf(out,
            "Command line options : tpcc <options> \n"
            "   -h --help              :  print help message \n"
            "   -k --scale_factor      :  scale factor \n"
            "   -d --duration          :  execution duration \n"
            "   -p --profile_duration  :  profile duration \n"
            "   -b --backend_count     :  # of backends \n"
            "   -w --warehouse_count   :  # of warehouses \n"
            "   -e --exp_backoff       :  enable exponential backoff \n"
            "   -a --affinity          :  enable client affinity \n"
            "   -l --loader_count      :  # of loaders \n"
            "   -B --bp_mode           :  0 (DRAM|DRAM|SSD), default | 1 (DRAM|NVM|SSD) | 2 (DRAM|SSD) | 3 (NVM|SSD) \n"
            "   -P --nvm_buf_path      :  directory in which the NVM buffer will be stored\n"
            "   -J --wal_path          :  logging directory\n"
            "   -D --db_path           :  directory where the database files will be stored. (Default: /mnt/optane/spitfire/tpcc )\n"
            "   -M --mini_page         :  enable the mini page optimization\n"
            "   -I --direct_io         :  enable direct io\n"
            "   -Q --dram_read_prob    :  probability of a page read being buffered in DRAM\n"
            "   -W --dram_write_prob   :  probability of a page write being buffered in DRAM\n"
            "   -E --nvm_read_prob     :  probability of a page read being buffered in NVM\n"
            "   -R --nvm_write_prob    :  probability of a page write being buffered in NVM\n"
            "   -T --dram_buf_num_pages:  # pages(16KB) in dram buffer pool\n"
            "   -Y --nvm_buf_num_pages :  # pages(16KB) in nvm buffer pool\n"
            "   -L --load_existing_db  :  whether to load database from db_path instead of generating it from scratch\n"
            "   -U --warmup_duration   :  warmup duration(s)\n"
            "   -A --enable_annealing  :  whether to enable simulated annealing for adaptive data migration\n"
            "   -t --enable_hymem      :  whether to enable the settings in three-tier-buffer by Renen et al.\n"
            "   -X --admission_set_sz  :  size of the admission queue in HyMem settings in percentage of # buffer pages in NVM\n"
    );
}

static struct option opts[] = {
        { "scale_factor", optional_argument, NULL, 'k' },
        { "duration", optional_argument, NULL, 'd' },
        { "profile_duration", optional_argument, NULL, 'p' },
        { "backend_count", optional_argument, NULL, 'b' },
        { "warehouse_count", optional_argument, NULL, 'w' },
        { "exp_backoff", no_argument, NULL, 'e' },
        { "affinity", no_argument, NULL, 'a' },
        { "loader_count", optional_argument, NULL, 'n' },
        { "bp_mode", optional_argument, NULL, 'B' },
        { "mini_page", optional_argument, NULL, 'M' },
        { "direct_io", optional_argument, NULL, 'I' },
        { "db_path", optional_argument, NULL, 'D' },
        { "nvm_buf_path", optional_argument, NULL, 'P' },
        { "wal_path", optional_argument, NULL, 'J' },
        { "dram_read_prob", optional_argument, NULL, 'Q' },
        { "dram_write_prob", optional_argument, NULL, 'W' },
        { "nvm_read_prob", optional_argument, NULL, 'E' },
        { "nvm_write_prob", optional_argument, NULL, 'R' },
        { "dram_buf_num_pages", optional_argument, NULL, 'T' },
        { "nvm_buf_num_pages", optional_argument, NULL, 'Y' },
        { "load_existing_db", optional_argument, NULL, 'L' },
        { "warmup_duration", optional_argument, NULL, 'U' },
        { "enable_annealing", optional_argument, NULL, 'A' },
        { "enable_hymem", optional_argument, NULL, 't' },
        { "admission_set_sz", optional_argument, NULL, 'X' },
        { NULL, 0, NULL, 0 }
};

void ValidateScaleFactor(const configuration &state) {
    if (state.scale_factor <= 0) {
        LOG_ERROR("Invalid scale_factor :: %lf", state.scale_factor);
        exit(EXIT_FAILURE);
    }

    LOG_TRACE("%s : %lf", "scale_factor", state.scale_factor);
}

void ValidateDuration(const configuration &state) {
    if (state.duration <= 0) {
        LOG_ERROR("Invalid duration :: %lf", state.duration);
        exit(EXIT_FAILURE);
    }

    LOG_TRACE("%s : %lf", "duration", state.duration);
}

void ValidateProfileDuration(const configuration &state) {
    if (state.profile_duration <= 0) {
        LOG_ERROR("Invalid profile_duration :: %lf", state.profile_duration);
        exit(EXIT_FAILURE);
    }

    LOG_TRACE("%s : %lf", "profile_duration", state.profile_duration);
}

void ValidateBackendCount(const configuration &state) {
    if (state.backend_count <= 0) {
        LOG_ERROR("Invalid backend_count :: %d", state.backend_count);
        exit(EXIT_FAILURE);
    }

    LOG_TRACE("%s : %d", "backend_count", state.backend_count);
}

void ValidateWarehouseCount(const configuration &state) {
    if (state.warehouse_count <= 0) {
        LOG_ERROR("Invalid warehouse_count :: %d", state.warehouse_count);
        exit(EXIT_FAILURE);
    }

    LOG_TRACE("%s : %d", "warehouse_count", state.warehouse_count);
}

void ValidateMigrationProbs(const configuration &state) {
    if (state.mg_policy.Dr < 0 || state.mg_policy.Dr > 1.0) {
        LOG_ERROR("Invalid dram_read_prob :: %lf", state.mg_policy.Dr);
        exit(EXIT_FAILURE);
    }
    if (state.mg_policy.Dw < 0 || state.mg_policy.Dw > 1.0) {
        LOG_ERROR("Invalid dram_read_write :: %lf", state.mg_policy.Dw);
        exit(EXIT_FAILURE);
    }

    if (state.mg_policy.Nr < 0 || state.mg_policy.Nr > 1.0) {
        LOG_ERROR("Invalid nvm_read_prob :: %lf", state.mg_policy.Nr);
        exit(EXIT_FAILURE);
    }

    if (state.mg_policy.Nw < 0 || state.mg_policy.Nw > 1.0) {
        LOG_ERROR("Invalid nvm_write_prob :: %lf", state.mg_policy.Nw);
        exit(EXIT_FAILURE);
    }

}


void ParseArguments(int argc, char *argv[], configuration &state) {
    // Default Values
    state.scale_factor = 1;
    state.duration = 10;
    state.profile_duration = 1;
    state.backend_count = 2;
    state.warehouse_count = 2;
    state.exp_backoff = false;
    state.affinity = false;
    state.loader_count = 1;


    // Parse args
    while (1) {
        int idx = 0;
        int c = getopt_long(argc, argv, "theLAMIagi:k:d:p:b:w:n:D:P:Q:W:X:B:E:U:R:T:Y:J:l:y:", opts, &idx);

        if (c == -1) break;

        switch (c) {
            case 'M':
                state.bp_config.enable_mini_page = true;
                break;
            case 'I':
                state.bp_config.enable_direct_io = true;
                break;
            case 'D':
                state.db_path = optarg;
                break;
            case 'A':
                state.enable_annealing = true;
                break;
            case 't':
                state.enable_hymem = true;
                break;
            case 'X':
                state.admission_set_size = atof(optarg);
                break;
            case 'P':
                state.nvm_path = optarg;
                break;
            case 'J':
                state.wal_path = optarg;
                break;
            case 'Q':
                state.mg_policy.Dr = atof(optarg);
                break;
            case 'W':
                state.mg_policy.Dw = atof(optarg);
                break;
            case 'B':
                state.bp_mode = BufferPoolMode(atoi(optarg));
                break;
            case 'E':
                state.mg_policy.Nr = atof(optarg);
                break;
            case 'U':
                state.warmup_duration = atof(optarg);
                break;
            case 'R':
                state.mg_policy.Nw = atof(optarg);
                break;
            case 'T':
                state.bp_config.dram_buf_pool_cap_in_bytes = atoi(optarg) * kPageSize;
                break;
            case 'Y':
                state.bp_config.nvm_buf_pool_cap_in_bytes = atoi(optarg) * kPageSize;
                break;
            case 'L':
                state.load_existing_db = true;
                break;
            case 'l':
                state.loader_count = atoi(optarg);
                break;
            case 'k':
                state.scale_factor = atof(optarg);
                break;
            case 'd':
                state.duration = atof(optarg);
                break;
            case 'p':
                state.profile_duration = atof(optarg);
                break;
            case 'b':
                state.backend_count = atoi(optarg);
                break;
            case 'w':
                state.warehouse_count = atoi(optarg);
                break;
            case 'e':
                state.exp_backoff = true;
                break;
            case 'a':
                state.affinity = true;
                break;
            case 'h':
                Usage(stderr);
                exit(EXIT_FAILURE);
                break;
            default:
                LOG_ERROR("Unknown option: -%c-", c);
                Usage(stderr);
                exit(EXIT_FAILURE);
        }
    }

    // Static TPCC parameters
    state.item_count = 100000 * state.scale_factor;
    state.districts_per_warehouse = 10;
    state.customers_per_district = 3000 * state.scale_factor;
    state.new_orders_per_district = 900 * state.scale_factor;

    // Print configuration
    ValidateScaleFactor(state);
    ValidateDuration(state);
    ValidateProfileDuration(state);
    ValidateBackendCount(state);
    ValidateWarehouseCount(state);
    ValidateMigrationProbs(state);

    LOG_TRACE("%s : %d", "Run client affinity", state.affinity);
    LOG_TRACE("%s : %d", "Run exponential backoff", state.exp_backoff);
}



void WriteOutput() {
    std::ofstream out("outputfile.summary");

    int total_profile_memory = 0;
    for (auto &entry : state.profile_memory) {
        total_profile_memory += entry;
    }

    LOG_INFO("----------------------------------------------------------");
    LOG_INFO("%lf %d %d :: %lf %lf %lf %d",
             state.scale_factor,
             state.backend_count,
             state.warehouse_count,
             state.warmup_throughput,
             state.throughput,
             state.abort_rate,
             total_profile_memory);

    out << state.scale_factor << " ";
    out << state.backend_count << " ";
    out << state.warehouse_count << " ";
    out << state.warmup_throughput << " ";
    out << state.throughput << " ";
    out << state.abort_rate << "\n";


    for (size_t round_id = 0; round_id < state.profile_throughput.size();
         ++round_id) {
        out << "[" << std::setw(3) << std::left
            << state.profile_duration * round_id << " - " << std::setw(3)
            << std::left << state.profile_duration * (round_id + 1)
            << " s]: " << state.profile_throughput[round_id] << " "
            << state.profile_abort_rate[round_id];
    }
    out.flush();
    out.close();
}


}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
