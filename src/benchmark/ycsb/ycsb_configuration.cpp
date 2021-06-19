//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb_configuration.cpp
//
// Identification: src/main/ycsb/ycsb_configuration.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <iomanip>
#include <algorithm>
#include <iostream>
#include <fstream>

#include "benchmark/ycsb/ycsb_configuration.h"
#include "util/logger.h"
#include "../../../include/benchmark/ycsb/ycsb_configuration.h"

namespace spitfire {
namespace benchmark {
namespace ycsb {

void Usage(FILE *out) {
  fprintf(out,
          "Command line options : ycsb <options> \n"
          "   -h --help              :  print help message \n"
          "   -k --scale_factor      :  # of K tuples \n"
          "   -d --duration          :  execution duration \n"
          "   -p --profile_duration  :  profile duration \n"
          "   -b --backend_count     :  # of backends \n"
          "   -o --operation_count   :  # of operations \n"
          "   -u --update_ratio      :  fraction of updates \n"
          "   -z --zipf_theta        :  theta to control skewness \n"
          "   -e --exp_backoff       :  enable exponential backoff \n"
          "   -l --loader_count      :  # of loaders \n"
          "   -B --bp_mode           :  0 (DRAM|DRAM|SSD), default | 1 (DRAM|NVM|SSD) | 2 (DRAM|SSD) | 3 (NVM|SSD) \n"
          "   -P --nvm_buf_path      :  directory in which the NVM buffer will be stored\n"
          "   -J --wal_path          :  logging directory\n"
          "   -D --db_path           :  directory where the database files will be stored\n"
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
          "   -s --shuffle_keys      :  whether to shuffle keys at startup (Default: fasle)\n"
          "   -t --enable_hymem      :  whether to enable HyMem settings"
          "   -X --admission_set_sz  :  size of the admission queue in HyMem settings in percentage of # buffer pages in NVM\n"
  );
}

static struct option opts[] = {
    { "scale_factor", optional_argument, NULL, 'k' },
    { "duration", optional_argument, NULL, 'd' },
    { "profile_duration", optional_argument, NULL, 'p' },
    { "backend_count", optional_argument, NULL, 'b' },
    { "operation_count", optional_argument, NULL, 'o' },
    { "update_ratio", optional_argument, NULL, 'u' },
    { "zipf_theta", optional_argument, NULL, 'z' },
    { "exp_backoff", no_argument, NULL, 'e' },
    { "string_mode", no_argument, NULL, 'm' },
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
    { "shuffle_keys", optional_argument, NULL, 's' },
    { "enable_hymem", optional_argument, NULL, 't' },
    { "admission_set_sz", optional_argument, NULL, 'X' },
    { NULL, 0, NULL, 0 }
};


void ValidateScaleFactor(const configuration &state) {
  if (state.scale_factor <= 0) {
    LOG_ERROR("Invalid scale_factor :: %d", state.scale_factor);
    exit(EXIT_FAILURE);
  }
}

void ValidateDuration(const configuration &state) {
  if (state.duration <= 0) {
    LOG_ERROR("Invalid duration :: %lf", state.duration);
    exit(EXIT_FAILURE);
  }

}

void ValidateProfileDuration(const configuration &state) {
  if (state.profile_duration <= 0) {
    LOG_ERROR("Invalid profile_duration :: %lf", state.profile_duration);
    exit(EXIT_FAILURE);
  }

}

void ValidateBackendCount(const configuration &state) {
  if (state.backend_count <= 0) {
    LOG_ERROR("Invalid backend_count :: %d", state.backend_count);
    exit(EXIT_FAILURE);
  }

}


void ValidateOperationCount(const configuration &state) {
  if (state.operation_count <= 0) {
    LOG_ERROR("Invalid operation_count :: %d", state.operation_count);
    exit(EXIT_FAILURE);
  }

}

void ValidateUpdateRatio(const configuration &state) {
  if (state.update_ratio < 0 || state.update_ratio > 1) {
    LOG_ERROR("Invalid update_ratio :: %lf", state.update_ratio);
    exit(EXIT_FAILURE);
  }

}

void ValidateZipfTheta(const configuration &state) {
  if (state.zipf_theta < 0 || state.zipf_theta > 1.0) {
    LOG_ERROR("Invalid zipf_theta :: %lf", state.zipf_theta);
    exit(EXIT_FAILURE);
  }

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
  state.duration = 30;
  state.profile_duration = 5;
  state.backend_count = 1;
  state.operation_count = 10;
  state.update_ratio = 0.5;
  state.zipf_theta = 0.0;
  state.exp_backoff = false;
  state.string_mode = false;
  state.loader_count = 1;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "hemsALMItk:d:p:b:c:o:X:u:z:l:y:U:B:D:Q:Y:P:W:E:R:T:J:", opts, &idx);

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
      case 'l':
        state.loader_count = atoi(optarg);
        break;
      case 'L':
        state.load_existing_db = true;
        break;
      case 'k':
        state.scale_factor = atoi(optarg);
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
      case 'o':
        state.operation_count = atoi(optarg);
        break;
      case 'u':
        state.update_ratio = atof(optarg);
        break;
      case 'z':
        state.zipf_theta = atof(optarg);
        break;
      case 'e':
        state.exp_backoff = true;
        break;
      case 'm':
        state.string_mode = true;
        break;
      case 's':
        state.shuffle_keys = true;
        break;
      case 'h':
        Usage(stderr);
        exit(EXIT_FAILURE);
        break;

      default:
        LOG_ERROR("Unknown option: -%c-", c);
        Usage(stderr);
        exit(EXIT_FAILURE);
        break;
    }
  }

  // Print configuration
  ValidateScaleFactor(state);
  ValidateDuration(state);
  ValidateProfileDuration(state);
  ValidateBackendCount(state);
  ValidateOperationCount(state);
  ValidateUpdateRatio(state);
  ValidateZipfTheta(state);
  ValidateMigrationProbs(state);
}


void WriteOutput() {
  std::ofstream out("outputfile.summary");

  int total_profile_memory = 0;
  for (auto &entry : state.profile_memory) {
    total_profile_memory += entry;
  }

  LOG_INFO("----------------------------------------------------------");
  LOG_INFO("%d %d %d %lf %lf :: %lf %lf %lf %d",
           state.scale_factor,
           state.backend_count,
           state.operation_count,
           state.update_ratio,
           state.zipf_theta,
           state.warmup_throughput,
           state.throughput,
           state.abort_rate,
           total_profile_memory);

  out << state.scale_factor << " ";
  out << state.backend_count << " ";
  out << state.operation_count << " ";
  out << state.update_ratio << " ";
  out << state.zipf_theta << " ";
  out << state.warmup_throughput << " ";
  out << state.throughput << " ";
  out << state.abort_rate << " ";
  out << total_profile_memory << "\n";

  for (size_t round_id = 0; round_id < state.profile_throughput.size();
       ++round_id) {
    out << "[" << std::setw(3) << std::left
        << state.profile_duration * round_id << " - " << std::setw(3)
        << std::left << state.profile_duration * (round_id + 1)
        << " s]: " << state.profile_throughput[round_id] << " "
        << state.profile_abort_rate[round_id] << " "
        << state.profile_memory[round_id] << "\n";
  }
  out.flush();
  out.close();
}

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
