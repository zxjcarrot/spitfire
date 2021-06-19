//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb_workload.h
//
// Identification: src/include/benchmark/ycsb/ycsb_workload.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "benchmark/benchmark_common.h"
#include "benchmark/ycsb/ycsb_configuration.h"
#include "engine/executor.h"

namespace spitfire {

namespace benchmark {
namespace ycsb {

extern configuration state;

void RunWorkload(ConcurrentBufferManager * buf_mgr, const std::vector<uint64_t>&);

void RunWarmupWorkload(ConcurrentBufferManager * buf_mgr, const std::vector<uint64_t>&);

bool RunMixed(ConcurrentBufferManager * buf_mgr, const size_t thread_id, ZipfDistribution &zipf, FastRandom &rng, const std::vector<uint64_t>&);

/////////////////////////////////////////////////////////

void PinToCore(size_t core);

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
