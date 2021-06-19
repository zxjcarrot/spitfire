//
// Created by zxjcarrot on 2020-03-28.
//

#ifndef SPITFIRE_TPCC_WORKLOAD_H
#define SPITFIRE_TPCC_WORKLOAD_H


//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpcc_workload.h
//
// Identification: src/include/benchmark/tpcc/tpcc_workload.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "benchmark/tpcc/tpcc_configuration.h"
#include "benchmark/tpcc/tpcc_loader.h"

namespace spitfire {
namespace benchmark {
namespace tpcc {

extern configuration state;

void RunWarmupWorkload(ConcurrentBufferManager *buf_mgr);

void RunWorkload(ConcurrentBufferManager * buf_mgr);

bool RunNewOrder(const size_t &thread_id, ConcurrentBufferManager * buf_mgr);

bool RunPayment(const size_t &thread_id, ConcurrentBufferManager * buf_mgr);

bool RunDelivery(const size_t &thread_id, ConcurrentBufferManager * buf_mgr);

bool RunOrderStatus(const size_t &thread_id, ConcurrentBufferManager * buf_mgr);

bool RunStockLevel(const size_t &thread_id, ConcurrentBufferManager * buf_mgr);

size_t GenerateWarehouseId(const size_t &thread_id);

/////////////////////////////////////////////////////////

void PinToCore(size_t core);

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton

#endif