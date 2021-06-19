//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb_loader.h
//
// Identification: src/include/benchmark/ycsb/ycsb_loader.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "engine/txn.h"
#include "benchmark/ycsb/ycsb_configuration.h"

namespace spitfire {
namespace benchmark {
namespace ycsb {

typedef DataTable<uint64_t, YCSBTuple> YCSBTable;

extern configuration state;

extern YCSBTable *user_table;

extern ThreadPool the_tp;

void CreateYCSBDatabase(ConcurrentBufferManager *buf_mgr);

void CreateYCSBDatabaseFromPersistentStorage(ConcurrentBufferManager *buf_mgr);

void LoadYCSBDatabase(ConcurrentBufferManager *buf_mgr);

void LoadYCSBRows(ConcurrentBufferManager *buf_mgr, const int begin_rowid, const int end_rowid);

void DestroyYCSBDatabase(ConcurrentBufferManager *buf_mgr);
}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
