//
// Created by zxjcarrot on 2020-02-15.
//

//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cuckoo_map.cpp
//
// Identification: src/container/cuckoo_map.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <functional>
#include <iostream>

#include "util/cuckoo_map.h"
#include "engine/txn.h"

namespace spitfire {

CUCKOO_MAP_TEMPLATE_ARGUMENTS
CUCKOO_MAP_TYPE::CuckooMap() {}

CUCKOO_MAP_TEMPLATE_ARGUMENTS
CUCKOO_MAP_TYPE::CuckooMap(size_t initial_size) : cuckoo_map(initial_size) {}

CUCKOO_MAP_TEMPLATE_ARGUMENTS
CUCKOO_MAP_TYPE::~CuckooMap() {}

CUCKOO_MAP_TEMPLATE_ARGUMENTS
bool CUCKOO_MAP_TYPE::Insert(const KeyType &key, ValueType value) {
    auto status = cuckoo_map.insert(key, value);
    //LOG_TRACE("insert status : %d", status);
    return status;
}

CUCKOO_MAP_TEMPLATE_ARGUMENTS
bool CUCKOO_MAP_TYPE::Update(const KeyType &key, ValueType value) {
    auto status = cuckoo_map.update(key, value);
    return status;
}

CUCKOO_MAP_TEMPLATE_ARGUMENTS
bool CUCKOO_MAP_TYPE::Erase(const KeyType &key) {
    auto status = cuckoo_map.erase(key);
    //LOG_TRACE("erase status : %d", status);

    return status;
}

CUCKOO_MAP_TEMPLATE_ARGUMENTS
bool CUCKOO_MAP_TYPE::Find(const KeyType &key, ValueType &value) const {
    auto status = cuckoo_map.find(key, value);
    //LOG_TRACE("find status : %d", status);
    return status;
}

CUCKOO_MAP_TEMPLATE_ARGUMENTS
bool CUCKOO_MAP_TYPE::Contains(const KeyType &key) {
    return cuckoo_map.contains(key);
}

CUCKOO_MAP_TEMPLATE_ARGUMENTS
void CUCKOO_MAP_TYPE::Clear() { cuckoo_map.clear(); }

CUCKOO_MAP_TEMPLATE_ARGUMENTS
size_t CUCKOO_MAP_TYPE::GetSize() const { return cuckoo_map.size(); }

CUCKOO_MAP_TEMPLATE_ARGUMENTS
bool CUCKOO_MAP_TYPE::IsEmpty() const { return cuckoo_map.empty(); }

CUCKOO_MAP_TEMPLATE_ARGUMENTS
CUCKOO_MAP_ITERATOR_TYPE
CUCKOO_MAP_TYPE::GetIterator() { return cuckoo_map.lock_table(); }

CUCKOO_MAP_TEMPLATE_ARGUMENTS
CUCKOO_MAP_ITERATOR_TYPE
CUCKOO_MAP_TYPE::GetConstIterator() const {
    // WARNING: This is a compiler hack and should never be used elsewhere
    // If you are considering using this, please ask Marcel first
    // We need the const iterator on the const object and the cuckoohash
    // library returns a lock_table object. The other option would be to
    // Modify the cuckoohash library which is not neat.
    auto locked_table = const_cast<CuckooMap *>(this)->cuckoo_map.lock_table();
    return locked_table;
}

// Explicit template instantiation
template class CuckooMap<uint32_t, uint32_t>;


// Used in Transaction Processing
template class CuckooMap<TuplePointer, RWType, TuplePointerHasher, TuplePointerComparator>;


// Used in Concurrent Buffer Manager
template class CuckooMap<pid_t, SharedPageDesc*, PidHasher, PidComparator>;

}