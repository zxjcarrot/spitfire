//
// Created by zxjcarrot on 2020-02-13.
//

#ifndef SPITFIRE_CUCKOO_MAP_H
#define SPITFIRE_CUCKOO_MAP_H

#include <cstdlib>
#include <cstring>
#include <cstdio>

#include "libcuckoo/cuckoohash_map.hh"
#include "libcuckoo/default_hasher.hh"
namespace spitfire {

// CUCKOO_MAP_TEMPLATE_ARGUMENTS
#define CUCKOO_MAP_TEMPLATE_ARGUMENTS                                \
  template <typename KeyType, typename ValueType, typename HashType, \
            typename PredType>

// CUCKOO_MAP_DEFAULT_ARGUMENTS
#define CUCKOO_MAP_DEFAULT_ARGUMENTS                    \
  template <typename KeyType, typename ValueType,       \
            typename HashType = DefaultHasher<KeyType>, \
            typename PredType = std::equal_to<KeyType>>

// CUCKOO_MAP_TYPE
#define CUCKOO_MAP_TYPE CuckooMap<KeyType, ValueType, HashType, PredType>

// Iterator type
#define CUCKOO_MAP_ITERATOR_TYPE \
  typename cuckoohash_map<KeyType, ValueType, HashType, PredType>::locked_table

CUCKOO_MAP_DEFAULT_ARGUMENTS
class CuckooMap {
public:

    CuckooMap();
    CuckooMap(size_t initial_size);
    ~CuckooMap();

    // Inserts a item
    bool Insert(const KeyType &key, ValueType value);

    // Extracts item with high priority
    bool Update(const KeyType &key, ValueType value);

    // Extracts the corresponding value
    bool Find(const KeyType &key, ValueType &value) const;

    // Delete key from the cuckoo_map
    bool Erase(const KeyType &key);

    // Checks whether the cuckoo_map contains key
    bool Contains(const KeyType &key);

    // Clears the tree (thread safe, not atomic)
    void Clear();

    // Returns item count in the cuckoo_map
    size_t GetSize() const;

    // Checks if the cuckoo_map is empty
    bool IsEmpty() const;

    // Lock the table and get iterator
    // The table would be unlock when the iterator
    // is out of scope
    CUCKOO_MAP_ITERATOR_TYPE
    GetIterator();

    CUCKOO_MAP_ITERATOR_TYPE
    GetConstIterator() const;

private:

    // cuckoo map
    typedef cuckoohash_map<KeyType, ValueType, HashType, PredType> cuckoo_map_t;

    cuckoo_map_t cuckoo_map;
};

}
#endif //SPITFIRE_CUCKOO_MAP_H
