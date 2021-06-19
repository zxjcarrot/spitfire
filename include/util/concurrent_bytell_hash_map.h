//
// Created by zxjcarrot on 2020-03-04.
//

#ifndef SPITFIRE_CONCURRENT_BYTELL_HASH_MAP_H
#define SPITFIRE_CONCURRENT_BYTELL_HASH_MAP_H
#include <mutex>
#include "flat_hash_map/bytell_hash_map.hpp"

namespace spitfire {
struct PaddedMutex {
    std::mutex mtx;
    char padding[64 - sizeof(mtx)];
};
template<typename K, typename V, typename H, int N = 7>
class concurrent_bytell_hash_map {
public:
    concurrent_bytell_hash_map() {}

    size_t GetBucket(size_t h) {
        return h & (kNumMaps - 1);
    }


    bool FindUnsafe(const K & k, V & v) {
        size_t h = hasher(k);
        auto bucket_idx = GetBucket(h);
        auto it = maps[bucket_idx].find(k);
        if (it == maps[bucket_idx].end())
            return false;
        v = it->second;
        return true;
    }

    bool Find(const K & k, V & v) {
        size_t h = hasher(k);
        auto bucket_idx = GetBucket(h);
        std::lock_guard<std::mutex> g(mutex_group[bucket_idx].mtx);
        auto it = maps[bucket_idx].find(k);
        if (it == maps[bucket_idx].end())
            return false;
        v = it->second;
        return true;
    }

    bool Insert(const K & k, const V & v) {
        size_t h = hasher(k);
        auto bucket_idx = GetBucket(h);
        std::lock_guard<std::mutex> g(mutex_group[bucket_idx].mtx);
        auto pair = maps[bucket_idx].insert(std::make_pair(k, v));
        if (pair.second == true) {
            return true;
        } else {
            return false;
        }
    }

    bool UpsertIf(const K & k, const V & v, std::function<bool (std::pair<const K, const V>)> pred) {
        size_t h = hasher(k);
        auto bucket_idx = GetBucket(h);
        std::lock_guard<std::mutex> g(mutex_group[bucket_idx].mtx);

        auto it = maps[bucket_idx].find(k);
        if (it == maps[bucket_idx].end()) {
            maps[bucket_idx].insert(std::make_pair(k, v));
            return true;
        }
        if (pred(std::make_pair(it->first, it->second))) {
            it->second = v;
            return true;
        }
        return false;
    }


    void LockOnKey(const K & k) {
        size_t h = hasher(k);
        auto bucket_idx = GetBucket(h);
        mutex_group[bucket_idx].mtx.lock();
    }

    void UnlockOnKey(const K & k) {
        size_t h = hasher(k);
        auto bucket_idx = GetBucket(h);
        mutex_group[bucket_idx].mtx.unlock();
    }

    bool UpsertIfUnsafe(const K & k, const V & v, std::function<bool (std::pair<const K, const V>)> pred) {
        size_t h = hasher(k);
        auto bucket_idx = GetBucket(h);
        auto it = maps[bucket_idx].find(k);
        if (it == maps[bucket_idx].end()) {
            maps[bucket_idx].insert(std::make_pair(k, v));
            return true;
        }
        if (pred(std::make_pair(it->first, it->second))) {
            it->second = v;
            return true;
        }
        return false;
    }

    bool Erase(const K & k) {
        size_t h = hasher(k);
        auto bucket_idx = GetBucket(h);
        std::lock_guard<std::mutex> g(mutex_group[bucket_idx].mtx);
        return maps[bucket_idx].erase(k) == 1;
    }

    void Iterate(std::function<void (const K & k, const V & v)> processor) {
        for (int i = 0; i < kNumMaps; ++i) {
            std::lock_guard<std::mutex> g(mutex_group[i].mtx);
            for (const auto & entry : maps[i]) {
                processor(entry.first, entry.second);
            }
        }
    }

    size_t Size() {
        size_t sz = 0;
        for (int i = 0; i < kNumMaps; ++i) {
            sz += maps[i].size();
        }
        return sz;
    }

    void Clear() {
        for (int i = 0; i < kNumMaps; ++i) {
            std::lock_guard<std::mutex> g(mutex_group[i].mtx);
            maps[i].clear();
        }
    }
private:
    constexpr static int kNumMaps = 1 << N;
    ska::bytell_hash_map<K, V, H> maps[kNumMaps];
    PaddedMutex mutex_group[kNumMaps];
    H hasher;
};

}
#endif //SPITFIRE_CONCURRENT_BYTELL_HASH_MAP_H
