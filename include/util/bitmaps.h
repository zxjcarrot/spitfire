//
// Created by zxjcarrot on 2020-03-23.
//

#ifndef SPITFIRE_BITMAPS_H
#define SPITFIRE_BITMAPS_H
#include <nmmintrin.h>
#include <cstdlib>
#include <cstdint>
namespace spitfire {

template<size_t bytes>
struct BitMap {

    BitMap() { ClearAll(); }

    constexpr static size_t kNumUINT64 = (bytes + 7) / 8;
    uint64_t data[kNumUINT64];

    int FirstNotSet() const {
        int num = sizeof(data) / sizeof(uint64_t);
        for (int i = 0; i < num; ++i) {
            if (~data[i] == 0)
                continue;
            return i * 64 + __builtin_ctzll(~data[i]);
        }
        return num * 64;
    }

    inline uint32_t BitOffToByteOff(uint32_t bit_off) const {
        return bit_off / 8;
    }

    inline uint32_t NumZeroesInUInt64(uint64_t x) const {
        //return __builtin_popcountll(~x);
        return _mm_popcnt_u64(~x);
    }

    uint32_t NumZeroesInRange(uint32_t start, uint32_t end) const {
        int int_pos_start = start >> 6;
        int int_pos_end = end >> 6;
        int pos_in_int_start = start & 63;
        int pos_in_int_end = end & 63;
        uint32_t zeroes = 0;
        for (int i = int_pos_start + 1; i < int_pos_end; ++i) {
            zeroes += NumZeroesInUInt64(data[i]);
        }
        if (int_pos_start != int_pos_end) {
            auto mask = (1ULL << pos_in_int_start) - 1;
            zeroes += NumZeroesInUInt64(mask | data[int_pos_start]);
            mask = pos_in_int_end == 63 ? 0 : 0xffffffffffffffff & (~((1ULL << (pos_in_int_end + 1)) - 1));
            zeroes += NumZeroesInUInt64(mask | data[int_pos_end]);
        } else {
            auto mask1 = (1ULL << pos_in_int_start) - 1;
            auto mask2 = pos_in_int_end == 63 ? 0 : 0xffffffffffffffff & (~((1ULL << (pos_in_int_end + 1)) - 1));
            zeroes = NumZeroesInUInt64(data[int_pos_start] | mask1 | mask2);
        }
        return zeroes;
    }

    bool Test(size_t p) const {
        return data[p / 64] & (1ULL << (p % 64));
    }

    void Set(size_t p) {
        data[p / 64] |= (1ULL << (p % 64));
    }

    void Clear(size_t p) {
        data[p / 64] &= ~(1ULL << (p % 64));
    }

    void ClearAll() {
        memset(data, 0, sizeof(data));
    }

    void SetAll() {
        memset(data, 0xff, sizeof(data));
    }

    char *Data() {
        return reinterpret_cast<char *>(data);
    }

    size_t LengthInBytes() const {
        return sizeof(data);
    }

    size_t Count() const {
        size_t n = 0;
        for (int i = 0; i < sizeof(data) / sizeof(uint64_t); ++i) {
            n += __builtin_popcountll(data[i]);
        }
        return n;
    }

    size_t Count(std::vector<int> &bits) const {
        size_t n = 0;
        int bpos = 0;
        for (int i = 0; i < sizeof(data) / sizeof(uint64_t); ++i) {
            int res = __builtin_popcountll(data[i]);
            if (res != 0) {
                auto t = data[i];
                int j = 0;
                while (j < 64) {
                    if ((1ULL << j) & t)
                        bits.push_back(bpos + j);
                    ++j;
                }
            }
            bpos += 64;
            n += res;
        }
        return n;
    }
};


struct AtomicBitmap {
    size_t num_bits = 0;
    size_t num_uint64s = 0;
    std::vector<std::atomic<uint64_t>> data;;

    AtomicBitmap(size_t bits): num_bits(bits), num_uint64s((bits + 63) / 64), data(num_uint64s) {
        ClearAll();
    }

    int TakeFirstNotSet(int start_bit) {
        int num = data.size();
        int start_di = start_bit / 64;
        for (int i = start_di; i < num; ++i) {
            do {
                auto d = data[i].load();
                if (~d == 0)
                    break; // Try next uint64_t
                int pos = __builtin_ctzll(~d);
                auto new_d = d | (1ULL << pos);
                if (data[i].compare_exchange_strong(d, new_d) == true) {
                    return i * 64 + pos;
                }
            } while (true);
        }
        for (int i = 0; i < start_di; ++i) {
            do {
                auto d = data[i].load();
                if (~d == 0)
                    break; // Try next uint64_t
                int pos = __builtin_ctzll(~d);
                auto new_d = d | (1ULL << pos);
                if (data[i].compare_exchange_strong(d, new_d) == true) {
                    return i * 64 + pos;
                }
            } while (true);
        }
        return -1;
    }

    uint32_t BitOffToByteOff(uint32_t bit_off) {
        return bit_off / 8;
    }

    inline uint32_t NumZeroesInUInt64(uint64_t x) {
        return _mm_popcnt_u64(~x);
    }

    bool Test(size_t p) {
        return data[p / 64].load() & (1ULL << (p % 64));
    }

    void Clear(size_t p) {
        size_t int_pos = p / 64;
        size_t pos_in_int = p % 64;
        while (true) {
            auto d = data[int_pos].load();
            assert(d & (1ULL << pos_in_int));
            auto new_d = d & ~(1ULL << pos_in_int);
            if (data[int_pos].compare_exchange_strong(d, new_d))
                break;
        }
    }

    void ClearAll() {
        for (size_t i = 0 ; i < data.size(); ++i) {
            data[i].store(0);
        }
    }

    void SetAll() {
        for (size_t i = 0 ; i < data.size(); ++i) {
            data[i].store(std::numeric_limits<uint64_t >::max());
        }
    }

    inline size_t LengthInBytes() {
        return data.size() * sizeof(uint64_t);
    }

    size_t Count() {
        size_t n = 0;
        for (int i = 0; i < LengthInBytes() / sizeof(uint64_t); ++i) {
            n += __builtin_popcountll(data[i].load());
        }
        return n;
    }
};

}
#endif //SPITFIRE_BITMAPS_H
