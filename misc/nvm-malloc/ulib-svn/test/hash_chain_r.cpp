#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <ulib/hash_func.h>
#include <ulib/util_class.h>
#include <ulib/hash_chain_r.h>
#include <ulib/math_rand_prot.h>

using namespace ulib;

struct str {
	const char *c_str;

	str(const char *s = 0)
		: c_str(s) { }

	operator size_t() const
	{ return hash_fast64((const unsigned char *)c_str, strlen(c_str), 0); }

	bool operator==(const str &other) const
	{ return strcmp(c_str, other.c_str) == 0; }
};

struct combiner : public do_nothing_combiner<uint32_t> {
	virtual void
	operator() (uint32_t &sum, const uint32_t &v) const
	{ sum += v; }
};

int main()
{
	chain_hash_map_r<str, int> months(128);

	months["january"] = 31;
	months["february"] = 28;
	months["march"] = 31;
	months["april"] = 30;
	months["may"] = 31;
	months["june"] = 30;
	months["july"] = 31;
	months["august"] = 31;
	months["september"] = 30;
	months["october"] = 31;
	months["november"] = 30;
	months["december"] = 31;

	assert(months["september"] == 30);
	assert(months["april"] == 30);
	assert(months["february"] == 28);
	assert(months["december"] == 31);

	chain_hash_map_r<uint64_t, int> map(1024);

	map[1] = 2;
	map[2] = 1;
	map[3] = 3;

	uint64_t seed = time(NULL);
	uint64_t num = seed;

	// insert 100000 random numbers
	for (int i = 0; i < 100000; ++i) {
		RAND_XORSHIFT(num, 7, 5, 47);
		map[num] = -1;
	}
	// check if all random numbers can be found
	num = seed;
	for (int i = 0; i < 100000; ++i) {
		RAND_XORSHIFT(num, 7, 5, 47);
		assert(map[num] == -1);
	}
	// then remove all random numbers
	num = seed;
	for (int i = 0; i < 100000; ++i) {
		RAND_XORSHIFT(num, 7, 5, 47);
		map.erase(num);
	}
	// ensure all random numbers were erased
	num = seed;
	for (int i = 0; i < 100000; ++i) {
		RAND_XORSHIFT(num, 7, 5, 47);
		assert(map[num] == 0); // default value for new elemnets is zero
	}

	chain_hash_map_r<uint64_t, uint32_t, ulib_except, combiner> wc(128);

	wc.combine(1, 1);
	assert(wc[1] == 1);
	wc.combine(1, 3);
	assert(wc[1] == 4);

	wc[100]	 = 2;
	wc[1234] = 100;
	wc[667]	 = 200;
	wc[139]	 = 139;

	uint32_t size = 0;

	printf("nbucket=%zu\n", wc.bucket_count());

	for (chain_hash_map_r<uint64_t, uint32_t, ulib_except, combiner>::iterator it = wc.begin();
	     it != wc.end(); ++it)
		++size;

	assert(size == 5);
	assert(wc.size() == 5);

	chain_hash_map_r<uint64_t, uint32_t, ulib_except, combiner> wc1(1);
	wc1.insert(1, 1);
	assert(wc1.size() == 1);

	printf("passed\n");

	return 0;
}
