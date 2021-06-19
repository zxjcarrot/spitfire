#include <time.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <ulib/hash_func.h>
#include <ulib/hash_open.h>
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

int main()
{
	open_hash_map<str, int> months;

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

	open_hash_map<uint64_t, int> map;

	map[1] = 2;
	map[2] = 1;
	map[3] = 3;

	open_hash_map<uint64_t, int> copy1(map);
	assert(copy1[1] == 2);
	assert(copy1[2] == 1);
	assert(copy1[3] == 3);

	open_hash_map<uint64_t, int> copy2 = map;
	assert(copy2[1] == 2);
	assert(copy2[2] == 1);
	assert(copy2[3] == 3);

	assert(copy1.size() == 3);
	assert(copy1.size() == copy2.size());
	copy2 = copy1;
	assert(copy1.size() == copy2.size());
	copy2 = copy2;
	assert(copy2[1] == 2);
	assert(copy2[2] == 1);
	assert(copy2[3] == 3);
	assert(copy1.size() == copy2.size());

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
	assert(map.size() == 3);
	// ensure all random numbers were erased
	num = seed;
	for (int i = 0; i < 100000; ++i) {
		RAND_XORSHIFT(num, 7, 5, 47);
		assert(map[num] == 0); // default value for new elemnets is zero
	}

	printf("passed\n");

	return 0;
}
