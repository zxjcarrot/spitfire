#include <time.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include <ulib/hash_func.h>
#include <ulib/hash_open.h>

#define SIZE (100 * 1024 * 1024 + 3)

using namespace ulib;

int main()
{
	char buf[4096] = { 0 };
	open_hash_set<uint64_t> hashes;

	for (unsigned s = 0; s < 1000; ++s) {
		for (unsigned i = 0; i < sizeof(buf); ++i) {
			uint64_t hash = hash_fast64(buf, i, s);
			if (hashes.contain(hash)) {
				fprintf(stderr, "%016llx already exists\n",
					(unsigned long long) hash);
				exit(EXIT_FAILURE);
			} else
				hashes.insert(hash);
		}
	}

	uint64_t nil = 0;

	printf("0 fasthash64: %016llx\n", (unsigned long long)hash_fast64(&nil, sizeof(nil), 0));
	printf("1 fasthash64: %016llx\n", (unsigned long long)hash_fast64(&nil, sizeof(nil), 1));
	printf("0 fasthash32: %08x\n",	hash_fast32(&nil, sizeof(nil), 0));
	printf("1 fasthash32: %08x\n",	hash_fast32(&nil, sizeof(nil), 1));

// Test vectors:
//	A1, A2, A3 = 12983ffe21252f81, 884fc614fc2fa70e, d9712048288274b2
//	B1, B2, B3 = f3e342c9aea341d9, f91881833784ba2f, 3696ebaba379d7d2
//	a1, a2, a3 = 0e8cef83, 73dfe0fa, 4f11546a
//	b1, b2, b3 = babfff10, 3e6c38ac, 6ce2ec27

	char *buf1 = new char [SIZE];
	memset(buf1, 0x00, SIZE);
	assert(hash_fast64(buf1, SIZE, 0) == 0x12983ffe21252f81ul);
	assert(hash_ferm64(buf1, SIZE, 0) == 0xf3e342c9aea341d9ul);
	assert(hash_fast32(buf1, SIZE, 0) == 0x0e8cef83u);
	assert(hash_ferm32(buf1, SIZE, 0) == 0xbabfff10u);
	memset(buf1, 0x00, SIZE);
	assert(hash_fast64(buf1, SIZE, 0xfeedbeef) == 0x884fc614fc2fa70eul);
	assert(hash_ferm64(buf1, SIZE, 0xfeedbeef) == 0xf91881833784ba2ful);
	assert(hash_fast32(buf1, SIZE, 0xfeedbeef) == 0x73dfe0fau);
	assert(hash_ferm32(buf1, SIZE, 0xfeedbeef) == 0x3e6c38acu);
	memset(buf1, 0xA5, SIZE);
	assert(hash_fast64(buf1, SIZE, 0) == 0xd9712048288274b2ul);
	assert(hash_ferm64(buf1, SIZE, 0) == 0x3696ebaba379d7d2ul);
	assert(hash_fast32(buf1, SIZE, 0) == 0x4f11546au);
	assert(hash_ferm32(buf1, SIZE, 0) == 0x6ce2ec27u);

	printf("passed\n");

	return 0;
}
