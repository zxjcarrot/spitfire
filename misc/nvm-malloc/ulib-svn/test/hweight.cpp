#include <stdio.h>
#include <stdint.h>
#include <assert.h>
#include <ulib/math_bit.h>
#include <ulib/math_rand_prot.h>
#include <ulib/os_rdtsc.h>

uint64_t u, v, w;

void hweight64_test()
{
	for (int i = 0; i < 100000; ++i) {
		uint64_t r = RAND_NR_NEXT(u, v, w);
		assert(__builtin_popcountll(r) == hweight64(r));
	}
}

void hweight32_test()
{
	for (int i = 0; i < 100000; ++i) {
		uint32_t r = RAND_NR_NEXT(u, v, w);
		assert(__builtin_popcountl(r) == hweight32(r));
	}
}

void hweight32_hakmem_test()
{
	for (int i = 0; i < 100000; ++i) {
		uint32_t r = RAND_NR_NEXT(u, v, w);
		assert(__builtin_popcountl(r) == hweight32_hakmem(r));
	}
}

void hweight16_test()
{
	for (int i = 0; i < 100000; ++i) {
		uint16_t r = RAND_NR_NEXT(u, v, w);
		assert(__builtin_popcount(r) == hweight16(r));
	}
}

void hweight15_test()
{
	for (int i = 0; i < 100000; ++i) {
		uint16_t r = RAND_NR_NEXT(u, v, w) & 0x7fff;
		if (r == 0x7fffu)
			--r;
		assert(__builtin_popcount(r) == hweight15(r));
	}
}

int main()
{
	uint64_t seed = rdtsc();
	RAND_NR_INIT(u, v, w, seed);
	hweight64_test();
	hweight32_test();
	hweight32_hakmem_test();
	hweight16_test();
	hweight15_test();

	printf("passed\n");
	return 0;
}
