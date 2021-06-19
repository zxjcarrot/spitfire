#include <stdio.h>
#include <stdint.h>
#include <assert.h>
#include <ulib/math_bit.h>
#include <ulib/util_timer.h>

#define TIME(st) ({							\
			ulib_timer_t _timer;				\
			timer_start(&_timer);				\
			volatile unsigned int _r = st;			\
			printf(#st " elapsed: %f\n", timer_stop(&_timer)); \
			_r;						\
		})

uint64_t u, v, w;

unsigned int hweight64_time()
{
	unsigned int r = 0;

	for (uint64_t i = 0; i < 100000000; ++i)
		r += hweight64(i);
	return r;
}

unsigned int hweight32_time()
{
	unsigned int r = 0;

	for (uint32_t i = 0; i < 100000000; ++i)
		r += hweight32(i);
	return r;
}

unsigned int popcount_time()
{
	unsigned int r = 0;

	for (uint32_t i = 0; i < 100000000; ++i)
		r += __builtin_popcount(i);
	return r;
}

unsigned int popcountl_time()
{
	unsigned int r = 0;

	for (uint64_t i = 0; i < 100000000; ++i)
		r += __builtin_popcountl(i);
	return r;
}

unsigned int hweight32_hakmem_time()
{
	unsigned int r = 0;

	for (uint32_t i = 0; i < 100000000; ++i)
		r += hweight32_hakmem(i);
	return r;
}

unsigned int hweight16_time()
{
	unsigned int r = 0;

	for (uint32_t i = 0; i < 100000000; ++i)
		r += hweight16(i);
	return r;
}

unsigned int hweight15_time()
{
	unsigned int r = 0;

	for (uint32_t i = 0; i < 100000000; ++i)
		r += hweight15(i);
	return r;
}

int main()
{
	TIME(hweight64_time());
	TIME(hweight32_time());
	TIME(hweight32_hakmem_time());
	TIME(hweight16_time());
	TIME(hweight15_time());
	TIME(popcount_time());
	TIME(popcountl_time());
	printf("passed\n");
	return 0;
}

