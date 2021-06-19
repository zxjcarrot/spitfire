#include <stdio.h>
#include <ulib/math_rand_prot.h>
#include <ulib/util_timer.h>

#define TIME(st) ({							\
			ulib_timer_t _timer;				\
			timer_start(&_timer);				\
			volatile uint64_t _r = st;			\
			printf(#st " elapsed: %f\n", timer_stop(&_timer)); \
			_r;						\
		})

uint64_t rand_int_mix64_time()
{
	int i = 0;
	uint64_t k = 1;

	for (i = 0; i < 100000000; ++i)
		RAND_INT_MIX64(k);
	return k;
}

uint64_t rand_int2_mix64_time()
{
	int i = 0;
	uint64_t k = 1;

	for (i = 0; i < 100000000; ++i)
		RAND_INT2_MIX64(k);
	return k;
}

uint64_t rand_int3_mix64_time()
{
	int i = 0;
	uint64_t k = 1;

	for (i = 0; i < 100000000; ++i)
		RAND_INT3_MIX64(k);
	return k;
}

uint64_t rand_int4_mix64_time()
{
	int i = 0;
	uint64_t k = 1;

	for (i = 0; i < 100000000; ++i)
		RAND_INT4_MIX64(k);
	return k;
}

uint64_t fer_mix64_time()
{
	int i = 0;
	uint64_t k = 1;

	for (i = 0; i < 100000000; ++i)
		FER_MIX64(k);
	return k;
}

int main()
{
	TIME(rand_int_mix64_time());
	TIME(rand_int2_mix64_time());
	TIME(rand_int3_mix64_time());
	TIME(rand_int4_mix64_time());
	TIME(fer_mix64_time());
	printf("passed\n");

	return 0;
}
