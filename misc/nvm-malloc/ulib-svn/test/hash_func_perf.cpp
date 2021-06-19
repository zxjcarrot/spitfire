#include <stdio.h>
#include <string.h>
#include <ulib/hash_func.h>
#include <ulib/util_timer.h>

#define SIZE (100 * 1024 * 1024 + 3)

#define TIME(st) ({							\
			ulib_timer_t _timer;				\
			timer_start(&_timer);				\
			volatile uint64_t __r = st;			\
			printf(#st " elapsed: %f\n", timer_stop(&_timer)); \
			__r;						\
		})

int main()
{
	uint64_t A1, A2, A3;
	uint32_t a1, a2, a3;
	uint64_t B1, B2, B3;
	uint32_t b1, b2, b3;

	char *buf = new char [SIZE];
	memset(buf, 0x00, SIZE);
	A1 = TIME(hash_fast64(buf, SIZE, 0));
	B1 = TIME(hash_ferm64(buf, SIZE, 0));
	a1 = TIME(hash_fast32(buf, SIZE, 0));
	b1 = TIME(hash_ferm32(buf, SIZE, 0));
	memset(buf, 0x00, SIZE);
	A2 = TIME(hash_fast64(buf, SIZE, 0xfeedbeef));
	B2 = TIME(hash_ferm64(buf, SIZE, 0xfeedbeef));
	a2 = TIME(hash_fast32(buf, SIZE, 0xfeedbeef));
	b2 = TIME(hash_ferm32(buf, SIZE, 0xfeedbeef));
	memset(buf, 0xA5, SIZE);
	A3 = TIME(hash_fast64(buf, SIZE, 0));
	B3 = TIME(hash_ferm64(buf, SIZE, 0));
	a3 = TIME(hash_fast32(buf, SIZE, 0));
	b3 = TIME(hash_ferm32(buf, SIZE, 0));

	printf("A1, A2, A3 = %016lx, %016lx, %016lx\n", A1, A2, A3);
	printf("B1, B2, B3 = %016lx, %016lx, %016lx\n", B1, B2, B3);
	printf("a1, a2, a3 = %08x, %08x, %08x\n", a1, a2, a3);
	printf("b1, b2, b3 = %08x, %08x, %08x\n", b1, b2, b3);

	printf("passed\n");

	return 0;
}
