#include <time.h>
#include <stdio.h>
#include <assert.h>
#include <ulib/math_bit.h>
#include <ulib/math_rand_prot.h>

int main()
{
	uint64_t u, v, w;
	uint64_t seed = time(0);

	RAND_NR_INIT(u, v, w, seed);

	for (int i = 0; i < 100; i++)
		printf("rand number = %llx\n", (unsigned long long)RAND_NR_NEXT(u, v, w));

	uint64_t r = RAND_NR_NEXT(u, v, w);
	uint64_t s = BIN_TO_GRAYCODE(r);
	uint64_t t = BIN_TO_GRAYCODE(r + 1);
	assert(hweight64(t ^ s) == 1);
	GRAYCODE_TO_BIN64(s);

	assert(rev8(5) == 160);
	assert(rev8_hakmem(5) == 160);

	uint64_t hi, lo;
	MULQ(0x1234567887654321ul, 0x77665544332211fful, lo, hi);
	assert(hi == 611815671993850618UL);
	assert(lo == 14353276178066116319UL);

	if (s != r)
		fprintf(stderr, "expected %016llx, acutal %016llx\n",
			(unsigned long long)r,
			(unsigned long long)s);
	else
		printf("passed\n");

	return 0;
}
