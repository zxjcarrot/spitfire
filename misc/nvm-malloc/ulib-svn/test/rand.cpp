#include <time.h>
#include <stdio.h>
#include <assert.h>
#include <ulib/math_rand_prot.h>

int main()
{
	uint64_t u, v, w;
	uint64_t seed = time(0);

	RAND_NR_INIT(u, v, w, seed);

	for (int i = 0; i < 100; i++)
		printf("rand number = %llx\n", (unsigned long long)RAND_NR_NEXT(u, v, w));

	uint64_t h = 0;

	printf("rand int mix1 = %llx\n", (unsigned long long)RAND_INT_MIX64(h));
	printf("rand int mix2 = %llx\n", (unsigned long long)RAND_INT2_MIX64(h));
	printf("rand int mix3 = %llx\n", (unsigned long long)RAND_INT3_MIX64(h));
	printf("rand int mix4 = %llx\n", (unsigned long long)RAND_INT4_MIX64(h));

	uint64_t r = RAND_NR_NEXT(u, v, w);
	uint64_t s = r;
	RAND_INT4_MIX64(s);
	RAND_INT4_MIX64_INV(s);

	if (s != r)
		fprintf(stderr, "expected %016llx, acutal %016llx\n",
			(unsigned long long)r,
			(unsigned long long)s);
	else
		printf("passed\n");

	return 0;
}
