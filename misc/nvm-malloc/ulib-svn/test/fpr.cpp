#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <ulib/bfilter.h>
#include <ulib/os_rdtsc.h>
#include <ulib/math_rand_prot.h>

int main(int argc, char *argv[])
{
	struct bloom_filter bf;
	int m = argc > 1? atoi(argv[1]): 100;
	int n = argc > 2? atoi(argv[2]): 1000;
	int t = argc > 3? atoi(argv[3]): 1000000;
	int i, s;
	uint64_t u, v, w, k;

	if (bfilter_create(&bf, n, m)) {
		fprintf(stderr, "create bloom filter failed\n");
		exit(EXIT_FAILURE);
	}

	for (i = 0; i < m; ++i)
		bfilter_set(&bf, &i, sizeof(i));

	k = rdtsc();
	RAND_NR_INIT(u, v, w, k);

	s = 0;
	for (i = 0; i < t; ++i) {
		k = RAND_NR_NEXT(u, v, w);
		RAND_INT3_MIX64(k);
		s += bfilter_get(&bf, &k, sizeof(k));
	}
	bfilter_destroy(&bf);

	printf("fpr:%f\n", (float)s / t);

	printf("passed\n");

	return 0;
}
