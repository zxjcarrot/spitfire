#include <stdio.h>
#include <stdlib.h>
#include <ulib/math_rng_zipf.h>

int main(int argc, char *argv[])
{
	int   n = 10;
	int   t = 10;
	int   i;
	float s = 1.0;
	struct zipf_rng rng;

	if (argc > 3) {
		n = atoi(argv[1]);
		s = atof(argv[2]);
		t = atoi(argv[3]);
	}

	zipf_rng_init(&rng, n, s);

	for (i = 0; i < t; i++)
		printf("%d\n", zipf_rng_next(&rng));

	printf("passed\n");

	return 0;
}
