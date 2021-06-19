#include <stdio.h>
#include <stdlib.h>
#include <ulib/math_rng_gamma.h>

int main(int argc, char *argv[])
{
	int i;
	int t = 10;
	double alpha = 2.0;
	double beta = 1.0;

	gamma_rng_t rng;

	if (argc > 1)
		t = atoi(argv[1]);

	gamma_rng_init(&rng);

	for (i = 0; i < t; i++)
		printf("%f\n", gamma_rng_next(&rng, alpha, beta));

	printf("passed\n");

	return 0;
}
