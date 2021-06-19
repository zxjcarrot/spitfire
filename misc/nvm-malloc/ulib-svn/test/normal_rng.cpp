#include <stdio.h>
#include <stdlib.h>
#include <ulib/math_rng_normal.h>

int main(int argc, char *argv[])
{
	int i;
	int t = 10;
	struct normal_rng rng;

	if (argc > 1)
		t = atoi(argv[1]);

	normal_rng_init(&rng);

	for (i = 0; i < t; i++)
		printf("%f\n", normal_rng_next(&rng));

	printf("passed\n");

	return 0;
}
