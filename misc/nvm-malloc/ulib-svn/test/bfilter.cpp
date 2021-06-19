#include <stdio.h>
#include <assert.h>
#include <ulib/bfilter.h>

int main()
{
	struct bloom_filter bf;

	assert(bfilter_create(&bf, 1000, 100) == 0);
	bfilter_zero(&bf);

	assert(bfilter_get(&bf, "xyz", 3) == 0);
	bfilter_set(&bf, "xyz", 3);
	assert(bfilter_get(&bf, "xyz", 3) == 1);

	bfilter_destroy(&bf);

	printf("passed\n");

	return 0;
}
