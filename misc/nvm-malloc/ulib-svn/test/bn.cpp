#include <stdio.h>
#include <assert.h>
#include <ulib/math_bn.h>

int main()
{
	assert(mpower(2, 0, 3) == 1);
	assert(mpower(3, 100, 4) == 1);
	assert(mpower(3, 10000000, 4) == 1);
	assert(mpower(5, 1000, 17) == 16);

	printf("passed\n");

	return 0;
}
