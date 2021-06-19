#include <stdio.h>
#include <assert.h>
#include <ulib/math_lcm.h>

int main()
{
	assert(42 == lcm(21, 14));
	assert(42 == lcm(14, 21));

	printf("passed\n");

	return 0;
}
