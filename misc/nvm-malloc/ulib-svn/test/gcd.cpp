#include <stdio.h>
#include <assert.h>
#include <ulib/math_gcd.h>

int main()
{
	assert(7 == gcd(21, 14));
	assert(7 == gcd(14, 21));

	long x, y;
	egcd(3, 2, &x, &y);
	assert(x == 1 && y == -1);
	egcd(2, 3, &x, &y);
	assert(x == -1 && y == 1);

	assert(3 == invert(8, 3));

	printf("passed\n");

	return 0;
}
