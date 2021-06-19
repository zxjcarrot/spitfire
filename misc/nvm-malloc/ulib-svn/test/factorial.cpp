#include <stdio.h>
#include <math.h>
#include <assert.h>
#include <ulib/math_factorial.h>

#define FLOAT_EXACT_EQUAL(x,y) (fabs((x) - (y)) < 0.01)
#define FLOAT_APPRO_EQUAL(x,y) (fabs((x) - (y)) < 0.1)

int main()
{
	assert(ln_factorial(0) == 0);
	assert(FLOAT_EXACT_EQUAL(1.7917595, ln_factorial(3)));
	assert(FLOAT_EXACT_EQUAL(8.5251614, ln_factorial(7)));
	assert(FLOAT_EXACT_EQUAL(42.335616, ln_factorial(20)));
	assert(FLOAT_EXACT_EQUAL(863.23199, ln_factorial(200)));

	assert(FLOAT_APPRO_EQUAL(1, factorial(0)));
	assert(FLOAT_APPRO_EQUAL(1, factorial(1)));
	assert(FLOAT_APPRO_EQUAL(24, factorial(4)));
	assert(FLOAT_APPRO_EQUAL(120, factorial(5)));

	assert(FLOAT_EXACT_EQUAL(0, ln_comb(0, 0)));
	assert(FLOAT_EXACT_EQUAL(0, ln_comb(1, 1)));
	assert(FLOAT_EXACT_EQUAL(3.555348, ln_comb(7, 3)));
	assert(FLOAT_EXACT_EQUAL(1.609438, ln_comb(5, 4)));
	assert(FLOAT_EXACT_EQUAL(14.48910, ln_comb(24, 10)));

	assert(FLOAT_EXACT_EQUAL(1, comb(0, 0)));
	assert(FLOAT_EXACT_EQUAL(1, comb(1, 1)));
	assert(FLOAT_EXACT_EQUAL(1, comb(1, 0)));
	assert(FLOAT_EXACT_EQUAL(0, comb(1, 2)));
	assert(FLOAT_EXACT_EQUAL(35, comb(7, 3)));
	assert(FLOAT_EXACT_EQUAL(6, comb(4, 2)));

	printf("passed\n");
	return 0;
}
