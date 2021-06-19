#include <stdio.h>
#include <assert.h>
#include <ulib/bitmap.h>

int main()
{
	DEFINE_BITMAP(bm, 1000);

	bitmap_zero(bm, 1000);
	bitmap_set(bm, 3, 2);
	assert(!test_bit(2, bm));
	assert(test_bit(3, bm));
	assert(test_bit(4, bm));
	assert(!test_bit(5, bm));

	int bit;
	int start = 3;
	for_each_set_bit(bit, bm, 1000)
		assert(bit == start++);

	printf("passed\n");

	return 0;
}
