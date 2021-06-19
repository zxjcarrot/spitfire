#include <stdio.h>
#include <assert.h>
#include <ulib/sort_heap_prot.h>

#define LESSTHAN(x, y) ((x) < (y))

DEFINE_HEAPSORT(test, int, LESSTHAN);

int main()
{
	int data[] = { 0, -1, 3, 100, 8 };

	heapsort_test(data, data + sizeof(data)/sizeof(data[0]));

	assert(data[0] == -1);
	assert(data[1] == 0);
	assert(data[2] == 3);
	assert(data[3] == 8);
	assert(data[4] == 100);

	printf("passed\n");

	return 0;
}
