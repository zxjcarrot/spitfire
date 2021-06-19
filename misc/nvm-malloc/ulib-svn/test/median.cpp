#include <time.h>
#include <stdio.h>
#include <assert.h>
#include <algorithm>
#include <ulib/sort_median_prot.h>

#define LESSTHAN(x, y) (*(x) < *(y))

DEFINE_MEDIAN(test, int, LESSTHAN);

int main()
{
	srand((int)time(0));

	for (int j = 0; j < 100; ++j) {
		int ne = rand() % 67321 + 1;
		int k = rand() % ne;
		int m;
		int *data = new int [ne];

		printf("number of testing numbers: %d\n", ne);
		printf("median: %d\n", k);

		for (int i = 0; i < ne; ++i)
			data[i] = rand();

		median_test(data, data + k, data + ne);
		m = data[k];

		// verification
		std::sort(data, data + ne);
		assert(m == data[k]);
		delete [] data;
	}

	printf("passed\n");

	return 0;
}
