#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <ulib/math_comb.h>

int main()
{
	combiter_t ci;

	assert(comb_begin(3, 2, &ci) == 0);

	// index of loop
	int s = 0;

	do {
		comb_t comb;
		// get current combination
		if (comb_get(&ci, &comb))
			break;

		// enumerates all elements contained in this combination
		int elem;
		int t = 0;
		while ((elem = comb_elem(&comb)) != -1) {
			switch (s) {
			case 0: // first round 011
				switch (t) {
				case 0: assert(elem == 1);
					break;
				case 1: assert(elem == 2);
					break;
				default: fprintf(stderr, "elem error\n");
					exit(EXIT_FAILURE);
				}
				break;
			case 1: // second round 101
				switch (t) {
				case 0: assert(elem == 1);
					break;
				case 1: assert(elem == 3);
					break;
				default: fprintf(stderr, "elem error\n");
					exit(EXIT_FAILURE);
				}
				break;
			case 2: // third round 110
				switch (t) {
				case 0: assert(elem == 2);
					break;
				case 1: assert(elem == 3);
					break;
				default: fprintf(stderr, "elem error\n");
					exit(EXIT_FAILURE);
				}
				break;
			default: // no more rounds
				fprintf(stderr, "combination error\n");
				exit(EXIT_FAILURE);
			}
			t++;
		}
		assert(t == 2);
		s++;
	} while (!comb_next(&ci));

	// exactly three combinations for choosing 2 from 3 elements
	assert(s == 3);

	printf("passed\n");

	return 0;
}
