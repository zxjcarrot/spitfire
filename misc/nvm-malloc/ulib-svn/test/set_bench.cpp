#include <time.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <set>
#include <ulib/util_algo.h>
#include <ulib/util_timer.h>
#include <ulib/math_rand_prot.h>

using namespace std;

uint64_t u, v, w;
#define myrand()  RAND_NR_NEXT(u, v, w)

struct set_elem {
	int key;

	bool operator<(const set_elem &other) const;
};

static inline int set_elem_cmp(const void *a, const void *b)
{
	return generic_compare(
		((struct set_elem *)a)->key,
		((struct set_elem *)b)->key);
}

bool set_elem::operator<(const set_elem &other) const
{
	return set_elem_cmp(this, &other) < 0;
}

int main(int argc, char *argv[])
{
	int num = 1000000;
	set<set_elem> myset;

	if (argc > 1)
		num = atoi(argv[1]);

	uint64_t seed = time(NULL);
	RAND_NR_INIT(u, v, w, seed);

	ulib_timer_t timer;
	timer_start(&timer);
	for (int i = 0; i < num; ++i) {
		set_elem t;
		t.key = myrand();
		myset.insert(t);
	}
	printf("Inserting 1M elems elapsed: %f\n", timer_stop(&timer));

	timer_start(&timer);
	for (int i = 0; i < 1000000; ++i) {
		set_elem t;
		t.key = myrand();
		myset.find(t);
	}
	printf("Searching 10M elems elapsed: %f\n", timer_stop(&timer));

	printf("passed\n");

	return 0;
}
