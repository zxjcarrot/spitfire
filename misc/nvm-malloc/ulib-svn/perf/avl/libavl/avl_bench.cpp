#include <time.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <ulib/util_algo.h>
#include <ulib/util_timer.h>
#include <ulib/math_rand_prot.h>
#include "avl.h"

uint64_t u, v, w;
#define myrand()  RAND_NR_NEXT(u, v, w)

struct node {
	uint64_t key;
};

static inline int node_cmp(const void *a, const void *b, void *)
{
	return generic_compare(
		((struct node *)a)->key,
		((struct node *)b)->key);
}

static inline void node_destroy(void *t, void *)
{
	delete (node *)t;
}

int main(int argc, char *argv[])
{
	int num = 1000000;
	avl_table *root;

	if (argc > 1)
		num = atoi(argv[1]);

	uint64_t seed = time(NULL);
	RAND_NR_INIT(u, v, w, seed);

	root = avl_create(node_cmp, NULL, &avl_allocator_default);

	ulib_timer_t timer;
	timer_start(&timer);
	for (int i = 0; i < num; ++i) {
		node *t = new node;
		t->key = myrand();
		avl_insert(root, t);
	}
	printf("Inserting 1M elems elapsed: %f\n", timer_stop(&timer));

	timer_start(&timer);
	for (int i = 0; i < 1000000; ++i) {
		node t;
		t.key = myrand();
		avl_find(root, &t);
	}
	printf("Searching 10M elems elapsed: %f\n", timer_stop(&timer));

	timer_start(&timer);
	avl_destroy(root, node_destroy);
	printf("Deleting 1M elems elapsed: %f\n", timer_stop(&timer));

	return 0;
}
