#include <time.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <ulib/tree.h>
#include <ulib/util_algo.h>
#include <ulib/util_timer.h>
#include <ulib/math_rand_prot.h>

uint64_t u, v, w;
#define myrand()  RAND_NR_NEXT(u, v, w)

struct tree_node {
	struct tree_root_np link;
	int key;
};

static inline int tree_node_cmp(const void *a, const void *b)
{
	return generic_compare(
		((struct tree_node *)a)->key,
		((struct tree_node *)b)->key);
}

int main(int argc, char *argv[])
{
	int num = 1000000;
	tree_root_np *root = NULL;

	if (argc > 1)
		num = atoi(argv[1]);

	uint64_t seed = time(NULL);
	RAND_NR_INIT(u, v, w, seed);

	ulib_timer_t timer;
	timer_start(&timer);
	for (int i = 0; i < num; ++i) {
		tree_node *t = new tree_node;
		t->key = myrand();
		if (&t->link != splay_map_np(&t->link, tree_node_cmp, &root))
			delete t;
	}
	printf("Inserting 1M elems elapsed: %f\n", timer_stop(&timer));

	timer_start(&timer);
	for (int i = 0; i < 1000000; ++i) {
		tree_node t;
		t.key = myrand();
		splay_search_np(&t.link, tree_node_cmp, &root);
	}
	printf("Searching 10M elems elapsed: %f\n", timer_stop(&timer));

	printf("passed\n");

	return 0;
}
