#include <time.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <ulib/tree.h>
#include <ulib/tree_util.h>
#include <ulib/util_algo.h>
#include <ulib/util_timer.h>
#include <ulib/math_rand_prot.h>

uint64_t u, v, w;
#define myrand()  RAND_NR_NEXT(u, v, w)

struct avl_node {
	struct avl_root link;
	int key;
};

static inline int avl_node_cmp(const void *a, const void *b)
{
	return generic_compare(
		((struct avl_node *)a)->key,
		((struct avl_node *)b)->key);
}

int main(int argc, char *argv[])
{
	int num = 1000000;
	avl_root *root = NULL;

	if (argc > 1)
		num = atoi(argv[1]);

	uint64_t seed = time(NULL);
	RAND_NR_INIT(u, v, w, seed);

	ulib_timer_t timer;
	timer_start(&timer);
	for (int i = 0; i < num; ++i) {
		avl_node *t = new avl_node;
		t->key = myrand();
		if (&t->link != avl_map(&t->link, avl_node_cmp, &root))
			delete t;
	}
	printf("Inserting 1M elems elapsed: %f\n", timer_stop(&timer));

	printf("Height: %d\n", TREE_HEIGHT(root));

	timer_start(&timer);
	for (int i = 0; i < 1000000; ++i) {
		avl_node t;
		t.key = myrand();
		TREE_SEARCH(&t.link, avl_node_cmp, root);
	}
	printf("Searching 10M elems elapsed: %f\n", timer_stop(&timer));

	avl_node *pos, *tmp;
	timer_start(&timer);
	avl_for_each_entry_safe(pos, tmp, root, link) {
		avl_del(&pos->link, &root);
		delete pos;
	}
	printf("Deleting 1M elems elapsed: %f\n", timer_stop(&timer));


	printf("passed\n");

	return 0;
}
