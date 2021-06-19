#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <time.h>
#include <ulib/util_algo.h>
#include <ulib/sort_list.h>

struct list_node {
	struct list_head link;
	int data;
};

struct list_node_forward {
	struct list_node_forward *next;
	int data;
};

int comp_list_node(void *, const void *x, const void *y)
{
	return generic_compare(((const list_node *)x)->data, ((const list_node *)y)->data);
}

int comp_list_node_forward(void *, const void *x, const void *y)
{
	return generic_compare(((const list_node_forward *)x)->data, ((const list_node_forward *)y)->data);
}

int main()
{
	LIST_HEAD(head);
	struct list_node_forward fhead, *fn = &fhead;
	struct list_node *node;
	srand((int)time(NULL));


	// insert some nodes
	for (int i = 0; i < 10000; i++) {
		node = new list_node;
		node->data = rand();
		list_add_tail(&node->link, &head);
		fn->next = new list_node_forward;
		fn->next->data = rand();
		fn = fn->next;
	}

	fn->next = NULL;

	list_sort(NULL, &head, comp_list_node);
	list_sort_forward(NULL, (list_head_forward *)&fhead, comp_list_node_forward);

	// verify and sorted list
	struct list_node *tmp;
	list_for_each_entry_safe(node, tmp, &head, link) {
		if (&tmp->link != &head)
			assert(comp_list_node(NULL, node, tmp) <= 0);
		delete node;
	}

	for (fn = fhead.next; fn;) {
		list_node_forward *next = fn->next? fn->next: fn;
		assert(comp_list_node_forward(NULL, fn, next) <= 0);
		delete fn;
		if (fn == next)
			break;
		fn = next;
	}

	printf("passed\n");

	return 0;
}
