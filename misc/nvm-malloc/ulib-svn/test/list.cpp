#include <stdio.h>
#include <assert.h>
#include <ulib/list.h>

struct list_node {
	// this member is required for list node
	struct list_head link;
	// associated data
	int data;
};

int main()
{
	// declare an empty list
	LIST_HEAD(head);
	struct list_node *node;
	int nnode = 0;

	// add some nodes to list
	for (int i = 0; i < 100; i++) {
		node = new list_node;
		node->data = i;
		list_add_tail(&node->link, &head);
	}

	// walk all nodes
	list_for_each_entry(node, &head, link) {
		printf("list node data %d\n", node->data);
		++nnode;
	}

	assert(nnode == 100);

	// free the list
	struct list_node *tmp;
	nnode = 0;
	list_for_each_entry_safe(node, tmp, &head, link) {
		delete node;
		++nnode;
	}

	assert(nnode == 100);

	printf("passed\n");

	return 0;
}
