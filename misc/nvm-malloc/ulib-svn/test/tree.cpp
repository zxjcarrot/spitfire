//
// This file demonstrates the use of trees declared in tree.h. The
// following example takes binary search tree (struct tree_root) as an
// exmaple. The use of splay tree and AVL tree are similar.
//

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <assert.h>
#include <ulib/util_algo.h> // generic_compare()
#include <ulib/tree.h>

struct tree_node {
	// this member is required for tree node structure
	struct tree_root link;
	// associated data for the node
	int data;
};

// function comparing two tree nodes
int comp_tree_node(const void *x, const void *y)
{
	// retrive the containers of node x and y
	tree_node *node_x = tree_entry(x, struct tree_node, link);
	tree_node *node_y = tree_entry(y, struct tree_node, link);

	// for tree_node struct, since the tree_root member is the
	// first member of the structure, we may alternatively convert
	// the pointer x and y directly to node_x and node_y through
	// type cast. This method is faster.

	return generic_compare(node_x->data, node_y->data);
}

int main()
{
	// define an empty tree
	struct tree_root *root = 0;
	struct tree_node *node;

	srand((int)time(NULL));

	// insert several nodes
	for (int i = 0; i < 100;) {
		node = new tree_node;
		node->data = rand();
		if (&node->link != tree_map(&node->link, comp_tree_node, &root)) {
			// a node with the same data already exists, no need to add
			delete node;
		} else {
			// new node is successfully added
			++i;
		}
	}

	// walk all the nodes
	int nnode = 0;
	tree_for_each_entry(node, root, link) {
		printf("data for current node is %d\n", node->data);
		++nnode;
	}

	assert(nnode == 100);

	// free the tree, use the 'safe' version of iteration
	nnode = 0;
	struct tree_node *tmp;
	tree_for_each_entry_safe(node, tmp, root, link) {
		// NOTE: first delete this node before freeing it
		tree_del(&node->link, &root);
		delete node;
		++nnode;
	}

	assert(nnode == 100);

	printf("passed\n");

	return 0;
}
