/* The MIT License

   Copyright (C) 2011, 2012 Zilong Tan (eric.zltan@gmail.com)

   Permission is hereby granted, free of charge, to any person obtaining
   a copy of this software and associated documentation files (the
   "Software"), to deal in the Software without restriction, including
   without limitation the rights to use, copy, modify, merge, publish,
   distribute, sublicense, and/or sell copies of the Software, and to
   permit persons to whom the Software is furnished to do so, subject to
   the following conditions:

   The above copyright notice and this permission notice shall be
   included in all copies or substantial portions of the Software.

   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
   NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
   BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
   ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
   CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE.
*/

#include "tree.h"

struct tree_root_np *
tree_search(struct tree_root_np *entry,
	    int (*compare)(const void *, const void *),
	    struct tree_root_np *root)
{
	int sgn;
	while (root) {
		sgn = compare(entry, root);
		if (sgn == 0)
			return root;
		if (sgn < 0)
			root = root->left;
		else
			root = root->right;
	}
	return root;
}

struct tree_root_np *
tree_min(struct tree_root_np *root)
{
	if (root) {
		while (root->left)
			root = root->left;
	}
	return root;
}

struct tree_root_np *
tree_max(struct tree_root_np *root)
{
	if (root) {
		while (root->right)
			root = root->right;
	}
	return root;
}

struct tree_root *
tree_successor(struct tree_root *entry)
{
	struct tree_root *succ = NULL;

	if (entry) {
		if (entry->right)
			return (struct tree_root *)TREE_MIN(entry->right);
		succ = entry->parent;
		while (succ && entry == succ->right) {
			entry = succ;
			succ = succ->parent;
		}
	}
	return succ;
}

struct tree_root *
tree_predecessor(struct tree_root *entry)
{
	struct tree_root *pred = NULL;

	if (entry) {
		if (entry->left)
			return (struct tree_root *)TREE_MAX(entry->left);
		pred = entry->parent;
		while (pred && entry == pred->left) {
			entry = pred;
			pred = pred->parent;
		}
	}
	return pred;
}

static inline void
__rotate_left(struct tree_root *entry, struct tree_root **root)
{
	struct tree_root *child;

	child = entry->right;
	entry->right = child->left;
	if (child->left)
		child->left->parent = entry;
	child->parent = entry->parent;
	if (entry->parent == NULL)
		*root = child;
	else if (entry == entry->parent->left)
		entry->parent->left = child;
	else
		entry->parent->right = child;
	child->left = entry;
	entry->parent = child;
}

static inline void
__rotate_right(struct tree_root *entry,
	       struct tree_root **root)
{
	struct tree_root *child;

	child = entry->left;
	entry->left = child->right;
	if (child->right)
		child->right->parent = entry;

	child->parent = entry->parent;

	if (entry->parent == NULL)
		*root = child;
	else if (entry == entry->parent->left)
		entry->parent->left = child;
	else
		entry->parent->right = child;
	child->right = entry;
	entry->parent = child;
}

void tree_add(struct tree_root *new,
	      int (*compare)(const void *, const void *),
	      struct tree_root **root)
{
	int sgn = 0;
	struct tree_root *next = *root;
	struct tree_root *cur  = NULL;

	INIT_TREE_ROOT(new);

	while (next) {
		cur = next;
		sgn = compare(new, next);
		if (sgn < 0)
			next = next->left;
		else
			next = next->right;
	}

	new->parent = cur;
	if (cur == NULL)
		*root = new;
	else if (sgn < 0)
		cur->left = new;
	else
		cur->right = new;
}

struct tree_root *
tree_map(struct tree_root *new,
	 int (*compare)(const void *, const void *),
	 struct tree_root **root)
{
	int sgn = 0;
	struct tree_root *next = *root;
	struct tree_root *cur  = NULL;

	INIT_TREE_ROOT(new);

	while (next) {
		cur = next;
		sgn = compare(new, next);
		if (sgn == 0)
			return next;
		else if (sgn < 0)
			next = next->left;
		else
			next = next->right;
	}

	new->parent = cur;
	if (cur == NULL)
		*root = new;
	else if (sgn < 0)
		cur->left = new;
	else
		cur->right = new;
	return new;
}

void
tree_del(struct tree_root *entry, struct tree_root **root)
{
	struct tree_root *child;
	struct tree_root *succ;

	if (entry->left == NULL || entry->right == NULL)
		succ = entry;
	else
		succ = TREE_SUCCESSOR(entry);
	if (succ->left)
		child = succ->left;
	else
		child = succ->right;

	if (child)
		child->parent = succ->parent;
	if (succ->parent == NULL)
		*root = child;
	else if (succ == succ->parent->left)
		succ->parent->left = child;
	else
		succ->parent->right = child;

	if (succ != entry) {
		succ->left = entry->left;
		if (entry->left)
			entry->left->parent = succ;
		succ->right = entry->right;
		if (entry->right)
			entry->right->parent = succ;
		succ->parent = entry->parent;
		if (entry->parent == NULL)
			*root = succ;
		else if (entry == entry->parent->left)
			entry->parent->left = succ;
		else
			entry->parent->right = succ;
	}
}

#define SPLAY_ROTATE_RIGHT(entry, tmp) do {				\
		(entry)->left = (tmp)->right;				\
		if ((tmp)->right) (tmp)->right->parent = (entry);	\
		(tmp)->right = (entry);					\
		(tmp)->parent = (entry)->parent;			\
		(entry)->parent = (tmp);				\
		(entry) = (tmp);					\
	} while (0)

#define SPLAY_ROTATE_LEFT(entry, tmp) do {			\
		(entry)->right = (tmp)->left;			\
		if ((tmp)->left) (tmp)->left->parent = (entry);	\
		(tmp)->left = (entry);				\
		(tmp)->parent = (entry)->parent;		\
		(entry)->parent = (tmp);			\
		(entry) = (tmp);				\
	} while (0)

#define SPLAY_LINK_RIGHT(entry, large) do {	\
		(large)->left = (entry);	\
		(entry)->parent = (large);	\
		(large) = (entry);		\
		(entry) = (entry)->left;	\
	} while (0)

#define SPLAY_LINK_LEFT(entry, small) do {	\
		(small)->right = (entry);	\
		(entry)->parent = (small);	\
		(small) = (entry);		\
		(entry) = (entry)->right;	\
	} while (0)

#define SPLAY_ASSEMBLE(head, node, small, large) do {		\
		(small)->right = (head)->left;			\
		if ((head)->left)				\
			(head)->left->parent = (small);		\
		(large)->left = (head)->right;			\
		if ((head)->right)				\
			(head)->right->parent = (large);	\
		(head)->left = (node)->right;			\
		if ((node)->right)				\
			(node)->right->parent = (head);		\
		(head)->right = (node)->left;			\
		if ((node)->left)				\
			(node)->left->parent = (head);		\
	} while (0)

#define SPLAY_ROTATE_RIGHT_NP(entry, tmp) do {	\
		(entry)->left = (tmp)->right;	\
		(tmp)->right = (entry);		\
		(entry) = (tmp);		\
	} while (0)

#define SPLAY_ROTATE_LEFT_NP(entry, tmp) do {	\
		(entry)->right = (tmp)->left;	\
		(tmp)->left = (entry);		\
		(entry) = (tmp);		\
	} while (0)

#define SPLAY_LINK_RIGHT_NP(entry, large) do {	\
		(large)->left = (entry);	\
		(large) = (entry);		\
		(entry) = (entry)->left;	\
	} while (0)

#define SPLAY_LINK_LEFT_NP(entry, small) do {	\
		(small)->right = (entry);	\
		(small) = (entry);		\
		(entry) = (entry)->right;	\
	} while (0)

#define SPLAY_ASSEMBLE_NP(head, node, small, large) do {	\
		(small)->right = (head)->left;			\
		(large)->left = (head)->right;			\
		(head)->left = (node)->right;			\
		(head)->right = (node)->left;			\
	} while (0)

struct tree_root *
splay_search(struct tree_root *entry,
	     int (*compare)(const void *, const void *),
	     struct tree_root **root)
{
	int cmp;
	TREE_ROOT(node);  /* node for assembly use */
	struct tree_root *small, *large, *head, *tmp;

	head = *root;
	small = large = &node;

	while ((cmp = compare(entry, head)) != 0) {
		if (cmp < 0) {
			tmp = head->left;
			if (tmp == NULL)
				break;
			if (compare(entry, tmp) < 0) {
				SPLAY_ROTATE_RIGHT(head, tmp);
				if (head->left == NULL)
					break;
			}
			SPLAY_LINK_RIGHT(head, large);
		} else {
			tmp = head->right;
			if (tmp == NULL)
				break;
			if (compare(entry, tmp) > 0) {
				SPLAY_ROTATE_LEFT(head, tmp);
				if (head->right == NULL)
					break;
			}
			SPLAY_LINK_LEFT(head, small);
		}
	}
	head->parent = NULL;
	SPLAY_ASSEMBLE(head, &node, small, large);
	*root = head;

	if (cmp != 0)
		return NULL;

	return head;
}

struct tree_root *
splay_map(struct tree_root *new,
	  int (*compare)(const void *, const void *),
	  struct tree_root **root)
{
	int cmp;
	TREE_ROOT(node);  /* node for assembly use */
	struct tree_root *small, *large, *head, *tmp;

	INIT_TREE_ROOT(new);
	small = large = &node;
	head = *root;

	while (head && (cmp = compare(new, head)) != 0) {
		if (cmp < 0) {
			tmp = head->left;
			if (tmp == NULL) {
				/* zig */
				SPLAY_LINK_RIGHT(head, large);
				break;
			}
			cmp = compare(new, tmp);
			if (cmp < 0) {
				/* zig-zig */
				SPLAY_ROTATE_RIGHT(head, tmp);
				SPLAY_LINK_RIGHT(head, large);
			} else if (cmp > 0) {
				/* zig-zag */
				SPLAY_LINK_RIGHT(head, large);
				SPLAY_LINK_LEFT(head, small);
			} else {
				/* zig */
				SPLAY_LINK_RIGHT(head, large);
				break;
			}
		} else {
			tmp = head->right;
			if (tmp == NULL) {
				/* zag */
				SPLAY_LINK_LEFT(head, small);
				break;
			}
			cmp = compare(new, tmp);
			if (cmp > 0) {
				/* zag-zag */
				SPLAY_ROTATE_LEFT(head, tmp);
				SPLAY_LINK_LEFT(head, small);
			} else if (cmp < 0) {
				/* zag-zig */
				SPLAY_LINK_LEFT(head, small);
				SPLAY_LINK_RIGHT(head, large);
			} else {
				/* zag */
				SPLAY_LINK_LEFT(head, small);
				break;
			}
		}
	}

	if (head == NULL)
		head = new;

	head->parent = NULL;

	SPLAY_ASSEMBLE(head, &node, small, large);

	*root = head;

	return head;
}

struct tree_root_np *
splay_search_np(struct tree_root_np *entry,
		int (*compare)(const void *, const void *),
		struct tree_root_np **root)
{
	int cmp;
	TREE_ROOT_NP(node);  /* node for assembly use */
	struct tree_root_np *small, *large, *head, *tmp;

	head = *root;
	small = large = &node;

	while ((cmp = compare(entry, head)) != 0) {
		if (cmp < 0) {
			tmp = head->left;
			if (tmp == NULL)
				break;
			if (compare(entry, tmp) < 0) {
				SPLAY_ROTATE_RIGHT_NP(head, tmp);
				if (head->left == NULL)
					break;
			}
			SPLAY_LINK_RIGHT_NP(head, large);
		} else {
			tmp = head->right;
			if (tmp == NULL)
				break;
			if (compare(entry, tmp) > 0) {
				SPLAY_ROTATE_LEFT_NP(head, tmp);
				if (head->right == NULL)
					break;
			}
			SPLAY_LINK_LEFT_NP(head, small);
		}
	}

	SPLAY_ASSEMBLE_NP(head, &node, small, large);
	*root = head;

	if (cmp != 0)
		return NULL;

	return head;
}

struct tree_root_np *
splay_map_np(struct tree_root_np *new,
	     int (*compare)(const void *, const void *),
	     struct tree_root_np **root)
{
	int cmp;
	TREE_ROOT_NP(node);  /* node for assembly use */
	struct tree_root_np *small, *large, *head, *tmp;

	INIT_TREE_ROOT_NP(new);
	small = large = &node;
	head = *root;

	while (head && (cmp = compare(new, head)) != 0) {
		if (cmp < 0) {
			tmp = head->left;
			if (tmp == NULL) {
				/* zig */
				SPLAY_LINK_RIGHT_NP(head, large);
				break;
			}
			cmp = compare(new, tmp);
			if (cmp < 0) {
				/* zig-zig */
				SPLAY_ROTATE_RIGHT_NP(head, tmp);
				SPLAY_LINK_RIGHT_NP(head, large);
			} else if (cmp > 0) {
				/* zig-zag */
				SPLAY_LINK_RIGHT_NP(head, large);
				SPLAY_LINK_LEFT_NP(head, small);
			} else {
				/* zig */
				SPLAY_LINK_RIGHT_NP(head, large);
				break;
			}
		} else {
			tmp = head->right;
			if (tmp == NULL) {
				/* zag */
				SPLAY_LINK_LEFT_NP(head, small);
				break;
			}
			cmp = compare(new, tmp);
			if (cmp > 0) {
				/* zag-zag */
				SPLAY_ROTATE_LEFT_NP(head, tmp);
				SPLAY_LINK_LEFT_NP(head, small);
			} else if (cmp < 0) {
				/* zag-zig */
				SPLAY_LINK_LEFT_NP(head, small);
				SPLAY_LINK_RIGHT_NP(head, large);
			} else {
				/* zag */
				SPLAY_LINK_LEFT_NP(head, small);
				break;
			}
		}
	}

	if (head == NULL)
		head = new;

	SPLAY_ASSEMBLE_NP(head, &node, small, large);

	*root = head;

	return head;
}

static inline void
__avl_balance(struct avl_root *new, struct avl_root **root)
{
	int balance = 0;
	struct avl_root *child, *grandson;

	while (new->parent && balance == 0) {
		balance = new->parent->balance;
		if (new == new->parent->left)
			new->parent->balance--;
		else
			new->parent->balance++;
		new = new->parent;
	}

	if (new->balance == -2) {
		child = new->left;
		if (child->balance == -1) {
			__rotate_right((struct tree_root *)new,
				       (struct tree_root **)root);
			child->balance = 0;
			new->balance = 0;
		} else {
			grandson = child->right;
			__rotate_left((struct tree_root *)child,
				      (struct tree_root **)root);
			__rotate_right((struct tree_root *)new,
				       (struct tree_root **)root);
			if (grandson->balance == -1) {
				child->balance = 0;
				new->balance = 1;
			} else if (grandson->balance == 0) {
				child->balance = 0;
				new->balance = 0;
			} else {
				child->balance = -1;
				new->balance = 0;
			}
			grandson->balance = 0;
		}
	} else if (new->balance == 2) {
		child = new->right;
		if (child->balance == 1) {
			__rotate_left((struct tree_root *)new,
				      (struct tree_root **)root);
			child->balance = 0;
			new->balance = 0;
		} else {
			grandson = child->left;
			__rotate_right((struct tree_root *)child,
				       (struct tree_root **)root);
			__rotate_left((struct tree_root *)new,
				      (struct tree_root **)root);
			if (grandson->balance == -1) {
				child->balance = 1;
				new->balance = 0;
			} else if (grandson->balance == 0) {
				child->balance = 0;
				new->balance = 0;
			} else {
				child->balance = 0;
				new->balance = -1;
			}
			grandson->balance = 0;
		}
	}
}

void avl_add(struct avl_root *new,
	     int (*compare)(const void *, const void *),
	     struct avl_root **root)
{
	new->balance = 0;
	TREE_ADD(new, compare, root);
	__avl_balance(new, root);
}

struct avl_root *
avl_map(struct avl_root *new,
	int (*compare)(const void *, const void *),
	struct avl_root **root)
{
	struct avl_root *node;

	new->balance = 0;
	node = (struct avl_root *)TREE_MAP(new, compare, root);
	if (node != new)
		return node;
	__avl_balance(new, root);

	return new;
}

void avl_del(struct avl_root *entry, struct avl_root **root)
{
	int dir = 0;
	int dir_next = 0;
	struct avl_root *unbalanced, *child, *succ, *parent;

	if (entry->right == NULL) {
		/* Case 1: entry has no right child */
		if (entry->left)
			entry->left->parent = entry->parent;
		if (entry->parent == NULL) {
			*root = entry->left;
			return;
		} else if (entry == entry->parent->left) {
			entry->parent->left = entry->left;
			dir = 0;
		} else {
			entry->parent->right = entry->left;
			dir = 1;
		}
		unbalanced = entry->parent;
	} else if (entry->right->left == NULL) {
		/* Case 2: entry's right child has no left child */
		entry->right->left = entry->left;
		if (entry->left)
			entry->left->parent = entry->right;
		entry->right->parent = entry->parent;
		if (entry->parent == NULL)
			*root = entry->right;
		else if (entry == entry->parent->left)
			entry->parent->left = entry->right;
		else
			entry->parent->right = entry->right;
		entry->right->balance = entry->balance;
		unbalanced = entry->right;
		dir = 1;
	} else {
		/* Case 3: entry's right child has a left child */
		succ = (struct avl_root *)TREE_SUCCESSOR(entry);
		if (succ->right)
			succ->right->parent = succ->parent;
		succ->parent->left = succ->right;
		unbalanced = succ->parent;
		succ->left = entry->left;
		entry->left->parent = succ;
		succ->right = entry->right;
		entry->right->parent = succ;
		succ->parent = entry->parent;
		if (entry->parent == NULL)
			*root = succ;
		else if (entry == entry->parent->left)
			entry->parent->left = succ;
		else
			entry->parent->right = succ;
		succ->balance = entry->balance;
		dir = 0;
	}

	for (;;) {
		parent = unbalanced->parent;
		if (parent)
			dir_next = (unbalanced == parent->right);
		if (dir == 0) {
			++unbalanced->balance;
			if (unbalanced->balance == 1)
				break;
			if (unbalanced->balance == 2) {
				child = unbalanced->right;
				if (child->balance == -1) {
					succ = child->left;
					__rotate_right((struct tree_root *)child,
						       (struct tree_root **)root);
					__rotate_left((struct tree_root *)unbalanced,
						      (struct tree_root **)root);
					if (succ->balance == -1) {
						child->balance = 1;
						unbalanced->balance = 0;
					} else if (succ->balance == 0) {
						child->balance = 0;
						unbalanced->balance = 0;
					} else {
						child->balance = 0;
						unbalanced->balance = -1;
					}
					succ->balance = 0;
				} else {
					__rotate_left((struct tree_root *)unbalanced,
						      (struct tree_root **)root);
					if (child->balance == 0) {
						child->balance = -1;
						unbalanced->balance = 1;
						break;
					} else {
						child->balance = 0;
						unbalanced->balance = 0;
					}
				}
			}
		} else {
			--unbalanced->balance;
			if (unbalanced->balance == -1)
				break;
			if (unbalanced->balance == -2) {
				child = unbalanced->left;
				if (child->balance == 1) {
					succ = child->right;
					__rotate_left((struct tree_root *)child,
						      (struct tree_root **)root);
					__rotate_right((struct tree_root *)unbalanced,
						       (struct tree_root **)root);
					if (succ->balance == -1) {
						child->balance = 0;
						unbalanced->balance = 1;
					} else if (succ->balance == 0) {
						child->balance = 0;
						unbalanced->balance = 0;
					} else {
						child->balance = -1;
						unbalanced->balance = 0;
					}
					succ->balance = 0;
				} else {
					__rotate_right((struct tree_root *)unbalanced,
						       (struct tree_root **)root);
					if (child->balance == 0) {
						child->balance = 1;
						unbalanced->balance = -1;
						break;
					} else {
						child->balance = 0;
						unbalanced->balance = 0;
					}
				}
			}
		}
		if (parent == NULL)
			break;
		dir = dir_next;
		unbalanced = parent;
	}
}
