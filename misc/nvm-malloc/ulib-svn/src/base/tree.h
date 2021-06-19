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

#ifndef _ULIB_TREE_H
#define _ULIB_TREE_H

#include <stddef.h>

#ifdef __cplusplus
#define new _new_
#endif

struct tree_root_np {
	struct tree_root_np *left, *right;
};

struct tree_root {
	struct tree_root *left, *right, *parent;
};

struct avl_root {
	struct avl_root *left, *right, *parent;
	int balance:3;
};

#define TREE_ROOT_NP_INIT { NULL, NULL }

#define TREE_ROOT_NP(name)				\
	struct tree_root_np name = TREE_ROOT_NP_INIT

#define INIT_TREE_ROOT_NP(ptr) do {			\
		(ptr)->left = (ptr)->right = NULL;	\
	} while (0)

#define TREE_ROOT_INIT { NULL, NULL, NULL }

#define TREE_ROOT(name)				\
	struct tree_root name = TREE_ROOT_INIT

#define INIT_TREE_ROOT(ptr) do {		\
		(ptr)->left   = NULL;		\
		(ptr)->right  = NULL;		\
		(ptr)->parent = NULL;		\
	} while (0)

#define AVL_ROOT_INIT { NULL, NULL, NULL, 0 }

#define AVL_ROOT(name)				\
	struct avl_root name = AVL_ROOT_INIT;

#define INIT_AVL_ROOT(ptr) do {			\
		INIT_TREE_ROOT(ptr);		\
		(ptr)->balance = 0;		\
	} while (0)

#define TREE_NP_ISEMPTY(ptr)  ((ptr) == NULL)
#define TREE_ISEMPTY(ptr)     TREE_NP_ISEMPTY(ptr)
#define SPLAY_ISEMPTY(ptr)    TREE_NP_ISEMPTY(ptr)
#define AVL_ISEMPTY(ptr)      TREE_NP_ISEMPTY(ptr)

#ifdef __cplusplus
extern "C" {
#endif

struct tree_root_np *
tree_search(struct tree_root_np *entry,
	    int (*compare)(const void *, const void *),
	    struct tree_root_np *root);

#define TREE_SEARCH(entry, comp, root)					\
	tree_search((struct tree_root_np*)(entry),			\
		    (int (*)(const void *, const void *))(comp),	\
		    (struct tree_root_np*)(root))

struct tree_root_np *
tree_min(struct tree_root_np *root);

#define TREE_MIN(root)				\
	tree_min((struct tree_root_np*)(root))

struct tree_root_np *
tree_max(struct tree_root_np *root);

#define TREE_MAX(root)				\
	tree_max((struct tree_root_np*)(root))

struct tree_root *
tree_successor(struct tree_root *entry);

#define TREE_SUCCESSOR(entry)				\
	tree_successor((struct tree_root*)(entry))

struct tree_root *
tree_predecessor(struct tree_root *entry);

#define TREE_PREDECESSOR(entry)				\
	tree_predecessor((struct tree_root*)(entry))

/* add a new entry ignoring any duplicates */
void tree_add(struct tree_root *new,
	      int (*compare)(const void *, const void *),
	      struct tree_root **root);

#define TREE_ADD(new, comp, root)				\
	tree_add((struct tree_root*)(new),			\
		 (int (*)(const void *, const void *))(comp),	\
		 (struct tree_root**)(root))

/* add a unique entry
   if the entry already exists, return the old one; otherwise
   the new entry will be returned. */
struct tree_root *
tree_map(struct tree_root *new,
	 int (*compare)(const void *, const void *),
	 struct tree_root **root);

#define TREE_MAP(new, comp, root)				\
	tree_map((struct tree_root*)(new),			\
		 (int (*)(const void *, const void *))(comp),	\
		 (struct tree_root**)(root))

void tree_del(struct tree_root *entry, struct tree_root **root);

#define TREE_DEL(entry, root)			\
	tree_del((struct tree_root*)(entry),	\
		 (struct tree_root**)(root))

/* in addition to the tree_map, this operation splays the tree */
struct tree_root *
splay_map(struct tree_root *new,
	  int (*compare)(const void *, const void *),
	  struct tree_root **root);

#define SPLAY_MAP(new, comp, root)				\
	splay_map((struct tree_root*)(new),			\
		  (int (*)(const void *, const void *))(comp),	\
		  (struct tree_root**)(root))

struct tree_root_np *
splay_map_np(struct tree_root_np *new,
	     int (*compare)(const void *, const void *),
	     struct tree_root_np **root);

#define SPLAY_MAP_NP(new, comp, root)					\
	splay_map_np((struct tree_root_np*)(new),			\
		     (int (*)(const void *, const void *))(comp),	\
		     (struct tree_root_np**)(root))

struct tree_root *
splay_search(struct tree_root *entry,
	     int (*compare)(const void *, const void *),
	     struct tree_root **root);

#define SPLAY_SEARCH(entry, comp, root)					\
	splay_search((struct tree_root*)(entry),			\
		     (int (*)(const void *, const void *))(comp),	\
		     (struct tree_root**)(root))

struct tree_root_np *
splay_search_np(struct tree_root_np *entry,
		int (*compare)(const void *, const void *),
		struct tree_root_np **root);

#define SPLAY_SEARCH_NP(entry, comp, root)				\
	splay_search_np((struct tree_root_np*)(entry),			\
			(int (*)(const void *, const void *))(comp),	\
			(struct tree_root_np**)(root))

void avl_add(struct avl_root *new,
	     int (*compare)(const void *, const void *),
	     struct avl_root **root);

#define AVL_ADD(new, comp, root)				\
	avl_add((struct avl_root*)(new),			\
		(int (*)(const void *, const void *))(comp),	\
		(struct avl_root**)(root))

struct avl_root *
avl_map(struct avl_root *new,
	int (*compare)(const void *, const void *),
	struct avl_root **root);

#define AVL_MAP(new, comp, root)				\
	avl_map((struct avl_root*)(new),			\
		(int (*)(const void *, const void *))(comp),	\
		(struct avl_root**)(root))

void avl_del(struct avl_root *entry, struct avl_root **root);

#define AVL_DEL(entry, root)			\
	avl_del((struct avl_root*)(entry),	\
		(struct avl_root**)(root))

#ifdef __cplusplus
}
#endif

/* retrieves the address of the host struct.
 * @ptr is the pointer to the embedded struct of which the name is
 * provided by @member, and @type indicates the host struct type. */
#define tree_entry(ptr, type, member)					\
	((type *)((char *)(ptr)-(unsigned long)(&((type *)0)->member)))

#define avl_entry(ptr, type, member) tree_entry(ptr, type, member)

#define tree_for_each(pos, root)				\
	for (pos = (typeof(pos))TREE_MIN(root); pos != NULL;	\
	     pos = (typeof(pos))TREE_SUCCESSOR(pos))

#define tree_for_each_prev(pos, root)				\
	for (pos = (typeof(pos))TREE_MAX(root); pos != NULL;	\
	     pos = (typeof(pos))TREE_PREDECESSOR(pos))

#define tree_for_each_safe(pos, n, root)				\
	for (pos = (typeof(pos))TREE_MIN(root), n = (typeof(n))TREE_SUCCESSOR(pos); \
	     pos != NULL;						\
	     pos = n, n = n? (typeof(n))TREE_SUCCESSOR(n): NULL)

#define tree_for_each_entry(pos, root, member)				\
	for (pos = tree_entry(TREE_MIN(root), typeof(*pos), member);	\
	     &pos->member != NULL;					\
	     pos = tree_entry(TREE_SUCCESSOR(&pos->member), typeof(*pos), member))

#define tree_for_each_entry_safe(pos, n, root, member)			\
	for (pos = tree_entry(TREE_MIN(root), typeof(*pos), member),	\
		     n = tree_entry(TREE_SUCCESSOR(&pos->member), typeof(*pos), member); \
	     &pos->member != NULL;					\
	     pos = n, n = tree_entry(&n->member != NULL?		\
				     TREE_SUCCESSOR(&n->member): NULL,	\
				     typeof(*n), member))

#define avl_for_each(pos, root)						\
	for (pos = (struct avl_root *)TREE_MIN(root); pos != NULL;	\
	     pos = (struct avl_root *)TREE_SUCCESSOR(pos))

#define avl_for_each_prev(pos, root)					\
	for (pos = (struct avl_root *)TREE_MAX(root); pos != NULL;	\
	     pos = (struct avl_root *)TREE_PREDECESSOR(pos))

#define avl_for_each_safe(pos, n, root)					\
	for (pos = (struct avl_root *)TREE_MIN(root),			\
		     n = (struct avl_root *)TREE_SUCCESSOR(pos);	\
	     pos != NULL;						\
	     pos = n, n = n != NULL? (struct avl_root *)TREE_SUCCESSOR(n): NULL)

#define avl_for_each_entry(pos, root, member)				\
	for (pos = tree_entry(TREE_MIN(root), typeof(*pos), member);	\
	     &pos->member != NULL;					\
	     pos = tree_entry(TREE_SUCCESSOR(&pos->member), typeof(*pos), member))

#define avl_for_each_entry_safe(pos, n, root, member)			\
	for (pos = tree_entry(TREE_MIN(root), typeof(*pos), member),	\
		     n = tree_entry(TREE_SUCCESSOR(&pos->member), typeof(*pos), member); \
	     &pos->member != NULL;					\
	     pos = n, n = tree_entry(&n->member != NULL?		\
				     TREE_SUCCESSOR(&n->member): NULL,	\
				     typeof(*n), member))

#ifdef __cplusplus
#undef new
#endif

#endif	/* _ULIB_TREE_H */
