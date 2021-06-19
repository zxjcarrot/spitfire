/* The MIT License

   Copyright (C) 2011 Zilong Tan (eric.zltan@gmail.com)

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

/* This implementation was ported from Linux kernel. */

#include <string.h>
#include "sort_list.h"

#define MAX_LIST_LENGTH_BITS 64

/*
 * Returns a list organized in an intermediate format suited
 * to chaining of merge() calls: null-terminated, no reserved or
 * sentinel head node, "prev" links not maintained.
 */
static struct list_head *merge(void *priv,
			       int (*cmp)(void *priv, const void *, const void *),
			       struct list_head *a, struct list_head *b)
{
	struct list_head head, *tail = &head;

	while (a && b) {
		/* if equal, take 'a' -- important for sort stability */
		if ((*cmp)(priv, a, b) <= 0) {
			tail->next = a;
			a = a->next;
		} else {
			tail->next = b;
			b = b->next;
		}
		tail = tail->next;
	}
	tail->next = a?:b;
	return head.next;
}

/*
 * Combine final list merge with restoration of standard doubly-linked
 * list structure.  This approach duplicates code from merge(), but
 * runs faster than the tidier alternatives of either a separate final
 * prev-link restoration pass, or maintaining the prev links
 * throughout.
 */
static void merge_and_restore_back_links(void *priv,
					 int (*cmp)(void *priv, const void *, const void *),
					 struct list_head *head,
					 struct list_head *a, struct list_head *b)
{
	struct list_head *tail = head;

	while (a && b) {
		/* if equal, take 'a' -- important for sort stability */
		if ((*cmp)(priv, a, b) <= 0) {
			tail->next = a;
			a->prev = tail;
			a = a->next;
		} else {
			tail->next = b;
			b->prev = tail;
			b = b->next;
		}
		tail = tail->next;
	}
	tail->next = a ? : b;

	do {
		tail->next->prev = tail;
		tail = tail->next;
	} while (tail->next);

	tail->next = head;
	head->prev = tail;
}

void list_sort(void *priv, struct list_head *head,
	       int (*cmp)(void *priv, const void *, const void *))
{
	struct list_head *part[MAX_LIST_LENGTH_BITS + 1]; /* sorted partial lists
							     -- last slot is a sentinel */
	int lev;  /* index into part[] */
	int max_lev = 0;
	struct list_head *list;

	if (list_empty(head))
		return;

	memset(part, 0, sizeof(part));

	head->prev->next = NULL;
	list = head->next;

	while (list) {
		struct list_head *cur = list;
		list = list->next;
		cur->next = NULL;

		for (lev = 0; part[lev]; lev++) {
			cur = merge(priv, cmp, part[lev], cur);
			part[lev] = NULL;
		}
		if (lev > max_lev)
			max_lev = lev;
		part[lev] = cur;
	}

	for (lev = 0; lev < max_lev; lev++)
		if (part[lev])
			list = merge(priv, cmp, part[lev], list);

	merge_and_restore_back_links(priv, cmp, head, part[max_lev], list);
}

void list_sort_forward(void *priv, struct list_head_forward *head,
		       int (*cmp)(void *priv, const void *, const void *))
{
	struct list_head_forward *part[MAX_LIST_LENGTH_BITS + 1]; /* sorted partial lists
							     -- last slot is a sentinel */
	int lev;  /* index into part[] */
	int max_lev = 0;
	struct list_head_forward *list;

	if (head->next == NULL)
		return;

	memset(part, 0, sizeof(part));

	list = head->next;

	while (list) {
		struct list_head_forward *cur = list;
		list = list->next;
		cur->next = NULL;

		for (lev = 0; part[lev]; lev++) {
			cur = (struct list_head_forward *)
				merge(priv, cmp, (struct list_head *)part[lev],
				      (struct list_head *)cur);
			part[lev] = NULL;
		}
		if (lev > max_lev)
			max_lev = lev;
		part[lev] = cur;
	}

	for (lev = 0; lev <= max_lev; lev++)
		if (part[lev])
			list = (struct list_head_forward *)
				merge(priv, cmp, (struct list_head *)part[lev],
				      (struct list_head *)list);

	head->next = list;
}
