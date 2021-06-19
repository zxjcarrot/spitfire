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

#include <stdio.h>
#include <stdint.h>
#include "tree_util.h"

int tree_height(struct tree_root_np *root)
{
	int hl, hr;

	if (root == NULL)
		return 0;

	hl = tree_height(root->left);
	hr = tree_height(root->right);

	if (hl > hr)
		return hl + 1;

	return hr + 1;
}

int tree_verify(struct tree_root *root,
		int (*compare) (const void *, const void *))
{
	struct tree_root *p;
	struct tree_root *last = NULL;
	size_t count = 0;

	if (root == NULL)
		return 0;

	tree_for_each(p, root) {
		if (last == NULL)
			last = p;
		else if (compare(p, last) <= 0)
			return -1;
		++count;
	}

	if (count != TREE_COUNT(root))
		return -1;

	return 0;
}

size_t tree_count(struct tree_root_np * root)
{
	if (root == NULL)
		return 0;
	return tree_count(root->left) + tree_count(root->right) + 1;
}

static inline void
__tree_print(struct tree_root *root,
	     void (*callback)(struct tree_root *),
	     uint64_t bitpath)
{
	int n;
	int h = 0;

	if (root->left)
		__tree_print(root->left, callback, bitpath << 1);
	if (root->right)
		__tree_print(root->right, callback, bitpath << 1 | 1);

	for (n = 32; n > 0; n >>= 1) {
		if ((bitpath & (uint64_t)(-((uint64_t)1 << (64 - n)))) == 0) {
			bitpath <<= n;
			h += n;
		}
	}
	bitpath <<= 1;	/* skip the tree marker bit */
	h = 63 - h;
	while (h-- > 0) {
		if (bitpath & 0x8000000000000000ULL)
			putchar('R');
		else
			putchar('L');
		bitpath <<= 1;
	}
	putchar('\t');
	if (callback)
		callback(root);
}

void tree_print(struct tree_root *root, void (*callback)(struct tree_root *))
{
	if (root)
		__tree_print(root, callback, 1);
}
