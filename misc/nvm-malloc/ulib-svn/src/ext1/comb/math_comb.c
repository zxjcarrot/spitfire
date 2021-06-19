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
#include <math_bit.h>
#include "math_comb.h"

int comb_begin(int m, int n, combiter_t *iter)
{
	if (iter == NULL || n > m || n < 0 || m > 64)
		return -1;

	iter->cur = (1ULL << n) - 1;
	iter->max = iter->cur << (m - n);

	return 0;
}

int comb_next(combiter_t *iter)
{
	if (iter == NULL)
		return -1;
	if (iter->cur == 0 || iter->cur >= iter->max)
		return -1;

	iter->cur = hweight_next64(iter->cur);

	return 0;
}

int comb_get(combiter_t *iter, comb_t *comb)
{
	if (iter == NULL || comb == NULL)
		return -1;
	if (iter->cur == 0)
		return -1;
	*comb = iter->cur;
	return 0;
}

int comb_elem(comb_t *comb)
{
	int idx;

	if (comb == NULL)
		return -1;
	if (*comb == 0)
		return -1;

	idx = ffs64(*comb);
	*comb &= *comb - 1;

	return idx;
}
