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

#ifndef _ULIB_MATH_COMB_H
#define _ULIB_MATH_COMB_H

#include <stdint.h>

typedef uint64_t comb_t;

typedef struct {
	comb_t max;
	comb_t cur;
} combiter_t;

#ifdef __cplusplus
extern "C" {
#endif

/**
 * comb_begin - begin the combination iteration by initializing @iter
 * @m:	  total number of elements
 * @n:	  number of elements to choose
 * @iter: generated combination iterator
 * Note:  the max @m supported is 64
 */
int comb_begin(int m, int n, combiter_t *iter);

/**
 * comb_next - generate next iterator
 * @iter: combination iterator
 */
int comb_next(combiter_t *iter);

/**
 * comb_get - get combination from iterator
 * @iter: iterator for the combination
 * @comb: output combination
 */
int comb_get(combiter_t *iter, comb_t *comb);

/**
 * comb_elem - get and remove an element from the combination
 * @comb: input & output combination
 * Note: the elements are numbered starting from 1
 */
int comb_elem(comb_t *comb);

#ifdef __cplusplus
}
#endif

#endif
