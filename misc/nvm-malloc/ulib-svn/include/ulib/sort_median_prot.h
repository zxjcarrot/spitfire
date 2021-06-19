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

#ifndef _ULIB_SORT_MEDIAN_PROT_H
#define _ULIB_SORT_MEDIAN_PROT_H

#include "util_algo.h"

/**
 * median - partition an array into tree parts:
 * array[<median] <= array[median] <= array[>median]
 */
#define DEFINE_MEDIAN(name, type, lt)				\
	static inline void					\
	median_##name(type *base, type *median, type *last)	\
	{							\
		register type *s, *t, *p, *q, *m;		\
		type e;						\
								\
		s = base;					\
		t = last - 1;					\
		while (s < t) {					\
			p = s;					\
			q = t;					\
			m = p + (q - p) / 2;			\
			if (lt(m, p))				\
				_swap(*m, *p);			\
			if (lt(q, m)) {				\
				_swap(*q, *m);			\
				if (lt(m, p))			\
					_swap(*m, *p);		\
			}					\
			e = *m;					\
			for (;;) {				\
				do ++p; while (lt(p, &e));	\
				do --q; while (lt(&e, q));	\
				if (p >= q)			\
					break;			\
				_swap(*p, *q);			\
			}					\
			if (p > median)				\
				t = p - 1;			\
			else					\
				s = p;				\
		}						\
	}

#endif	/* _ULIB_SORT_MEDIAN_PROT_H */
