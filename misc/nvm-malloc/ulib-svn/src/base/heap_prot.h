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

#ifndef _ULIB_HEAP_PROT_H
#define _ULIB_HEAP_PROT_H

#define HEAP_LEFT(i)   ((i) * 2 + 1)
#define HEAP_RIGHT(i)  ((i) * 2 + 2)
#define HEAP_PARENT(i) (((i) - 1) / 2)

#define DEFINE_HEAP(name, type, lt)					\
	static inline void						\
	heap_push_##name(type *base, size_t hole, size_t top, type val)	\
	{								\
		register size_t parent = HEAP_PARENT(hole);		\
									\
		while (hole > top && lt(base[parent], val)) {		\
			base[hole] = base[parent];			\
			hole = parent;					\
			parent = HEAP_PARENT(parent);			\
		}							\
		base[hole] = val;					\
	}								\
									\
	static inline void						\
	heap_adjust_##name(type *base, size_t size, size_t hole, type val) \
	{								\
		register size_t large = HEAP_RIGHT(hole);		\
		register size_t top = hole;				\
									\
		while (large < size) {					\
			if (lt(base[large], base[large - 1]))		\
				large--;				\
			base[hole] = base[large];			\
			hole  = large;					\
			large = HEAP_RIGHT(large);			\
		}							\
		if (large == size) {					\
			base[hole] = base[large - 1];			\
			hole = large - 1;				\
		}							\
		heap_push_##name(base, hole, top, val);			\
	}								\
									\
	static inline void						\
	heap_init_##name(type *base, type *last)			\
	{								\
		register size_t size   = last - base;			\
		register size_t parent = size / 2 - 1;			\
									\
		if (size < 2)						\
			return;						\
		for (;;) {						\
			heap_adjust_##name(base, size, parent, base[parent]); \
				if (parent == 0)			\
					break;				\
				parent--;				\
		}							\
	}								\
									\
	static inline void						\
	heap_pop_##name(type *base, type *rear, type *res, type val)	\
	{								\
		*res = base[0];						\
		heap_adjust_##name(base, rear - base, 0, val);		\
	}								\
									\
	static inline void						\
	heap_pop_to_rear_##name(type *base, type *last)			\
	{								\
		heap_pop_##name(base, last - 1, last - 1, *(last - 1));	\
	}								\
									\
	static inline void						\
	heap_sort_##name(type *base, type *last)			\
	{								\
		while (last - base > 1)					\
			heap_pop_to_rear_##name(base, last--);		\
	}								\

#endif	/* _ULIB_HEAP_PROT_H */
