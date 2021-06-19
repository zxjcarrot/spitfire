/* The MIT License

   Copyright (C) 2011, 2012 Zilong Tan (eric.zltan@gmail.com)

   Permission is hereby granted, free of charge, to any person
   obtaining a copy of this software and associated documentation
   files (the "Software"), to deal in the Software without
   restriction, including without limitation the rights to use, copy,
   modify, merge, publish, distribute, sublicense, and/or sell copies
   of the Software, and to permit persons to whom the Software is
   furnished to do so, subject to the following conditions:

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

/* This implementation is based on the one from Linux kernel. */

#ifndef _ULIB_BITMAP_H
#define _ULIB_BITMAP_H

#include <string.h>
#include "math_bit.h"

#ifdef __cplusplus
#define new _new_
#endif

/*
 * A bitmap can be seen as an unsigned long array thus operations that
 * apply to a single unsigned long or unsigned long array can also be
 * used here. Some useful operations can be found in math_bit.h.
 *
 * Note that nbits should be always a compile time evaluable constant.
 * Otherwise many inlines will generate horrible code.
 *
 * bitmap_zero(dst, nbits)			*dst = 0UL
 * bitmap_fill(dst, nbits)			*dst = ~0UL
 * bitmap_copy(dst, src, nbits)			*dst = *src
 * bitmap_and(dst, src1, src2, nbits)		*dst = *src1 & *src2
 * bitmap_or(dst, src1, src2, nbits)		*dst = *src1 | *src2
 * bitmap_xor(dst, src1, src2, nbits)		*dst = *src1 ^ *src2
 * bitmap_andnot(dst, src1, src2, nbits)	*dst = *src1 & ~(*src2)
 * bitmap_complement(dst, src, nbits)		*dst = ~(*src)
 * bitmap_equal(src1, src2, nbits)		Are *src1 and *src2 equal?
 * bitmap_intersects(src1, src2, nbits)		Do *src1 and *src2 overlap?
 * bitmap_subset(src1, src2, nbits)		Is *src1 a subset of *src2?
 * bitmap_empty(src, nbits)			Are all bits zero in *src?
 * bitmap_full(src, nbits)			Are all bits set in *src?
 * bitmap_weight(src, nbits)			Hamming Weight: number set bits
 * bitmap_set(dst, pos, nbits)			Set specified bit area
 * bitmap_clear(dst, pos, nbits)		Clear specified bit area
 * bitmap_find_next_zero_area(buf, len, pos, n, mask)	Find bit free area
 * bitmap_shift_right(dst, src, n, nbits)	*dst = *src >> n
 * bitmap_shift_left(dst, src, n, nbits)	*dst = *src << n
 * bitmap_remap(dst, src, old, new, nbits)	*dst = map(old, new)(src)
 * bitmap_bitremap(oldbit, old, new, nbits)	newbit = map(old, new)(oldbit)
 * bitmap_onto(dst, orig, relmap, nbits)	*dst = orig relative to relmap
 * bitmap_fold(dst, orig, sz, nbits)		dst bits = orig bits mod sz
 * bitmap_snprintf(buf, len, src, nbits)	Print bitmap src to buf
 * bitmap_parse(buf, buflen, dst, nbits)	Parse bitmap dst from kernel buf
 * bitmap_scnlistprintf(buf, len, src, nbits)	Print bitmap src as list to buf
 * bitmap_parselist(buf, dst, nbits)		Parse bitmap dst from list
 * bitmap_find_free_region(bitmap, bits, order)	Find and allocate bit region
 * bitmap_release_region(bitmap, pos, order)	Free specified bit region
 * bitmap_allocate_region(bitmap, pos, order)	Allocate specified bit region
 */

#ifdef __cplusplus
extern "C" {
#endif

int  __bitmap_empty(const unsigned long *bitmap, int bits);
int  __bitmap_full(const unsigned long *bitmap, int bits);
int  __bitmap_equal(const unsigned long *bitmap1,
		    const unsigned long *bitmap2, int bits);
void __bitmap_complement(unsigned long *dst, const unsigned long *src, int bits);
void __bitmap_shift_right(unsigned long *dst,
			  const unsigned long *src, int shift, int bits);
void __bitmap_shift_left(unsigned long *dst,
			 const unsigned long *src, int shift, int bits);
int  __bitmap_and(unsigned long *dst, const unsigned long *bitmap1,
		  const unsigned long *bitmap2, int bits);
void __bitmap_or(unsigned long *dst, const unsigned long *bitmap1,
		 const unsigned long *bitmap2, int bits);
void __bitmap_xor(unsigned long *dst, const unsigned long *bitmap1,
		  const unsigned long *bitmap2, int bits);
int  __bitmap_andnot(unsigned long *dst, const unsigned long *bitmap1,
		     const unsigned long *bitmap2, int bits);
int  __bitmap_intersects(const unsigned long *bitmap1,
			 const unsigned long *bitmap2, int bits);
int  __bitmap_subset(const unsigned long *bitmap1,
		     const unsigned long *bitmap2, int bits);
int  __bitmap_weight(const unsigned long *bitmap, int bits);

void bitmap_set(unsigned long *map, int i, int len);
void bitmap_clear(unsigned long *map, int start, int nr);
unsigned long bitmap_find_next_zero_area(unsigned long *map,
					 unsigned long size,
					 unsigned long start,
					 unsigned int nr,
					 unsigned long align_mask);

int  bitmap_snprintf(char *buf, unsigned int len,
		     const unsigned long *src, int nbits);
int  __bitmap_parse(const char *buf, unsigned int buflen,
		    unsigned long *dst, int nbits);
int  bitmap_parse_user(const char *ubuf, unsigned int ulen,
		       unsigned long *dst, int nbits);
int  bitmap_scnlistprintf(char *buf, unsigned int len,
			  const unsigned long *src, int nbits);
int  bitmap_parselist(const char *buf, unsigned long *maskp,
		      unsigned int nmaskbits);
void bitmap_remap(unsigned long *dst, const unsigned long *src,
		  const unsigned long *old, const unsigned long *new, int bits);
int  bitmap_bitremap(int oldbit,
		     const unsigned long *old, const unsigned long *new, int bits);
void bitmap_onto(unsigned long *dst, const unsigned long *orig,
		 const unsigned long *relmap, int bits);
void bitmap_fold(unsigned long *dst, const unsigned long *orig,
		 int sz, int bits);
int  bitmap_find_free_region(unsigned long *bitmap, int bits, int order);
void bitmap_release_region(unsigned long *bitmap, int pos, int order);
int  bitmap_allocate_region(unsigned long *bitmap, int pos, int order);
void bitmap_copy_le(void *dst, const unsigned long *src, int nbits);

#ifdef __cplusplus
}
#endif

#define DEFINE_BITMAP(name,bits)		\
	unsigned long name[BITS_TO_LONGS(bits)]

#define BITMAP_LAST_WORD_MASK(nbits)				\
	(							\
		((nbits) % BITS_PER_LONG) ?			\
		(1UL<<((nbits) % BITS_PER_LONG))-1 : ~0UL	\
		)

#define small_const_nbits(nbits)					\
	(__builtin_constant_p(nbits) && (nbits) <= BITS_PER_LONG)

static inline void bitmap_zero(unsigned long *dst, int nbits)
{
	if (small_const_nbits(nbits))
		*dst = 0UL;
	else {
		int len = BITS_TO_LONGS(nbits) * sizeof(unsigned long);
		memset(dst, 0, len);
	}
}

static inline void bitmap_fill(unsigned long *dst, int nbits)
{
	size_t nlongs = BITS_TO_LONGS(nbits);
	if (!small_const_nbits(nbits)) {
		int len = (nlongs - 1) * sizeof(unsigned long);
		memset(dst, 0xff,  len);
	}
	dst[nlongs - 1] = BITMAP_LAST_WORD_MASK(nbits);
}

static inline void bitmap_copy(unsigned long *dst, const unsigned long *src,
			       int nbits)
{
	if (small_const_nbits(nbits))
		*dst = *src;
	else {
		int len = BITS_TO_LONGS(nbits) * sizeof(unsigned long);
		memcpy(dst, src, len);
	}
}

static inline int bitmap_and(unsigned long *dst, const unsigned long *src1,
			     const unsigned long *src2, int nbits)
{
	if (small_const_nbits(nbits))
		return (*dst = *src1 & *src2) != 0;
	return __bitmap_and(dst, src1, src2, nbits);
}

static inline void bitmap_or(unsigned long *dst, const unsigned long *src1,
			     const unsigned long *src2, int nbits)
{
	if (small_const_nbits(nbits))
		*dst = *src1 | *src2;
	else
		__bitmap_or(dst, src1, src2, nbits);
}

static inline void bitmap_xor(unsigned long *dst, const unsigned long *src1,
			      const unsigned long *src2, int nbits)
{
	if (small_const_nbits(nbits))
		*dst = *src1 ^ *src2;
	else
		__bitmap_xor(dst, src1, src2, nbits);
}

static inline int bitmap_andnot(unsigned long *dst, const unsigned long *src1,
				const unsigned long *src2, int nbits)
{
	if (small_const_nbits(nbits))
		return (*dst = *src1 & ~(*src2)) != 0;
	return __bitmap_andnot(dst, src1, src2, nbits);
}

static inline void bitmap_complement(unsigned long *dst, const unsigned long *src,
				     int nbits)
{
	if (small_const_nbits(nbits))
		*dst = ~(*src) & BITMAP_LAST_WORD_MASK(nbits);
	else
		__bitmap_complement(dst, src, nbits);
}

static inline int bitmap_equal(const unsigned long *src1,
			       const unsigned long *src2, int nbits)
{
	if (small_const_nbits(nbits))
		return ! ((*src1 ^ *src2) & BITMAP_LAST_WORD_MASK(nbits));
	else
		return __bitmap_equal(src1, src2, nbits);
}

static inline int bitmap_intersects(const unsigned long *src1,
				    const unsigned long *src2, int nbits)
{
	if (small_const_nbits(nbits))
		return ((*src1 & *src2) & BITMAP_LAST_WORD_MASK(nbits)) != 0;
	else
		return __bitmap_intersects(src1, src2, nbits);
}

static inline int bitmap_subset(const unsigned long *src1,
				const unsigned long *src2, int nbits)
{
	if (small_const_nbits(nbits))
		return ! ((*src1 & ~(*src2)) & BITMAP_LAST_WORD_MASK(nbits));
	else
		return __bitmap_subset(src1, src2, nbits);
}

static inline int bitmap_empty(const unsigned long *src, int nbits)
{
	if (small_const_nbits(nbits))
		return ! (*src & BITMAP_LAST_WORD_MASK(nbits));
	else
		return __bitmap_empty(src, nbits);
}

static inline int bitmap_full(const unsigned long *src, int nbits)
{
	if (small_const_nbits(nbits))
		return ! (~(*src) & BITMAP_LAST_WORD_MASK(nbits));
	else
		return __bitmap_full(src, nbits);
}

static inline int bitmap_weight(const unsigned long *src, int nbits)
{
	if (small_const_nbits(nbits))
		return hweight_long(*src & BITMAP_LAST_WORD_MASK(nbits));
	return __bitmap_weight(src, nbits);
}

static inline void bitmap_shift_right(unsigned long *dst,
				      const unsigned long *src, int n, int nbits)
{
	if (small_const_nbits(nbits))
		*dst = *src >> n;
	else
		__bitmap_shift_right(dst, src, n, nbits);
}

static inline void bitmap_shift_left(unsigned long *dst,
				     const unsigned long *src, int n, int nbits)
{
	if (small_const_nbits(nbits))
		*dst = (*src << n) & BITMAP_LAST_WORD_MASK(nbits);
	else
		__bitmap_shift_left(dst, src, n, nbits);
}

static inline int bitmap_parse(const char *buf, unsigned int buflen,
			       unsigned long *maskp, int nmaskbits)
{
	return __bitmap_parse(buf, buflen, maskp, nmaskbits);
}

#ifdef __cplusplus
#undef new
#endif

#endif /* _ULIB_BITMAP_H */
