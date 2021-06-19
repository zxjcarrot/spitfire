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

/* Define BIT_HAS_FAST_MULT if multiplication is preferable. */

#ifndef _ULIB_MATH_BIT_H
#define _ULIB_MATH_BIT_H

#include <stdint.h>

#define BITS_PER_BYTE	    8
#define BITS_PER_LONG	    __WORDSIZE

#define DIV_ROUND_UP(n,d)   (((n) + (d) - 1) / (d))
#define BITS_TO_LONGS(nr)   DIV_ROUND_UP(nr, BITS_PER_BYTE * sizeof(long))
#define BIT_WORD(nr)	    ((nr) / BITS_PER_LONG)
#define BIT_MASK(nr)	    (1UL << ((nr) % BITS_PER_LONG))
#define ALIGN_MASK(x, mask) (((x) + (mask)) & ~(mask))
#define ALIGN(x, a)	    ALIGN_MASK(x, (typeof(x))(a) - 1)
#define ROR64(x, r)	    ((x) >> (r) | (x) << (64 - (r)))
#define MULQ(x, y, lo, hi)						\
	asm("mulq %3" : "=a" (lo), "=d" (hi) : "%0" (x), "rm" (y))

#define SIGN(x)		    (((v) > 0) - ((v) < 0))
#define OPPOSITE_SIGN(x,y)  ((x) ^ (y) < 0)
#define ABS(x) ({							\
			typeof(x) _t = (x) >> sizeof(x) * BITS_PER_BYTE - 1; \
			((x) ^ _t) - _t; })
#define XOR_MIN(x,y)	    ((y) ^ (((x) ^ (y)) & -((x) < (y))))
#define XOR_MAX(x,y)	    ((x) ^ (((x) ^ (y)) & -((x) < (y))))

#define BIN_TO_GRAYCODE(b)  ((b) ^ ((b) >> 1))
#define GRAYCODE_TO_BIN32(g)  ({		\
			(g) ^= (g) >> 1;	\
			(g) ^= (g) >> 2;	\
			(g) ^= (g) >> 4;	\
			(g) ^= (g) >> 8;	\
			(g) ^= (g) >> 16;	\
		})
#define GRAYCODE_TO_BIN64(g)  ({		\
			GRAYCODE_TO_BIN32(g);	\
			(g) ^= (g) >> 32;	\
		})

/* flip the bits in w according to the flag f and the mask m. */
#define BIT_FLIP(w,m,f)	   ((w) ^ ((-(f) ^ (w)) & (m)))

#define HAS_ZERO32(x)	    HAS_LESS32(x,1)
#define HAS_ZERO64(x)	    HAS_LESS64(x,1)

#define HAS_VALUE32(x,v)    HAS_ZERO32((x) ^ (~(uint32_t)0/255 * (v)))
#define HAS_VALUE64(x,v)    HAS_ZERO64((x) ^ (~(uint64_t)0/255 * (v)))

/* require x>=0, 0<=v<=128 */
#define HAS_LESS32(x,v)	    (((x) - ~(uint32_t)0/255 * (v)) & ~(x) & ~(uint32_t)0/255 * 128)
#define HAS_LESS64(x,v)	    (((x) - ~(uint64_t)0/255 * (v)) & ~(x) & ~(uint64_t)0/255 * 128)
#define COUNT_LESS32(x,v)						\
	(((~(uint32_t)0/255 * (127 + (v)) - ((x) & ~(uint32_t)0/255 * 127)) & \
	  ~(x) & ~(uint32_t)0/255 * 128)/128 % 255)
#define COUNT_LESS64(x,v)						\
	(((~(uint64_t)0/255 * (127 + (v)) - ((x) & ~(uint64_t)0/255 * 127)) & \
	  ~(x) & ~(uint64_t)0/255 * 128)/128 % 255)

/* require x>=0; 0<=v<=127 */
#define HAS_MORE32(x,v)	    (((x) + ~(uint32_t)0/255 * (127 - (v)) | (x)) &~(uint32_t)0/255 * 128)
#define HAS_MORE64(x,v)	    (((x) + ~(uint64_t)0/255 * (127 - (v)) | (x)) &~(uint64_t)0/255 * 128)
#define COUNT_MORE32(x,v)						\
	(((((x) & ~(uint32_t)0/255 * 127) + ~(uint32_t)0/255 * (127 - (v)) | \
	   (x)) & ~(uint32_t)0/255 * 128)/128 % 255)
#define COUNT_MORE64(x,v)						\
	(((((x) & ~(uint64_t)0/255 * 127) + ~(uint64_t)0/255 * (127 - (v)) | \
	   (x)) & ~(uint64_t)0/255 * 128)/128 % 255)

#define ROUND_UP32(x) ({			\
			(x)--;			\
			(x) |= (x) >> 1;	\
			(x) |= (x) >> 2;	\
			(x) |= (x) >> 4;	\
			(x) |= (x) >> 8;	\
			(x) |= (x) >> 16;	\
			(x)++;			\
		})

#define ROUND_UP64(x) ({				\
			(x)--;				\
			(x) |= (x) >> 1;		\
			(x) |= (x) >> 2;		\
			(x) |= (x) >> 4;		\
			(x) |= (x) >> 8;		\
			(x) |= (x) >> 16;		\
			(x) |= (uint64_t)(x) >> 32;	\
			(x)++;				\
		})

static inline void set_bit(int nr, volatile unsigned long *addr)
{
	*(addr + BIT_WORD(nr)) |= BIT_MASK(nr);
}

static inline void clear_bit(int nr, volatile unsigned long *addr)
{
	*(addr + BIT_WORD(nr)) &= ~BIT_MASK(nr);
}

static inline void change_bit(int nr, volatile unsigned long *addr)
{
	*(addr + BIT_WORD(nr)) ^= BIT_MASK(nr);
}

static inline int test_bit(int nr, const volatile unsigned long *addr)
{
	return 1UL & (addr[BIT_WORD(nr)] >> (nr & (BITS_PER_LONG-1)));
}

/* Require a < 2^15 - 1.
 * This implementation though elegant might be slow because of division. */
static inline unsigned int hweight15(uint16_t a)
{
	return ((a * 01000010000100001ull) & 0x111111111111111ull) % 15;
}

static inline unsigned int hweight16(uint16_t w)
{
	unsigned int res = w - ((w >> 1) & 0x5555);
	res = (res & 0x3333) + ((res >> 2) & 0x3333);
	res = (res + (res >> 4)) & 0x0F0F;
	return (res + (res >> 8)) & 0x00FF;
}

static inline unsigned int hweight32(uint32_t w)
{
#ifdef BIT_HAS_FAST_MULT
	w -= (w >> 1) & 0x55555555;
	w =  (w & 0x33333333) + ((w >> 2) & 0x33333333);
	w =  (w + (w >> 4)) & 0x0f0f0f0f;
	return (w * 0x01010101) >> 24;
#else
	uint32_t res = w - ((w >> 1) & 0x55555555);
	res = (res & 0x33333333) + ((res >> 2) & 0x33333333);
	res = (res + (res >> 4)) & 0x0F0F0F0F;
	res = res + (res >> 8);
	return (res + (res >> 16)) & 0x000000FF;
#endif
}

static inline unsigned int hweight32_hakmem(uint32_t a)
{
	uint32_t t;

	t = a - ((a >> 1) & 033333333333)
		- ((a >> 2) & 011111111111);
	return ((t + (t >> 3)) & 030707070707) % 63;
}

static inline unsigned int hweight64(uint64_t w)
{
#if BITS_PER_LONG == 32
	return hweight32((unsigned int)(w >> 32)) +
		hweight32((unsigned int)w);
#elif BITS_PER_LONG == 64
#ifdef BIT_HAS_FAST_MULT
	w -= (w >> 1) & 0x5555555555555555ul;
	w =  (w & 0x3333333333333333ul) + ((w >> 2) & 0x3333333333333333ul);
	w =  (w + (w >> 4)) & 0x0f0f0f0f0f0f0f0ful;
	return (w * 0x0101010101010101ul) >> 56;
#else
	uint64_t res = w - ((w >> 1) & 0x5555555555555555ul);
	res = (res & 0x3333333333333333ul) + ((res >> 2) & 0x3333333333333333ul);
	res = (res + (res >> 4)) & 0x0F0F0F0F0F0F0F0Ful;
	res = res + (res >> 8);
	res = res + (res >> 16);
	return (res + (res >> 32)) & 0x00000000000000FFul;
#endif
#endif
}

static inline unsigned int hweight_long(unsigned long a)
{
	return sizeof(a) == 4? hweight32(a): hweight64(a);
}

/* reverse the bits of a byte */
static inline unsigned char rev8_hakmem(unsigned char b)
{
	return ((b * 0x000202020202ULL) & 0x010884422010ULL) % 1023;
}

/* devised by Sean Anderson */
static inline unsigned char rev8(unsigned char b)
{
        return ((b * 0x80200802ULL) & 0x0884422110ULL) * 0x0101010101ULL >> 32;
}

static inline uint32_t rev32(uint32_t n)
{
	n = ((n & 0xAAAAAAAA) >> 1) | ((n & 0x55555555) << 1);
	n = ((n & 0xCCCCCCCC) >> 2) | ((n & 0x33333333) << 2);
	n = ((n & 0xF0F0F0F0) >> 4) | ((n & 0x0F0F0F0F) << 4);
	n = ((n & 0xFF00FF00) >> 8) | ((n & 0x00FF00FF) << 8);
	return (n >> 16) | (n << 16);
}

static inline uint64_t rev64(uint64_t n)
{
	n = ((n & 0xAAAAAAAAAAAAAAAAULL) >> 1) | ((n & 0x5555555555555555ULL) << 1);
	n = ((n & 0xCCCCCCCCCCCCCCCCULL) >> 2) | ((n & 0x3333333333333333ULL) << 2);
	n = ((n & 0xF0F0F0F0F0F0F0F0ULL) >> 4) | ((n & 0x0F0F0F0F0F0F0F0FULL) << 4);
	n = ((n & 0xFF00FF00FF00FF00ULL) >> 8) | ((n & 0x00FF00FF00FF00FFULL) << 8);
	n = ((n & 0xFFFF0000FFFF0000ULL) >> 16)| ((n & 0x0000FFFF0000FFFFULL) << 16);
	return (n >> 32) | (n << 32);
}

static inline int ispow2_32(uint32_t n)
{
	return (n & (n - 1)) == 0;
}

static inline int ispow2_64(uint64_t n)
{
	return (n & (n - 1)) == 0;
}

/* see fls64 */
static inline int fls32(uint32_t x)
{
	int r = 32;

	if (!x)
		return 0;
	if (!(x & 0xffff0000u)) {
		x <<= 16;
		r -= 16;
	}
	if (!(x & 0xff000000u)) {
		x <<= 8;
		r -= 8;
	}
	if (!(x & 0xf0000000u)) {
		x <<= 4;
		r -= 4;
	}
	if (!(x & 0xc0000000u)) {
		x <<= 2;
		r -= 2;
	}
	if (!(x & 0x80000000u)) {
		x <<= 1;
		r -= 1;
	}
	return r;
}

/* find the last set bit in a 64-bit word
 *
 * This is defined in a similar way as the libc and compiler builtin
 * ffsll, but returns the position of the most significant set bit.
 *
 * fls64(value) returns 0 if value is 0 or the position of the last
 * set bit if value is nonzero. The last (most significant) bit is
 * at position 64.
 */
static inline int fls64(uint64_t x)
{
	uint32_t h = x >> 32;
	if (h)
		return fls32(h) + 32;
	return fls32(x);
}

/* find the first set bit
 * This is defined the same way as
 * the libc and compiler builtin ffs routines, therefore
 * differs in spirit from the above ffz (man ffs). */
static inline int ffs32(uint32_t x)
{
	int r = 1;

	if (!x)
		return 0;
	if (!(x & 0xffff)) {
		x >>= 16;
		r += 16;
	}
	if (!(x & 0xff)) {
		x >>= 8;
		r += 8;
	}
	if (!(x & 0xf)) {
		x >>= 4;
		r += 4;
	}
	if (!(x & 3)) {
		x >>= 2;
		r += 2;
	}
	if (!(x & 1)) {
		x >>= 1;
		r += 1;
	}
	return r;
}

/* find the first set bit
 * This is defined the same way as
 * the libc and compiler builtin ffs routines, therefore
 * differs in spirit from the above ffz (man ffs). */
static inline int ffs64(uint64_t word)
{
	uint32_t h = word & 0xffffffff;
	if (!h)
		return ffs32(word >> 32) + 32;
	return ffs32(h);
}

/* use gcc builtin instead if possible */
#define __ffs(w) (__builtin_ffsl(w) - 1)

/*
  static inline unsigned long __ffs(unsigned long word)
  {
  int num = 0;

  #if __WORDSIZE == 64
  if ((word & 0xffffffff) == 0) {
  num += 32;
  word >>= 32;
  }
  #endif
  if ((word & 0xffff) == 0) {
  num += 16;
  word >>= 16;
  }
  if ((word & 0xff) == 0) {
  num += 8;
  word >>= 8;
  }
  if ((word & 0xf) == 0) {
  num += 4;
  word >>= 4;
  }
  if ((word & 0x3) == 0) {
  num += 2;
  word >>= 2;
  }
  if ((word & 0x1) == 0)
  num += 1;
  return num;
  }
*/

/* ffz - find the first zero in word.
 * Undefined if no zero exists, so code should check against ~0UL first. */
#define ffz(x)	__ffs(~(x))

/* find the minimum integer that is larger than a and has the same
 * hamming weight */
static inline uint32_t hweight_next32(uint32_t a)
{
	uint32_t c = a & -a;
	uint32_t r = a + c;
	return (((r ^ a) >> 2) / c) | r;
}

static inline uint64_t hweight_next64(uint64_t a)
{
	uint64_t c = a & -a;
	uint64_t r = a + c;
	return (((r ^ a) >> 2) / c) | r;
}

/* find the next set bit in a memory region */
static inline unsigned long
find_next_bit(const unsigned long *addr, unsigned long size, unsigned long offset)
{
	const unsigned long *p = addr + BIT_WORD(offset);
	unsigned long result = offset & ~(BITS_PER_LONG-1);
	unsigned long tmp;

	if (offset >= size)
		return size;
	size -= result;
	offset %= BITS_PER_LONG;
	if (offset) {
		tmp = *(p++);
		tmp &= (~0UL << offset);
		if (size < BITS_PER_LONG)
			goto found_first;
		if (tmp)
			goto found_middle;
		size -= BITS_PER_LONG;
		result += BITS_PER_LONG;
	}
	while (size & ~(BITS_PER_LONG-1)) {
		if ((tmp = *(p++)))
			goto found_middle;
		result += BITS_PER_LONG;
		size -= BITS_PER_LONG;
	}
	if (!size)
		return result;
	tmp = *p;

found_first:
	tmp &= (~0UL >> (BITS_PER_LONG - size));
	if (tmp == 0UL)		/* Are any bits set? */
		return result + size;	/* Nope. */
found_middle:
	return result + __ffs(tmp);
}

static inline unsigned long
find_next_zero_bit(const unsigned long *addr, unsigned long size, unsigned long offset)
{
	const unsigned long *p = addr + BIT_WORD(offset);
	unsigned long result = offset & ~(BITS_PER_LONG-1);
	unsigned long tmp;

	if (offset >= size)
		return size;
	size -= result;
	offset %= BITS_PER_LONG;
	if (offset) {
		tmp = *(p++);
		tmp |= ~0UL >> (BITS_PER_LONG - offset);
		if (size < BITS_PER_LONG)
			goto found_first;
		if (~tmp)
			goto found_middle;
		size -= BITS_PER_LONG;
		result += BITS_PER_LONG;
	}
	while (size & ~(BITS_PER_LONG-1)) {
		if (~(tmp = *(p++)))
			goto found_middle;
		result += BITS_PER_LONG;
		size -= BITS_PER_LONG;
	}
	if (!size)
		return result;
	tmp = *p;

found_first:
	tmp |= ~0UL << size;
	if (tmp == ~0UL)	/* Are any bits zero? */
		return result + size;	/* Nope. */
found_middle:
	return result + ffz(tmp);
}

static inline unsigned long
find_first_bit(const unsigned long *addr, unsigned long size)
{
	const unsigned long *p = addr;
	unsigned long result = 0;
	unsigned long tmp;

	while (size & ~(BITS_PER_LONG-1)) {
		if ((tmp = *(p++)))
			goto found;
		result += BITS_PER_LONG;
		size -= BITS_PER_LONG;
	}
	if (!size)
		return result;

	tmp = (*p) & (~0UL >> (BITS_PER_LONG - size));
	if (tmp == 0UL)		/* Are any bits set? */
		return result + size;	/* Nope. */
found:
	return result + __ffs(tmp);
}

static inline unsigned long
find_first_zero_bit(const unsigned long *addr, unsigned long size)
{
	const unsigned long *p = addr;
	unsigned long result = 0;
	unsigned long tmp;

	while (size & ~(BITS_PER_LONG-1)) {
		if (~(tmp = *(p++)))
			goto found;
		result += BITS_PER_LONG;
		size -= BITS_PER_LONG;
	}
	if (!size)
		return result;

	tmp = (*p) | (~0UL << size);
	if (tmp == ~0UL)	/* Are any bits zero? */
		return result + size;	/* Nope. */
found:
	return result + ffz(tmp);
}

#define for_each_set_bit(bit, addr, size)			\
	for ((bit) = find_first_bit((addr), (size));		\
	     (bit) < (size);					\
	     (bit) = find_next_bit((addr), (size), (bit) + 1))

#endif	/* _ULIB_MATH_BIT_H */
