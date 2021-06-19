/* The MIT License

   Copyright (C) 2012 Zilong Tan (eric.zltan@gmail.com)

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

#ifndef _ULIB_OS_ATOMIC_INTEL64_H
#define _ULIB_OS_ATOMIC_INTEL64_H

#include <stdint.h>

#ifndef __always_inline
#define __always_inline inline
#endif

#define DEFINE_ATOMICS(S,T,X)						\
	static __always_inline T atomic_cmpswp##S(volatile void *ptr, T comparand, T value) \
	{								\
		T result;						\
									\
		__asm__ __volatile__("lock\n\tcmpxchg" X " %2,%1"	\
				     : "=a"(result), "=m"(*(volatile T*)ptr) \
				     : "q"(value), "0"(comparand), "m"(*(volatile T*)ptr) \
				     : "memory");			\
		return result;						\
	}								\
									\
	static __always_inline T atomic_fetchadd##S(volatile void *ptr, T addend) \
	{								\
		T result;						\
									\
		__asm__ __volatile__("lock\n\txadd" X " %0,%1"		\
				     : "=r"(result),"=m"(*(volatile T*)ptr) \
				     : "0"(addend), "m"(*(volatile T*)ptr) \
				     : "memory");			\
		return result;						\
	}								\
									\
	static __always_inline T atomic_fetchstore##S(volatile void *ptr, T value) \
	{								\
		T result;						\
									\
		__asm__ __volatile__("lock\n\txchg" X " %0,%1"		\
				     : "=r"(result),"=m"(*(volatile T*)ptr) \
				     : "0"(value), "m"(*(volatile T*)ptr) \
				     : "memory");			\
		return result;						\
	}								\
									\
	static __always_inline char atomic_test_and_set_bit##S(volatile void *ptr, int value) \
	{								\
		char result;						\
									\
		__asm__ __volatile__("lock\n\tbts %2,%1\n\t"		\
				     "sbb %0,%0"			\
				     : "=r"(result), "=m"(*(volatile T*)ptr) \
				     : "Ir"(value)			\
				     : "memory");			\
									\
		return result;						\
	}								\
									\
	static __always_inline void atomic_or##S(volatile void *ptr, T value) \
	{								\
		__asm__ __volatile__("lock\n\tor" X " %1,%0"		\
				     : "=m"(*(volatile T*)ptr)		\
				     : "r"(value), "m"(*(volatile T*)ptr) \
				     : "memory");			\
	}								\
									\
	static __always_inline void atomic_and##S(volatile void *ptr, T value) \
	{								\
		__asm__ __volatile__("lock\n\tand" X " %1,%0"		\
				     : "=m"(*(volatile T*)ptr)		\
				     : "r"(value), "m"(*(volatile T*)ptr) \
				     : "memory");			\
	}								\
									\
	static __always_inline void atomic_inc##S(volatile void *ptr)	\
	{								\
		__asm__ __volatile__("lock\n\tinc" X " %0"		\
				     : "=m"(*(volatile T*)ptr)		\
				     : "m"(*(volatile T*)ptr)		\
				     : "memory");			\
	}								\
									\
	static __always_inline void atomic_dec##S(volatile void *ptr)	\
	{								\
		__asm__ __volatile__("lock\n\tdec" X " %0"		\
				     : "=m"(*(volatile T*)ptr)		\
				     : "m"(*(volatile T*)ptr)		\
				     : "memory");			\
	}								\
									\
	static __always_inline void atomic_add##S(volatile void *ptr, T value) \
	{								\
		__asm__ __volatile__("lock\n\tadd" X " %1,%0"		\
				     : "=m"(*(volatile T*)ptr)		\
				     : "r"(value), "m"(*(volatile T*)ptr) \
				     : "memory");			\
	}

DEFINE_ATOMICS(8,  int8_t,  "b")
DEFINE_ATOMICS(16, int16_t, "w")
DEFINE_ATOMICS(32, int32_t, "l")
DEFINE_ATOMICS(64, int64_t, "q")

#define atomic_barrier()   __asm__ __volatile__("": : :"memory")
#define atomic_cpu_relax() __asm__ __volatile__("pause": : :"memory")

#endif
