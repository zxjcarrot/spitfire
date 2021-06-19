/* The MIT License

   Copyright (C) 2012 Zilong Tan (eric.zltan@gmail.com)
   2005 kazutomo (kazutomo@mcs.anl.gov)

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

#ifndef __ULIB_RDTSC_H
#define __ULIB_RDTSC_H

#include <stdint.h>

#if defined(__i386__)

static __inline__ uint64_t rdtsc(void)
{
	uint64_t x;
	__asm__ volatile (".byte 0x0f, 0x31" : "=A" (x));
	return x;
}

#elif defined(__x86_64__)

static __inline__ uint64_t rdtsc(void)
{
	unsigned hi, lo;
	__asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
	return ((uint64_t)lo) | (((uint64_t)hi) << 32);
}

#elif defined(__powerpc__)

static __inline__ uint64_t rdtsc(void)
{
	uint64_t result = 0;
	unsigned long upper, lower, tmp;

	__asm__ volatile(
		"0:		    \n"
		"\tmftbu   %0	    \n"
		"\tmftb	   %1	    \n"
		"\tmftbu   %2	    \n"
		"\tcmpw	   %2,%0    \n"
		"\tbne	   0b	    \n"
		: "=r"(upper),"=r"(lower),"=r"(tmp)
		);

	result = upper;
	result = result << 32;
	result = result | lower;
	return result;
}

#else

#error "No tick counter is available!"

#endif

#endif	/*  __ULIB_RDSTC_H */
