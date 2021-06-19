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

#ifndef _ULIB_RNG_ZIPF_H
#define _ULIB_RNG_ZIPF_H

#include <stdint.h>

struct zipf_rng {
	int   range;
	float sum;
	float s;
	uint64_t u, v, w;  /* int rng context */
};

#ifdef __cplusplus
extern "C" {
#endif

/**
 * zipf_rng_init - create a zipf rng with density function:
 *		   p(x) = 1/x^s, where x = 1,2,...,range.
 * @rng:   rng context
 * @range: random integer range
 * @s:	   zipf distribution parameter
 */
void zipf_rng_init(struct zipf_rng *rng, int range, float s);

/**
 * zipf_rng_next - generate a random integer
 * @rng: rng context
 */
int  zipf_rng_next(struct zipf_rng *rng);

#ifdef __cplusplus
}
#endif

#endif	/* _ULIB_RNG_ZIPF_H */
