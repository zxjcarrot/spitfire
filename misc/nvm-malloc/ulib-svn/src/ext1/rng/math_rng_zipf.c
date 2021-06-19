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

#include <time.h>
#include <math.h>
#include <stdio.h>
#include <math_rand_prot.h>
#include "math_rng_zipf.h"

static inline int isequalf(float a, float b)
{
	return fabs(a - b) < 0.000001;
}

void zipf_rng_init(struct zipf_rng *rng, int range, float s)
{
	uint64_t seed = (uint64_t) time(NULL);

	rng->range = range++;
	rng->s = s;

	if (isequalf(s, 1.0))
		rng->sum = log(range);
	else
		rng->sum = (pow(range, 1.0 - s) - 1.0)/(1.0 - s);

	RAND_NR_INIT(rng->u, rng->v, rng->w, seed);
}

int zipf_rng_next(struct zipf_rng *rng)
{
	double m = RAND_NR_DOUBLE(RAND_NR_NEXT(rng->u, rng->v, rng->w));

	if (isequalf(rng->s, 1.0))
		return (int)exp(m * rng->sum);

	return (int)pow(m * rng->sum * (1.0 - rng->s) + 1.0, 1.0/(1.0 - rng->s));
}
