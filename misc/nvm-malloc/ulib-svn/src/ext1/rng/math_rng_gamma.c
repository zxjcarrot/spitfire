/* The MIT License

   Copyright (C) 2013  Zilong Tan (eric.zltan@gmail.com)
   Copyright (C) 2009, 2011 by Attractive Chaos <attractor@live.co.uk>

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
#include <math_rand_prot.h>
#include "math_rng_gamma.h"

void gamma_rng_init(gamma_rng_t *rng)
{
	normal_rng_init(rng);
}

double gamma_rng_next(gamma_rng_t *rng, double a, double b)
{
	if (a < 1.0) {
		double u;
		do {
			u = RAND_NR_DOUBLE(RAND_NR_NEXT(rng->u, rng->v, rng->w));
		} while (u == 0.0);
		return gamma_rng_next(rng, 1.0 + a, b) * pow(u, 1.0 / a);
	}

	double x, v, u;
	double d = a - 1.0 / 3.0;
	double c = (1.0 / 3.0) / sqrt(d);

	for (;;) {
		do {
			x = normal_rng_next(rng);
			v = 1.0 + c * x;
		} while (v <= 0.0);
		v = v * v * v;
		do {
			u = RAND_NR_DOUBLE(RAND_NR_NEXT(rng->u, rng->v, rng->w));
		} while (u == 0.0);
		if (u < 1.0 - 0.0331 * x * x * x * x)
			break;
		if (log(u) < 0.5 * x * x + d * (1 - v + log(v)))
			break;
	}
	return b * d * v;
}
