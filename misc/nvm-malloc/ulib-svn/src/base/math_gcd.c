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

#include "math_gcd.h"

unsigned long gcd(unsigned long a, unsigned long b)
{
	unsigned long r;

	while (b) {
		r = a % b;
		a = b;
		b = r;
	}
	return a;
}

void egcd(unsigned long a, unsigned long b, long *x, long *y)
{
	long x1, x2, x3, y1, y2, y3;
	unsigned long r, q;

	x1 = 1;
	x2 = 0;
	y1 = 0;
	y2 = 1;

	while (b != 0) {
		q = a / b;
		r = a % b;
		a = b;
		b = r;
		x3 = x1 - q * x2;
		x1 = x2;
		x2 = x3;
		y3 = y1 - q * y2;
		y1 = y2;
		y2 = y3;
	}
	*x = x1;
	*y = y1;
}

unsigned long invert(unsigned long m, unsigned long b)
{
	long s, t;

	egcd(m, b, &t, &s);

	if (s < 0)
		return m + s;
	return s;
}
