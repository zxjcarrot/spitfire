/* The MIT License

   Copyright (C) 2012 Zilong Tan (eric.zltan@gmail.com)

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

#include <stdio.h>
#include <stdint.h>
#include "os_rdtsc.h"
#include "math_rand_prot.h"
#include "mr_dataset.h"

namespace ulib {

namespace mapreduce {

dataset_zipf::dataset_zipf(size_t size, size_t range, float s)
	: _buf(new int[size]), _size(size)
{
	zipf_rng rng;
	zipf_rng_init(&rng, range, s);
	for (size_t i = 0; i < size; ++i)
		_buf[i] = zipf_rng_next(&rng);
}

dataset_zipf::~dataset_zipf()
{ delete [] _buf; }

dataset_random::dataset_random(size_t size, size_t range)
	: _buf(new int[size]), _size(size)
{
	uint64_t t = rdtsc();
	for (size_t i = 0; i < size; ++i) {
		uint64_t r = i + t;
		RAND_INT4_MIX64(r);
		_buf[i] = r % range;
	}
}

dataset_random::~dataset_random()
{ delete [] _buf; }

}  // namespace mapreduce

}  // namespace ulib
