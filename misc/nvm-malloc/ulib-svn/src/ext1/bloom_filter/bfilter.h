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

#ifndef _ULIB_BLOOM_FILTER_H
#define _ULIB_BLOOM_FILTER_H

#include <stdint.h>

struct bloom_filter {
	unsigned long *bitmap;
	unsigned long  nbits;
	unsigned long  nelem;  /* estimated number of elements */
	int	       nfunc;  /* number of hash functions */
	uint64_t      *seeds;  /* seeds for hash functions */
};

#ifdef __cplusplus
extern "C" {
#endif

int  bfilter_create(struct bloom_filter *bf, unsigned long nbits, unsigned long nelem);
void bfilter_destroy(struct bloom_filter *bf);
void bfilter_zero(struct bloom_filter *bf);
void bfilter_set(struct bloom_filter *bf, const void *buf, unsigned long buflen);
int  bfilter_get(struct bloom_filter *bf, const void *buf, unsigned long buflen);
void bfilter_set_hash(struct bloom_filter *bf, unsigned long hash);
int  bfilter_get_hash(struct bloom_filter *bf, unsigned long hash);

#ifdef __cplusplus
}
#endif

#endif
