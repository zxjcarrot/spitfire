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

#include <stdlib.h>
#include <math.h>
#include <time.h>
#include <hash_func.h>
#include <bitmap.h>
#include <math_rand_prot.h>
#include "bfilter.h"

#define OPTIMAL_NFUNC(nbits, nelem) ((int)ceilf((9 * nbits)/(13.0 * nelem)))
#define HASH_FUNCTION(buf,len,seed) hash_fast64(buf, len, seed)

static inline void
__init_seeds(uint64_t *seeds, int nseed)
{
	uint64_t x, y, z;
	uint64_t seed = (uint64_t)time(NULL);
	int i;

	RAND_NR_INIT(x, y, z, seed);
	for (i = 0; i < nseed; i++)
		seeds[i] = RAND_NR_NEXT(x, y, z);
}

int bfilter_create(struct bloom_filter *bf, unsigned long nbits, unsigned long nelem)
{
	bf->nbits = nbits;
	bf->nelem = nelem;
	bf->nfunc = OPTIMAL_NFUNC(nbits, nelem);
	bf->seeds = (uint64_t *) malloc(8 * bf->nfunc);
	if (bf->seeds == NULL)
		return -1;
	bf->bitmap = (unsigned long *) malloc(BITS_TO_LONGS(nbits)*sizeof(long));
	if (bf->bitmap == NULL) {
		free(bf->seeds);
		return -1;
	}
	__init_seeds(bf->seeds, bf->nfunc);
	bfilter_zero(bf);
	return 0;
}

void bfilter_destroy(struct bloom_filter *bf)
{
	free(bf->seeds);
	free(bf->bitmap);
}

void bfilter_zero(struct bloom_filter *bf)
{
	bitmap_zero(bf->bitmap, bf->nbits);
}

void bfilter_set(struct bloom_filter *bf, const void *buf, unsigned long buflen)
{
	uint64_t hash;
	int i;

	for (i = 0; i < bf->nfunc; i++) {
		hash = HASH_FUNCTION(buf, buflen, bf->seeds[i]);
		bfilter_set_hash(bf, hash);
	}
}

int bfilter_get(struct bloom_filter *bf, const void *buf, unsigned long buflen)
{
	uint64_t hash;
	int i;

	for (i = 0; i < bf->nfunc; i++) {
		hash = HASH_FUNCTION(buf, buflen, bf->seeds[i]);
		if (bfilter_get_hash(bf, hash) == 0)
			return 0;
	}

	return 1;
}

void bfilter_set_hash(struct bloom_filter *bf, unsigned long hash)
{
	unsigned long *addr;
	hash = hash % bf->nbits;
	addr = bf->bitmap + BIT_WORD(hash);
	set_bit(hash & (BITS_PER_LONG - 1), addr);
}

int bfilter_get_hash(struct bloom_filter *bf, unsigned long hash)
{
	unsigned long *addr;
	hash = hash % bf->nbits;
	addr = bf->bitmap + BIT_WORD(hash);
	return test_bit(hash & (BITS_PER_LONG - 1), addr);
}
