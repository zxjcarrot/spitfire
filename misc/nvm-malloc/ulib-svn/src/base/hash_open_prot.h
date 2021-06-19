/* The MIT License

   Copyright (C) 2011, 2012, 2013 Zilong Tan (eric.zltan@gmail.com)
   Copyright (c) 2008, 2009, 2011 by Attractive Chaos <attractor@live.co.uk>

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

#ifndef _ULIB_HASH_OPEN_PROT_H
#define _ULIB_HASH_OPEN_PROT_H

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "util_algo.h"

#if __WORDSIZE == 64

typedef uint64_t oh_iter_t;
typedef uint64_t oh_size_t;

#define OH_ISDEL(flag, i)	 ( ((flag)[(i) >> 5] >> (((i) & 0x1fU) << 1)) & 1      )
#define OH_ISEMPTY(flag, i)	 ( ((flag)[(i) >> 5] >> (((i) & 0x1fU) << 1)) & 2      )
#define OH_ISEITHER(flag, i)	 ( ((flag)[(i) >> 5] >> (((i) & 0x1fU) << 1)) & 3      )
#define OH_CLEAR_DEL(flag, i)	 (  (flag)[(i) >> 5] &= ~(1ul << (((i) & 0x1fU) << 1)) )
#define OH_CLEAR_EMPTY(flag, i)	 (  (flag)[(i) >> 5] &= ~(2ul << (((i) & 0x1fU) << 1)) )
#define OH_CLEAR_BOTH(flag, i)	 (  (flag)[(i) >> 5] &= ~(3ul << (((i) & 0x1fU) << 1)) )
#define OH_SET_DEL(flag, i)	 (  (flag)[(i) >> 5] |=	 (1ul << (((i) & 0x1fU) << 1)) )

#define OH_FLAGS_BYTE(nb)	 ( (nb) < 32? 8: (nb) >> 2 )

#else

typedef uint32_t oh_iter_t;
typedef uint32_t oh_size_t;

#define OH_ISDEL(flag, i)	 ( ((flag)[(i) >> 4] >> (((i) & 0xfU) << 1)) & 1      )
#define OH_ISEMPTY(flag, i)	 ( ((flag)[(i) >> 4] >> (((i) & 0xfU) << 1)) & 2      )
#define OH_ISEITHER(flag, i)	 ( ((flag)[(i) >> 4] >> (((i) & 0xfU) << 1)) & 3      )
#define OH_CLEAR_DEL(flag, i)	 (  (flag)[(i) >> 4] &= ~(1ul << (((i) & 0xfU) << 1)) )
#define OH_CLEAR_EMPTY(flag, i)	 (  (flag)[(i) >> 4] &= ~(2ul << (((i) & 0xfU) << 1)) )
#define OH_CLEAR_BOTH(flag, i)	 (  (flag)[(i) >> 4] &= ~(3ul << (((i) & 0xfU) << 1)) )
#define OH_SET_DEL(flag, i)	 (  (flag)[(i) >> 4] |=	 (1ul << (((i) & 0xfU) << 1)) )

#define OH_FLAGS_BYTE(nb)	 ( (nb) < 16? 4: (nb) >> 2 )

#endif

/* error codes for openhash_set() */
enum {
	OH_INS_ERR = 0,	 /**< element exists */
	OH_INS_NEW = 1,	 /**< element was placed at a new bucket */
	OH_INS_DEL = 2	 /**< element was placed at a deleted bucket */
};

#define OH_LOAD_FACTOR 0.7

#define DEFINE_OPENHASH_RAW(name, key_t, keyref_t, val_t, ismap, hashfn, equalfn) \
	typedef struct {						\
		oh_size_t nbucket;					\
		oh_size_t nelem;					\
		oh_size_t noccupied;					\
		oh_size_t bound;					\
		oh_size_t *flags;					\
		key_t	  *keys;					\
		val_t	  *vals;					\
	} openhash_##name##_t;						\
									\
	static inline openhash_##name##_t *				\
	openhash_init_##name()						\
	{								\
		return (openhash_##name##_t*)				\
			calloc(1, sizeof(openhash_##name##_t));		\
	}								\
									\
	static inline void						\
	openhash_destroy_##name(openhash_##name##_t *h)			\
	{								\
		if (h) {						\
			free(h->flags);					\
			free(h->keys);					\
			free(h->vals);					\
			free(h);					\
		}							\
	}								\
									\
	static inline void						\
	openhash_clear_##name(openhash_##name##_t *h)			\
	{								\
		if (h && h->flags) {					\
			memset(h->flags, 0xaa, OH_FLAGS_BYTE(h->nbucket)); \
			h->nelem  = 0;					\
			h->noccupied = 0;				\
		}							\
	}								\
									\
	static inline oh_iter_t						\
	openhash_get_##name(const openhash_##name##_t *h, keyref_t key) \
	{								\
		if (h->nbucket) {					\
			oh_size_t i, k, last;				\
			oh_size_t step = 0;				\
			oh_size_t mask = h->nbucket - 1;		\
			k = hashfn(key);				\
			i = k & mask;					\
			last = i;					\
			while (!OH_ISEMPTY(h->flags, i) &&		\
			       (OH_ISDEL(h->flags, i) || !equalfn(h->keys[i], key))) { \
				i = (i + ++step) & mask;		\
				if (i == last)				\
					return h->nbucket;		\
			}						\
			return OH_ISEMPTY(h->flags, i)? h->nbucket : i;	\
		} else							\
			return 0;					\
	}								\
									\
	static inline int						\
	openhash_resize_##name(openhash_##name##_t *h, oh_size_t nbucket) \
	{								\
		oh_size_t *flags;					\
		key_t	  *keys;					\
		val_t	  *vals;					\
		oh_size_t  mask = nbucket - 1;				\
		oh_size_t  j, flen;					\
		if (h->nelem >= (oh_size_t)(nbucket * OH_LOAD_FACTOR))	\
			return -1;					\
		flen  = OH_FLAGS_BYTE(nbucket);				\
		flags = (oh_size_t *) malloc(flen);			\
		if (flags == NULL)					\
			return -1;					\
		memset(flags, 0xaa, flen);				\
		if (h->nbucket < nbucket) {				\
			keys = (key_t*)	realloc(h->keys, nbucket * sizeof(key_t)); \
			if (keys == NULL) {				\
				free(flags);				\
				return -1;				\
			}						\
			h->keys = keys;					\
			if (ismap) {					\
				vals = (val_t*)	realloc(h->vals, nbucket * sizeof(val_t)); \
				if (vals == NULL) {			\
					free(flags);			\
					return -1;			\
				}					\
				h->vals = vals;				\
			}						\
		}							\
		for (j = 0; j != h->nbucket; ++j) {			\
			if (OH_ISEITHER(h->flags, j) == 0) {		\
				key_t key = h->keys[j];			\
				val_t val;				\
				if (ismap) val = h->vals[j];		\
				OH_SET_DEL(h->flags, j);		\
				for (;;) {				\
					oh_size_t i, k;			\
					oh_size_t step = 0;		\
					k = hashfn(key);		\
					i = k & mask;			\
					while (!OH_ISEMPTY(flags, i))	\
						i = (i + ++step) & mask; \
					OH_CLEAR_EMPTY(flags, i);	\
					if (i < h->nbucket && OH_ISEITHER(h->flags, i) == 0) { \
						_swap(h->keys[i], key);	\
						if (ismap) _swap(h->vals[i], val); \
						OH_SET_DEL(h->flags, i); \
					} else {			\
						h->keys[i] = key;	\
						if (ismap) h->vals[i] = val; \
						break;			\
					}				\
				}					\
			}						\
		}							\
		if (h->nbucket > nbucket) {				\
			keys = (key_t*) realloc(h->keys, nbucket * sizeof(key_t)); \
			if (keys) h->keys = keys;			\
			if (ismap) {					\
				vals = (val_t*) realloc(h->vals, nbucket * sizeof(val_t)); \
				if (vals) h->vals = vals;		\
			}						\
		}							\
		free(h->flags);						\
		h->flags = flags;					\
		h->nbucket = nbucket;					\
		h->noccupied = h->nelem;				\
		h->bound = (oh_size_t)(h->nbucket * OH_LOAD_FACTOR);	\
		return 0;						\
	}								\
									\
	static inline oh_iter_t						\
	openhash_set_##name(openhash_##name##_t *h, keyref_t key, int *ret) \
	{								\
		oh_size_t i, x, k, step, mask, site, last;		\
		if (h->noccupied >= h->bound) {				\
			if (h->nbucket) {				\
				if (openhash_resize_##name(h, h->nbucket << 1)) \
					return h->nbucket;		\
			} else {					\
				if (openhash_resize_##name(h, 2))	\
					return h->nbucket;		\
			}						\
		}							\
		site = h->nbucket;					\
		mask = h->nbucket - 1;					\
		x = site;						\
		k = hashfn(key);					\
		i = k & mask;						\
		if (OH_ISEMPTY(h->flags, i))				\
			x = i;						\
		else {							\
			last = i;					\
			step = 0;					\
			while (!OH_ISEMPTY(h->flags, i) &&		\
			       (OH_ISDEL(h->flags, i) || !equalfn(h->keys[i], key))) { \
				if (OH_ISDEL(h->flags, i))		\
					site = i;			\
				i = (i + ++step) & mask;		\
				if (i == last) {			\
					x = site;			\
					break;				\
				}					\
			}						\
			if (x == h->nbucket) {				\
				if (OH_ISEMPTY(h->flags, i) && site != h->nbucket) \
					x = site;			\
				else					\
					x = i;				\
			}						\
		}							\
		if (OH_ISEMPTY(h->flags, x)) {				\
			h->keys[x] = key;				\
			OH_CLEAR_BOTH(h->flags, x);			\
			++h->nelem;					\
			++h->noccupied;					\
			*ret = OH_INS_NEW;				\
		} else if (OH_ISDEL(h->flags, x)) {			\
			h->keys[x] = key;				\
			OH_CLEAR_BOTH(h->flags, x);			\
			++h->nelem;					\
			*ret = OH_INS_DEL;				\
		} else							\
			*ret = OH_INS_ERR;				\
		return x;						\
	}								\
									\
	static inline void						\
	openhash_del_##name(openhash_##name##_t *h, oh_iter_t x)	\
	{								\
		if (x != h->nbucket && !OH_ISEITHER(h->flags, x)) {	\
			OH_SET_DEL(h->flags, x);			\
			--h->nelem;					\
		}							\
	}

/* Provide two versions: C and C++. The C++ version uses reference to
 * save the cost of copying parameters. */
#ifdef __cplusplus
#define DEFINE_OPENHASH(name, key_t, val_t, ismap, hashfn, equalfn)	\
	DEFINE_OPENHASH_RAW(name, key_t, const key_t &, val_t,		\
			    ismap, hashfn, equalfn)
#else
#define DEFINE_OPENHASH(name, key_t, val_t, ismap, hashfn, equalfn)	\
	DEFINE_OPENHASH_RAW(name, key_t, key_t, val_t, ismap,		\
			    hashfn, equalfn)
#endif

/*------------------------- Human Interface -------------------------*/

/* Identity hash function, converting a key to an integer. This coerce
 * the key to be of an integer type or integer interpretable. */
#define openhash_hashfn(key) (oh_size_t)(key)

/* boolean function that tests whether two keys are equal */
#define openhash_equalfn(a, b) ((a) == (b))

/* openhash type */
#define openhash_t(name) openhash_##name##_t

/* Return the key/value associated with the iterator */
#define openhash_key(h, x) ((h)->keys[x])
#define openhash_value(h, x) ((h)->vals[x])

/* Core openhash functions. */
#define openhash_init(name) openhash_init_##name()
#define openhash_destroy(name, h) openhash_destroy_##name(h)
#define openhash_clear(name, h) openhash_clear_##name(h)
/* The resize function is called automatically when the specified load
 * limit is reached. Thus don't call it manually unless you have to. */
#define openhash_resize(name, h, s) openhash_resize_##name(h, s)

/* Insert a new element without replacing the old one, if any.
 * Upon completion, r will hold the error code as defined above.
 * Returns an iterator to the new or former occupant. */
#define openhash_set(name, h, k, r) openhash_set_##name(h, k, r)
#define openhash_get(name, h, k) openhash_get_##name(h, k)
/* delete an element by iterator */
#define openhash_del(name, h, x) openhash_del_##name(h, x)
/* test whether an iterator is valid */
#define openhash_exist(h, x) (!OH_ISEITHER((h)->flags, (x)))

/* Iterator functions. */
#define openhash_begin(h) (oh_iter_t)(0)
#define openhash_end(h) ((h)->nbucket)

/* number of elements in the hash table */
#define openhash_size(h) ((h)->nelem)
/* return the current capacity of the hash table */
#define openhash_nbucket(h) ((h)->nbucket)

#endif	/* _ULIB_HASH_OPEN_PROT_H */
