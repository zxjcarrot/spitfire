/* The MIT License

   Copyright (C) 2012, 2013 Zilong Tan (eric.zltan@gmail.com)

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

/* This file implements the hash table which uses list for collision
 * resolution. Concurrency control, which is based on region locks, is
 * also supported and thus it is thread-safe, enabling concurrent
 * execution of any combination of the operations provided on the hash
 * table.
 *
 * The theoretical bounds, which are perfectly practical, for the
 * insertion/search costs are: the lower bound O(1/T + 1/L - 1/(TL)),
 * and the upper bound O(1/T + 1/L), given that the costs for
 * insertion/search are constants in single-threaded scenario. T is
 * the number of concurrent operations(threads) at any instant and L
 * is the number of chains specified initializing the hash table.
 *
 * Referring to the cost bounds, the cost that posed by region locks
 * would become trivial in practice as long as sufficient chains are
 * used initializing the hash table. The suggested value for the
 * number of chains are 64 or 128; however, it is largely processor
 * specific. Having more threads and processors could increase the
 * number of chains accordingly, with the help of the cost bounds. */

#ifndef _ULIB_HASH_CHAIN_PROT_H
#define _ULIB_HASH_CHAIN_PROT_H

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* the stack size for mergesort */
#define MAX_CH_SIZE_BITS 64

/* Note that equalfn only tests if two keys are equal while cmpfn
 * compares the keys, which is usually slower than equalfn. The
 * isolation of the two functions might serve to improve the
 * performance. */
#define DEFINE_CHAINHASH(name, keytype, valtype, ismap, hashfn, equalfn, cmpfn) \
	typedef struct chain_set_entry_##name {				\
		struct chain_set_entry_##name *snap;			\
		struct chain_set_entry_##name *next;			\
		keytype key;						\
	} chainhash_set_entry_##name##_t;				\
									\
	typedef struct chain_map_entry_##name {				\
		struct chain_map_entry_##name *snap;			\
		struct chain_map_entry_##name *next;			\
		keytype key;						\
		valtype val;						\
	} chainhash_map_entry_##name##_t;				\
									\
	typedef struct {						\
		size_t mask;						\
		chainhash_set_entry_##name##_t **tbl;			\
		chainhash_set_entry_##name##_t *snap;			\
	} chainhash_##name##_t;						\
									\
	typedef struct {						\
		chainhash_##name##_t *ch;				\
		chainhash_set_entry_##name##_t *entry;			\
		chainhash_set_entry_##name##_t *parent;			\
		size_t index;						\
	} chainhash_itr_##name##_t;					\
									\
	static inline chainhash_##name##_t *				\
	chainhash_init_##name(size_t min)				\
	{								\
		chainhash_##name##_t *ch = (chainhash_##name##_t *)	\
			malloc(sizeof(chainhash_##name##_t));		\
		if (ch == NULL)						\
			return NULL;					\
		--min;							\
		min |= min >> 1;					\
		min |= min >> 2;					\
		min |= min >> 4;					\
		min |= min >> 8;					\
		min |= min >> 16;					\
		if (sizeof(size_t) == 8)				\
			min |= (uint64_t)min >> 32;			\
		ch->tbl = (chainhash_set_entry_##name##_t **)		\
			calloc(min + 1, sizeof(chainhash_set_entry_##name##_t *)); \
		if (ch->tbl == NULL) {					\
			free(ch);					\
			return NULL;					\
		}							\
		ch->mask = min;						\
		ch->snap = NULL;					\
		return ch;						\
	}								\
									\
	static inline void						\
	chainhash_clear_##name(chainhash_##name##_t *ch)		\
	{								\
		size_t i;						\
		size_t mask = ch->mask;					\
		ch->snap = NULL;					\
		for (i = 0; i <= mask; ++i) {				\
			if (ch->tbl[i]) {				\
				chainhash_set_entry_##name##_t *e = ch->tbl[i]; \
				do {					\
					chainhash_set_entry_##name##_t *t = e->next; \
					free(e);			\
					e = t;				\
				} while (e);				\
				ch->tbl[i] = NULL;			\
			}						\
		}							\
	}								\
									\
	static inline void						\
	chainhash_destroy_##name(chainhash_##name##_t *ch)		\
	{								\
		if (ch) {						\
			chainhash_clear_##name(ch);			\
			free(ch->tbl);					\
			free(ch);					\
		}							\
	}								\
									\
	static inline chainhash_itr_##name##_t				\
	chainhash_get_##name(chainhash_##name##_t *ch, const keytype key) \
	{								\
		size_t h = hashfn(key) & ch->mask;			\
		chainhash_set_entry_##name##_t *e = ch->tbl[h];		\
		chainhash_set_entry_##name##_t *p = NULL;		\
		chainhash_itr_##name##_t itr;				\
		itr.ch = ch;						\
		itr.index = h;						\
		while (e) {						\
			if (equalfn(key, e->key))			\
				break;					\
			p = e;						\
			e = e->next;					\
		}							\
		itr.entry = e;						\
		itr.parent = p;						\
		return itr;						\
	}								\
									\
	static inline chainhash_itr_##name##_t				\
	chainhash_set_uniq_##name(chainhash_##name##_t *ch,		\
				  const keytype key, int *exist)	\
	{								\
		chainhash_itr_##name##_t itr = chainhash_get_##name(ch, key); \
		if (itr.entry == NULL) {				\
			*exist = 0;					\
			chainhash_set_entry_##name##_t *e = (chainhash_set_entry_##name##_t *) \
				malloc((ismap)? sizeof(chainhash_map_entry_##name##_t):	\
				       sizeof(chainhash_set_entry_##name##_t));	\
			if (e == NULL) 					\
				return itr;				\
			e->next = NULL;					\
			e->snap = NULL;					\
			e->key	= key;					\
			if (itr.parent)					\
				itr.parent->next = e;			\
			else						\
				ch->tbl[itr.index] = e;			\
			itr.entry = e;					\
		} else							\
			*exist = 1;					\
		return itr;						\
	}								\
									\
	static inline chainhash_itr_##name##_t				\
	chainhash_set_##name(chainhash_##name##_t *ch, const keytype key) \
	{								\
		size_t h = hashfn(key) & ch->mask;			\
		chainhash_itr_##name##_t itr;				\
		itr.ch = ch;						\
		itr.parent = NULL;					\
		itr.index = h;						\
		chainhash_set_entry_##name##_t *e = (chainhash_set_entry_##name##_t *) \
			malloc((ismap)? sizeof(chainhash_map_entry_##name##_t): \
			       sizeof(chainhash_set_entry_##name##_t));	\
		itr.entry = e;						\
		if (e == NULL)						\
			return itr;					\
		e->next = ch->tbl[h];					\
		e->snap = NULL;						\
		e->key = key;						\
		ch->tbl[h]= e;						\
		return itr;						\
	}								\
									\
	static inline void						\
	chainhash_del_##name(chainhash_itr_##name##_t itr)		\
	{								\
		if (itr.entry == NULL)					\
			return;						\
		if (itr.parent == NULL)					\
			itr.ch->tbl[itr.index] = itr.entry->next;	\
		else							\
			itr.parent->next = itr.entry->next;		\
		free(itr.entry);					\
	}								\
									\
	static inline chainhash_itr_##name##_t				\
	chainhash_iterator_##name(chainhash_##name##_t *ch)		\
	{								\
		size_t i = 0;						\
		chainhash_itr_##name##_t itr;				\
		itr.ch = ch;						\
		itr.parent = NULL;					\
		if (ch->snap) { /* walk current snap */			\
			itr.index = hashfn(ch->snap->key) & ch->mask;	\
			itr.entry = ch->snap;				\
			chainhash_set_entry_##name##_t *p = NULL;	\
			chainhash_set_entry_##name##_t *e = ch->tbl[itr.index]; \
			while (!equalfn(ch->snap->key, e->key)) {	\
				p = e;					\
				e = e->next;				\
			}						\
			itr.parent = p;					\
			return itr;					\
		}							\
		while (i <= ch->mask && ch->tbl[i] == NULL)		\
			++i;						\
		itr.index = i;						\
		if (i <= ch->mask)					\
			itr.entry = ch->tbl[i];				\
		else							\
			itr.entry = NULL;				\
		return itr;						\
	}								\
									\
	static inline int						\
	chainhash_advance_##name(chainhash_itr_##name##_t *itr)		\
	{								\
		if (itr->entry == NULL)					\
			return -1;					\
		if (itr->ch->snap) { /* walk through snap */		\
			itr->entry = itr->entry->snap;			\
			if (itr->entry) {				\
				itr->index = hashfn(itr->entry->key) & itr->ch->mask; \
				chainhash_set_entry_##name##_t *p = NULL; \
				chainhash_set_entry_##name##_t *e = itr->ch->tbl[itr->index]; \
				while (!equalfn(itr->entry->key, e->key)) { \
					p = e;				\
					e = e->next;			\
				}					\
				itr->parent = p;			\
			}						\
			return 0;					\
		}							\
		if (itr->entry->next) {					\
			itr->parent = itr->entry;			\
			itr->entry = itr->entry->next;			\
			return 0;					\
		}							\
		if (itr->index < itr->ch->mask) {			\
			size_t i = itr->index;				\
			while (itr->ch->tbl[++i] == NULL) {		\
				if (i == itr->ch->mask)			\
					return -1;			\
			}						\
			itr->parent = NULL;				\
			itr->entry = itr->ch->tbl[i];			\
			itr->index = i;					\
			return 0;					\
		}							\
		return -1;						\
	}								\
									\
	static inline chainhash_set_entry_##name##_t *			\
	__chainhash_merge_##name(chainhash_set_entry_##name##_t *a,	\
				 chainhash_set_entry_##name##_t *b)	\
	{								\
		chainhash_set_entry_##name##_t *head, *tail;		\
		tail = (chainhash_set_entry_##name##_t *)&head;		\
		while (a && b) {					\
			/* if equal, take 'a' -- important for sort stability */ \
			if (cmpfn(a->key, b->key) <= 0) {		\
				tail->snap = a;				\
				a = a->snap;				\
			} else {					\
				tail->snap = b;				\
				b = b->snap;				\
			}						\
			tail = tail->snap;				\
		}							\
		tail->snap = a?:b;					\
		return head;						\
	}								\
									\
	static inline void						\
	chainhash_sort_##name(chainhash_##name##_t *ch)			\
	{								\
		chainhash_set_entry_##name##_t *part[MAX_CH_SIZE_BITS + 1]; \
		int lev;						\
		int max_lev = 0;					\
		chainhash_set_entry_##name##_t *list;			\
		if (ch->snap == NULL)					\
			return;						\
		memset(part, 0, sizeof(part));				\
		for (list = ch->snap; list;) {				\
			chainhash_set_entry_##name##_t *cur = list;	\
			list = list->snap;				\
			cur->snap = NULL;				\
			for (lev = 0; part[lev]; ++lev) {		\
				cur = __chainhash_merge_##name(part[lev], cur);	\
					part[lev] = NULL;		\
			}						\
			if (lev > max_lev)				\
				max_lev = lev;				\
			part[lev] = cur;				\
		}							\
		for (lev = 0; lev <= max_lev; ++lev)			\
			if (part[lev])					\
				list = __chainhash_merge_##name(part[lev], list); \
		ch->snap = list;					\
	}								\
									\
	static inline void						\
	chainhash_snap_##name(chainhash_##name##_t *ch)			\
	{								\
		chainhash_set_entry_##name##_t *cur, *last;		\
		size_t i = 0;						\
		while (i <= ch->mask && ch->tbl[i] == NULL)		\
			++i;						\
		if (i <= ch->mask)					\
			cur = ch->tbl[i];				\
		else							\
			return;						\
		ch->snap = cur;						\
		last = cur;						\
		for (;;) {						\
			last->snap = cur;				\
			last = cur;					\
			if (cur->next)					\
				cur = cur->next;			\
			else if (i < ch->mask) {			\
				while (ch->tbl[++i] == NULL) {		\
					if (i == ch->mask)		\
						goto done;		\
				}					\
				cur = ch->tbl[i];			\
			} else						\
				goto done;				\
		}							\
	done:								\
		last->snap = NULL;					\
	}

/*------------------------- Human Interface -------------------------*/

/* Identity hash function, converting a key to an integer. This coerce
 * the key to be of an integer type or integer interpretable. */
#define chainhash_hashfn(key) (size_t)(key)

/* Boolean function that tests whether two keys are equal. This
 * function is generally much more efficient than cmpfn. */
#define chainhash_equalfn(a, b) ((a) == (b))

/* Function that compares two keys, returns negative, zero, and
 * positive values for less than, equal, and greater than
 * respectively. */
#define chainhash_cmpfn(a, b) (((a) > (b)) - ((a) < (b)))

/* chainhash and its iterator types */
#define chainhash_t(name) chainhash_##name##_t
#define chainhash_itr_t(name) chainhash_itr_##name##_t

/* get the key/value associated with the iterator */
#define chainhash_key(x) ((x).entry->key)
/* Note that only maps have values */
#define chainhash_value(name, x) (((chainhash_map_entry_##name##_t *)(x).entry)->val)

/* Core chainhash functions.
 * min is the minimum number of chains to use, which will be rounded
 * to the nearest power-of-two boundary. */
#define chainhash_init(name, min) chainhash_init_##name(min)
#define chainhash_destroy(name, h) chainhash_destroy_##name(h)
#define chainhash_clear(name, h) chainhash_clear_##name(h)
/* does not check for the duplicates */
#define chainhash_set(name, h, k) chainhash_set_##name(h, k)
#define chainhash_set_uniq(name, h, k, dup) chainhash_set_uniq_##name(h, k, dup)
#define chainhash_get(name, h, k) chainhash_get_##name(h, k)
/* delete an item through iterator */
#define chainhash_del(name, x) chainhash_del_##name(x)

/* Iterator functions */
#define chainhash_begin(name, h) chainhash_iterator_##name(h)
#define chainhash_advance(name, x) chainhash_advance_##name(x)
#define chainhash_end(x) ((x).entry == NULL)

/* Take a snapshot of the current chainhash, causing the iteration
 * order to update accordingly. New added elements after the
 * snapshot will remain invisible during the iteration until another
 * snapshot is taken. */
#define chainhash_snap(name, h) chainhash_snap_##name(h)

/* Sort a snapshot, requiring a snapshot taken first. */
#define chainhash_sort(name, h) chainhash_sort_##name(h)

/* return the number of chains used */
#define chainhash_nbucket(h) ((h)->mask + 1)

#endif /* _ULIB_HASH_CHAIN_PROT_H */
