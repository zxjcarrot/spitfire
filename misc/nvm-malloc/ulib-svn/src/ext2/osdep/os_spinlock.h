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

/* For ticket spin lock, define LARGE_CPUSET when there are more than
 * 255 CPUs */

#ifndef _ULIB_OS_SPINLOCK_H
#define _ULIB_OS_SPINLOCK_H

#include <stdio.h>
#include <stdint.h>

#ifdef __cplusplus
#define new _new_
#endif

#ifndef __always_inline
#define __always_inline inline
#endif

#define ACCESS_ONCE(x) (*(volatile typeof(x) *)&(x))

#ifdef __x86_64__
#include "os_atomic_intel64.h"
#endif

typedef int8_t xchg_lock_t;

/* each mcs acquirer owns one mcs_entry */
typedef struct mcs_entry {
	struct mcs_entry *next;
	int8_t lock;
} mcs_entry_t;

/* the mcs lock type */
typedef mcs_entry_t * mcs_lock_t;

/* Note the K42 lock was patented by IBM, we add it here just for
 * testing. */
typedef struct k42_lock {
	struct k42_lock *next;
	struct k42_lock *tail;
} k42_lock_t;

/* the ticket spin lock */
#ifdef LARGE_CPUSET
typedef uint16_t ticket_t;
typedef uint32_t ticketpair_t;
typedef uint64_t ticketquad_t;
#else
typedef uint8_t	 ticket_t;
typedef uint16_t ticketpair_t;
typedef uint32_t ticketquad_t;
#endif

#define TICKET_SHIFT   (sizeof(ticket_t) * 8)

typedef union ticket_lock {
	ticketpair_t head_tail;
	struct raw_tickets {
		ticket_t head, tail;
	} tickets;
} ticket_lock_t;

/* the ticket spin rwlock */
typedef union ticket_rwlock
{
	ticketquad_t full;
	ticketpair_t write_read;
	struct raw_rwtickets {
		ticket_t write;
		ticket_t read;
		ticket_t user;
	} tickets;
} ticket_rwlock_t;

#define RWTICKET_SHIFT (sizeof(ticketpair_t) * 8)


/****************************************************************
 *		 XCHG spin lock implementation
 *  Two versions of lockfunc are provided, the first is based on
 *  test-and-set, which is efficient but suffers from contention,
 *  while the other implementation uses test-test-and-set, which
 *  performs better than the former under heavy contention.
 */
#define spin_init_xchg(ptr) do { *(int8_t *)(ptr) = 0; } while (0)

/* test-and-set lock */
static __always_inline void spin_lock_xchg(int8_t *lock)
{
	while (atomic_fetchstore8(lock, 1))
		atomic_cpu_relax();
}

/* test-test-and-test lock which is supposed to be better than
 * test-and-set lock */
static __always_inline void spin_lock_xchg2(int8_t *lock)
{
	for (;;) {
		while (*lock)
			atomic_cpu_relax();
		if (!atomic_fetchstore8(lock, 1))
			return;
	}
}

/* try locking both test-and-set and test-test-and-set locks */
static __always_inline int8_t spin_trylock_xchg(int8_t *lock)
{
	return atomic_fetchstore8(lock, 1);
}

/* unlock both test-and-set and test-test-and-set locks */
static __always_inline void spin_unlock_xchg(int8_t *lock)
{
	*lock = 0;
}

/******************************************************************
 *		   MCS spin lock implementation
 * The MCS lock is better than XCHG when contention is a serious
 * problem, and it is slighly faster than the k42 lock, which is
 * another MCS variant. The difference between them is that MCS
 * requires each thread holding a mcs_entry_t object while k42 doesn't
 * have this requirement.
 *
 * Two versions of unlock function are provided, the first uses CAS
 * and the second uses XCHG instead. If used with the latter one, the
 * order of unlock may not be FIFO.
 */
#define spin_init_mcs(ptr) do { *(mcs_lock_t *)(ptr) = NULL; } while (0)

static __always_inline void spin_lock_mcs(mcs_lock_t *lock, mcs_entry_t *me)
{
	mcs_entry_t *tail;

	me->next = NULL;
	tail = (mcs_entry_t *) atomic_fetchstore64(lock, (int64_t)me);
	if (tail == NULL)
		return;

	me->lock = 1;
	/* make sure we have the lock set before linking to the
	   predecessor, otherwise deadlock may occur when the
	   predecessor unlocks earlier. */
	atomic_barrier();
	tail->next = me;
	atomic_barrier();

	while (me->lock)
		atomic_cpu_relax();
}

static __always_inline int spin_trylock_mcs(mcs_lock_t *lock, mcs_entry_t *me)
{
	me->next = NULL;
	return atomic_cmpswp64(lock, 0, (int64_t)me);
}

/* mcs unlock with CAS, which maintains FIFO order */
static __always_inline void spin_unlock_mcs_cas(mcs_lock_t *lock, mcs_entry_t *me)
{
	if (!me->next) {
		if (atomic_cmpswp64(lock, (int64_t)me, 0) == (int64_t)me)
			return;
		/* wait for the successor to appear*/
		while (!me->next)
			atomic_cpu_relax();
	}
	/* unlock the next */
	me->next->lock = 0;
}

/* mcs unlock without CAS, which has no guarantee for FIFO order */
static __always_inline void spin_unlock_mcs_xchg(mcs_lock_t *lock, mcs_entry_t *me)
{
	mcs_entry_t *tail, *succ;

	if (!me->next) {
		tail = (mcs_entry_t *) atomic_fetchstore64(lock, 0);
		if (tail == me)
			return;
		/* new acquiers may have replaced the tail, restore
		 * the tail back */
		succ = (mcs_entry_t *) atomic_fetchstore64(lock, (int64_t)tail);
		/* wait for the successor to appear */
		while (!me->next)
			atomic_cpu_relax();
		/* succ will not stop until it has a next pointer,
		   because tail is not succ now */
		if (succ)
			succ->next = me->next;
		else
			me->next->lock = 0;
	} else
		me->next->lock = 0;
}

/******************************************************************
 *		   K42 spin lock implementation
 * This is the IBM's K42 spin lock and it is patented, which means
 * you can't use it unless you pay loyality to IBM. We add it here
 * for testing and comparing purpose.
 *
 * The lock is essentially the same as the above MCS implementation,
 * except that K42 embeds the mcs_entry_t into the thread stack. Test
 * shows that k42 is slightly slower than MCS.
 */
#define spin_init_k42(ptr) do { (ptr)->next = (ptr)->tail = NULL; } while (0)

static __always_inline void spin_lock_k42(k42_lock_t *lock)
{
	k42_lock_t  me;
	k42_lock_t *tail, *succ;

	me.next = NULL;

	tail = (k42_lock_t *) atomic_fetchstore64(&lock->tail, (int64_t)&me);
	if (tail) {
		me.tail = (k42_lock_t *) 1;
		atomic_barrier();
		tail->next = &me;
		atomic_barrier();

		while (me.tail)
			atomic_cpu_relax();
	}

	succ = me.next;

	if (!succ) {
		lock->next = NULL;
		if (atomic_cmpswp64(&lock->tail, (int64_t)&me, (int64_t)&lock->next)
		    != (int64_t)&me) {
			while (!me.next)
				atomic_cpu_relax();
			lock->next = me.next;
		}
	} else
		lock->next = succ;

	atomic_barrier();
}

static __always_inline void spin_unlock_k42(k42_lock_t *lock)
{
	k42_lock_t *succ = ACCESS_ONCE(lock->next);

	if (!succ) {
		if (atomic_cmpswp64(&lock->tail, (int64_t)&lock->next, 0)
		    == (int64_t)&lock->next)
			return;

		while (!lock->next)
			atomic_cpu_relax();

		succ = ACCESS_ONCE(lock->next);
	}

	succ->tail = NULL;
}

static __always_inline int spin_trylock_k42(k42_lock_t *lock)
{
	return atomic_cmpswp64(&lock->tail, 0, (int64_t)&lock->next);
}

/********************************************************************
 *		   Ticket spin lock implementation
 * This is the Linux kernel's spin lock implementation, which is also
 * the most competing one. It has outstanding performnace, though
 * slightly slower than XCHG spin lock under low contention, and is
 * quite scalable.
 */
#define spin_init_ticket(ptr) do { (ptr)->head_tail = 0; } while (0)

static __always_inline void spin_lock_ticket(ticket_lock_t *lock)
{
	register union ticket_lock inc;

	inc.head_tail = 1 << TICKET_SHIFT;

#ifdef LARGE_CPUSET
	inc.head_tail = atomic_fetchadd32(&lock->tickets, inc.head_tail);
#else
	inc.head_tail = atomic_fetchadd16(&lock->tickets, inc.head_tail);
#endif

	for (;;) {
		if (inc.tickets.head == inc.tickets.tail)
			break;
		atomic_cpu_relax();
		inc.tickets.head = ACCESS_ONCE(lock->tickets.head);
	}
}

static __always_inline int spin_trylock_ticket(ticket_lock_t *lock)
{
	ticket_lock_t old, new;

	old.head_tail = ACCESS_ONCE(lock->head_tail);
	if (old.tickets.head != old.tickets.tail)
		return -1;

	new.head_tail = old.head_tail + (1 << TICKET_SHIFT);

	/* cmpxchg is a full barrier, so nothing can move before it */
#ifdef LARGE_CPUSET
	return atomic_cmpswp32(&lock->head_tail, old.head_tail, new.head_tail) != (int32_t)old.head_tail;
#else
	return atomic_cmpswp16(&lock->head_tail, old.head_tail, new.head_tail) != (int16_t)old.head_tail;
#endif
}

static __always_inline void spin_unlock_ticket(ticket_lock_t *lock)
{
#ifdef LARGE_CPUSET
	atomic_inc16(&lock->tickets.head);
#else
	atomic_inc8(&lock->tickets.head);
#endif
}

/********************************************************************
 *		  RW Ticket spin lock implementation
 * The ticket rwlock takes advantage of the ticket lock because only
 * one writer can write at any moment. Thus the tickets can be used to
 * queue those writers and we also need an additional reader counter.
 */

#define spin_init_rwticket(ptr)			\
	do { (ptr)->full = 0; } while (0)

static __always_inline void spin_wrlock_ticket(ticket_rwlock_t *lock)
{
	register union ticket_rwlock inc;

	inc.full = 1ul << RWTICKET_SHIFT;

#ifdef LARGE_CPUSET
	inc.full = atomic_fetchadd64(&lock->full, inc.full);
#else
	inc.full = atomic_fetchadd32(&lock->full, inc.full);
#endif

	for (;;) {
		if (inc.tickets.write == inc.tickets.user)
			break;
		atomic_cpu_relax();
		inc.tickets.write = ACCESS_ONCE(lock->tickets.write);
	}
}

static __always_inline void spin_wrunlock_ticket(ticket_rwlock_t *lock)
{
	register union ticket_rwlock old;

	old.full = ACCESS_ONCE(lock->full);

	++old.tickets.write;
	++old.tickets.read;

	lock->write_read = old.write_read;
}

static __always_inline int spin_trywrlock_ticket(ticket_rwlock_t *lock)
{
	ticket_rwlock_t old, new;

	old.full = ACCESS_ONCE(lock->full);
	if (old.tickets.write != old.tickets.user)
		return -1;

	new.full = old.full + (1ul << RWTICKET_SHIFT);

	/* cmpxchg is a full barrier, so nothing can move before it */
#ifdef LARGE_CPUSET
	return atomic_cmpswp64(&lock->full, old.full, new.full) != (int64_t)old.full;
#else
	return atomic_cmpswp32(&lock->full, old.full, new.full) != (int32_t)old.full;
#endif
}

static __always_inline void spin_rdlock_ticket(ticket_rwlock_t *lock)
{
	register union ticket_rwlock inc;

	inc.full = 1ul << RWTICKET_SHIFT;

#ifdef LARGE_CPUSET
	inc.full = atomic_fetchadd64(&lock->full, inc.full);
#else
	inc.full = atomic_fetchadd32(&lock->full, inc.full);
#endif

	for (;;) {
		if (inc.tickets.read == inc.tickets.user)
			break;
		atomic_cpu_relax();
		inc.tickets.read = ACCESS_ONCE(lock->tickets.read);
	}

	++lock->tickets.read;
}

static __always_inline void spin_rdunlock_ticket(ticket_rwlock_t *lock)
{
#ifdef LARGE_CPUSET
	atomic_inc16(&lock->tickets.write);
#else
	atomic_inc8(&lock->tickets.write);
#endif
}

static __always_inline int spin_tryrdlock_ticket(ticket_rwlock_t *lock)
{
	ticket_rwlock_t old, new;

	old.full = ACCESS_ONCE(lock->full);
	if (old.tickets.read != old.tickets.user)
		return -1;

	/* for consistence, we can't use ++new.user here */
	new.full = old.full + (1ul << RWTICKET_SHIFT);

	++new.tickets.read;

	/* cmpxchg is a full barrier, so nothing can move before it */
#ifdef LARGE_CPUSET
	return atomic_cmpswp64(&lock->full, old.full, new.full) != (int64_t)old.full;
#else
	return atomic_cmpswp32(&lock->full, old.full, new.full) != (int32_t)old.full;
#endif
}

#ifdef __cplusplus
#define _new_ new
#endif

#endif
