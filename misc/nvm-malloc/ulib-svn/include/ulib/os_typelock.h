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

#ifndef _ULIB_OS_TYPELOCK_H
#define _ULIB_OS_TYPELOCK_H

#include <pthread.h>
#include <ulib/os_spinlock.h>

namespace ulib {

//---------------------------------------------------------------------
//		     Various Mutex Initializers
//=====================================================================

template<typename _Mutex>
static __always_inline void
typemutex_init(_Mutex *) { }

template<>
__always_inline void
typemutex_init(pthread_mutex_t *lock)
{
	pthread_mutex_init(lock, NULL);
}

template<>
__always_inline void
typemutex_init(pthread_spinlock_t *lock)
{
	pthread_spin_init(lock, 0);
}

template<>
__always_inline void
typemutex_init(xchg_lock_t *lock)
{
	spin_init_xchg(lock);
}

template<>
__always_inline void
typemutex_init(mcs_lock_t *lock)
{
	spin_init_mcs(lock);
}

template<>
__always_inline void
typemutex_init(k42_lock_t *lock)
{
	spin_init_k42(lock);
}

template<>
__always_inline void
typemutex_init(ticket_lock_t *lock)
{
	spin_init_ticket(lock);
}

// rwlocks can also be used as mutexes
template<>
__always_inline void
typemutex_init(pthread_rwlock_t *lock)
{
	pthread_rwlock_init(lock, NULL);
}

template<>
__always_inline void
typemutex_init(ticket_rwlock_t *lock)
{
	spin_init_rwticket(lock);
}

//---------------------------------------------------------------------
//		       Various Mutex Destructors
//=====================================================================

template<typename _Mutex>
static __always_inline void
typemutex_destroy(_Mutex *) { }

template<>
__always_inline void
typemutex_destroy(pthread_mutex_t *lock)
{
	pthread_mutex_destroy(lock);
}

template<>
__always_inline void
typemutex_destroy(pthread_spinlock_t *lock)
{
	pthread_spin_destroy(lock);
}

template<>
__always_inline void
typemutex_destroy(xchg_lock_t *) { }

template<>
__always_inline void
typemutex_destroy(mcs_lock_t *) { }

template<>
__always_inline void
typemutex_destroy(k42_lock_t *) { }

template<>
__always_inline void
typemutex_destroy(ticket_lock_t *) { }

// likewise, it is OK to use rwlocks as mutexes
template<>
__always_inline void
typemutex_destroy(pthread_rwlock_t *lock)
{
	pthread_rwlock_destroy(lock);
}

template<>
__always_inline void
typemutex_destroy(ticket_rwlock_t *) { }

//---------------------------------------------------------------------
//		   Various Mutex Locking Functions
// Note that we do not implement a wrapper for MCS lock because extra
// argument is required.
//=====================================================================

template<typename _Mutex>
static __always_inline void
typemutex_lock(_Mutex *) { }

template<>
__always_inline void
typemutex_lock(pthread_mutex_t *lock)
{
	pthread_mutex_lock(lock);
}

template<>
__always_inline void
typemutex_lock(pthread_spinlock_t *lock)
{
	pthread_spin_lock(lock);
}

template<>
__always_inline void
typemutex_lock(xchg_lock_t *lock)
{
	// Note that we choose to use the test-and-set method, though
	// it raises contention problems. If you are looking for
	// contention free locks, try MCS or ticket locks.
	spin_lock_xchg(lock);
}

template<>
__always_inline void
typemutex_lock(k42_lock_t *lock)
{
	spin_lock_k42(lock);
}

template<>
__always_inline void
typemutex_lock(ticket_lock_t *lock)
{
	spin_lock_ticket(lock);
}

//---------------------------------------------------------------------
//		  Various Mutex Unlocking Functions
// Note that we do not implement a wrapper for MCS lock because extra
// argument is required.
//=====================================================================

template<typename _Mutex>
static __always_inline void
typemutex_unlock(_Mutex *) { }

template<>
__always_inline void
typemutex_unlock(pthread_mutex_t *lock)
{
	pthread_mutex_unlock(lock);
}

template<>
__always_inline void
typemutex_unlock(pthread_spinlock_t *lock)
{
	pthread_spin_unlock(lock);
}

template<>
__always_inline void
typemutex_unlock(xchg_lock_t *lock)
{
	spin_unlock_xchg(lock);
}

template<>
__always_inline void
typemutex_unlock(k42_lock_t *lock)
{
	spin_unlock_k42(lock);
}

template<>
__always_inline void
typemutex_unlock(ticket_lock_t *lock)
{
	spin_unlock_ticket(lock);
}

//---------------------------------------------------------------------
//		     Various Mutex TryLock Functions
//=====================================================================

template<typename _Mutex>
static __always_inline int
typemutex_trylock(_Mutex *)
{
	return -1;
}

template<>
__always_inline int
typemutex_trylock(pthread_mutex_t *lock)
{
	return pthread_mutex_trylock(lock);
}

template<>
__always_inline int
typemutex_trylock(pthread_spinlock_t *lock)
{
	return pthread_spin_trylock(lock);
}

template<>
__always_inline int
typemutex_trylock(xchg_lock_t *lock)
{
	return spin_trylock_xchg(lock);
}

template<>
__always_inline int
typemutex_trylock(k42_lock_t *lock)
{
	return spin_trylock_k42(lock);
}

template<>
__always_inline int
typemutex_trylock(ticket_lock_t *lock)
{
	return spin_trylock_ticket(lock);
}

//---------------------------------------------------------------------
//		     Various RWLock Initializers
//=====================================================================

template<typename _RWLock>
static __always_inline void
typerwlock_init(_RWLock *) { }

template<>
__always_inline void
typerwlock_init(pthread_rwlock_t *lock)
{
	pthread_rwlock_init(lock, NULL);
}

template<>
__always_inline void
typerwlock_init(ticket_rwlock_t *lock)
{
	spin_init_rwticket(lock);
}

//---------------------------------------------------------------------
//		     Various RWLock Destructors
//=====================================================================

template<typename _RWLock>
static __always_inline void
typerwlock_destroy(_RWLock *) { }

template<>
__always_inline void
typerwlock_destroy(pthread_rwlock_t *lock)
{
	pthread_rwlock_destroy(lock);
}

template<>
__always_inline void
typerwlock_destroy(ticket_rwlock_t *) { }

//---------------------------------------------------------------------
//		   Various RWLock RDLocking Functions
//=====================================================================

template<typename _RWLock>
static __always_inline void
typerwlock_rdlock(_RWLock *) { }

template<>
__always_inline void
typerwlock_rdlock(pthread_rwlock_t *lock)
{
	pthread_rwlock_rdlock(lock);
}

template<>
__always_inline void
typerwlock_rdlock(ticket_rwlock_t *lock)
{
	spin_rdlock_ticket(lock);
}

//---------------------------------------------------------------------
//		 Various RWLock Tryrdlocking Functions
//=====================================================================

template<typename _RWLock>
static __always_inline int
typerwlock_tryrdlock(_RWLock *)
{
	return -1;
}

template<>
__always_inline int
typerwlock_tryrdlock(pthread_rwlock_t *lock)
{
	return pthread_rwlock_tryrdlock(lock);
}

template<>
__always_inline int
typerwlock_tryrdlock(ticket_rwlock_t *lock)
{
	return spin_tryrdlock_ticket(lock);
}

//---------------------------------------------------------------------
//		  Various RWLock RDUnlocking Functions
//=====================================================================

template<typename _RWLock>
static __always_inline void
typerwlock_rdunlock(_RWLock *) { }

template<>
__always_inline void
typerwlock_rdunlock(pthread_rwlock_t *lock)
{
	pthread_rwlock_unlock(lock);
}

template<>
__always_inline void
typerwlock_rdunlock(ticket_rwlock_t *lock)
{
	spin_rdunlock_ticket(lock);
}

//---------------------------------------------------------------------
//		  Various RWLock WRLocking Functions
//=====================================================================

template<typename _RWLock>
static __always_inline void
typerwlock_wrlock(_RWLock *) { }

template<>
__always_inline void
typerwlock_wrlock(pthread_rwlock_t *lock)
{
	pthread_rwlock_wrlock(lock);
}

template<>
__always_inline void
typerwlock_wrlock(ticket_rwlock_t *lock)
{
	spin_wrlock_ticket(lock);
}

//---------------------------------------------------------------------
//		 Various RWLock Trywrlocking Functions
//=====================================================================

template<typename _RWLock>
static __always_inline int
typerwlock_trywrlock(_RWLock *)
{
	return -1;
}

template<>
__always_inline int
typerwlock_trywrlock(pthread_rwlock_t *lock)
{
	return pthread_rwlock_trywrlock(lock);
}

template<>
__always_inline int
typerwlock_trywrlock(ticket_rwlock_t *lock)
{
	return spin_trywrlock_ticket(lock);
}

//---------------------------------------------------------------------
//		  Various RWLock WRUnlocking Functions
//=====================================================================

template<typename _RWLock>
static __always_inline void
typerwlock_wrunlock(_RWLock *) { }

template<>
__always_inline void
typerwlock_wrunlock(pthread_rwlock_t *lock)
{
	pthread_rwlock_unlock(lock);
}

template<>
__always_inline void
typerwlock_wrunlock(ticket_rwlock_t *lock)
{
	spin_wrunlock_ticket(lock);
}

}  // namespace ulib

#endif	/* _ULIB_OS_TYPELOCK_H */
