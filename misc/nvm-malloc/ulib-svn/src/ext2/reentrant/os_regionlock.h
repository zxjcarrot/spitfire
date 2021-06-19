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

#ifndef _ULIB_OS_REGIONLOCK_H
#define _ULIB_OS_REGIONLOCK_H

#include <assert.h>
#include <stdint.h>
#include <ulib/math_bit.h>
#include "os_typelock.h"

namespace ulib {

template<typename _Mutex>
class region_mutex {
public:
	typedef _Mutex mutex_type;

	region_mutex(size_t min)
	{
		assert(min);
		assert(sizeof(min) == 4 || sizeof(min) == 8);
		if (sizeof(min) == 8)
			ROUND_UP64(min);
		else
			ROUND_UP32(min);
		_mask = min - 1;
		_mutexes = new _Mutex [min];
		for (size_t i = 0; i <= _mask; ++i)
			typemutex_init(&_mutexes[i]);
	}

	region_mutex(const region_mutex &other)
	{
		_mask  = other.bucket_count() - 1;
		_mutexes = new _Mutex [_mask + 1];
		for (size_t i = 0; i <= _mask; ++i)
			typemutex_init(&_mutexes[i]);
	}

	virtual
	~region_mutex()
	{
		for (size_t i = 0; i <= _mask; ++i)
			typemutex_destroy(_mutexes + i);
		delete [] _mutexes;
	}

	region_mutex &
	operator= (const region_mutex &other)
	{
		if (bucket_count() != other.bucket_count()) {
			for (size_t i = 0; i <= _mask; ++i)
				typemutex_destroy(_mutexes + i);
			delete [] _mutexes;
			_mask  = other.bucket_count() - 1;
			_mutexes = new _Mutex [_mask + 1];
			for (size_t i = 0; i <= _mask; ++i)
				typemutex_init(&_mutexes[i]);
		}
		return *this;
	}

	virtual void
	lock(size_t h) const
	{ typemutex_lock(_mutexes + (h & _mask)); }

	// returns 0 if the mutex was acquired, nonzero otherwise
	virtual int
	trylock(size_t h) const
	{ return typemutex_trylock(_mutexes + (h & _mask)); }

	virtual void
	unlock(size_t h) const
	{ typemutex_unlock(_mutexes + (h & _mask)); }

	virtual size_t
	bucket_count() const
	{ return _mask + 1; }

protected:
	size_t	 _mask;
	_Mutex * _mutexes;
};

template<typename _RWLock>
struct region_rwlock : public region_mutex<_RWLock> {
	typedef _RWLock rwlock_type;

	region_rwlock(size_t min) : region_mutex<_RWLock>(min) { }

	region_rwlock(const region_rwlock &other)
		: region_mutex<_RWLock>(other) { }

	~region_rwlock() { }

	region_rwlock &
	operator= (const region_rwlock &other)
	{ *(region_mutex<_RWLock> *)this = other;  }

	// returns 0 if the mutex was acquired, nonzero otherwise
	int
	tryrdlock(size_t h) const
	{ return typerwlock_tryrdlock(region_mutex<_RWLock>::_mutexes +
				      (h & region_mutex<_RWLock>::_mask)); }

	// returns 0 if the mutex was acquired, nonzero otherwise
	int
	trywrlock(size_t h) const
	{ return typerwlock_trywrlock(region_mutex<_RWLock>::_mutexes +
				      (h & region_mutex<_RWLock>::_mask)); }

	void
	rdlock(size_t h) const
	{ typerwlock_rdlock(region_mutex<_RWLock>::_mutexes +
			    (h & region_mutex<_RWLock>::_mask)); }

	void
	wrlock(size_t h) const
	{ typerwlock_wrlock(region_mutex<_RWLock>::_mutexes +
			    (h & region_mutex<_RWLock>::_mask)); }

	void
	rdunlock(size_t h) const
	{ typerwlock_rdunlock(region_mutex<_RWLock>::_mutexes +
			      (h & region_mutex<_RWLock>::_mask)); }

	void
	wrunlock(size_t h) const
	{ typerwlock_wrunlock(region_mutex<_RWLock>::_mutexes +
			      (h & region_mutex<_RWLock>::_mask)); }

	// the following functions serve as overriders
	void
	lock(size_t h) const
	{ wrlock(h); }

	void
	unlock(size_t h) const
	{ wrunlock(h); }

	// returns 0 if the mutex was acquired, nonzero otherwise
	int
	trylock(size_t h) const
	{ return trywrlock(h); }
};

}  // namespace ulib

#endif	/* _ULIB_OS_REGIONLOCK_H */
