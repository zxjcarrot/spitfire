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

#ifndef _ULIB_HASH_CHAIN_R_H
#define _ULIB_HASH_CHAIN_R_H

#include <assert.h>
#include <ulib/util_class.h>
#include <ulib/hash_chain.h>
#include "os_regionlock.h"

namespace ulib {

template<
	typename _Key, typename _Val,
	typename _Except       = ulib_except,
	typename _Combiner     = do_nothing_combiner<_Val>,
	typename _RegionRWLock = region_rwlock<pthread_rwlock_t> >
class chain_hash_map_r :
		public chain_hash_map<_Key, _Val, _Except>,
		public _RegionRWLock
{
public:
	typedef _Key key_type;
	typedef _Val value_type;
	typedef typename chain_hash_map<_Key,_Val,_Except>::size_type	     size_type;
	typedef typename chain_hash_map<_Key,_Val,_Except>::pointer	     pointer;
	typedef typename chain_hash_map<_Key,_Val,_Except>::const_pointer    const_pointer;
	typedef typename chain_hash_map<_Key,_Val,_Except>::reference	     reference;
	typedef typename chain_hash_map<_Key,_Val,_Except>::const_reference  const_reference;
	typedef typename chain_hash_map<_Key,_Val,_Except>::hashing	     hashing;
	typedef typename chain_hash_map<_Key,_Val,_Except>::hashing_iterator hashing_iterator;
	typedef typename chain_hash_map<_Key,_Val,_Except>::iterator	     iterator;
	typedef typename chain_hash_map<_Key,_Val,_Except>::const_iterator   const_iterator;

	chain_hash_map_r(size_t min_bucket, size_t min_lock)
		: chain_hash_map<_Key,_Val,_Except>(min_bucket),
		  _RegionRWLock(min_lock)
	{ assert(min_bucket >= min_lock); } // this is important for locking consistency

	chain_hash_map_r(size_t min_bucket)
		: chain_hash_map<_Key,_Val,_Except>(min_bucket),
		  _RegionRWLock(min_bucket)  // the same as min_bucket, maximizing _RegionRWLock
	{ }

	chain_hash_map_r(const chain_hash_map_r &other)
		: chain_hash_map<_Key,_Val,_Except>(other),
		  _RegionRWLock(((const _RegionRWLock *)&other)->bucket_count())
	{ }

	chain_hash_map_r &
	operator= (const chain_hash_map_r &other)
	{
		*(chain_hash_map<_Key,_Val,_Except> *)this =
			*(const chain_hash_map<_Key,_Val,_Except> *)&other;
		*(_RegionRWLock *)this = *(const _RegionRWLock *)&other;
		return *this;
	}

	virtual
	~chain_hash_map_r() { }

	size_type
	bucket_count() const
	{ return ((chain_hash_map<_Key,_Val,_Except> *)this)->bucket_count(); }

	bool
	contain(const _Key &key) const
	{
		size_t h = key;
		this->rdlock(h);
		bool retval = chain_hash_map<_Key,_Val,_Except>::contain(key);
		this->rdunlock(h);
		return retval;
	}

	iterator
	insert(const _Key &key, const _Val &val)
	{
		size_t h = key;
		this->wrlock(h);
		iterator itr = chain_hash_map<_Key,_Val,_Except>::insert(key, val);
		this->wrunlock(h);
		return itr;
	}

	iterator
	find_or_insert(const _Key &key, const _Val &val)
	{
		size_t h = key;
		this->wrlock(h);
		iterator itr = chain_hash_map<_Key,_Val,_Except>::find_or_insert(key, val);
		this->wrunlock(h);
		return itr;
	}

	reference
	operator[](const _Key &key)
	{ return *find_or_insert(key, _Val()); }

	iterator
	find(const _Key &key)
	{
		size_t h = key;
		this->rdlock(h);
		iterator itr = chain_hash_map<_Key,_Val,_Except>::find(key);
		this->rdunlock(h);
		return itr;
	}

	const_iterator
	find(const _Key &key) const
	{
		size_t h = key;
		this->rdlock(h);
		const_iterator itr = chain_hash_map<_Key,_Val,_Except>::find(key);
		this->rdunlock(h);
		return itr;
	}

	void
	combine(const _Key &key, const _Val &val)
	{
		size_t h = key;
		this->wrlock(h);
		_combiner(chain_hash_map<_Key,_Val,_Except>::operator[](key), val);
		this->wrunlock(h);
	}

	void
	erase(const _Key &key)
	{
		size_t h = key;
		this->wrlock(h);
		chain_hash_map<_Key,_Val,_Except>::erase(key);
		this->wrunlock(h);
	}

	void
	erase(const iterator &it)
	{
		size_t h = it.key();
		this->wrlock(h);
		chain_hash_map<_Key,_Val,_Except>::erase(it);
		this->wrunlock(h);
	}

private:
	_Combiner _combiner;
};

}  // namespace ulib

#endif	/* _ULIB_HASH_CHAIN_R_H */
