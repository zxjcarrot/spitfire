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

#ifndef _ULIB_INTERM_H
#define _ULIB_INTERM_H

#include <pthread.h>
#include <assert.h>
#include <string.h>
#include <ulib/os_regionlock.h>
#include <ulib/hash_chain.h>

namespace ulib
{

namespace mapreduce
{

// store based on chain hash table
template<class _Key, class _Val>
class chain_hash_store :
		public chain_hash_map<_Key, _Val>,
		public region_mutex<xchg_lock_t>
{
public:
	typedef typename chain_hash_map<_Key,_Val>::size_type       size_type;
	typedef typename chain_hash_map<_Key,_Val>::pointer         pointer;
	typedef typename chain_hash_map<_Key,_Val>::const_pointer   const_pointer;
	typedef typename chain_hash_map<_Key,_Val>::reference       reference;
	typedef typename chain_hash_map<_Key,_Val>::const_reference const_reference;
	typedef typename chain_hash_map<_Key,_Val>::iterator        iterator;
	typedef typename chain_hash_map<_Key,_Val>::const_iterator  const_iterator;

	chain_hash_store(size_t min_bucket, size_t min_lock)
		: chain_hash_map<_Key,_Val>(min_bucket),
		  region_mutex<xchg_lock_t>(min_lock)
	{ assert(min_bucket >= min_lock); }

	chain_hash_store(size_t min_bucket)
		: chain_hash_map<_Key,_Val>(min_bucket), region_mutex<xchg_lock_t>(min_bucket) { }

	// DO NOT copy the elements
	chain_hash_store(const chain_hash_store &other)
		: chain_hash_map<_Key,_Val>(other),
		  region_mutex<xchg_lock_t>(((const region_mutex<xchg_lock_t> *)&other)->bucket_count()) { }

	void
	lock(size_t h)
	{ region_mutex<xchg_lock_t>::lock(h); }

	void
	unlock(size_t h)
	{ region_mutex<xchg_lock_t>::unlock(h); }

	// again DO NOT copy the elements
	chain_hash_store &
	operator= (const chain_hash_store &other)
	{
		*(chain_hash_map<_Key,_Val> *)this =
			*(const chain_hash_map<_Key,_Val> *)&other;
		*(region_mutex<xchg_lock_t> *)this = *(const region_mutex<xchg_lock_t> *)&other;
		return *this;
	}

	size_type
	bucket_count() const
	{ return ((chain_hash_map<_Key,_Val> *)this)->bucket_count(); }
};

}  // namespace mapreduce

}  // namespace ulib

#endif  /* _ULIB_INTERM_H */
