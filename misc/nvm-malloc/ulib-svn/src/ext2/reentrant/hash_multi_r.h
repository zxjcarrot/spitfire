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

/* Although multihash is slower than openhash as a whole, it preserves
 * several properties openhash lacks. Notably, it has an inherented
 * support for concurrent access. A fine-grained locking strategy is
 * readily available for multihash.
 *
 * Please note that only the combine() method is thread-safe, this is
 * because writing to the open hashmap may be accompanied with
 * resizing of the map, rendering any references or iterators to
 * the previous map invalid.
 */


#ifndef _ULIB_HASH_MULTI_R_H
#define _ULIB_HASH_MULTI_R_H

#include <assert.h>
#include <ulib/util_class.h>
#include <ulib/hash_open.h>
#include "os_regionlock.h"

namespace ulib {

template<
	typename _Key, typename _Val,
	typename _Except      = ulib_except,
	typename _Combiner    = do_nothing_combiner<_Val>,
	typename _RegionMutex = region_mutex<ticket_lock_t> >
class multi_hash_map : public _RegionMutex
{
public:
	typedef open_hash_map<_Key, _Val, _Except>	hash_map_type;
	typedef typename hash_map_type::key_type	key_type;
	typedef typename hash_map_type::value_type	value_type;
	typedef typename hash_map_type::size_type	size_type;
	typedef typename hash_map_type::pointer		pointer;
	typedef typename hash_map_type::const_pointer	const_pointer;
	typedef typename hash_map_type::reference	reference;
	typedef typename hash_map_type::const_reference const_reference;

	multi_hash_map(size_t mhash)
		: _RegionMutex(mhash)
	{
		assert(mhash > 0);
		_mask = _RegionMutex::bucket_count() - 1;
		_ht   = new hash_map_type [_mask + 1];
	}

	virtual
	~multi_hash_map()
	{ delete [] _ht; }

	struct iterator
	{
		typedef typename multi_hash_map<_Key, _Val, _Except, _Combiner>::key_type   key_type;
		typedef typename multi_hash_map<_Key, _Val, _Except, _Combiner>::value_type value_type;
		typedef typename multi_hash_map<_Key, _Val, _Except, _Combiner>::size_type  size_type;
		typedef typename multi_hash_map<_Key, _Val, _Except, _Combiner>::reference  reference;
		typedef typename multi_hash_map<_Key, _Val, _Except, _Combiner>::pointer    pointer;

		iterator(size_t id, size_t nht, hash_map_type *ht,
			 const typename hash_map_type::iterator &itr)
			: _hid(id), _nht(nht), _ht(ht), _cur(itr) { }

		iterator() { }

		key_type &
		key() const
		{ return _cur.key(); }

		reference
		value() const
		{ return _cur.value(); }

		reference
		operator*() const
		{ return value(); }

		pointer
		operator->() const
		{ return &(operator*()); }

		iterator&
		operator++()
		{
			if (_hid < _nht) {
				++_cur;
				if (_cur == _ht[_hid].end()) {
					while (++_hid < _nht && _ht[_hid].size() == 0)
						;
					if (_hid < _nht)
						_cur = _ht[_hid].begin();
					else
						_cur = _ht[_nht - 1].end();
				}
			}
			return *this;
		}

		iterator
		operator++(int)
		{
			iterator old = *this;
			++*this;
			return old;
		}

		bool
		operator==(const iterator &other) const
		{ return _hid == other._hid && _cur == other._cur; }

		bool
		operator!=(const iterator &other) const
		{ return _hid != other._hid || _cur != other._cur; }

		size_t _hid;
		size_t _nht;
		hash_map_type *_ht;
		typename hash_map_type::iterator _cur;
	};

	struct const_iterator
	{
		typedef const typename multi_hash_map<_Key, _Val, _Except, _Combiner>::key_type	  key_type;
		typedef const typename multi_hash_map<_Key, _Val, _Except, _Combiner>::value_type value_type;
		typedef typename multi_hash_map<_Key, _Val, _Except, _Combiner>::size_type	  size_type;
		typedef typename multi_hash_map<_Key, _Val, _Except, _Combiner>::const_reference  reference;
		typedef typename multi_hash_map<_Key, _Val, _Except, _Combiner>::const_pointer	  pointer;

		const_iterator(size_t id, size_t nht, const hash_map_type *ht,
			       const typename hash_map_type::const_iterator &itr)
			: _hid(id), _nht(nht), _ht(ht), _cur(itr) { }

		const_iterator() { }

		const_iterator(const iterator &it)
			: _hid(it._hid), _nht(it._nht), _ht(it._ht), _cur(it._cur) { }

		key_type &
		key() const
		{ return _cur.key(); }

		reference
		value() const
		{ return _cur.value(); }

		reference
		operator*() const
		{ return value(); }

		pointer
		operator->() const
		{ return &(operator*()); }

		const_iterator &
		operator++()
		{
			if (_hid < _nht) {
				++_cur;
				if (_cur == _ht[_hid].end()) {
					while (++_hid < _nht && _ht[_hid].size() == 0)
						;
					if (_hid < _nht)
						_cur = _ht[_hid].begin();
					else
						_cur = _ht[_nht - 1].end();
				}
			}
			return *this;
		}

		const_iterator
		operator++(int)
		{
			const_iterator old = *this;
			++*this;
			return old;
		}

		bool
		operator==(const const_iterator &other) const
		{ return _hid == other._hid && _cur == other._cur; }

		bool
		operator!=(const const_iterator &other) const
		{ return _hid != other._hid || _cur != other._cur; }

		size_t _hid;
		size_t _nht;
		const hash_map_type *_ht;
		typename hash_map_type::const_iterator _cur;
	};

	iterator
	begin()
	{
		size_t hid = 0;
		while (hid <= _mask && _ht[hid].size() == 0)
			++hid;
		if (hid <= _mask)
			return iterator(hid, _mask + 1, _ht, _ht[hid].begin());
		else
			return end();
	}

	iterator
	end()
	{ return iterator(_mask + 1, _mask + 1, _ht, _ht[_mask].end()); }

	const_iterator
	begin() const
	{
		size_t hid = 0;
		while (hid <= _mask && _ht[hid].size() == 0)
			++hid;
		if (hid <= _mask)
			return const_iterator(hid, _mask + 1, _ht, _ht[hid].begin());
		else
			return end();
	}

	const_iterator
	end() const
	{ return const_iterator(_mask + 1, _mask + 1, _ht, _ht[_mask].end()); }

	iterator
	insert(const _Key &key, const _Val &val, bool replace = false)
	{
		size_t m = (size_t)key & _mask;
		typename hash_map_type::iterator it =
			_ht[m].insert(key, val, replace);
		return it == _ht[m].end()? end(): iterator(m, _mask + 1, _ht, it);
	}

	iterator
	find_or_insert(const _Key &key, const _Val &val)
	{
		size_t m = (size_t)key & _mask;
		typename hash_map_type::iterator it =
			_ht[m].find_or_insert(key, val);
		return it == _ht[m].end()? end(): iterator(m, _mask + 1, _ht, it);
	}

	reference
	operator[](const _Key &key)
	{ return *find_or_insert(key, _Val()); }

	iterator
	find(const _Key &key)
	{
		size_t m = (size_t)key & _mask;
		typename hash_map_type::iterator it = _ht[m].find(key);
		return it == _ht[m].end()? end(): iterator(m, _mask + 1, _ht, it);
	}

	const_iterator
	find(const _Key &key) const
	{
		size_t m = (size_t)key & _mask;
		typename hash_map_type::const_iterator it = _ht[m].find(key);
		return it == _ht[m].end()? end(): const_iterator(m, _mask + 1, _ht, it);
	}

	void
	combine(const _Key &key, const _Val &val)
	{
		size_t h = key;
		size_t m = h & _mask;
		this->lock(h);
		_combiner(_ht[m][key], val);
		this->unlock(h);
	}

	void
	erase(const _Key &key)
	{ _ht[(size_t)key & _mask].erase(key); }

	void
	erase(const iterator &it)
	{ _ht[it._hid].erase(it._cur); }

	void
	clear()
	{
		for (size_t i = 0; i <= _mask; ++i)
			_ht[i].clear();
	}

	size_t
	bucket_count() const
	{ return _mask + 1; }

	size_t
	size() const
	{
		size_t n = 0;
		for (size_t i = 0; i <= _mask; ++i)
			n += _ht[i].size();
		return n;
	}

private:
	multi_hash_map(const multi_hash_map &other) { }

	multi_hash_map &
	operator= (const multi_hash_map &other)
	{ return *this; }

	size_t _mask;
	hash_map_type *_ht;
	_Combiner _combiner;
};

}  // namespace ulib

#endif	/* _ULIB_HASH_MULTI_R_H */
