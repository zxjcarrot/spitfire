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

/* Although chainhash is slower than alignhash as a whole, it preserves
 * several properties alignhash lacks. Notably, it has an inherented
 * support for concurrent access. A fine-grained locking strategy is
 * readily available for chainhash.
 */


#ifndef _ULIB_HASH_CHAIN_H
#define _ULIB_HASH_CHAIN_H

#include <ulib/util_class.h>
#include <ulib/hash_chain_prot.h>

namespace ulib {

template<class _Key, class _Val, class _Except = ulib_except>
class chain_hash_map
{
public:
	DEFINE_CHAINHASH(inclass, _Key, _Val, 1, chainhash_hashfn, chainhash_equalfn, chainhash_cmpfn);

	typedef _Key	    key_type;
	typedef _Val	    value_type;
	typedef size_t	    size_type;
	typedef _Val *	    pointer;
	typedef const _Val* const_pointer;
	typedef _Val &	    reference;
	typedef const _Val& const_reference;
	typedef chainhash_t(inclass)   * hashing;
	typedef chainhash_itr_t(inclass) hashing_iterator;

	struct iterator
	{
		typedef size_t size_type;
		typedef _Val&  reference;
		typedef _Val*  pointer;
		typedef chainhash_itr_t(inclass) hashing_iterator;

		chainhash_itr_t(inclass) _cur;

		iterator(hashing_iterator itr)
			: _cur(itr) { }

		iterator() { }

		_Key &
		key() const
		{ return chainhash_key(_cur); }

		reference
		value() const
		{ return chainhash_value(inclass, _cur); }

		reference
		operator*() const
		{ return value(); }

		pointer
		operator->() const
		{ return &(operator*()); }

		iterator&
		operator++()
		{
			if (chainhash_advance(inclass, &_cur))
				_cur.entry = NULL;
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
		{ return _cur.entry == other._cur.entry; }

		bool
		operator!=(const iterator &other) const
		{ return _cur.entry != other._cur.entry; }
	};

	struct const_iterator
	{
		typedef size_t size_type;
		typedef const _Val& reference;
		typedef const _Val* pointer;
		typedef const chainhash_itr_t(inclass) hashing_iterator;

		chainhash_itr_t(inclass) _cur;

		const_iterator(hashing_iterator itr)
			: _cur(itr) { }

		const_iterator() { }

		const_iterator(const iterator &it)
			: _cur(it._cur) { }

		const _Key &
		key() const
		{ return chainhash_key(_cur); }

		reference
		value() const
		{ return chainhash_value(inclass, _cur); }

		reference
		operator*() const
		{ return value(); }

		pointer
		operator->() const
		{ return &(operator*()); }

		const_iterator &
		operator++()
		{
			if (chainhash_advance(inclass, &_cur))
				_cur.entry = NULL;
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
		{ return _cur.entry == other._cur.entry; }

		bool
		operator!=(const const_iterator &other) const
		{ return _cur.entry != other._cur.entry; }
	};

	chain_hash_map(size_t min)
	{
		_hashing = chainhash_init(inclass, min);
		if (_hashing == 0)
			throw _Except();
	}

	chain_hash_map(const chain_hash_map &other)
	{
		_hashing = chainhash_init(inclass, other.bucket_count());
		if (_hashing == 0)
			throw _Except();
		for (const_iterator it = other.begin(); it != other.end(); ++it)
			insert(it.key(), it.value());
	}

	chain_hash_map &
	operator= (const chain_hash_map &other)
	{
		if (&other != this) {
			clear();
			for (const_iterator it = other.begin(); it != other.end(); ++it)
				insert(it.key(), it.value());
		}
		return *this;
	}

	virtual
	~chain_hash_map()
	{ chainhash_destroy(inclass, _hashing); }

	iterator
	begin()
	{ return iterator(chainhash_begin(inclass, _hashing)); }

	iterator
	end()
	{
		chainhash_itr_t(inclass) itr;
		itr.entry = NULL;
		return iterator(itr);
	}

	const_iterator
	begin() const
	{ return const_iterator(chainhash_begin(inclass, _hashing)); }

	const_iterator
	end() const
	{
		chainhash_itr_t(inclass) itr;
		itr.entry = NULL;
		return const_iterator(itr);
	}

	bool
	contain(const _Key &key) const
	{ return chainhash_get(inclass, _hashing, key).entry != NULL; }

	iterator
	insert(const _Key &key, const _Val &val)
	{
		hashing_iterator itr = chainhash_set(inclass, _hashing, key);
		if (itr.entry == NULL)
			throw _Except();
		chainhash_value(inclass, itr) = val;
		return iterator(itr);
	}

	iterator
	find_or_insert(const _Key &key, const _Val &val)
	{
		int exist;
		hashing_iterator itr =
			chainhash_set_uniq(inclass, _hashing, key, &exist);
		if (itr.entry == NULL)
			throw _Except();
		if (!exist)
			chainhash_value(inclass, itr) = val;
		return iterator(itr);
	}

	reference
	operator[](const _Key &key)
	{ return *find_or_insert(key, _Val()); }

	iterator
	find(const _Key &key)
	{ return iterator(chainhash_get(inclass, _hashing, key)); }

	const_iterator
	find(const _Key &key) const
	{ return const_iterator(chainhash_get(inclass, _hashing, key)); }

	void
	erase(const _Key &key)
	{ chainhash_del(inclass, chainhash_get(inclass, _hashing, key)); }

	void
	erase(const iterator &it)
	{ chainhash_del(inclass, it._cur); }

	void
	clear()
	{ chainhash_clear(inclass, _hashing); }

	void
	snap()
	{ chainhash_snap(inclass, _hashing); }

	void
	sort()
	{ chainhash_sort(inclass, _hashing); }

	size_t
	size() const
	{
		size_t n = 0;
		for (const_iterator it = begin(); it != end(); ++it)
			++n;
		return n;
	}

	size_t
	bucket_count() const
	{ return chainhash_nbucket(_hashing); }

private:
	hashing _hashing;
};

}  // namespace ulib

#endif	/* _ULIB_HASH_CHAIN_H */
