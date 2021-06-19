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

/*
  This file provides proxy classes for openhash. The origial
  implementation is in C, which is located in openhash_tpl.h. A few
  points should be noted before using these classes:

  (1) Several implicit operators are used for key, namely '==', '<',
  and the unsigned long operator. These operators must be implemented
  on the part of the key class.

  (2) Openhash iterator does not have 'first' and 'second' members,
  key() and value() are respectively responsible for accessing the key
  and value associated with the iterator instead.
*/

#ifndef _ULIB_HASH_OPEN_H
#define _ULIB_HASH_OPEN_H

#include <ulib/util_class.h>
#include <ulib/hash_open_prot.h>

namespace ulib {

template<class _Key, class _Val, class _Except = ulib_except>
class open_hash_map
{
public:
	DEFINE_OPENHASH(inclass, _Key, _Val, 1, openhash_hashfn, openhash_equalfn);

	typedef _Key	    key_type;
	typedef _Val	    value_type;
	typedef oh_iter_t   size_type;
	typedef _Val *	    pointer;
	typedef const _Val* const_pointer;
	typedef _Val &	    reference;
	typedef const _Val& const_reference;
	typedef oh_iter_t   hashing_iterator;
	typedef openhash_t(inclass) * hashing;

	struct iterator
	{
		typedef oh_iter_t size_type;
		typedef _Val& reference;
		typedef _Val* pointer;
		typedef openhash_t(inclass) * hashing;
		typedef oh_iter_t hashing_iterator;

		hashing		 _hashing;
		hashing_iterator _cur;

		iterator(hashing h, hashing_iterator itr)
			: _hashing(h), _cur(itr) { }

		iterator() { }

		_Key &
		key() const
		{ return openhash_key(_hashing, _cur); }

		reference
		value() const
		{ return openhash_value(_hashing, _cur); }

		reference
		operator*() const
		{ return value(); }

		pointer
		operator->() const
		{ return &(operator*()); }

		iterator&
		operator++()
		{
			if (_cur != openhash_end(_hashing))
				++_cur;
			while (_cur != openhash_end(_hashing) && !openhash_exist(_hashing, _cur))
				++_cur;
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
		{ return _cur == other._cur; }

		bool
		operator!=(const iterator &other) const
		{ return _cur != other._cur; }
	};

	struct const_iterator
	{
		typedef oh_iter_t size_type;
		typedef const _Val& reference;
		typedef const _Val* pointer;
		typedef const openhash_t(inclass) * hashing;
		typedef oh_iter_t hashing_iterator;

		hashing		 _hashing;
		hashing_iterator _cur;

		const_iterator(const hashing h, hashing_iterator itr)
			: _hashing(h), _cur(itr) { }

		const_iterator() { }

		const_iterator(const iterator &it)
			: _hashing(it._hashing), _cur(it._cur) { }

		const _Key &
		key() const
		{ return openhash_key(_hashing, _cur); }

		reference
		value() const
		{ return openhash_value(_hashing, _cur); }

		reference
		operator*() const
		{ return value(); }

		pointer
		operator->() const
		{ return &(operator*()); }

		const_iterator&
		operator++()
		{
			if (_cur != openhash_end(_hashing))
				++_cur;
			while (_cur != openhash_end(_hashing) && !openhash_exist(_hashing, _cur))
				++_cur;
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
		{ return _cur == other._cur; }

		bool
		operator!=(const const_iterator &other) const
		{ return _cur != other._cur; }
	};

	open_hash_map()
	{
		_hashing = openhash_init(inclass);
		if (_hashing == 0)
			throw _Except();
	}

	open_hash_map(const open_hash_map &other)
	{
		_hashing = openhash_init(inclass);
		if (_hashing == 0)
			throw _Except();
		for (const_iterator it = other.begin(); it != other.end(); ++it)
			insert(it.key(), it.value());
	}

	open_hash_map &
	operator= (const open_hash_map &other)
	{
		if (&other != this) {
			clear();
			for (const_iterator it = other.begin(); it != other.end(); ++it)
				insert(it.key(), it.value());
		}
		return *this;
	}

	virtual
	~open_hash_map()
	{ openhash_destroy(inclass, _hashing); }

	size_type
	size() const
	{ return openhash_size(_hashing); }

	bool
	empty() const
	{ return size() == 0; }

	iterator
	begin()
	{
		for (size_type itr = openhash_begin(_hashing);
		     itr != openhash_end(_hashing); ++itr)
			if (openhash_exist(_hashing, itr))
				return iterator(_hashing, itr);
		return end();
	}

	iterator
	end()
	{ return iterator(_hashing, openhash_end(_hashing)); }

	const_iterator
	begin() const
	{
		for (size_type itr = openhash_begin(_hashing);
		     itr != openhash_end(_hashing); ++itr)
			if (openhash_exist(_hashing, itr))
				return iterator(_hashing, itr);
		return end();
	}

	const_iterator
	end() const
	{ return iterator(_hashing, openhash_end(_hashing)); }

	size_type
	bucket_count() const
	{ return openhash_nbucket(_hashing); }

	bool
	contain(const _Key &key) const
	{ return openhash_get(inclass, _hashing, key) != openhash_end(_hashing); }

	iterator
	insert(const _Key &key, const _Val &val, bool replace = false)
	{
		int ret = OH_INS_ERR;
		hashing_iterator itr = openhash_set(inclass, _hashing, key, &ret);
		if (itr == openhash_end(_hashing))
			throw _Except();
		if (ret != OH_INS_ERR || replace)
			openhash_value(_hashing, itr) = val;
		return iterator(_hashing, itr);
	}

	iterator
	find_or_insert(const _Key &key, const _Val &val)
	{
		int ret = OH_INS_ERR;
		hashing_iterator itr = openhash_set(inclass, _hashing, key, &ret);
		if (itr == openhash_end(_hashing))
			throw _Except();
		if (ret != OH_INS_ERR)
			openhash_value(_hashing, itr) = val;
		return iterator(_hashing, itr);
	}

	reference
	operator[](const _Key &key)
	{ return *find_or_insert(key, _Val()); }

	iterator
	find(const _Key &key)
	{ return iterator(_hashing, openhash_get(inclass, _hashing, key)); }

	const_iterator
	find(const _Key &key) const
	{ return const_iterator(_hashing, openhash_get(inclass, _hashing, key)); }

	void
	erase(const _Key &key)
	{ openhash_del(inclass, _hashing, openhash_get(inclass, _hashing, key)); }

	void
	erase(const iterator &it)
	{ openhash_del(inclass, _hashing, it._cur); }

	void
	clear()
	{ openhash_clear(inclass, _hashing); }

	int
	resize(size_t n)
	{
		if (n & (n - 1)) {
			// reject any value that is not the power of two
			return -1;
		}
		return openhash_resize(inclass, _hashing, n);
	}

private:
	hashing _hashing;
};

template<class _Key, class _Except = ulib_except>
class open_hash_set
{
public:
	DEFINE_OPENHASH(inclass, _Key, int, 0, openhash_hashfn, openhash_equalfn);

	typedef _Key	  key_type;
	typedef oh_iter_t size_type;
	typedef oh_iter_t hashing_iterator;
	typedef openhash_t(inclass) * hashing;

	struct iterator
	{
		typedef oh_iter_t size_type;
		typedef openhash_t(inclass) * hashing;
		typedef oh_iter_t hashing_iterator;

		hashing		 _hashing;
		hashing_iterator _cur;

		iterator(hashing h, hashing_iterator itr)
			: _hashing(h), _cur(itr) { }

		iterator() { }

		_Key &
		key() const
		{ return openhash_key(_hashing, _cur); }

		bool
		value() const
		{  return _cur != openhash_end(_hashing); }

		bool
		operator*() const
		{ return value(); }

		iterator&
		operator++()
		{
			if (_cur != openhash_end(_hashing))
				++_cur;
			while (_cur != openhash_end(_hashing) && !openhash_exist(_hashing, _cur))
				++_cur;
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
		{ return _cur == other._cur; }

		bool
		operator!=(const iterator &other) const
		{ return _cur != other._cur; }
	};

	struct const_iterator
	{
		typedef oh_iter_t size_type;
		typedef const openhash_t(inclass) * hashing;
		typedef oh_iter_t hashing_iterator;

		hashing		 _hashing;
		hashing_iterator _cur;

		const_iterator(const hashing h, hashing_iterator itr)
			: _hashing(h), _cur(itr) { }

		const_iterator() { }

		const_iterator(const iterator &it)
			: _hashing(it._hashing), _cur(it._cur) { }

		const _Key &
		key() const
		{ return openhash_key(_hashing, _cur); }

		bool
		value() const
		{  return _cur != openhash_end(_hashing); }

		bool
		operator*() const
		{ return value(); }

		const_iterator&
		operator++()
		{
			if (_cur != openhash_end(_hashing))
				++_cur;
			while (_cur != openhash_end(_hashing) && !openhash_exist(_hashing, _cur))
				++_cur;
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
		{ return _cur == other._cur; }

		bool
		operator!=(const const_iterator &other) const
		{ return _cur != other._cur; }
	};

	open_hash_set()
	{
		_hashing = openhash_init(inclass);
		if (_hashing == 0)
			throw _Except();
	}

	open_hash_set(const open_hash_set &other)
	{
		_hashing = openhash_init(inclass);
		if (_hashing == 0)
			throw _Except();
		for (const_iterator it = other.begin(); it != other.end(); ++it)
			insert(it.key());
	}

	open_hash_set &
	operator= (const open_hash_set &other)
	{
		if (&other != this) {
			clear();
			for (const_iterator it = other.begin(); it != other.end(); ++it)
				insert(it.key());
		}
		return *this;
	}

	virtual
	~open_hash_set()
	{ openhash_destroy(inclass, _hashing); }

	size_type
	size() const
	{ return openhash_size(_hashing); }

	bool
	empty() const
	{ return size() == 0; }

	iterator
	begin()
	{
		for (size_type itr = openhash_begin(_hashing);
		     itr != openhash_end(_hashing); ++itr)
			if (openhash_exist(_hashing, itr))
				return iterator(_hashing, itr);
		return end();
	}

	iterator
	end()
	{ return iterator(_hashing, openhash_end(_hashing)); }

	const_iterator
	begin() const
	{
		for (size_type itr = openhash_begin(_hashing);
		     itr != openhash_end(_hashing); ++itr)
			if (openhash_exist(_hashing, itr))
				return iterator(_hashing, itr);
		return end();
	}

	const_iterator
	end() const
	{ return iterator(_hashing, openhash_end(_hashing)); }

	size_type
	bucket_count() const
	{ return openhash_nbucket(_hashing); }

	bool
	contain(const _Key &key) const
	{ return openhash_get(inclass, _hashing, key) != openhash_end(_hashing); }

	iterator
	insert(const _Key &key)
	{
		int ret;
		hashing_iterator itr = openhash_set(inclass, _hashing, key, &ret);
		if (itr == openhash_end(_hashing))
			throw _Except();
		return iterator(_hashing, itr);
	}

	bool
	operator[](const _Key &key) const
	{ return contain(key); }

	iterator
	find(const _Key &key)
	{ return iterator(_hashing, openhash_get(inclass, _hashing, key)); }

	const_iterator
	find(const _Key &key) const
	{ return const_iterator(_hashing, openhash_get(inclass, _hashing, key)); }

	void
	erase(const _Key &key)
	{ openhash_del(inclass, _hashing, openhash_get(inclass, _hashing, key)); }

	void
	erase(const iterator &it)
	{ openhash_del(inclass, _hashing, it._cur); }

	void
	clear()
	{ openhash_clear(inclass, _hashing); }

	int
	resize(size_t n)
	{
		if (n & (n - 1)) {
			// reject any value that is not the power of two
			return -1;
		}
		return openhash_resize(inclass, _hashing, n);
	}

private:
	hashing _hashing;
};

}  // namespace ulib

#endif	/* _ULIB_HASH_OPEN_H */
