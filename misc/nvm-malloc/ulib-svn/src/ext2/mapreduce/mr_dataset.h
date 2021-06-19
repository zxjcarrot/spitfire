/* The MIT License

   Copyright (C) 2012 Zilong Tan (eric.zltan@gmail.com)

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

#ifndef _ULIB_DATASET_H
#define _ULIB_DATASET_H

#include "math_rng_zipf.h"

namespace ulib
{

namespace mapreduce
{

// generate dataset of Zipf distribution
class dataset_zipf
{
public:
	typedef int record_type;

	// range: range for each element in the dataset
	// s: distribution parameter -- the exponent
	dataset_zipf(size_t size, size_t range, float s);
	~dataset_zipf();

	struct iterator {
		iterator(int *p = 0)
			: _pos(p) { }

		int &
		operator *()
		{ return *_pos;	}

		iterator
		operator +(size_t dist)
		{ return iterator(_pos + dist);	}

		iterator &
		operator++()
		{
			++_pos;
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
		operator!=(const iterator &other) const
		{ return _pos != other._pos; }

		int *_pos;
	};

	struct const_iterator {
		const_iterator(const int *p = 0)
			: _pos(p) { }

		const_iterator(const iterator &other)
			: _pos(other._pos) { }

		const int &
		operator *()
		{ return *_pos;	}

		const_iterator
		operator +(size_t dist)
		{ return const_iterator(_pos + dist); }

		const_iterator &
		operator++()
		{
			++_pos;
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
		operator!=(const const_iterator &other) const
		{ return _pos != other._pos; }

		const int *_pos;
	};

	iterator
	begin()
	{ return iterator(_buf); }

	const_iterator
	begin() const
	{ return const_iterator(_buf); }

	iterator
	end()
	{ return iterator(_buf + _size); }

	const_iterator
	end() const
	{ return const_iterator(_buf + _size); }

	size_t
	size() const
	{ return _size;	}

private:
	int   *_buf;
	size_t _size;
};

class dataset_random
{
public:
	typedef size_t record_type;

	// range: range for each element in the dataset
	dataset_random(size_t size, size_t range);
	~dataset_random();

	struct iterator {
		iterator(int *p = 0)
			: _pos(p) { }

		int &
		operator *()
		{ return *_pos;	}

		iterator
		operator +(size_t dist)
		{ return iterator(_pos + dist); }

		iterator &
		operator++()
		{
			++_pos;
			return *this;
		}

		iterator
		operator++(int) {
			iterator old = *this;
			++*this;
			return old;
		}

		bool
		operator!=(const iterator &other) const
		{ return _pos != other._pos; }

		int *_pos;
	};

	struct const_iterator {
		const_iterator(const int *p = 0)
			: _pos(p) { }

		const_iterator(const iterator &other)
			: _pos(other._pos) { }

		const int &
		operator *()
		{ return *_pos;	}

		const_iterator
		operator +(size_t dist)
		{ return const_iterator(_pos + dist); }

		const_iterator &
		operator++()
		{
			++_pos;
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
		operator!=(const const_iterator &other) const
		{ return _pos != other._pos; }

		const int *_pos;
	};

	iterator
	begin()
	{ return iterator(_buf); }

	const_iterator
	begin() const
	{ return const_iterator(_buf); }

	iterator
	end()
	{ return iterator(_buf + _size); }

	const_iterator
	end() const
	{ return const_iterator(_buf + _size); }

	size_t
	size() const
	{ return _size; }

private:
	int   *_buf;
	size_t _size;
};

}  // namespace mapreduce

}  // namespace ulib

#endif // _ULIB_DATASET_H
