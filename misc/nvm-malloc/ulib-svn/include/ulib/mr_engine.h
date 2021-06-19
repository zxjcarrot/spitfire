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

#ifndef _ULIB_MR_ENGINE_H
#define _ULIB_MR_ENGINE_H

#include <vector>
#include <utility>

#include <ulib/math_rand_prot.h>
#include <ulib/os_thread.h>
#include "mr_dataset.h"
#include "mr_interm.h"

namespace ulib
{

namespace mapreduce
{

// a combiner is built on an aggregate sum and is associative
template<typename V>
class combiner
{
public:
	typedef V value_type;

	combiner(V &val) : _value(val) { }

	virtual	    combiner &
	operator+=(const V &other) = 0;

	operator V()
	{ return _value; }

	const V &
	value() const
	{ return _value; }

protected:
	V &_value;
};

// The abstraction for a mapper. A mapper is the function to transform
// a record into a set of intermediate key/value pairs.
// The R, K and V are the record type, key type and value type
// respectively.
template<typename R, typename K, typename V>
class mapper
{
public:
	typedef R record_type;
	typedef K key_type;
	typedef V value_type;

	typedef typename std::vector< std::pair<K, V> >::iterator	iterator;
	typedef typename std::vector< std::pair<K, V> >::const_iterator const_iterator;

	mapper() : _pos(0) { }

	virtual
	~mapper() { }

	void
	emit(const K &key, const V &value) {
		if (_pos < _pairs.size()) {
			_pairs[_pos].first  = key;
			_pairs[_pos].second = value;
		} else
			_pairs.push_back(std::pair<K,V>(key, value));
		++_pos;
	}

	// The function that implements the map() logic.
	// Call emit() to produce intermeidate key/value pairs.
	virtual void
	operator()(const R &rec) = 0;

	iterator
	begin()
	{ return _pairs.begin(); }

	iterator
	end()
	{ return _pairs.begin() + _pos; }

	const_iterator
	begin() const
	{ return _pairs.begin(); }

	const_iterator
	end() const
	{ return _pairs.begin() + _pos; }

	size_t
	size() const
	{ return _pos; }

	void
	reset()
	{ _pos = 0; }

protected:
	size_t _pos;
	std::vector< std::pair<K,V> > _pairs;
};

// the basic reducer, which is merely the same as the combiner
template<typename V>
struct reducer : public combiner<V> {
	typedef typename combiner<V>::value_type value_type;

	reducer(V &val) : combiner<V>(val) { }
};

// the basic partition that performs: 1) converting a key to size_t
// via hashing, and 2) comparing two keys.
template<typename K>
class partition
{
public:
	typedef K key_type;

	partition(const K &key) : _key(key) { }

	// algthough the key itself needs to implement the hashing,
	// this serves as an enhancement
	virtual
	operator size_t() const = 0;

	// both equality and comparison are provided because equality
	// is often more efficient than comparison, and thus
	// comparison is only used for sorting.
	virtual bool
	operator==(const partition &other) const
	{ return _key == other.key(); }

	virtual bool
	operator <(const partition &other) const
	{ return _key <  other.key(); }

	virtual bool
	operator >(const partition &other) const
	{ return _key >  other.key(); }

	const K &
	key() const
	{ return _key; }

protected:
	K _key;
};

// abstraction for a task, in fact it does not follow the rigorous
// MapReduce paradigm of which a task has independent space for both
// input and output. By contrast, in this task abstraction the
// intermediate storage is shared among all mappers.
template<
	typename S,  // concurrent intermediate storage
	typename M,  // mapper class
	typename R,  // reducer class
	template<typename K> class P, // partition template
	typename I>  // dataset iterator
class task : public ulib::thread
{
public:
	task(S &s, I begin, I end)
		: _store(s), _begin(begin), _end(end) { }

	// ensures that thread is done before destructing the obj
	virtual
	~task()
	{ stop_and_join(); }

private:
	int
	run() { // make it private as only the thread can call the function
		M m;
		for (I i = _begin; i != _end; ++i) {
			m(*i);	// produce intermediate key/value pairs
			for (typename M::const_iterator it = m.begin(); it != m.end(); ++it) {
				size_t h = P<typename M::key_type>(it->first);
				_store.lock(h);
				R(_store[it->first]) += it->second;
				_store.unlock(h);
			}
			m.reset();
		}
		return 0;
	}

	S &_store;
	I  _begin;
	I  _end;
};

// a job is defined as the set of a dataset, a mapper that converts
// data records to key/value pairs, a partition that spreads the
// key/value pairs evenly to slots, and a reducer that combine the
// values associated with the same key.
template<
	template<typename K, typename V> class S, // intermediate storage template
	class M,  // mapper class
	class R,  // reducer class
	template<typename K> class P,  // partition template
	class D>  // dateset
class job
{
public:
	typedef M mapper_type;
	typedef R reducer_type;
	typedef D dataset_type;
	typedef P<typename M::key_type> partition_type;
	typedef S<partition_type, typename M::value_type> result_type;

	job(result_type &r, const D &d)
		: _result(r), _dataset(d) { }

	void
	exec(int ntask) {
		task<result_type, M, R, P, typename D::const_iterator> **tasks =
			new task<result_type, M, R, P, typename D::const_iterator> *[ntask];
		// assuming approximate equality in the processing
		// time of records follows the simple partition:
		size_t len = _dataset.size() / ntask;
		for (int i = 0; i < ntask - 1; ++i)
			tasks[i] = new task<result_type, M, R, P, typename D::const_iterator>
				(_result, _dataset.begin() + len * i, _dataset.begin() + len * (i + 1));
		tasks[ntask - 1] = new task<result_type, M, R, P, typename D::const_iterator>
			(_result, _dataset.begin() + len * (ntask - 1), _dataset.end());
		for (int i = 0; i < ntask; ++i)
			tasks[i]->start();
		for (int i = 0; i < ntask; ++i)
			delete tasks[i];
		delete [] tasks;
	}

private:
	result_type &_result;
	const D	    &_dataset;
};

// this implements an additive combiner
template<typename V>
class typical_reducer : public reducer<V>
{
public:
	typical_reducer(V &val) : reducer<V>(val) { }

	virtual	    typical_reducer &
	operator+=(const V &other) {
		// the value can customize the addition by overloading
		// its += operator.
		combiner<V>::_value += other;
		return *this;
	}
};

template<typename K>
class typical_partition : public partition<K>
{
public:
	typical_partition(const K &key)
		: partition<K>(key) { }

	operator size_t() const {
		uint64_t h = partition<K>::_key;
		RAND_INT3_MIX64(h); // hash function enhancement
		return h;
	}

	bool
	operator==(const partition<K> &other) const
	{ return partition<K>::_key == other.key(); }

	bool
	operator <(const partition<K> &other) const
	{ return partition<K>::_key < other.key(); }

	bool
	operator >(const partition<K> &other) const
	{ return partition<K>::_key > other.key(); }
};

// a typical yet flexible job
template<class M, class D>
class typical_job :
		public job<chain_hash_store, M,
			   typical_reducer<typename M::value_type>,
			   typical_partition, D>
{
public:
	typedef job<chain_hash_store, M,
		    typical_reducer<typename M::value_type>,
		    typical_partition, D> job_type;

	typedef typename job_type::mapper_type	  mapper_type;
	typedef typename job_type::reducer_type	  reducer_type;
	typedef typename job_type::dataset_type	  dataset_type;
	typedef typename job_type::partition_type partition_type;
	typedef typename job_type::result_type	  result_type;

	typical_job(result_type &r, const D &d)
		: job_type(r, d) { }
};

}  // namespace mapreduce

}  // namespace ulib

#endif	// _ULIB_MR_ENGINE_H
