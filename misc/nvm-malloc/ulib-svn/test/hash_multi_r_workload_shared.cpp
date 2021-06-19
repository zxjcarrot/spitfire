#include <unistd.h>
#include <stdint.h>
#include <stdio.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include <ulib/os_thread.h>
#include <ulib/util_timer.h>
#include <ulib/hash_multi_r.h>
#include <ulib/math_rand_prot.h>

using namespace ulib;

// hash_multi current does not allow reading and writing at the same time
#define R_TH_NUM     0
#define W_TH_NUM     2
#define MASK	     0xfffff

// working time in seconds
#define WORKING_TIME 5

pid_t gettid()
{ return syscall( __NR_gettid ); }

multi_hash_map<int,int> shared_map(16);

class writer : public thread {
public:
	int
	run()
	{
		while (is_running()) {
			uint64_t r = _seed++;
			shared_map.combine(RAND_INT4_MIX64(r) & MASK, r);
			++_cnt;
		}
		return 0;
	}

	writer()
		: _cnt(0)
	{
		_seed = gettid();
		RAND_INT4_MIX64(_seed);
		_seed += time(NULL);
	}

	~writer()
	{ stop_and_join(); }

	uint64_t
	count() const
	{ return _cnt; }

private:
	uint64_t _cnt;
	uint64_t _seed;
};

class reader : public thread {
public:
	int
	run()
	{
		while (is_running()) {
			uint64_t r = _seed++;
			shared_map.find(RAND_INT4_MIX64(r) & MASK);
			++_cnt;
		}
		return 0;
	}

	reader()
		: _cnt(0)
	{
		_seed = gettid();
		RAND_INT4_MIX64(_seed);
		_seed += time(NULL);
	}

	~reader()
	{ stop_and_join(); }

	uint64_t
	count() const
	{ return _cnt; }

private:
	uint64_t _cnt;
	uint64_t _seed;
};

int main()
{
	reader r[R_TH_NUM];
	writer w[W_TH_NUM];

	for (int i = 0; i < R_TH_NUM; ++i)
		r[i].start();
	for (int i = 0; i < W_TH_NUM; ++i)
		w[i].start();

	ulib_timer_t timer;
	timer_start(&timer);

	sleep(WORKING_TIME);

	for (int i = 0; i < R_TH_NUM; ++i)
		r[i].stop_and_join();
	for (int i = 0; i < W_TH_NUM; ++i)
		w[i].stop_and_join();

	float elapsed = timer_stop(&timer);

	uint64_t r_cnt = 0;
	uint64_t w_cnt = 0;
	for (int i = 0; i < R_TH_NUM; ++i)
		r_cnt += r[i].count();
	for (int i = 0; i < W_TH_NUM; ++i)
		w_cnt += w[i].count();

	printf("total ops   :%lu read, %lu write\n", (unsigned long)r_cnt, (unsigned long)w_cnt);
	printf("ns_per_read :%10lu op/s\n", (unsigned long)(r_cnt / elapsed));
	printf("ns_per_write:%10lu op/s\n", (unsigned long)(w_cnt / elapsed));

	printf("passed\n");

	return 0;
}
