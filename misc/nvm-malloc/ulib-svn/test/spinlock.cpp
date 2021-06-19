#include <stdio.h>
#include <time.h>
#include <assert.h>
#include <ulib/os_rdtsc.h>
#include <ulib/os_spinlock.h>
#include <ulib/os_thread.h>
#include <ulib/util_log.h>

class delay_inc_mcs_cas : public ulib::thread {
public:
	delay_inc_mcs_cas(mcs_lock_t *lock, int *counter)
		: _lock(lock), _counter(counter) { }

	~delay_inc_mcs_cas()
	{ stop_and_join(); }

	int
	run()
	{
		ULIB_DEBUG("thread %ld lock entry address: %016lx", pthread_self(), (unsigned long)&_lockent);
		ULIB_DEBUG("thread %ld begin acquiring lock ...", pthread_self());
		spin_lock_mcs(_lock, &_lockent);
		ULIB_DEBUG("thread %ld acquired lock", pthread_self());
		int cur = *_counter;
		ULIB_DEBUG("thread %ld before sleep counter is %d", pthread_self(), cur);
		timespec ts = { 0, 10000000 };
		nanosleep(&ts, NULL);
		ULIB_DEBUG("thread %ld sleep done", pthread_self());
		*_counter = cur + 1;
		ULIB_DEBUG("thread %ld begin releasing lock ...", pthread_self());
		spin_unlock_mcs_cas(_lock, &_lockent);
		ULIB_DEBUG("thread %ld released lock", pthread_self());
		return 0;
	}

private:
	mcs_lock_t * _lock;
	int	   * _counter;
	mcs_entry_t  _lockent;
};

class delay_inc_mcs_xchg : public ulib::thread {
public:
	delay_inc_mcs_xchg(mcs_lock_t *lock, int *counter)
		: _lock(lock), _counter(counter) { }

	~delay_inc_mcs_xchg()
	{ stop_and_join(); }

	int
	run()
	{
		ULIB_DEBUG("thread %ld lock entry address: %016lx", pthread_self(), (unsigned long)&_lockent);
		ULIB_DEBUG("thread %ld begin acquiring lock ...", pthread_self());
		spin_lock_mcs(_lock, &_lockent);
		ULIB_DEBUG("thread %ld acquired lock", pthread_self());
		int cur = *_counter;
		ULIB_DEBUG("thread %ld before sleep counter is %d", pthread_self(), cur);
		timespec ts = { 0, 10000000 };
		nanosleep(&ts, NULL);
		ULIB_DEBUG("thread %ld sleep done", pthread_self());
		*_counter = cur + 1;
		ULIB_DEBUG("thread %ld begin releasing lock ...", pthread_self());
		spin_unlock_mcs_xchg(_lock, &_lockent);
		ULIB_DEBUG("thread %ld released lock", pthread_self());
		return 0;
	}

private:
	mcs_lock_t * _lock;
	int	   * _counter;
	mcs_entry_t  _lockent;
};

class delay_inc_k42 : public ulib::thread {
public:
	delay_inc_k42(k42_lock_t *lock, int *counter)
		: _lock(lock), _counter(counter) { }

	~delay_inc_k42()
	{ stop_and_join(); }

	int
	run()
	{
		ULIB_DEBUG("thread %ld begin acquiring lock ...", pthread_self());
		spin_lock_k42(_lock);
		ULIB_DEBUG("thread %ld acquired lock", pthread_self());
		int cur = *_counter;
		ULIB_DEBUG("thread %ld before sleep counter is %d", pthread_self(), cur);
		timespec ts = { 0, 10000000 };
		nanosleep(&ts, NULL);
		ULIB_DEBUG("thread %ld sleep done", pthread_self());
		*_counter = cur + 1;
		ULIB_DEBUG("thread %ld begin releasing lock ...", pthread_self());
		spin_unlock_k42(_lock);
		ULIB_DEBUG("thread %ld released lock", pthread_self());
		return 0;
	}

private:
	k42_lock_t * _lock;
	int	   * _counter;
};

class delay_inc_ticket : public ulib::thread {
public:
	delay_inc_ticket(ticket_lock_t *lock, int *counter)
		: _lock(lock), _counter(counter) { }

	~delay_inc_ticket()
	{ stop_and_join(); }

	int
	run()
	{
		ULIB_DEBUG("thread %ld begin acquiring lock ...", pthread_self());
		spin_lock_ticket(_lock);
		ULIB_DEBUG("thread %ld acquired lock", pthread_self());
		int cur = *_counter;
		ULIB_DEBUG("thread %ld before sleep counter is %d", pthread_self(), cur);
		timespec ts = { 0, 10000000 };
		nanosleep(&ts, NULL);
		ULIB_DEBUG("thread %ld sleep done", pthread_self());
		*_counter = cur + 1;
		ULIB_DEBUG("thread %ld begin releasing lock ...", pthread_self());
		spin_unlock_ticket(_lock);
		ULIB_DEBUG("thread %ld released lock", pthread_self());
		return 0;
	}

private:
	ticket_lock_t * _lock;
	int	   * _counter;
};

volatile int shared = 0;

class writer_ticket : public ulib::thread {
public:
	writer_ticket(volatile int *p, ticket_rwlock_t *lock)
		: _share(p), _lock(lock) { }

	int
	run()
	{
		spin_wrlock_ticket(_lock);
		++*_share;
		timespec ts = { 0, 0 };
		ts.tv_nsec = rdtsc() % 100000000;
		nanosleep(&ts, NULL);
		++*_share;
		spin_wrunlock_ticket(_lock);
		return 0;
	}

	~writer_ticket()
	{ stop_and_join(); }

private:
	volatile int *	  _share;
	ticket_rwlock_t * _lock;
};

class reader_ticket : public ulib::thread {
public:
	reader_ticket(volatile int *p, ticket_rwlock_t *lock)
		: _share(p), _lock(lock) { }

	int
	run()
	{
		while (is_running()) {
			spin_rdlock_ticket(_lock);
			assert((*_share & 1) == 0);
			spin_rdunlock_ticket(_lock);
			timespec ts = { 0, 0 };
			ts.tv_nsec = rdtsc() % 10000000;
			nanosleep(&ts, NULL);
		}
		return 0;
	}

	~reader_ticket()
	{ stop_and_join(); }

private:
	volatile int *	  _share;
	ticket_rwlock_t * _lock;
};

int main()
{
	int8_t lock = 0;

	// return 0 if successful
	assert(spin_trylock_xchg(&lock) == 0);
	assert(spin_trylock_xchg(&lock) == 1);
	spin_unlock_xchg(&lock);
	assert(spin_trylock_xchg(&lock) == 0);
	spin_unlock_xchg(&lock);
	spin_lock_xchg(&lock);
	assert(spin_trylock_xchg(&lock) == 1);

	mcs_lock_t  mcslock;
	mcs_entry_t mcsent;
	spin_init_mcs(&mcslock);
	assert(spin_trylock_mcs(&mcslock, &mcsent) == 0);
	assert(spin_trylock_mcs(&mcslock, &mcsent));
	spin_unlock_mcs_cas(&mcslock, &mcsent);
	assert(spin_trylock_mcs(&mcslock, &mcsent) == 0);
	spin_unlock_mcs_xchg(&mcslock, &mcsent);
	assert(mcslock == NULL);

	delay_inc_mcs_cas *di_cas[64];
	int counter = 0;
	for (unsigned i = 0; i < sizeof(di_cas)/sizeof(di_cas[0]); ++i) {
		di_cas[i] = new delay_inc_mcs_cas(&mcslock, &counter);
		di_cas[i]->start();
	}
	for (unsigned i = 0; i < sizeof(di_cas)/sizeof(di_cas[0]); ++i)
		delete di_cas[i];
	assert(counter == sizeof(di_cas)/sizeof(di_cas[0]));

	delay_inc_mcs_xchg *di_xchg[64];
	counter = 0;
	for (unsigned i = 0; i < sizeof(di_xchg)/sizeof(di_xchg[0]); ++i) {
		di_xchg[i] = new delay_inc_mcs_xchg(&mcslock, &counter);
		di_xchg[i]->start();
	}
	for (unsigned i = 0; i < sizeof(di_xchg)/sizeof(di_xchg[0]); ++i)
		delete di_xchg[i];
	assert(counter == sizeof(di_xchg)/sizeof(di_xchg[0]));

	k42_lock_t k42lock;
	spin_init_k42(&k42lock);
	assert(spin_trylock_k42(&k42lock) == 0);
	assert(spin_trylock_k42(&k42lock));
	spin_unlock_k42(&k42lock);
	assert(spin_trylock_k42(&k42lock) == 0);
	spin_unlock_k42(&k42lock);
	spin_lock_k42(&k42lock);
	spin_unlock_k42(&k42lock);
	spin_lock_k42(&k42lock);
	spin_unlock_k42(&k42lock);
	assert(spin_trylock_k42(&k42lock) == 0);
	assert(spin_trylock_k42(&k42lock));
	spin_unlock_k42(&k42lock);

	delay_inc_k42 *di_k42[64];
	counter = 0;
	for (unsigned i = 0; i < sizeof(di_k42)/sizeof(di_k42[0]); ++i) {
		di_k42[i] = new delay_inc_k42(&k42lock, &counter);
		di_k42[i]->start();
	}
	for (unsigned i = 0; i < sizeof(di_k42)/sizeof(di_k42[0]); ++i)
		delete di_k42[i];
	assert(counter == sizeof(di_k42)/sizeof(di_k42[0]));

	ticket_lock_t tklock;
	spin_init_ticket(&tklock);
	assert(spin_trylock_ticket(&tklock) == 0);
	assert(spin_trylock_ticket(&tklock));
	spin_unlock_ticket(&tklock);
	assert(spin_trylock_ticket(&tklock) == 0);
	spin_unlock_ticket(&tklock);

	delay_inc_ticket *di_ticket[64];
	counter = 0;
	for (unsigned i = 0; i < sizeof(di_ticket)/sizeof(di_ticket[0]); ++i) {
		di_ticket[i] = new delay_inc_ticket(&tklock, &counter);
		di_ticket[i]->start();
	}
	for (unsigned i = 0; i < sizeof(di_ticket)/sizeof(di_ticket[0]); ++i)
		delete di_ticket[i];
	assert(counter == sizeof(di_ticket)/sizeof(di_ticket[0]));

	ticket_rwlock_t tkrw;

	spin_init_rwticket(&tkrw);
	ULIB_DEBUG("w=%u, r=%u, u=%u", tkrw.tickets.write, tkrw.tickets.read, tkrw.tickets.user);
	assert(spin_tryrdlock_ticket(&tkrw) == 0);
	ULIB_DEBUG("w=%u, r=%u, u=%u", tkrw.tickets.write, tkrw.tickets.read, tkrw.tickets.user);
	assert(spin_tryrdlock_ticket(&tkrw) == 0);
	ULIB_DEBUG("w=%u, r=%u, u=%u", tkrw.tickets.write, tkrw.tickets.read, tkrw.tickets.user);
	assert(spin_trywrlock_ticket(&tkrw));
	assert(spin_trywrlock_ticket(&tkrw));
	spin_rdunlock_ticket(&tkrw);
	ULIB_DEBUG("w=%u, r=%u, u=%u", tkrw.tickets.write, tkrw.tickets.read, tkrw.tickets.user);
	assert(spin_trywrlock_ticket(&tkrw));
	spin_rdunlock_ticket(&tkrw);
	ULIB_DEBUG("w=%u, r=%u, u=%u", tkrw.tickets.write, tkrw.tickets.read, tkrw.tickets.user);
	assert(spin_trywrlock_ticket(&tkrw) == 0);
	ULIB_DEBUG("w=%u, r=%u, u=%u", tkrw.tickets.write, tkrw.tickets.read, tkrw.tickets.user);
	assert(spin_trywrlock_ticket(&tkrw));
	assert(spin_tryrdlock_ticket(&tkrw));
	spin_wrunlock_ticket(&tkrw);
	assert(spin_trywrlock_ticket(&tkrw) == 0);
	spin_wrunlock_ticket(&tkrw);

	reader_ticket *tk_rdr[32];
	writer_ticket *tk_wrt[32];
	for (unsigned int i = 0; i < sizeof(tk_rdr)/sizeof(tk_rdr[0]); ++i) {
		tk_rdr[i] = new reader_ticket(&shared, &tkrw);
		tk_rdr[i]->start();
	}
	for (unsigned int i = 0; i < sizeof(tk_wrt)/sizeof(tk_wrt[0]); ++i) {
		tk_wrt[i] = new writer_ticket(&shared, &tkrw);
		tk_wrt[i]->start();
	}
	for (unsigned int i = 0; i < sizeof(tk_wrt)/sizeof(tk_wrt[0]); ++i) {
		delete tk_wrt[i];
		delete tk_rdr[i];
	}
	assert(spin_trywrlock_ticket(&tkrw) == 0);

	printf("passed\n");

	return 0;
}
