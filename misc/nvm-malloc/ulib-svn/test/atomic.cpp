#include <assert.h>
#include <stdio.h>
#include <ulib/os_atomic_intel64.h>

int main()
{
	atomic_barrier();

	uint64_t a = 0;

	assert(atomic_cmpswp64(&a, 1, 2) == 0);
	assert(a == 0);
	assert(atomic_cmpswp64(&a, 0, 2) == 0);
	assert(a == 2);
	assert(atomic_cmpswp16(&a, 0, 2) == 2);
	assert(atomic_cmpswp16(&a, 2, 0) == 2);
	assert(atomic_cmpswp16(&a, 0, 2) == 0);
	assert(a == 2);
	assert(atomic_fetchadd64(&a, 1) == 2);
	assert(a == 3);
	assert(atomic_fetchadd64(&a, -1) == 3);
	assert(a == 2);
	assert(atomic_fetchstore64(&a, 5) == 2);
	assert(a == 5);
	assert(atomic_test_and_set_bit64(&a, 0) == -1);
	assert(a == 5);
	assert(atomic_test_and_set_bit64(&a, 1) == 0);
	assert(a == 7);
	atomic_and64(&a, ~7ul);
	assert(a == 0);
	atomic_or64(&a, (1ull << 63));
	assert(a == (1ull << 63));
	assert(atomic_test_and_set_bit64(&a, 63) == -1);
	atomic_or8(&a, 1);
	assert(a == ((1ull << 63) | 1));
	atomic_and8(&a, (int8_t)~1u);
	assert(a == (1ull << 63));
	atomic_and64(&a, ~(1ull << 63));
	assert(a == 0);
	atomic_inc64(&a);
	assert(a == 1);
	atomic_dec64(&a);
	assert(a == 0);
	atomic_add64(&a, -1);
	assert(a == (uint64_t)-1);
	atomic_cpu_relax();

	printf("passed\n");

	return 0;
}
