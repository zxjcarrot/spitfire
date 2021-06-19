#include <stdio.h>
#include <stdint.h>
#include <assert.h>
#include <ulib/math_rand_prot.h>

int main()
{
	uint64_t h = 0x1234567887654321ull;
	RAND_INT3_MIX64(h);
	RAND_INT3_MIX64_INV(h);
	assert(h == 0x1234567887654321ull);
	printf("passed\n");
	return 0;
}
