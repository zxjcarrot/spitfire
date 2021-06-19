#include <stdio.h>
#include <assert.h>
#include <ulib/crypt_sha1.h>

int main()
{
	sha1_ctx_t ctx;

	sha1_init(&ctx);
	sha1_update(&ctx, (const unsigned char*)"SHA1 hello world string message", 31);
	sha1_finalize(&ctx);

	uint8_t *digest = SHA1_DIGEST(&ctx);

	assert(*(uint32_t *)&digest[0]	== 0x0f38349au);
	assert(*(uint32_t *)&digest[4]	== 0x9053606du);
	assert(*(uint32_t *)&digest[8]	== 0xcbe578cau);
	assert(*(uint32_t *)&digest[12] == 0x7a8fae05u);
	assert(*(uint32_t *)&digest[16] == 0x109db0e1u);

	printf("passed\n");

	return 0;
}
