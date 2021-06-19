#include <stdio.h>
#include <assert.h>
#include <ulib/crypt_md5.h>

int main()
{
	md5_ctx_t ctx;

	md5_init(&ctx);
	md5_update(&ctx, (const unsigned char*)"", 0);
	md5_finalize(&ctx);

	uint8_t *digest = MD5_DIGEST(&ctx);
	assert(*(uint64_t *)&digest[0] == 0x04b2008fd98c1dd4ull);
	assert(*(uint64_t *)&digest[8] == 0x7e42f8ec980980e9ull);

	md5_init(&ctx);
	md5_update(&ctx, (const unsigned char*)"MD5 hello world string message", 30);
	md5_finalize(&ctx);
	assert(*(uint64_t *)&digest[0] == 0x8cfe3ef35b47f0d6ull);
	assert(*(uint64_t *)&digest[8] == 0x4caec3658551bc8full);

	printf("passed\n");

	return 0;
}
