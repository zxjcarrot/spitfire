#include <ulib/crypt_sha256.h>	// put it at the beginning for independence test
#include <ulib/util_hexdump.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

const unsigned char test_key1[] =
	"hello\n";
const unsigned char test_vec1[32] =
{ 0x58, 0x91, 0xb5, 0xb5, 0x22, 0xd5, 0xdf, 0x08, 0x6d, 0x0f, 0xf0, 0xb1, 0x10, 0xfb, 0xd9, 0xd2,
  0x1b, 0xb4, 0xfc, 0x71, 0x63, 0xaf, 0x34, 0xd0, 0x82, 0x86, 0xa2, 0xe8, 0x46, 0xf6, 0xbe, 0x03 };

int main()
{
	sha256_ctx_t ctx;

	unsigned char hash[32] = { 0 };

	sha256_init(&ctx);
	sha256_update(&ctx, test_key1, strlen((const char *)test_key1));
	sha256_finalize(&ctx, hash);

	print_hex_dump_bytes("SHA256KEY\t", DUMP_PREFIX_OFFSET, test_key1, strlen((const char *)test_key1));
	print_hex_dump_bytes("SHA256SUM\t", DUMP_PREFIX_OFFSET, hash, sizeof(hash));

	assert(memcmp(test_vec1, hash, sizeof(test_vec1)) == 0);

	printf("passed\n");

	return 0;
}
