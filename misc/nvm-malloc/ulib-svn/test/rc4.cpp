#include <ulib/crypt_rc4.h>  // put it before stdio.h for independence test
#include <ulib/util_hexdump.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

const unsigned char test_key1[] =
	"Key";
// key stream for ASCII key "Key"
const unsigned char test_vec1[] =
{ 0xeb, 0x9f, 0x77, 0x81, 0xb7, 0x34 };

const unsigned char test_key2[] =
	"Wiki";
// key stream for ASCII key "Wiki"
const unsigned char test_vec2[] =
{ 0x60, 0x44, 0xdb, 0x6d, 0x41, 0xb7 };

const unsigned char test_key3[] =
	"Secret";
// key stream for ASCII key "Secret"
const unsigned char test_vec3[] =
{ 0x04, 0xd4, 0x6b, 0x05, 0x3c, 0xa8 };

int main()
{
	rc4_ks_t key;

	rc4_setks(test_key1, strlen((const char *)test_key1), &key);
	unsigned char output1[sizeof(test_vec1)] = { 0 };
	print_hex_dump_bytes("OUT\t", DUMP_PREFIX_OFFSET, output1, sizeof(output1));
	rc4_crypt(output1, sizeof(output1), &key);
	print_hex_dump_bytes("VEC\t", DUMP_PREFIX_OFFSET, output1, sizeof(output1));
	assert(memcmp(test_vec1, output1, sizeof(test_vec1)) == 0);

	rc4_setks(test_key2, strlen((const char *)test_key2), &key);
	unsigned char output2[sizeof(test_vec1)] = { 0 };
	print_hex_dump_bytes("OUT\t", DUMP_PREFIX_OFFSET, output2, sizeof(output2));
	rc4_crypt(output2, sizeof(output2), &key);
	print_hex_dump_bytes("VEC\t", DUMP_PREFIX_OFFSET, output2, sizeof(output2));
	assert(memcmp(test_vec2, output2, sizeof(test_vec2)) == 0);

	rc4_setks(test_key3, strlen((const char *)test_key3), &key);
	unsigned char output3[sizeof(test_vec1)] = { 0 };
	print_hex_dump_bytes("OUT\t", DUMP_PREFIX_OFFSET, output3, sizeof(output3));
	rc4_crypt(output3, sizeof(output3), &key);
	print_hex_dump_bytes("VEC\t", DUMP_PREFIX_OFFSET, output3, sizeof(output3));
	assert(memcmp(test_vec3, output3, sizeof(test_vec3)) == 0);

	printf("passed\n");

	return 0;
}
