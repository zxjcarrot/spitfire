// include this before other header for independence test
#include <ulib/crypt_aes.h>
#include <ulib/util_hexdump.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

const unsigned char test_key1[16] =
{ 0x80 };
const unsigned char test_vec1[16] =
{ 0x0e, 0xdd, 0x33, 0xd3, 0xc6, 0x21, 0xe5, 0x46, 0x45, 0x5b, 0xd8, 0xba, 0x14, 0x18, 0xbe, 0xc8 };

const unsigned char test_key2[16] =
{ 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff };
const unsigned char test_vec2[16] =
{ 0xa1, 0xf6, 0x25, 0x8c, 0x87, 0x7d, 0x5f, 0xcd, 0x89, 0x64, 0x48, 0x45, 0x38, 0xbf, 0xc9, 0x2c };

// for cbc mode with 192-bit key
// initial vector
const unsigned char test_vec3[16] =
{ 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F };
const unsigned char test_key3[24] =
{ 0x8e, 0x73, 0xb0, 0xf7, 0xda, 0x0e, 0x64, 0x52, 0xc8, 0x10, 0xf3, 0x2b, 0x80, 0x90, 0x79, 0xe5,
  0x62, 0xf8, 0xea, 0xd2, 0x52, 0x2c, 0x6b, 0x7b };
// plaintext
const unsigned char test_vec4[16] =
{ 0x6b, 0xc1, 0xbe, 0xe2, 0x2e, 0x40, 0x9f, 0x96, 0xe9, 0x3d, 0x7e, 0x11, 0x73, 0x93, 0x17, 0x2a };
// ciphertext
const unsigned char test_vec5[16] =
{ 0x4f, 0x02, 0x1d, 0xb2, 0x43, 0xbc, 0x63, 0x3d, 0x71, 0x78, 0x18, 0x3a, 0x9f, 0xa0, 0x71, 0xe8 };

int main()
{
	aes_ks_t key;

	aes_setks_encrypt(test_key1, 128, &key);
	unsigned char output1[16] = { 0 };
	print_hex_dump_bytes("KEY\t", DUMP_PREFIX_OFFSET, test_key1, sizeof(test_key1));
	aes_ecb_encrypt(output1, output1, &key);
	print_hex_dump_bytes("VEC\t", DUMP_PREFIX_OFFSET, output1, sizeof(output1));
	assert(memcmp(test_vec1, output1, sizeof(test_vec1)) == 0);

	aes_setks_encrypt(test_key2, 128, &key);
	unsigned char output2[16] = { 0 };
	print_hex_dump_bytes("KEY\t", DUMP_PREFIX_OFFSET, test_key2, sizeof(test_key2));
	aes_ecb_encrypt(output2, output2, &key);
	print_hex_dump_bytes("VEC\t", DUMP_PREFIX_OFFSET, output2, sizeof(output2));
	assert(memcmp(test_vec2, output2, sizeof(test_vec2)) == 0);

	const unsigned char zeros[16] = { 0 };

	aes_setks_decrypt(test_key1, 128, &key);
	unsigned char output3[16] = { 0 };
	aes_ecb_decrypt(test_vec1, output3, &key);
	assert(memcmp(output3, zeros, sizeof(output3)) == 0);

	aes_setks_decrypt(test_key2, 128, &key);
	unsigned char output4[16] = { 0 };
	aes_ecb_decrypt(test_vec2, output4, &key);
	assert(memcmp(output4, zeros, sizeof(output4)) == 0);

	aes_setks_encrypt(test_key3, 192, &key);
	unsigned char output5[16] = { 0 };
	print_hex_dump_bytes("KEY\t", DUMP_PREFIX_OFFSET, test_key3, sizeof(test_key3));
	unsigned char ivec1[16];
	memcpy(ivec1, test_vec3, sizeof(ivec1));
	aes_cbc_encrypt(test_vec4, output5, ivec1, 1, &key);
	print_hex_dump_bytes("VEC\t", DUMP_PREFIX_OFFSET, test_vec3, sizeof(test_vec3));
	print_hex_dump_bytes("VEC\t", DUMP_PREFIX_OFFSET, test_vec4, sizeof(test_vec4));
	print_hex_dump_bytes("VEC\t", DUMP_PREFIX_OFFSET, output5, sizeof(output5));
	assert(memcmp(test_vec5, output5, sizeof(test_vec5)) == 0);
	unsigned char output6[16] = { 0 };
	aes_setks_decrypt(test_key3, 192, &key);
	memcpy(ivec1, test_vec3, sizeof(ivec1));
	aes_cbc_decrypt(test_vec5, output6, ivec1, 1, &key);
	assert(memcmp(test_vec4, output6, sizeof(test_vec4)) == 0);

	const unsigned char str[128] =
		"this is a long string for AES cbc mode testing. Padding..................END";
	aes_setks_encrypt(test_key3, 192, &key);
	unsigned char output7[128];
	memcpy(ivec1, test_vec3, sizeof(ivec1));
	aes_cbc_encrypt(str, output7, ivec1, sizeof(str)/AES_BLOCK_SIZE, &key);
	print_hex_dump_bytes("STR\t", DUMP_PREFIX_OFFSET, output7, sizeof(output7));
	aes_setks_decrypt(test_key3, 192, &key);
	memcpy(ivec1, test_vec3, sizeof(ivec1));
	unsigned char output8[128];
	aes_cbc_decrypt(output7, output8, ivec1, sizeof(output7)/AES_BLOCK_SIZE, &key);
	print_hex_dump_bytes("STR\t", DUMP_PREFIX_OFFSET, output8, sizeof(output8));
	assert(memcmp(str, output8, sizeof(str)) == 0);

	printf("passed\n");

	return 0;
}
