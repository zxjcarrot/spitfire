/* The MIT License

   Copyright (C) 2011 Zilong Tan (eric.zltan@gmail.com)

   Permission is hereby granted, free of charge, to any person obtaining
   a copy of this software and associated documentation files (the
   "Software"), to deal in the Software without restriction, including
   without limitation the rights to use, copy, modify, merge, publish,
   distribute, sublicense, and/or sell copies of the Software, and to
   permit persons to whom the Software is furnished to do so, subject to
   the following conditions:

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

/*
**********************************************************************
** Copyright (C) 1990, RSA Data Security, Inc. All rights reserved. **
**								    **
** License to copy and use this software is granted provided that   **
** it is identified as the "RSA Data Security, Inc. MD5 Message	    **
** Digest Algorithm" in all material mentioning or referencing this **
** software or this function.					    **
**								    **
** License is also granted to make and use derivative works	    **
** provided that such works are identified as "derived from the RSA **
** Data Security, Inc. MD5 Message Digest Algorithm" in all	    **
** material mentioning or referencing the derived work.		    **
**								    **
** RSA Data Security, Inc. makes no representations concerning	    **
** either the merchantability of this software or the suitability   **
** of this software for any particular purpose.	 It is provided "as **
** is" without express or implied warranty of any kind.		    **
**								    **
** These notices must be retained in any copies of any part of this **
** documentation and/or software.				    **
**********************************************************************
*/

#include "crypt_md5.h"

static uint8_t padding_[64] = {
	0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
};

/* F, G and H are basic MD5 functions: selection, majority, parity */
#define F(x, y, z) (((x) & (y)) | ((~x) & (z)))
#define G(x, y, z) (((x) & (z)) | ((y) & (~z)))
#define H(x, y, z) ((x) ^ (y) ^ (z))
#define I(x, y, z) ((y) ^ ((x) | (~z)))

/* ROTATE_LEFT rotates x left n bits */
#define ROTATE_LEFT(x, n) (((x) << (n)) | ((x) >> (32-(n))))

/* FF, GG, HH, and J transformations for rounds 1, 2, 3, and 4 */
/* Rotation is separate from addition to prevent recomputation */
#define FT(a, b, c, d, x, s, ac)				\
	{(a) += F ((b), (c), (d)) + (x) + (uint32_t)(ac);	\
		(a) = ROTATE_LEFT ((a), (s));			\
		(a) += (b);					\
	}

#define GT(a, b, c, d, x, s, ac)				\
	{(a) += G ((b), (c), (d)) + (x) + (uint32_t)(ac);	\
		(a) = ROTATE_LEFT ((a), (s));			\
		(a) += (b);					\
	}

#define HT(a, b, c, d, x, s, ac)				\
	{(a) += H ((b), (c), (d)) + (x) + (uint32_t)(ac);	\
		(a) = ROTATE_LEFT ((a), (s));			\
		(a) += (b);					\
	}

#define IT(a, b, c, d, x, s, ac)				\
	{(a) += I ((b), (c), (d)) + (x) + (uint32_t)(ac);	\
		(a) = ROTATE_LEFT ((a), (s));			\
		(a) += (b);					\
	}

/* Basic MD5 step. md5_transform buf based on in.
 */
static void md5_transform(uint32_t *buf, uint32_t *in)
{
	uint32_t a = buf[0], b = buf[1], c = buf[2], d = buf[3];

	/* Round 1 */
#define S11 7
#define S12 12
#define S13 17
#define S14 22
	FT ( a, b, c, d, in[ 0], S11, 3614090360ul); /* 1 */
	FT ( d, a, b, c, in[ 1], S12, 3905402710ul); /* 2 */
	FT ( c, d, a, b, in[ 2], S13,  606105819ul); /* 3 */
	FT ( b, c, d, a, in[ 3], S14, 3250441966ul); /* 4 */
	FT ( a, b, c, d, in[ 4], S11, 4118548399ul); /* 5 */
	FT ( d, a, b, c, in[ 5], S12, 1200080426ul); /* 6 */
	FT ( c, d, a, b, in[ 6], S13, 2821735955ul); /* 7 */
	FT ( b, c, d, a, in[ 7], S14, 4249261313ul); /* 8 */
	FT ( a, b, c, d, in[ 8], S11, 1770035416ul); /* 9 */
	FT ( d, a, b, c, in[ 9], S12, 2336552879ul); /* 10 */
	FT ( c, d, a, b, in[10], S13, 4294925233ul); /* 11 */
	FT ( b, c, d, a, in[11], S14, 2304563134ul); /* 12 */
	FT ( a, b, c, d, in[12], S11, 1804603682ul); /* 13 */
	FT ( d, a, b, c, in[13], S12, 4254626195ul); /* 14 */
	FT ( c, d, a, b, in[14], S13, 2792965006ul); /* 15 */
	FT ( b, c, d, a, in[15], S14, 1236535329ul); /* 16 */

	/* Round 2 */
#define S21 5
#define S22 9
#define S23 14
#define S24 20
	GT ( a, b, c, d, in[ 1], S21, 4129170786ul); /* 17 */
	GT ( d, a, b, c, in[ 6], S22, 3225465664ul); /* 18 */
	GT ( c, d, a, b, in[11], S23,  643717713ul); /* 19 */
	GT ( b, c, d, a, in[ 0], S24, 3921069994ul); /* 20 */
	GT ( a, b, c, d, in[ 5], S21, 3593408605ul); /* 21 */
	GT ( d, a, b, c, in[10], S22,	38016083ul); /* 22 */
	GT ( c, d, a, b, in[15], S23, 3634488961ul); /* 23 */
	GT ( b, c, d, a, in[ 4], S24, 3889429448ul); /* 24 */
	GT ( a, b, c, d, in[ 9], S21,  568446438ul); /* 25 */
	GT ( d, a, b, c, in[14], S22, 3275163606ul); /* 26 */
	GT ( c, d, a, b, in[ 3], S23, 4107603335ul); /* 27 */
	GT ( b, c, d, a, in[ 8], S24, 1163531501ul); /* 28 */
	GT ( a, b, c, d, in[13], S21, 2850285829ul); /* 29 */
	GT ( d, a, b, c, in[ 2], S22, 4243563512ul); /* 30 */
	GT ( c, d, a, b, in[ 7], S23, 1735328473ul); /* 31 */
	GT ( b, c, d, a, in[12], S24, 2368359562ul); /* 32 */

	/* Round 3 */
#define S31 4
#define S32 11
#define S33 16
#define S34 23
	HT ( a, b, c, d, in[ 5], S31, 4294588738ul); /* 33 */
	HT ( d, a, b, c, in[ 8], S32, 2272392833ul); /* 34 */
	HT ( c, d, a, b, in[11], S33, 1839030562ul); /* 35 */
	HT ( b, c, d, a, in[14], S34, 4259657740ul); /* 36 */
	HT ( a, b, c, d, in[ 1], S31, 2763975236ul); /* 37 */
	HT ( d, a, b, c, in[ 4], S32, 1272893353ul); /* 38 */
	HT ( c, d, a, b, in[ 7], S33, 4139469664ul); /* 39 */
	HT ( b, c, d, a, in[10], S34, 3200236656ul); /* 40 */
	HT ( a, b, c, d, in[13], S31,  681279174ul); /* 41 */
	HT ( d, a, b, c, in[ 0], S32, 3936430074ul); /* 42 */
	HT ( c, d, a, b, in[ 3], S33, 3572445317ul); /* 43 */
	HT ( b, c, d, a, in[ 6], S34,	76029189ul); /* 44 */
	HT ( a, b, c, d, in[ 9], S31, 3654602809ul); /* 45 */
	HT ( d, a, b, c, in[12], S32, 3873151461ul); /* 46 */
	HT ( c, d, a, b, in[15], S33,  530742520ul); /* 47 */
	HT ( b, c, d, a, in[ 2], S34, 3299628645ul); /* 48 */

	/* Round 4 */
#define S41 6
#define S42 10
#define S43 15
#define S44 21
	IT ( a, b, c, d, in[ 0], S41, 4096336452ul); /* 49 */
	IT ( d, a, b, c, in[ 7], S42, 1126891415ul); /* 50 */
	IT ( c, d, a, b, in[14], S43, 2878612391ul); /* 51 */
	IT ( b, c, d, a, in[ 5], S44, 4237533241ul); /* 52 */
	IT ( a, b, c, d, in[12], S41, 1700485571ul); /* 53 */
	IT ( d, a, b, c, in[ 3], S42, 2399980690ul); /* 54 */
	IT ( c, d, a, b, in[10], S43, 4293915773ul); /* 55 */
	IT ( b, c, d, a, in[ 1], S44, 2240044497ul); /* 56 */
	IT ( a, b, c, d, in[ 8], S41, 1873313359ul); /* 57 */
	IT ( d, a, b, c, in[15], S42, 4264355552ul); /* 58 */
	IT ( c, d, a, b, in[ 6], S43, 2734768916ul); /* 59 */
	IT ( b, c, d, a, in[13], S44, 1309151649ul); /* 60 */
	IT ( a, b, c, d, in[ 4], S41, 4149444226ul); /* 61 */
	IT ( d, a, b, c, in[11], S42, 3174756917ul); /* 62 */
	IT ( c, d, a, b, in[ 2], S43,  718787259ul); /* 63 */
	IT ( b, c, d, a, in[ 9], S44, 3951481745ul); /* 64 */

	buf[0] += a;
	buf[1] += b;
	buf[2] += c;
	buf[3] += d;
}

void md5_init(md5_ctx_t *ctx)
{
	ctx->i[0] = ctx->i[1] = (uint32_t) 0;

	/* Load magic initialization constants.
	 */
	ctx->buf[0] = (uint32_t) 0x67452301;
	ctx->buf[1] = (uint32_t) 0xefcdab89;
	ctx->buf[2] = (uint32_t) 0x98badcfe;
	ctx->buf[3] = (uint32_t) 0x10325476;
}

void md5_update(md5_ctx_t *ctx, const uint8_t *buf, size_t len)
{
	uint32_t in[16];
	int mdi, i, j;

	/* compute number of bytes mod 64 */
	mdi = (int)((ctx->i[0] >> 3) & 0x3F);

	/* update number of bits */
	if ((ctx->i[0] + ((uint32_t) len << 3)) < ctx->i[0])
		ctx->i[1]++;
	ctx->i[0] += ((uint32_t) len << 3);
	ctx->i[1] += ((uint32_t) len >> 29);

	while (len--) {
		/* add new character to buffer, increment mdi */
		ctx->in[mdi++] = *buf++;

		/* transform if necessary */
		if (mdi == 0x40) {
			for (i = 0, j = 0; i < 16; i++, j += 4)
				in[i] =
					(((uint32_t) ctx->in[j + 3]) << 24) |
					(((uint32_t) ctx->in[j + 2]) << 16) |
					(((uint32_t) ctx->in[j + 1]) << 8) |
					((uint32_t) ctx->in[j]);
			md5_transform(ctx->buf, in);
			mdi = 0;
		}
	}
}

void md5_finalize(md5_ctx_t *ctx)
{
	uint32_t in[16];
	int mdi, i, j, padlen;

	/* save number of bits */
	in[14] = ctx->i[0];
	in[15] = ctx->i[1];

	/* compute number of bytes mod 64 */
	mdi = (int)((ctx->i[0] >> 3) & 0x3F);

	/* pad out to 56 mod 64 */
	padlen = (mdi < 56) ? (56 - mdi) : (120 - mdi);
	md5_update(ctx, padding_, padlen);

	/* append length in bits and transform */
	for (i = 0, j = 0; i < 14; i++, j += 4)
		in[i] = (((uint32_t) ctx->in[j + 3]) << 24) |
			(((uint32_t) ctx->in[j + 2]) << 16) |
			(((uint32_t) ctx->in[j + 1]) << 8) |
			((uint32_t) ctx->in[j]);
	md5_transform(ctx->buf, in);

	/* store buffer in digest */
	for (i = 0, j = 0; i < 4; i++, j += 4) {
		ctx->digest[j] =
			(uint8_t)(ctx->buf[i] & 0xFF);
		ctx->digest[j + 1] =
			(uint8_t)((ctx->buf[i] >> 8) & 0xFF);
		ctx->digest[j + 2] =
			(uint8_t)((ctx->buf[i] >> 16) & 0xFF);
		ctx->digest[j + 3] =
			(uint8_t)((ctx->buf[i] >> 24) & 0xFF);
	}
}
