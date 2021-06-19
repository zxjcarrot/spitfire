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

#ifndef _ULIB_CRYPT_MD5_H
#define _ULIB_CRYPT_MD5_H

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

#include <stddef.h>
#include <stdint.h>

/* Data structure for MD5 (Message Digest) computation */
typedef struct {
	uint32_t i[2];	      /* number of _bits_ handled mod 2^64 */
	uint32_t buf[4];      /* scratch buffer */
	uint8_t in[64];	      /* input buffer */
	uint8_t digest[16];   /* actual digest after md5_finalize call */
} md5_ctx_t;

#define MD5_DIGEST(ctx) ((ctx)->digest)

#ifdef __cplusplus
extern "C" {
#endif

void md5_init(md5_ctx_t *ctx);
void md5_update(md5_ctx_t *ctx, const uint8_t *in, size_t len);
void md5_finalize(md5_ctx_t *ctx);

#ifdef __cplusplus
}
#endif

#endif	/* _ULIB_CRYPT_MD5_H */
