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
 * Copyright (C) 1998, 1999, 2000, 2001 Free Software Foundation, Inc.
 * Copyright (C) 2004 g10 Code GmbH
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.	 See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 */

/* SHA-1 coden take from gnupg 1.3.92.

   Note, that this is a simple tool to be used for MS Windows.
*/

#ifndef _ULIB_CRYPT_SHA1_H
#define _ULIB_CRYPT_SHA1_H

#include <stddef.h>
#include <stdint.h>

typedef struct {
	uint32_t  h0, h1, h2, h3, h4;
	uint32_t  nblocks;
	uint8_t buf[64];
	int  count;
} sha1_ctx_t;

/* returns a pointer to the 20 bytes digest */
#define SHA1_DIGEST(ctx) ((ctx)->buf)

#ifdef __cplusplus
extern "C" {
#endif

void sha1_init(sha1_ctx_t *hd);
void sha1_update(sha1_ctx_t *hd, const uint8_t *in, size_t len);
void sha1_finalize(sha1_ctx_t *hd);

#ifdef __cplusplus
}
#endif

#endif	/* _ULIB_CRYPT_SHA1_H */
