/* The MIT License

   Copyright (C) 2011, 2012, 2013 Zilong Tan (eric.zltan@gmail.com)

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

#ifndef _ULIB_STR_UTIL_H
#define _ULIB_STR_UTIL_H

#ifdef __cplusplus
extern "C" {
#endif

/* Return the start of the next line (deliminated by either '\n' or
 * '\0') and set the delimitor of the next line, if any, to '\0'. NULL
 * will be returned if no next line exists. */
char *nextline(char *buf, long size);

/* Get the id-th field (0-based) delimited by @delim. If @field is not
 * NULL, @field will hold a copy of up to @flen bytes of the field
 * including an appended '\0' in the end. */
const char *getfield(const char *from, const char *end,
		     int id, char *field, int flen, int delim);

#ifdef __cplusplus
}
#endif

#endif	/* _ULIB_STR_UTIL_H */
