/* The MIT License

   Copyright (C) 2011, 2012 Zilong Tan (eric.zltan@gmail.com)

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

#ifndef _ULIB_UTIL_CONSOLE_H
#define _ULIB_UTIL_CONSOLE_H

#define DEF_PROMPT "ULIB CMD> "

/*
 * cmdlet function prototype
 */
typedef int (*console_fcn_t) (int argc, const char *argv[]);

typedef struct {
	void * idx;
	char * pmpt;
	char * rbuf;
	int    rfd;
	int    rbuflen;
} console_t;

#ifdef __cplusplus
extern "C" {
#endif

int console_init(console_t *ctx);
int console_pmpt(console_t *ctx, const char *pmpt);
int console_bind(console_t *ctx, const char *cmdlet, console_fcn_t f);
int console_exec(console_t *ctx, const char *cmd);

/* enter command processing loop
 * @count: how many commands to process, -1 for infinite
 * @term:  terminating command */
int console_loop(console_t *ctx, int count, const char *term);

void console_destroy(console_t *ctx);

#ifdef __cplusplus
}
#endif

#endif	/* _ULIB_UTIL_CONSOLE_H */
