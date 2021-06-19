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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <hash_func.h>
#include <hash_open_prot.h>
#include "util_argv.h"

#define DEF_PROMPT "ULIB CMD> "
#define READ_BUFLEN 1024

static uint32_t
__hash_fast32_str(const char *s)
{
	return hash_fast64(s, strlen(s), 0);
}

#define STRHASH(s)   __hash_fast32_str(s)
#define STRCMP(x, y) (strcmp(x, y) == 0)

typedef int (*console_fcn_t) (int argc, const char *argv[]);

DEFINE_OPENHASH(cmdoht, const char *, console_fcn_t, 1, STRHASH, STRCMP);

typedef struct {
	openhash_t(cmdoht) * idx;
	char * pmpt;
	char * rbuf;
	int    rfd;
	int    rbuflen;
} console_t;

int console_init(console_t *ctx)
{
	ctx->rfd = STDIN_FILENO;
	ctx->rbuflen = READ_BUFLEN;
	ctx->rbuf = (char *) malloc(READ_BUFLEN);
	if (ctx->rbuf == NULL)
		return -1;
	ctx->pmpt = strdup(DEF_PROMPT);
	if (ctx->pmpt == NULL) {
		free(ctx->rbuf);
		return -1;
	}
	ctx->idx = openhash_init(cmdoht);
	if (ctx->idx == NULL) {
		free(ctx->rbuf);
		free(ctx->pmpt);
		return -1;
	}
	return 0;
}

int console_pmpt(console_t *ctx, const char *pmpt)
{
	free(ctx->pmpt);
	ctx->pmpt = strdup(pmpt);
	if (ctx->pmpt)
		return 0;
	return -1;
}

int console_bind(console_t *ctx, const char *cmdlet, console_fcn_t f)
{
	int ret = 0;
	char *str;
	uint32_t val;

	str = strdup(cmdlet);
	if (str == NULL)
		return -1;
	val = openhash_set(cmdoht, ctx->idx, str, &ret);
	if (val == openhash_end(ctx->idx) || ret == 0) {
		free(str);
		return -1;
	}
	openhash_value(ctx->idx, val) = f;
	return 0;
}

int console_exec(console_t *ctx, const char *cmd)
{
	int argc;
	int ret = -1;
	uint32_t val;
	console_fcn_t fcn;
	char **argv = argv_split(cmd, &argc);

	if (argv == NULL)
		return -1;
	if (argc > 0) {
		val = openhash_get(cmdoht, ctx->idx, argv[0]);
		if (val == openhash_end(ctx->idx))
			goto done;
		fcn = openhash_value(ctx->idx, val);
		ret = fcn(argc, (const char **)argv);
	} else
		ret = 0;
done:
	argv_free(argv);
	return ret;
}

static int __readline(console_t *ctx)
{
	int ret;
	int s = 0;
	char *p;

	while ((ret = read(ctx->rfd, &ctx->rbuf[s], 1)) > 0) {
		if (ctx->rbuf[s] == '\n') {
			ctx->rbuf[s + 1] = '\0';
			return 0;
		}
		if (s >= ctx->rbuflen - 2) {
			p = (char *) realloc(ctx->rbuf, ctx->rbuflen * 2);
			if (p == NULL)
				return -1;
			ctx->rbuf = p;
			ctx->rbuflen *= 2;
		}
		s++;
	}
	if (ret == 0)
		ctx->rbuf[s] = '\0';
	return ret;
}

int console_loop(console_t *ctx, int count, const char *term)
{
	char **argv;
	int argc;
	uint32_t val;
	console_fcn_t fcn;

	printf("%s", ctx->pmpt);
	fflush(stdout);

	while (!__readline(ctx) && ctx->rbuf[0] && count) {
		ctx->rbuf[strlen(ctx->rbuf) - 1] = '\0';  /* remove '\n' */
		if (count > 0)
			count--;
		argv = argv_split(ctx->rbuf, &argc);
		if (argv == NULL)
			return -1;
		if (argc > 0) {
			if (strcmp(argv[0], term) == 0) {
				argv_free(argv);
				return 0;
			}
			val = openhash_get(cmdoht, ctx->idx, argv[0]);
			if (val != openhash_end(ctx->idx)) {
				fcn = openhash_value(ctx->idx, val);
				fcn(argc, (const char **)argv);
			} else
				printf("%s command not found\n", argv[0]);
		}
		argv_free(argv);
		printf("%s", ctx->pmpt);
		fflush(stdout);
	}

	return 0;
}

void console_destroy(console_t *ctx)
{
	uint32_t i;
	free(ctx->rbuf);
	free(ctx->pmpt);
	for (i = openhash_begin(ctx->idx); i != openhash_end(ctx->idx); i++) {
		if (openhash_exist(ctx->idx, i))
			free((char *)openhash_key(ctx->idx, i));
	}
	openhash_destroy(cmdoht, ctx->idx);
}
