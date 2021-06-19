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

#define _XOPEN_SOURCE 500

#include <unistd.h>
#include <sys/stat.h>
#include "search_line.h"
#include "util_algo.h"
#include "str_util.h"

static inline char *
__seekline(char *base, int len)
{
	char *head = nextline(base, len);
	char *end;
	if (head == NULL)
		return NULL;
	end = nextline(head, len - (head - base));
	if (end == NULL)
		return NULL;
	return head;
}

static inline ssize_t
__findline(int fd, int (*comp) (const char *, void *),
	   void *param, size_t low, size_t high, int maxlen)
{
	size_t s, t, m;
	ssize_t nb;
	char buf[maxlen * 2];  /* including one extra byte for '\0' */
	char *line;
	int len = 0;

	if (low >= high)
		return -1;

	buf[0] = '\0';

	for (s = low, t = high; s < t;) {
		m = (s + t) / 2;
		/* the last byte of buf is reserved */
		nb = pread(fd, buf, sizeof(buf) - 1, m);
		if (nb <= 0) {
			t = m;
			continue;
		}

		/* one extra byte is included to get nextline work */
		len = (int) _min((ssize_t)(high - m), nb) + 1;

		line = __seekline(buf, len);
		if (line == NULL) {
			t = m;
			continue;
		}

		int cmp = comp(line, param);
		if (cmp == 0)
			return m + (line - buf);
		if (cmp < 0)
			s = m + 1;
		else
			t = m;
	}

	line = nextline(buf, len);
	if (line && comp(buf, param) == 0)
		return low;

	return -1;
}

ssize_t findline(int fd, int (*comp) (const char *, void *),
		 void *param, int maxlen)
{
	struct stat state;

	if (fstat(fd, &state))
		return -1;

	return __findline(fd, comp, param, 0, state.st_size, maxlen);
}

ssize_t findfirstline(int fd, int (*comp) (const char *, void *),
		      void *param, int maxlen)
{
	struct stat state;
	ssize_t pos;

	if (fstat(fd, &state))
		return -1;

	ssize_t init_pos = __findline(fd, comp, param, 0, state.st_size, maxlen);

	while (init_pos > 0) {
		pos = __findline(fd, comp, param, 0, init_pos, maxlen);
		if (pos == -1)
			break;
		init_pos = pos;
	}

	return init_pos;
}
