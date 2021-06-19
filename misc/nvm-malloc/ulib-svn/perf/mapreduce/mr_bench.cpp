/* The MIT License

   Copyright (C) 2012 Zilong Tan (eric.zltan@gmail.com)

   Permission is hereby granted, free of charge, to any person
   obtaining a copy of this software and associated documentation
   files (the "Software"), to deal in the Software without
   restriction, including without limitation the rights to use, copy,
   modify, merge, publish, distribute, sublicense, and/or sell copies
   of the Software, and to permit persons to whom the Software is
   furnished to do so, subject to the following conditions:

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
#include <getopt.h>
#include <ulib/util_timer.h>
#include <ulib/mr_dataset.h>
#include <ulib/mr_engine.h>
#include <ulib/hash_chain_r.h>

static const char *usage =
	"The Space-Sharing MapReduce Framework\n"
	"Copyright (C) 2012 Zilong Tan (eric.zltan@gmail.com)\n"
	"usage: %s [options]\n"
	"options:\n"
	"  -t<ntask>   - number of tasks, default is 1\n"
	"  -l<nlock>   - number of locks, default is 256\n"
	"  -k<nslot>   - number of slots, default is 10000000\n"
	"  -n<size>    - dataset size in elements, default is 10000000\n"
	"  -r<range>   - the range of value, default is 1000000\n"
	"  -s<exp>     - Zipf dataset parameter, default is 0\n"
	"  -w<file>    - output data set to file\n"
	"  -z	       - correctness check\n"
	"  -h	       - print this message\n";

using namespace ulib;
using namespace ulib::mapreduce;

template<typename R, typename K, typename V>
class wc_mapper : public mapper<R, K, V> {
public:
	void
	operator()(const R &rec)
	{ this->emit(rec, 1); }
};

int main(int argc, char *argv[])
{
	int    oc;
	int    ntask = 1;
	int    nlock = 256;
	size_t nslot = 10000000;
	int    range = 1000000;
	float  s     = 0.0;
	size_t size  = 10000000;
	bool   check = false;
	char  * file = NULL;

	while ((oc = getopt(argc, argv, "t:l:k:n:r:s:w:zh")) != -1) {
		switch (oc) {
		case 't':
			ntask = atoi(optarg);
			break;
		case 'l':
			nlock = atoi(optarg);
			break;
		case 'k':
			nslot = strtoul(optarg, 0, 10);
			break;
		case 'n':
			size  = strtoul(optarg, 0, 10);
			break;
		case 'r':
			range = atoi(optarg);
			break;
		case 's':
			s     = atof(optarg);
			break;
		case 'w':
			file = optarg;
			break;
		case 'z':
			check = true;
			break;
		case 'h':
			printf(usage, argv[0]);
			exit(EXIT_SUCCESS);
		default:
			exit(EXIT_FAILURE);
		}
	}

	typedef dataset_zipf DS;
	typedef wc_mapper<DS::record_type, int, size_t> MAPPER;
	typedef typical_job <MAPPER, DS> JOB;
	typedef JOB::reducer_type REDUCER;
	typedef JOB::result_type  RESULT;
	// for verification
	typedef chain_hash_map<RESULT::key_type, RESULT::value_type> CMAP;

	// three elements of a computation
	DS     dataset(size, range, s);
	RESULT result(nslot, nlock);
	JOB    job(result, dataset);

	timespec timer;
	timer_start(&timer);
	job.exec(ntask);
	float elapsed = timer_stop(&timer);

	// print job info
	printf("ntask=%d, nlock=%d, nslot=%lu, range=%d, s=%f, size=%lu, elapsed=%f\n",
	       ntask, nlock, nslot, range, s, size, elapsed);

	if (file) {
		FILE *fp = fopen(file, "wb");
		if (fp == NULL) {
			fprintf(stderr, "cannot open %s\n", file);
			exit(EXIT_FAILURE);
		}
		for (DS::const_iterator it = dataset.begin(); it != dataset.end(); ++it) {
			DS::record_type r = *it;
			fwrite(&r, sizeof(r), 1, fp);
		}
		fclose(fp);
	}

	if (check) {
		CMAP map(nslot);
		MAPPER m;
		fprintf(stderr, "Preparing correctness check ...\t");
		timer_start(&timer);
		for (DS::const_iterator it = dataset.begin(); it != dataset.end(); ++it) {
			m(*it);
			for (MAPPER::iterator mit = m.begin(); mit != m.end(); ++mit)
				REDUCER(map[mit->first]) += mit->second;
			m.reset();
		}
		elapsed = timer_stop(&timer);
		fprintf(stderr, "%f sec\n", elapsed);
		// perform forward and backward checks to guarantee
		// that the result set is exactly the same as the
		// CMAP.
		for (RESULT::const_iterator it = result.begin(); it != result.end(); ++it) {
			if (it.value() != map[it.key()]) {
				fprintf(stderr, "expect %lu, actual %lu for key %d\n",
					map[it.key()], it.value(), it.key().key());
				exit(EXIT_FAILURE);
			}
		}
		fprintf(stderr, "Forward check OK\n");
		for (CMAP::const_iterator it = map.begin(); it != map.end(); ++it) {
			if (it.value() != result[it.key()]) {
				fprintf(stderr, "expect %lu, actual %lu for key %d\n",
					result[it.key()], it.value(), it.key().key());
				exit(EXIT_FAILURE);
			}
		}
		fprintf(stderr, "Backward check OK\n");
	}

	return 0;
}
