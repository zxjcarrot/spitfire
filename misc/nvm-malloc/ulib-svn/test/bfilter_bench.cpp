#include <time.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <ulib/bfilter.h>
#include <ulib/math_rand_prot.h>

uint64_t u, v, w;
#define myrand()  RAND_NR_NEXT(u, v, w)

const char *usage =
	"%s [ins] [get]\n";

volatile long counter = 0;

static void sig_alarm_handler(int)
{
	printf("%ld per sec\n", counter);
	counter = 0;
	alarm(1);
}

void register_sig_handler()
{
	struct sigaction sigact;

	sigact.sa_handler = sig_alarm_handler;
	sigact.sa_flags = 0;
	if (sigaction(SIGALRM, &sigact, NULL)) {
		perror("sigaction");
		exit(-1);
	}
	alarm(1);
}

void constant_insert(long ins, long get)
{
	long t;

	struct bloom_filter bf;

	if (bfilter_create(&bf, 4 * ins, ins)) {
		fprintf(stderr, "alloc failed\n");
		return;
	}
	bfilter_zero(&bf);

	for (t = 0; t < ins; t++) {
		uint64_t num = myrand();
		bfilter_set(&bf, &num, sizeof(num));
		++counter;
	}

	printf("insertion done\n");

	for (t = 0; t < get; t++) {
		uint64_t num = myrand();
		bfilter_get(&bf, &num, sizeof(num));
		++counter;
	}

	printf("all done\n");
}

int main(int argc, char *argv[])
{
	long ins = 5000000;
	long get = 50000000;
	uint64_t seed = time(NULL);

	if (argc > 1)
		ins = atol(argv[1]);
	if (argc > 2)
		get = atol(argv[2]);

	RAND_NR_INIT(u, v, w, seed);

	register_sig_handler();

	constant_insert(ins, get);

	printf("passed\n");

	return 0;
}
