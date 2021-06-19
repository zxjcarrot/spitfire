#include <time.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <ulib/hash_chain_prot.h>
#include <ulib/math_rand_prot.h>

uint64_t u, v, w;
#define myrand()  RAND_NR_NEXT(u, v, w)

const char *usage =
	"%s [ins] [get]\n";

volatile long counter = 0;

DEFINE_CHAINHASH(myhash, uint64_t, uint64_t, 1, chainhash_hashfn, chainhash_equalfn, chainhash_cmpfn)

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

	chainhash_t(myhash) *my = chainhash_init(myhash, ins);

	if (my == NULL) {
		fprintf(stderr, "alloc failed\n");
		return;
	}

	for (t = 0; t < ins; t++) {
		chainhash_itr_t(myhash) itr =
			chainhash_set(myhash, my, myrand());
		if (!chainhash_end(itr))
			chainhash_value(myhash, itr) = t;
		counter++;
	}

	printf("insertion done\n");

	for (t = 0; t < get; t++) {
		chainhash_get(myhash, my, myrand());
		counter++;
	}

	chainhash_destroy(myhash, my);

	printf("all done\n");
}

int main(int argc, char *argv[])
{
	long ins = 1000000;
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
