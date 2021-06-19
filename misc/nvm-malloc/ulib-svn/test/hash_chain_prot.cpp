#include <stdio.h>
#include <assert.h>
#include <ulib/hash_chain_prot.h>

DEFINE_CHAINHASH(my, int, int, 1, chainhash_hashfn, chainhash_equalfn, chainhash_cmpfn);

int main()
{
	/* sanity check */
	int exist;
	chainhash_t(my) *ch = chainhash_init(my, 100);
	chainhash_destroy(my, ch);
	ch = chainhash_init(my, 1);
	chainhash_itr_t(my) it = chainhash_set(my, ch, 1);
	assert(!chainhash_end(it));
	assert(chainhash_key(it) == 1);
	chainhash_value(my, it) = 2;
	it = chainhash_get(my, ch, 1);
	assert(!chainhash_end(it));
	assert(chainhash_key(it) == 1);
	assert(chainhash_value(my, it) == 2);
	it = chainhash_set_uniq(my, ch, 1, &exist);
	assert(!chainhash_end(it));
	assert(exist);
	assert(chainhash_key(it) == 1);
	chainhash_value(my, it) = 3;
	it = chainhash_get(my, ch, 1);
	assert(!chainhash_end(it));
	assert(chainhash_key(it) == 1);
	assert(chainhash_value(my, it) == 3);
	it = chainhash_set_uniq(my, ch, 2, &exist);
	assert(!chainhash_end(it));
	assert(!exist);
	assert(chainhash_key(it) == 2);
	chainhash_value(my, it) = 1;
	it = chainhash_get(my, ch, 2);
	assert(!chainhash_end(it));
	assert(chainhash_key(it) == 2);
	assert(chainhash_value(my, it) == 1);
	chainhash_del(my, it);
	it = chainhash_get(my, ch, 2);
	assert(chainhash_end(it));
	it = chainhash_set(my, ch, 1);
	assert(!chainhash_end(it));
	chainhash_clear(my, ch);
	it = chainhash_get(my, ch, 1);
	assert(chainhash_end(it));
	chainhash_destroy(my, ch);

	/* snapshot test */
	ch = chainhash_init(my, 3);
	it = chainhash_set(my, ch, 3);
	assert(!chainhash_end(it));
	chainhash_value(my, it) = 1;
	it = chainhash_set(my, ch, 7);
	assert(!chainhash_end(it));
	chainhash_value(my, it) = 2;
	it = chainhash_set(my, ch, 5);
	assert(!chainhash_end(it));
	chainhash_value(my, it) = 3;
	it = chainhash_set(my, ch, 10);
	assert(!chainhash_end(it));
	chainhash_value(my, it) = 4;
	it = chainhash_set(my, ch, 0);
	assert(!chainhash_end(it));
	chainhash_value(my, it) = 5;
	it = chainhash_set(my, ch, 1);
	assert(!chainhash_end(it));
	chainhash_value(my, it) = 6;
	it = chainhash_set(my, ch, 5);
	assert(!chainhash_end(it));
	chainhash_value(my, it) = 7;
	printf("unordered iteration:\n");
	for (it = chainhash_begin(my, ch); !chainhash_end(it);) {
		printf("%d\t%d\n",
		       chainhash_key(it),
		       chainhash_value(my, it));
		if (chainhash_advance(my, &it))
			break;
	}
	chainhash_sort(my, ch);
	printf("before snapshot, sorted iteration:\n");
	for (it = chainhash_begin(my, ch); !chainhash_end(it);) {
		printf("%d\t%d\n",
		       chainhash_key(it),
		       chainhash_value(my, it));
		if (chainhash_advance(my, &it))
			break;
	}
	chainhash_snap(my, ch);
	chainhash_sort(my, ch);
	printf("after snapshot, sorted iteration:\n");
	for (it = chainhash_begin(my, ch); !chainhash_end(it);) {
		printf("%d\t%d\n",
		       chainhash_key(it),
		       chainhash_value(my, it));
		if (chainhash_advance(my, &it))
			break;
	}

	chainhash_destroy(my, ch);
	printf("passed\n");

	return 0;
}
