#include <stdio.h>
#include <assert.h>
#include <ulib/hash_chain.h>

using namespace ulib;

int main()
{
	chain_hash_map<int, int> ch(100);

	ch.find_or_insert(1,2);

	ch[1] = 2;
	chain_hash_map<int, int>::const_iterator it =
		ch.find(1);
	assert(it != ch.end());
	assert(it.key() == 1);
	assert(it.value() == 2);
	assert(ch[1] == 2);
	assert(ch[2] == 0);

	ch[40910] = 1;
	ch[1001] = 4;

	for (it = ch.begin(); it != ch.end(); ++it)
		printf("%d\t%d\n", it.key(), it.value());

	for (chain_hash_map<int, int>::iterator t = ch.begin(); t != ch.end(); t++)
		printf("%d\t%d\n", t.key(), t.value());

	printf("sort without snapshot:\n");
	ch.sort();
	for (chain_hash_map<int, int>::iterator t = ch.begin(); t != ch.end(); t++)
		printf("%d\t%d\n", t.key(), t.value());

	printf("sort after snapshot:\n");
	ch.snap();
	ch.sort();
	for (chain_hash_map<int, int>::iterator t = ch.begin(); t != ch.end(); t++)
		printf("%d\t%d\n", t.key(), t.value());

	printf("inserting an element after snapshot:\n");
	ch[999] = 10;
	for (chain_hash_map<int, int>::iterator t = ch.begin(); t != ch.end(); t++)
		printf("%d\t%d\n", t.key(), t.value());

	printf("inserting an element after snapshot, and re-snap:\n");
	ch.snap();
	for (chain_hash_map<int, int>::iterator t = ch.begin(); t != ch.end(); t++)
		printf("%d\t%d\n", t.key(), t.value());

	printf("when copied:\n");
	chain_hash_map<int, int> new_ch = ch;
	for (chain_hash_map<int, int>::iterator t = new_ch.begin(); t != new_ch.end(); t++)
		printf("%d\t%d\n", t.key(), t.value());

	ch.clear();

	for (chain_hash_map<int, int>::iterator t = ch.begin(); t != ch.end(); t++)
		printf("%d\t%d\n", t.key(), t.value());

	printf("passed\n");

	return 0;
}
