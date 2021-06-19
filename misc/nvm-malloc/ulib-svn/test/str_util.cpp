#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <ulib/str_util.h>

int main()
{
	const char *str =
		" hello  world string ___GARBAGE___";
	char field[6];
	char large[100];
	assert(getfield(str, str, 0, field, 0, ' ') == str);
	assert(getfield(str, str + strlen(str), 0, NULL, 0, ' ') == str);
	assert(getfield(str, str + strlen(str), 1, NULL, sizeof(field), ' ') == str + 1);
	assert(getfield(str, str + strlen(str), 1, field, sizeof(field), ' ') == str + 1);
	assert(strcmp(field, "hello") == 0);
	assert(getfield(str, str + strlen(str), 2, field, sizeof(field), ' ') == str + 7);
	assert(field[0] == 0);
	assert(getfield(str, str + strlen(str), 4, field, sizeof(field), ' ') == str + 14);
	assert(strcmp(field, "strin") == 0);  // yes, "strin" without 'g'
	assert(getfield(str, str + strlen(str), 5, field, sizeof(field), ' ') == str + 21);
	assert(getfield(str, str + strlen(str), 5, large, sizeof(large), ' ') == str + 21);
	assert(strcmp(large, "___GARBAGE___") == 0);
	assert(getfield(str, str + strlen(str), 6, field, sizeof(field), ' ') == str + strlen(str));
	printf("passed\n");

	return 0;
}
