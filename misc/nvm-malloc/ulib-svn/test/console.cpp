#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <ulib/util_console.h>

int g_ok = 0;

int cmd_set_ok(int, const char **)
{
	g_ok = 1;

	return 0;
}

int cmd_return_error(int, const char **)
{
	return -1;
}

int cmd_param_ok(int argc, const char **argv)
{
	if (argc == 2 && strcmp(argv[1], "hello") == 0)
		return 0;

	return -1;
}

int main()
{
	console_t con;

	assert(console_init(&con) == 0);
	assert(console_bind(&con, "set_ok", cmd_set_ok) == 0);
	assert(console_bind(&con, "err", cmd_return_error) == 0);
	assert(console_bind(&con, "param", cmd_param_ok) == 0);

	// verify that cmd_set_ok is called
	assert(console_exec(&con, "set_ok") == 0);
	assert(g_ok == 1);

	// verify that cmd_return_error is called
	// should return -1 since cmdfunc would fail
	assert(console_exec(&con, "err") == -1);

	// calling nonexisted command returns -1
	assert(console_exec(&con, "nonexist") == -1);

	// verify that parameter is parsed correctly
	assert(console_exec(&con, "param hello") == 0);

	console_destroy(&con);

	printf("passed\n");

	return 0;
}
