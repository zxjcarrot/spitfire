#include <ulib/util_log.h>

int main()
{
	ULIB_DEBUG("debug log");
	ULIB_WARNING("warning log");
	ULIB_NOTICE("notice log");
	ULIB_FATAL("fatal log");

	printf("passed\n");

	return 0;
}
