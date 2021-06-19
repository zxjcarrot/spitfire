#include <stdio.h>
#include <unistd.h>
#include <assert.h>
#include <ulib/os_thread.h>

volatile static bool success = false;

class mythread : public ulib::thread {
public:
	int run() {
		for (; _once || is_running();) {
			_once	= false;
			success = true;
			sleep(1);
		}
		return 0;
	}

	static volatile bool _once;
};

volatile bool mythread::_once = true;

int main()
{
	mythread thd;

	assert(thd.start() == 0);
	assert(thd.stop_and_join() == 0);
	if (success)
		printf("passed\n");
	else
		printf("failure\n");

	return 0;
}
