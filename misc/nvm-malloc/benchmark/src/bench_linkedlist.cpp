#include "common.h"

#include <atomic>
#include <cstdio>
#include <cstring>
#include <sstream>

std::vector<uint64_t> workerTimes;
uint64_t payload_size_min = 64;
uint64_t payload_size_max = 64;

struct node_t {
    void *next;
    void *prev;
    void *data;
    char __padding[40]; // so the whole struct is 64 byte for fairness
};

void worker(int id) {
    uint64_t idx = 0;
    volatile node_t *root = nullptr;
    nvb::timer timer;
    std::default_random_engine generator;
    std::uniform_int_distribution<uint64_t> distribution(payload_size_min, payload_size_max);
    auto randomPayload = std::bind(distribution, generator);

    // run the benchmark
    timer.start();
    for (idx=0; idx<100000; ++idx) {
        // allocate new node
        volatile node_t *new_node = (volatile node_t*) nvb::reserve(sizeof(node_t));
        new_node->next = nvb::rel((void*)root);
        new_node->prev = nullptr;
        new_node->data = nullptr;
        nvb::persist((void*)new_node, sizeof(node_t));
        if (root)
            nvb::activate((void*)new_node, (void**) &root->prev, (void*) new_node);
        else
            nvb::activate((void*)new_node);

        // allocate payload
        volatile void *payload = nvb::reserve(randomPayload());
        memset((void*) payload, 5, 64);
        nvb::activate((void*)payload, (void**) &new_node->data, (void*) payload);

        // set new node as root
        root = new_node;
    }

    // store result
    workerTimes[id] = timer.stop();
}

int main(int argc, char **argv) {
    if (argc < 3 || argc > 4) {
        std::cout << "usage: " << argv[0] << " <num_threads> <payload_size_min> [<payload_size_max>]" << std::endl;
        return -1;
    }
    size_t n_threads = atoi(argv[1]);
    payload_size_min = atoi(argv[2]);
    if (payload_size_min < 64) {
        std::cout << "WARNING: specified allocation size was less than minimum, using 64 bytes instead" << std::endl;
        payload_size_min = 64;
    }
    if (argc == 4) {
        payload_size_max = atoi(argv[3]);
    } else {
        payload_size_max = payload_size_min;
    }
    workerTimes.resize(n_threads, 0);
    nvb::initialize("/mnt/pmfs/nvb", 0);
    nvb::execute_in_pool(worker, n_threads);
    uint64_t avg = 0;
    for (auto t : workerTimes)
        avg += t;
    avg /= n_threads;
    std::cout << avg << std::endl;
    return 0;
}
