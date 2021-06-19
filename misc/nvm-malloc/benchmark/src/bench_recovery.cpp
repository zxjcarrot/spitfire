#include "common.h"

uint64_t payload_size_min = 64;
uint64_t payload_size_max = 64;

struct node_t {
    void *next;
    void *prev;
    void *data;
    char __padding[40]; // so the whole struct is 64 byte for fairness
};

void perform_allocations(const std::string root_id) {
    node_t *root = (node_t*) nvb::reserve_id(root_id, sizeof(node_t));
    root->next = nullptr;
    root->prev = nullptr;
    root->data = nullptr;
    nvb::persist(root, sizeof(node_t));
    nvb::activate_id(root_id);

    node_t *last_element = root;

    for (int i=0; i<1000; ++i) {
        // allocate new node
        node_t *new_node = (node_t*) nvb::reserve(sizeof(node_t));
        new_node->next = nullptr;
        new_node->prev = nvb::rel(last_element);
        new_node->data = nullptr;
        nvb::persist(new_node, sizeof(node_t));
        nvb::activate(new_node, &last_element->next, new_node);

        // allocate payload
        void *payload = nvb::reserve(256);
        *(int*)payload = i;
        nvb::persist(payload, 256);
        nvb::activate(payload, &new_node->data, payload);

        // remember new node as new last element
        last_element = new_node;
    }
}

/*void verify_recovery(const std::string root_id) {
    node_t *root = (node_t*) nvb::get_id(root_id);
    node_t *elem = root;

    int count = 0;
    while (true) {
        elem = (node_t*) nvb::abs(elem->next);
        if (*(int*)nvb::abs(elem->data) != count) {
            std::cout << "node " << count << " has incorrect payload!" << std::endl;
        }
        ++count;
        if (elem->next == 0)
            break;
    }
    std::cout << "verified recovery of " << count << " items for root '" << root_id << "'" << std::endl;
}*/

int main(int argc, char **argv) {
    if (argc < 3 || argc > 4) {
        std::cout << "usage: " << argv[0] << " <num_iterations> <payload_size_min> [payload_size_max]" << std::endl;
        return -1;
    }
    uint64_t n_iterations = atoi(argv[1]);
    payload_size_min = atoi(argv[2]);
    if (payload_size_min < 64) {
        std::cout << "WARNING: specified min payload size was less than minimum, using 64 bytes instead" << std::endl;
        payload_size_min = 64;
    }
    if (argc == 4) {
        payload_size_max = atoi(argv[3]);
        if (payload_size_max < payload_size_min) {
            std::cout << "WARNING: max payload size was less than min, using min instead" << std::endl;
            payload_size_max = payload_size_min;
        }
    } else {
        payload_size_max = payload_size_min;
    }

    nvb::timer timer;
    std::string rootId;

    // initialize nvm_malloc without recovery
    nvb::initialize("/mnt/pmfs/nvb", 0);

    // perform allocations and teardown
    for (uint64_t i=0; i<n_iterations; ++i) {
        rootId = "mylist" + std::to_string(i);
        perform_allocations(rootId);
    }

    nvb::teardown();

    // initialize nvm_malloc with recovery, stop the time
    timer.start();
    nvb::initialize("/mnt/pmfs/nvb", 1);
    uint64_t usec = timer.stop();
    std::cout << usec << std::endl;

    return 0;
}
