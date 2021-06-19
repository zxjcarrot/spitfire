nvm_malloc
==========

nvm_malloc is a guaranteed failure-atomic memory allocator for NVRAM with builtin ultra fast crash recovery.

# Prerequisites

Currently nvm_malloc is only compatible with Linux (tested on Ubuntu 12.04) due to reliance on the ulib library. To simulate NVRAM, a custom kernel with PMFS support is also required (https://github.com/linux-pmfs/pmfs).

# Installation

    $ make {debug|release}

# Usage

## Initializing nvm_malloc

Any application using nvm_malloc must first initialize it by providing a path to a working directory on PMFS and specify whether existing data - if any - should be overwritten

```c
#include <nvm_malloc.h>

nvm_initialize("/path/to/workingdir/", 0) /* initialize empty */
```

or recovered

```c
#include <nvm_malloc.h>

nvm_initialize("/path/to/workingdir/", 1) /* recover if possible, otherwise initialize empty */
```

## Relative and absolute pointers

nvm_malloc will ensure that a contiguous region of virtual addresses is reserved for NVRAM. However, the start address of the region is non-deterministic. If contents are recovered after a restart and the start address changes, any pointer variable stored on NVRAM would be invalidated. To solve this problem, any pointer variable stored on NVRAM must use a relative addressing scheme. nvm_malloc provides shorthand methods for conversion:

```c
void *absolute_pointer = /* ... */;

void *relative_pointer = nvm_rel(absolute_pointer);

absolute_pointer == nvm_abs(relative_pointer); /* --> true */
```

## Explicit persistency

Pretty much every modern CPU uses a hierarchy of caches and all updates to data - both on DRAM and NVRAM - will be performed within the caches. Cache lines are written back to physical memory in a non-deterministic fashion and this is a problem if we need to ensure that our changes reached physical NVRAM before we continue. Currently the only available option is to explicitly issue a cache line flush, which evicts a cache line and triggers a write back to memory. The unfortunate downside of this method is that subsequent accesses to the same data lose the advantage of the cache and must pay the penalty of direct memory access. nvm_malloc provides a shorthand method to flush data onto NVRAM:

```c
void nvm_persist(void *ptr, uint64_t n_bytes);
```

## Allocating persistent regions

One major problem with persistent memory is that allocated regions must be tracked at all times to avoid permanent memory leaks. Simultaneously, regions should be initialized before persistently linked into data structures to avoid costly sanity checks on recovery of an application. For this purpose, allocations in nvm_malloc are split into two distinct steps: reserve and activate. The reserve step performs the "classic" task of memory allocation by finding a suitable free region but does not mark it as used on NVRAM. Now the application can initialize the region, followed by the activation step which permanently marks it as used and establishes links to the region through either link pointers or named identifiers.

This concept is best explained by example. Say, you want to create a simple persistent doubly-linked list with elements looking like

```c
typedef struct {
    void *previous;
    void *next;
    void *data;
} node_t;
```

The first mode of allocation is allocation by ID. Here, a unique string identifier is passed to nvm_malloc along with the requested size:

```c
void* nvm_reserve_id(const char *id, uint64_t size);
void  nvm_activate_id(const char *id);
```

This is meant for "root objects", in our example the root element of the linked list:


```c
/* step 1: reserve space for the root node and assign it our ID */
void *root = nvm_reserve_id("myLinkedList", sizeof(node_t));

/* step 2: initialize the memory */
root->previous = NULL;
root->next = NULL;
root->data = NULL;

/* step 3: persist our modifications on NVRAM */
nvm_persist(root, sizeof(node_t));

/* step 4: until this point, the region would be discarded after a crash, so we need to activate it */
nvm_activate_id("myLinkedList");
```

Once nvm_activate_id returns, the region pointe to by ```root``` is guaranteed to be persisted and referenced by the specified ID. The activation method itself also works in a failure-atomic fashion, meaning that - should it be interrupted by a crash - the recovery upon restart will either fully replay or undo the allocation.

Since not every allocation needs to be named (here we only want an ID for the ```root``` element), nvm_malloc provides a second mode of allocation:

```c
void* nvm_reserve(uint64_t size);
void  nvm_activate(void *ptr, void **link_ptr1, void *target_val1, void **link_ptr2, void *target_val2)
```

Since no ID is passed that can be used to establish a reliable, persistent link, the application must provide up to two link pointers and target values to the activation call. Let's allocate another element of the list:

```c
/* step 1: reserve space for the node */
void *next_node = nvm_reserve(sizeof(node_t));

/* step 2: initialize the memory */
next_node->previous = nvm_rel(root); /* NOTE: use the relative addresses whenever storing pointers */
next_node->next = NULL;
next_node->data = NULL;

/* step 3: persist our modifications on NVRAM */
nvm_persist(next_node, sizeof(node_t));

/* step 4: activate the region and use root's next pointer as a link pointer */
nvm_activate(next_node, &root->next, next_node, NULL, NULL); /* if NULL is used for link_ptr1 or link_ptr2, it will be ignored */
```

In this example, we provided ```root->next``` as the link pointer and ```next_node``` as the target value. Note that no conversion to relative pointers is necessary here, nvm_malloc does that internally. Again, ```nvm_activate``` is failure-atomic and guarantees that either all changes will be undone or otherwise ```next_node``` is persisted on NVRAM and ```root->next``` will point to ```next_node```.

## Deallocation

Similar to the allocation concept, deallocations must ensure proper linkage amongst all non-volatile regions. Since a to-be-freed region is already initialized, a single call is sufficient though. Deallocations also work on either IDs or by providing link pointers that will be set atomically:

```c
void nvm_free_id(const char *id);
void nvm_free(void *ptr, void **link_ptr1, void *target_val1, void **link_ptr2, void *target_val2);
```

Providing link pointers is necessary if, for instance, we free a node within our doubly linked list and need to ensure that the neighboring elements reference each other once the node in between is properly deleted. Let's say we want to deallocate the ```next_node``` from the previous example:

```c
nvm_free(next_node, &root->next, NULL, NULL, NULL);
```

After ```nvm_free``` returns, ```root->next``` will point to ```NULL```, the second link pointer/target pair is ignored.

## Recovery

Persistent allocations are meaningless if we cannot retrieve former allocations. The recovery concept of nvm_malloc is contained within the named allocations, which allow for constant-time retrieval of persisted regions at any point in time via

```c
void* nvm_get_id(const char *id);
```

To complete our doubly-linked list example, let's implement proper recovery:

```c
nvm_initialize("/path/to/workingdir/", 1); /* enable the recovery flag */

node_t *root = nvm_get_id("myLinkedList");
```

That's all. For traversal of this linked list, keep in mind that all pointers are relative. The following code would achieve a full traversal of the list:

```c
node_t *current_node = root;
while (current_node != NULL) {
    /* do something with current_node */
    current_node = nvm_abs(current_node->next);
}
```

# Benchmarks

In order to run the benchmarks for nvm_malloc, build both the library and benchmark binaries and execute the Python script:

    $ make release
    $ cd benchmark
    $ make release
    $ python run_benchmarks.py <args>

The Python script will create plots in ./benchmark/plots and accepts the following arguments:

```
-h, --help                 show this help message and exit
--run-all                  run and plot all benchmarks
--run-alloc-free           run and plot the alloc-free benchmark
--run-alloc-free-alloc     run and plot the alloc-free-alloc benchmark
--run-fastalloc            run and plot the fastalloc benchmark
--run-linkedlist           run and plot the linked list benchmark
--run-recovery             run and plot the recovery benchmark
--threads-min THREADS_MIN  run benchmarks for at least THREADS_MIN threads (default: 1)
--threads-max THREADS_MAX  run benchmarks for at most THREADS_MAX threads (default: 20)
--payload-min PAYLOAD_MIN  allocate at least PAYLOAD_MIN bytes per allocation (default: 64)
--payload-max PAYLOAD_MAX  allocate at most PAYLOAD_MAX bytes per allocation (default: 64, must be >= PAYLOAD_MIN)
--with-jemalloc            include jemalloc in the benchmarks
--with-nofence             include a run with fences disabled
--with-noflush             include a run with flushes disabled
--with-none                include a run with both fences and flushes disabled
--ignore-cached            overwrite cached benchmark results for given parameters (if any)
```

# Original Author

Tim Berning (tim.berning@gmail.com)
