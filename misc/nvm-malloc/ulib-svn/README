------------------------
 About the ULIB Library
------------------------
The ulib library provides a set of extremely efficient data structures
and algorithms in C and C++, with a particular emphasis on items
concerning efficient data look-up, such as the hash tables,
self-adjusting trees, bloom filters, and etc. Besides, several
parallel building blocks are also provided. A detailed list of the
components can be found at the bottom of the page.

Each component of the ulib strives to be self-explanatory and
easy-to-use. Usage information can be located in the header file of
the component. Examples are also included, demonstrating the basic
usage of component.

Most of the ulib is my own work, and the rest was ported from other
open-source projects, such as the Linux kernel. Information about the
source is also retained.

Project Website:http://code.google.com/p/ulib

---------------------
 Compile the Library
---------------------
Use 'make' to build the library. Alternatively, you can use 'make
release' to build the library as well as strip the debug information.

It is recommended to perform a 'make test' once you have compiled the
library. This will start various self-tests.

-------------
 Source Tree
-------------
.
|-- include                  -- output headers for the library
|-- lib                      -- output static libraries
|-- perf                     -- performance benchmarks
|   |-- avl                  -- AVL tree performance
|   |   |-- libavl
|   |   |-- solaris
|   |   `-- ulib
|   |-- hashmap              -- hash map performance
|   |   `-- result
|   `-- mapreduce            -- MapReduce framework performance
|-- src
|   |-- base                 -- core items
|   |-- ext1                 -- extended items
|   |   |-- bloom_filter
|   |   |-- c++              -- C++ containers and wrapper classes
|   |   |-- comb             -- combinatorics enumerator
|   |   |-- console          -- command-line interpreter
|   |   `-- rng              -- various RNG's
|   `-- ext2                 -- advanced items
|       |-- mapreduce        -- MapReduce framework
|       |-- osdep            -- OS dependent items
|       |-- reentrant        -- thread-safe items
|       `-- thread           -- thread and scheduling primitives
`-- test                     -- tests for everything

------------
 Core Items
------------
bfilter.{h,c}: the Bloom filter
bitmap.{h,c}: generic bitmap
crypt_aes.{h,c}: the AES crypt
crypt_md5.{h,c}: the MD5 algorithm
crypt_rc4.{h,c}: the RC4 crypt
crypt_sha1.{h,c}: the SHA1 algorithm
crypt_sha256.{h,c}: the SHA256 algorithm
hash_open.h: C++ containers for the open addressing hashmap and hashset
hash_open_prot.h: prototypes for the open addressing hashmap and hashset
hash_chain.h: C++ container for the chain hashmap
hash_chain_prot.h: prototype for the chain hashmap
hash_func.{h,c}: hash functions
heap_prot.h: generic heap prototype
list.h: doubly linked list, can be used to implement queue and stack
math_bit.h: bit operations
math_bn.{h,c}: big number arithmetics
math_comb.{h,c}: combinatorics enumerator
math_factorial.{h,c}: factorial approximations
math_gcd.{h,c}: Euclidean and the Extended Euclidean GCD algorithms
math_lcm.{h,c}: the least common multiple
math_rand_prot.h: pseudo-random number generators, mix functions, and etc
math_rng_gamma.{h,c}: gamma distribution RNG
math_rng_normal.{h,c}: normal distribution RNG
math_rng_zipf.{h,c}: Zipf distribution RNG
search_line.{h,c}: binary search for the text lines
sort_heap_prot.h: prototype for the heapsort
sort_list.{h,c}: list sort
sort_median_prot.h: prototype for the median algorithm
str_util.{h,c}: parallel/supplementary string utilities
tree.{h,c}: various binary search trees
tree_util.{h,c}: tree utilities
ulib_ver.{h,c}: ulib version
util_algo.h: basic algorithms
util_console.{h,c}: command-line parser
util_hexdump: the hexdump utilities
util_log.h: logging utilities
util_timer.h: high-precision timer

----------------
 Parallel Items
----------------
hash_chain_r.h: concurrent chain hashmap
hash_multi_r.h: concurrent multiple hashmap
mr_dataset.{h,cpp}: the MapReduce data abstraction
mr_engine.h: the MapReduce engine
mr_interm.h: the MapReduce intermediate storage abstraction
os_atomic_intel64.h: atomic operations for the x86_64
os_rdtsc.h: the Intel rdtsc instruction
os_regionlock.h: region locks
os_spinlock.h: various spinlocks for the x86_64
os_thread.{h,cpp}: thread wrapper class
os_typelock.h: typed locks for C++
