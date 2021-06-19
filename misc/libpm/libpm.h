#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <stdint.h>
#include <string.h>
#include <stdarg.h>
#include <fcntl.h>
#include <dirent.h>
#include <pthread.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/param.h>

#include <mutex>

namespace libpm {

// Logging

#define DEBUG(...)
#define ASSERT(cnd)
#define ASSERTinfo(cnd, info)
#define ASSERTeq(lhs, rhs)
#define ASSERTne(lhs, rhs)

#define FATALSYS(...)\
  fatal(errno, __FILE__, __LINE__, __func__, __VA_ARGS__)
#define FATAL(...)\
  fatal(0, __FILE__, __LINE__, __func__, __VA_ARGS__)
#define USAGE(...)\
  usage(Usage, __VA_ARGS__)

/*
 #define DEBUG(...)\
  debug(__FILE__, __LINE__, __func__, __VA_ARGS__)
 // assert a condition is true
 #define ASSERT(cnd)\
  ((void)((cnd) || (fatal(0, __FILE__, __LINE__, __func__,\
  "assertion failure: %s", #cnd), 0)))
 // assertion with extra info printed if assertion fails
 #define ASSERTinfo(cnd, info) \
  ((void)((cnd) || (fatal(0, __FILE__, __LINE__, __func__,\
  "assertion failure: %s (%s = %s)", #cnd, #info, info), 0)))
 // assert two integer values are equal
 #define ASSERTeq(lhs, rhs)\
  ((void)(((lhs) == (rhs)) || (fatal(0, __FILE__, __LINE__, __func__,\
  "assertion failure: %s (%d) == %s (%d)", #lhs,\
  (lhs), #rhs, (rhs)), 0)))
 // assert two integer values are not equal
 #define ASSERTne(lhs, rhs)\
  ((void)(((lhs) != (rhs)) || (fatal(0, __FILE__, __LINE__, __func__,\
  "assertion failure: %s (%d) != %s (%d)", #lhs,\
  (lhs), #rhs, (rhs)), 0)))
 */

// size of the static area returned by pmem_static_area()
#define PMEM_STATIC_SIZE 4096

// number of onactive/onfree values allowed
#define PMEM_NUM_ON 3

#define MAX_PTRS 512

extern void* pmp;

struct static_info {
  unsigned int init;
  unsigned int itr;
  void* ptrs[MAX_PTRS];
};

extern struct static_info* sp;

#define ABS_PTR(p) ((decltype(p))(pmp + (uintptr_t)p))
#define REL_PTR(p) ((decltype(p))((uintptr_t)p - (uintptr_t)pmp))

/* 64B cache line size */
#define ALIGN 64
#define LIBPM 0x10000000

static inline void *
pmem_map(int fd, size_t len) {
  void *base;

  if ((base = mmap((caddr_t) LIBPM, len, PROT_READ | PROT_WRITE,
  MAP_SHARED, fd, 0)) == MAP_FAILED)
    return NULL;

  return base;
}

static inline void pmem_flush_cache(void *addr, size_t len,
                                    __attribute((unused)) int flags) {
  uintptr_t uptr = (uintptr_t) addr & ~(ALIGN - 1);
  uintptr_t end = (uintptr_t) addr + len;

  /* loop through 64B-aligned chunks covering the given range */
  for (; uptr < end; uptr += ALIGN)
    __builtin_ia32_clflush((void *) uptr);
}

static inline void pmem_persist(void *addr, size_t len, int flags) {
  pmem_flush_cache(addr, len, flags);
  __builtin_ia32_sfence();
}

void debug(const char *file, int line, const char *func, const char *fmt, ...);
void fatal(int err, const char *file, int line, const char *func,
           const char *fmt, ...);

void *pmemalloc_init(const char *path, size_t size);
void *pmemalloc_static_area();
void *pmemalloc_reserve(size_t size);
void pmemalloc_activate(void *abs_ptr_);
void pmemalloc_free(void *abs_ptr_);
void pmemalloc_check(const char *path);
unsigned int get_next_pp();

}
