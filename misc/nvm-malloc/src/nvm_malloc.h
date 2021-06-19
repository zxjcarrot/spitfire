/* Copyright (c) 2014 Tim Berning */

#ifndef NVMALLOC_H_
#define NVMALLOC_H_

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define NVM_ABS_TO_REL(base, ptr) ((uintptr_t)ptr - (uintptr_t)base)
#define NVM_REL_TO_ABS(base, ptr) (void*)((uintptr_t)base + (uintptr_t)ptr)

extern void* nvm_initialize(const char *workspace_path, int recover_if_possible);

extern void* nvm_reserve(uint64_t n_bytes);

extern void* nvm_reserve_id(const char *id, uint64_t n_bytes);

extern void nvm_activate(void *ptr, void **link_ptr1, void *target1, void **link_ptr2, void *target2);

extern void nvm_activate_id(const char *id);

extern void* nvm_get_id(const char *id);

extern void nvm_free(void *ptr, void **link_ptr1, void *target1, void **link_ptr2, void *target2);

extern void nvm_free_id(const char *id);

extern void nvm_persist(const void *ptr, uint64_t n_bytes);

extern void* nvm_abs(void *rel_ptr);

extern void* nvm_rel(void *abs_ptr);

extern void nvm_teardown();

#ifdef __cplusplus
}
#endif

#endif /* NVMALLOC_H_ */
