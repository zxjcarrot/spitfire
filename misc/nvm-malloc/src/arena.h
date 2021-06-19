/* Copyright (c) 2014 Tim Berning */

#ifndef ARENA_H_
#define ARENA_H_

#include "types.h"

#include <ulib/util_algo.h>

void arena_init(arena_t *arena, uint32_t id, nvm_chunk_header_t *first_chunk, int create_initial_block);

void* arena_allocate(arena_t *arena, uint32_t n_bytes);

void arena_free(void *ptr, void **link_ptr1, void *target1, void **link_ptr2, void *target2);

arena_run_t* arena_create_run_header(nvm_run_header_t *nvm_run);
arena_block_t* arena_create_block_header(nvm_block_header_t *nvm_block);

int run_node_compare(const void *_a, const void *_b);

int block_node_compare(const void *_a, const void *_b);

void arena_teardown(arena_t *arena);

#endif /* ARENA_H_ */
