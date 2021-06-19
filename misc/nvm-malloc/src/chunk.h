/* Copyright (c) 2014 Tim Berning */

#ifndef CHUNK_H_
#define CHUNK_H_

#include <stdint.h>

void* initalize_nvm_space(const char *workspace_path, uint64_t max_num_chunks);

void initialize_chunks();

uint64_t recover_chunks();

void* activate_more_chunks(uint64_t n_chunks);

void teardown_nvm_space();

#endif /* CHUNK_H_ */
