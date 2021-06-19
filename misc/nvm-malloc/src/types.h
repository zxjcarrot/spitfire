/* Copyright (c) 2014 Tim Berning */

#ifndef TYPES_H_
#define TYPES_H_

#include <stdint.h>
#include <pthread.h>
#include <ulib/tree.h>

/* global limits */
/* ------------- */

#define MAX_NVM_SPACE  (100ul * 1024*1024*1024) /* 100 GB */
#define MAX_NVM_CHUNKS (MAX_NVM_SPACE / CHUNK_SIZE)
#define INITIAL_ARENAS 20


/* internal macro of absolute/relative conversion marco with base fixed as nvm_start */
/* --------------------------------------------------------------------------------- */

#define __NVM_ABS_TO_REL(ptr) ((uintptr_t)ptr - (uintptr_t)nvm_start)
#define __NVM_ABS_TO_REL_WITH_NULL(ptr) ((void*)ptr == NULL ? (uintptr_t)NULL : ((uintptr_t)ptr - (uintptr_t)nvm_start))
#define __NVM_REL_TO_ABS(ptr) (void*)((uintptr_t)nvm_start + (uintptr_t)ptr)
#define __NVM_REL_TO_ABS_WITH_NULL(ptr) ((void*)ptr == NULL ? NULL : (void*)((uintptr_t)nvm_start + (uintptr_t)ptr))


/* common definitions */
/* ------------------ */

#define CACHE_LINE_SIZE     64
#define BLOCK_SIZE          4096
#define CHUNK_SIZE          (4ul * 1024ul * 1024ul) /* 4mb chunks */
#define NVM_CHUNK_SIGNATURE "***NVM_MALLOC_CHUNK_HEADER_SIGNATURE__/o/__***\0"
#define MAX_ID_LENGTH       54

#define NUM_ARENA_BINS      31
#define SCLASS_SMALL_MIN    CACHE_LINE_SIZE      /* minimum allocation size due to cache line flush requirements */
#define SCLASS_SMALL_MAX    (BLOCK_SIZE/2 - 64)  /* half block - 64B for header */
#define SCLASS_LARGE_MIN    (BLOCK_SIZE/2)       /* half block */
#define SCLASS_LARGE_MAX    (CHUNK_SIZE/2 - 64)  /* half chunk - 64B for header */


/* state/usage flags */
/* ----------------- */

#define STATE_MASK         ((char)15)
#define GET_STATE(state)   (state & STATE_MASK)

#define STATE_NONE         ((char)0)
#define STATE_INITIALIZING ((char)1)
#define STATE_INITIALIZED  ((char)2)
#define STATE_PREFREE      ((char)4)
#define STATE_FREEING      ((char)5)
#define STATE_PREACTIVATE  ((char)6)
#define STATE_ACTIVATING   ((char)7)

#define USAGE_MASK         (~STATE_MASK)
#define GET_USAGE(state)   (state & USAGE_MASK)

#define USAGE_DEFAULT      ((char)0)
#define USAGE_FREE         ((char)1 << 4)
#define USAGE_ARENA        ((char)2 << 4)
#define USAGE_BLOCK        ((char)3 << 4)
#define USAGE_RUN          ((char)4 << 4)
#define USAGE_HUGE         ((char)5 << 4)


/* some typedefs */
/* ------------- */

typedef struct tree_root node_t;

typedef struct nvm_object_table_entry_s nvm_object_table_entry_t;
typedef struct nvm_chunk_header_s nvm_chunk_header_t;
typedef struct nvm_ptrset_s nvm_ptrset_t;
typedef struct nvm_huge_header_s nvm_huge_header_t;
typedef struct nvm_block_header_s nvm_block_header_t;
typedef struct nvm_run_header_s nvm_run_header_t;

typedef struct object_table_entry_s object_table_entry_t;
typedef struct huge_s huge_t;
typedef struct arena_run_s arena_run_t;
typedef struct arena_block_s arena_block_t;
typedef struct arena_bin_s arena_bin_t;
typedef struct arena_s arena_t;


/* non-volatile structs */
/* -------------------- */

struct nvm_object_table_entry_s {
    char state;
    char id[55];
    uintptr_t ptr;
} __attribute__((aligned(CACHE_LINE_SIZE)));

struct nvm_chunk_header_s {
    char state;
    char signature[55];
    uintptr_t next_ot_chunk;
    nvm_object_table_entry_t object_table[63];
} __attribute__((aligned(BLOCK_SIZE)));

struct nvm_ptrset_s {
    uintptr_t ptr;
    uintptr_t value;
};

struct nvm_huge_header_s {
    char state;
    uint32_t n_chunks;
    nvm_ptrset_t on[2];
    char __padding[12];
} __attribute__((aligned(CACHE_LINE_SIZE)));

struct nvm_block_header_s {
    char state;
    uint32_t n_pages;
    nvm_ptrset_t on[2];
    uint32_t arena_id;
    char __padding[12];
} __attribute__((aligned(CACHE_LINE_SIZE)));

struct nvm_run_header_s {
    char state;
    uint16_t n_bytes;
    arena_run_t *vdata;
    nvm_ptrset_t on[2];
    char bitmap[8];
    int16_t bit_idx;
    uint16_t arena_id;
    uint32_t version;
} __attribute__((aligned(CACHE_LINE_SIZE)));


/* volatile structs */
/* ---------------- */

struct object_table_entry_s {
    char id[MAX_ID_LENGTH+1];
    uint64_t slot;
    void *data_ptr;
    nvm_object_table_entry_t *nvm_entry;
};

struct huge_s {
    node_t link; /* necessary to store chunks in trees */
    nvm_huge_header_t *nvm_chunk;
    uint32_t n_chunks;
};

struct arena_run_s {
    node_t link; /* necessary to store runs in trees */
    nvm_run_header_t *nvm_run;
    arena_bin_t *bin;
    uint16_t elem_size;
    uint16_t n_free;
    uint16_t n_max;
    char bitmap[8];
    arena_run_t *next;
};

struct arena_block_s {
    node_t link; /* necessary to store blocks in trees */
    nvm_block_header_t *nvm_block;
    uint16_t n_pages;
    arena_t *arena;
};

struct arena_bin_s {
    arena_run_t *current_run;
    uint32_t n_free;
    uint16_t n_runs;
    arena_run_t *runs;
    pthread_mutex_t mtx;
};

struct arena_s {
    uint32_t id;
    arena_bin_t bins[31];
    node_t *free_pageruns;
    pthread_mutex_t mtx;
};

/* make sure the NVRAM structs are correctly sized */
_Static_assert(sizeof(nvm_object_table_entry_t) == CACHE_LINE_SIZE, "object table entry size should be 64 bytes");
_Static_assert(sizeof(nvm_chunk_header_t) == BLOCK_SIZE, "chunk header size should be 4096 bytes");
_Static_assert(sizeof(nvm_huge_header_t) == CACHE_LINE_SIZE, "huge header size should be 64 bytes");
_Static_assert(sizeof(nvm_block_header_t) == CACHE_LINE_SIZE, "block header size should be 64 bytes");
_Static_assert(sizeof(nvm_run_header_t) == CACHE_LINE_SIZE, "run header size should be 64 bytes");

#endif /* TYPES_H_ */
