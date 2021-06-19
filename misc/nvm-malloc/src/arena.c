/* Copyright (c) 2014 Tim Berning */

#include "arena.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>

#include "chunk.h"
#include "util.h"

#define NVM_ABS_TO_REL(base, ptr) ((uintptr_t)ptr - (uintptr_t)base)
#define NVM_REL_TO_ABS(base, ptr) (void*)((uintptr_t)base + (uintptr_t)ptr)

extern void *nvm_start;
extern arena_t **arenas;
extern uint64_t current_version;

arena_run_t* arena_create_run(arena_t *arena, arena_bin_t *bin, uint32_t n_bytes);
nvm_block_header_t* arena_create_block(arena_t *arena, uint32_t n_pages);
arena_block_t* arena_add_chunk(arena_t *arena);

/* comparison function for a bin's run tree - sort by address on NVM */
int run_node_compare(const void *_a, const void *_b) {
    const arena_run_t *a = tree_entry(_a, arena_run_t, link);
    const arena_run_t *b = tree_entry(_b, arena_run_t, link);
    return generic_compare((uintptr_t)a->nvm_run, (uintptr_t)b->nvm_run);
}

/* comparison function for free pageruns - sort by size */
int block_node_compare(const void *_a, const void *_b) {
    const arena_block_t *a = tree_entry(_a, arena_block_t, link);
    const arena_block_t *b = tree_entry(_b, arena_block_t, link);
    return generic_compare(a->n_pages, b->n_pages);
}

static arena_block_t* tree_upper_bound(uint32_t req_pages, struct tree_root *root) {
    arena_block_t *entry;
    arena_block_t *last_larger = NULL;
    while (root) {
        entry = tree_entry(root, arena_block_t, link);
        if (entry->n_pages == req_pages) {
            return entry;
        } else if (entry->n_pages < req_pages){
            root = root->right;
        } else {
            last_larger = entry;
            root = root->left;
        }
    }
    return last_larger;
}

void arena_init(arena_t *arena, uint32_t id, nvm_chunk_header_t *first_chunk, int create_initial_block) {
    uint32_t i;
    arena_block_t *node;
    nvm_block_header_t *nvm_block;

    arena->id = id;
    arena->free_pageruns = NULL;
    pthread_mutex_init(&arena->mtx, NULL);

    /* initialize bins for small classes [64, 128, 192, ..., 1984] */
    for (i=0; i<31; ++i) {
        arena->bins[i].current_run = NULL;
        arena->bins[i].n_free = 0;
        arena->bins[i].n_runs = 0;
        arena->bins[i].runs = NULL;
        pthread_mutex_init(&arena->bins[i].mtx, NULL);
    }

    if (create_initial_block) {
        /* initialize tree for free blocks and insert initial free block */
        node = (arena_block_t*) malloc(sizeof(arena_block_t));
        node->nvm_block = nvm_block = (nvm_block_header_t*) (first_chunk+1);
        node->n_pages = CHUNK_SIZE / BLOCK_SIZE - 1;
        node->arena = arena;
        tree_add(&node->link, block_node_compare, &arena->free_pageruns);

        // TODO: this is not fully failure atomic...
        nvm_block->state = USAGE_FREE | STATE_INITIALIZED;
        nvm_block->n_pages = CHUNK_SIZE / BLOCK_SIZE - 1;
        nvm_block->arena_id = arena->id;
        PERSIST(nvm_block);
    }
}

void* arena_allocate(arena_t *arena, uint32_t n_bytes) {
    int i;
    char mask;
    arena_bin_t *bin = NULL;
    arena_run_t *run = NULL;
    nvm_block_header_t *nvm_block = NULL;
    void *result = NULL;

    assert(n_bytes <= SCLASS_LARGE_MAX);

    /* check whether this is a small or large request */
    if (n_bytes <= SCLASS_SMALL_MAX) {
        /* small request, round up to the nearest multiple of 64 */
        n_bytes = (n_bytes & ~63) + (n_bytes % 64 != 0 ? 64 : 0);
        bin = &arena->bins[n_bytes / 64 - 1];

        pthread_mutex_lock(&bin->mtx);

        if (bin->n_free == 0) {
            /* no more space in bin, allocate new run */
            if ((run = arena_create_run(arena, bin, n_bytes)) == NULL) {
                pthread_mutex_unlock(&bin->mtx);
                return NULL;
            }
            bin->current_run = run;
            bin->n_free += (BLOCK_SIZE-64)/n_bytes;
            bin->n_runs += 1;
        } else if (!bin->current_run || bin->current_run->n_free == 0) {
            /* current run is full but not bin, select another non-full one */
            run = bin->runs;
            bin->runs = run->next;
            bin->current_run = run;
        } else {
            run = bin->current_run;
        }

        /* now we are guaranteed to have space in current_run */
        for (i=0; i<(BLOCK_SIZE-64)/n_bytes; ++i) {
            mask = 1<<(i%8);
            if ((run->bitmap[i/8] & mask) == 0) {
                run->bitmap[i/8] |= mask;
                result = (void*) ((uintptr_t)(run->nvm_run+1) + n_bytes*i);
                break;
            }
        }
        run->n_free -= 1;
        bin->n_free -= 1;

        pthread_mutex_unlock(&bin->mtx);
    } else {
        /* large request, round up to the nearest multiple of BLOCK_SIZE */
        n_bytes = (n_bytes & ~4095) + (n_bytes % BLOCK_SIZE != 0 ? BLOCK_SIZE : 0);
        if ((nvm_block = arena_create_block(arena, n_bytes/BLOCK_SIZE)) == NULL) {
            return NULL;
        }
        result = (void*) (nvm_block + 1);
    }

    return result;
}

void arena_free(void *ptr, void **link_ptr1, void *target1, void **link_ptr2, void *target2) {
    arena_t *arena = NULL;
    arena_block_t *block = NULL;
    arena_bin_t *bin = NULL;
    arena_run_t *run = NULL, *tmp_run = NULL;
    nvm_block_header_t *nvm_block = NULL;
    nvm_run_header_t *nvm_run = NULL;
    int run_idx;

    /* first, get the nvm run/block metadata located at beginning of page */
    nvm_block = (nvm_block_header_t*) ((uintptr_t)ptr & ~4095);

    if (GET_USAGE(nvm_block->state) == USAGE_BLOCK) {
        /* freeing a large element */
        block = (arena_block_t*) malloc(sizeof(arena_block_t));
        block->nvm_block = nvm_block;
        block->n_pages = nvm_block->n_pages;
        block->arena = arenas[nvm_block->arena_id];
        arena = block->arena;

        /* store link pointers in header */
        if (link_ptr1) {
            nvm_block->on[0].ptr = __NVM_ABS_TO_REL(link_ptr1);
            nvm_block->on[0].value = __NVM_ABS_TO_REL_WITH_NULL(target1);
            if (link_ptr2) {
                nvm_block->on[1].ptr = __NVM_ABS_TO_REL(link_ptr2);
                nvm_block->on[1].value = __NVM_ABS_TO_REL_WITH_NULL(target2);
            }

            sfence();
            nvm_block->state = USAGE_BLOCK | STATE_FREEING;
            sfence();

            *link_ptr1 = (void*) __NVM_ABS_TO_REL_WITH_NULL(target1);
            PERSIST(*link_ptr1);
            if (link_ptr2) {
                *link_ptr2 = (void*) __NVM_ABS_TO_REL_WITH_NULL(target2);
                PERSIST(*link_ptr2);
            }
        }

        /* mark block as free on NVM */
        nvm_block->state = USAGE_FREE | STATE_INITIALIZED;
        sfence();
        memset(nvm_block->on, 0, 2*sizeof(nvm_ptrset_t));
        PERSIST(nvm_block);

        /* add the block back into the arena's free list */
        pthread_mutex_lock(&arena->mtx);
        tree_add(&block->link, block_node_compare, &arena->free_pageruns);
        pthread_mutex_unlock(&arena->mtx);

    } else if (GET_USAGE(nvm_block->state) == USAGE_RUN) {
        /* freeing a small element */
        nvm_run = (nvm_run_header_t*) nvm_block;

        /* make sure no concurrent deallocations/activations are performed on the same run */
        while (!__sync_bool_compare_and_swap(&nvm_run->state, (USAGE_RUN | STATE_INITIALIZED), (USAGE_RUN | STATE_PREFREE))) {}

        /* check if we need to create a VHeader */
        run = nvm_run->vdata;
        if (nvm_run->version < current_version) {
            printf("nvm_run->version < current_version!\n");
            tmp_run = arena_create_run_header(nvm_run);
            pthread_mutex_lock(&tmp_run->bin->mtx);
            /* after locking, make sure nobody else has created a VHeader yet */
            if (run == nvm_run->vdata) {
                nvm_run->vdata = tmp_run;
                sfence(); /* need to guarantee that vdata is set before version */
                nvm_run->version = current_version;
            } else {
                free(tmp_run);
            }
            pthread_mutex_unlock(&tmp_run->bin->mtx);
            run = nvm_run->vdata;
        }

        bin = run->bin;
        run_idx = ((uintptr_t)ptr - (uintptr_t)(nvm_run+1)) / run->elem_size;

        /* store bit to be changed */
        nvm_run->bit_idx = run_idx;

        /* store link pointers in header */
        if (link_ptr1) {
            nvm_run->on[0].ptr = __NVM_ABS_TO_REL(link_ptr1);
            nvm_run->on[0].value = __NVM_ABS_TO_REL_WITH_NULL(target1);
            if (link_ptr2) {
                nvm_run->on[1].ptr = __NVM_ABS_TO_REL(link_ptr2);
                nvm_run->on[1].value = __NVM_ABS_TO_REL_WITH_NULL(target2);
            }

            sfence();
            nvm_run->state = USAGE_RUN | STATE_FREEING;
            sfence();

            *link_ptr1 = (void*) __NVM_ABS_TO_REL_WITH_NULL(target1);
            PERSIST(*link_ptr1);
            if (link_ptr2) {
                *link_ptr2 = (void*) __NVM_ABS_TO_REL_WITH_NULL(target2);
                PERSIST(*link_ptr2);
            }
        }

        /* mark slot as free on NVM */
        sfence();
        nvm_run->bitmap[run_idx/8] &= ~(1<<(run_idx%8));
        sfence();
        nvm_run->state = USAGE_RUN | STATE_INITIALIZED;
        sfence();
        nvm_run->bit_idx = -1;
        memset(nvm_run->on, 0, 2*sizeof(nvm_ptrset_t));
        PERSIST(nvm_run);

        /* mark slot as free in volatile memory */
        pthread_mutex_lock(&bin->mtx);
        run->bitmap[run_idx/8] &= ~(1<<(run_idx%8));
        run->n_free += 1;
        bin->n_free += 1;
        /* if run was full, add it back to bin's free list */
        if (run != bin->current_run && run->n_free == 1) {
            run->next = bin->runs;
            bin->runs = run;
        }
        pthread_mutex_unlock(&bin->mtx);

    } else {
        /* false free if we get here */
        // TODO: handle this case
    }
}

arena_run_t* arena_create_run(arena_t *arena, arena_bin_t *bin, uint32_t n_bytes) {
    nvm_run_header_t *nvm_run = NULL;
    arena_block_t *free_block = NULL;
    arena_run_t *run = NULL;

    assert(n_bytes > 0);

    /* what comes next should be protected */
    pthread_mutex_lock(&arena->mtx);

    /* find a free block for one page */
    if ((free_block = tree_upper_bound(1, arena->free_pageruns)) == NULL) {
        if ((free_block = arena_add_chunk(arena)) == NULL) {
            return NULL;
        }
    } else {
        assert(free_block->n_pages >= 1);
        tree_del(&free_block->link, &arena->free_pageruns);
    }

    run = (arena_run_t*) malloc(sizeof(arena_run_t));
    run->bin = bin;
    run->elem_size = n_bytes;
    run->n_free = run->n_max = (BLOCK_SIZE-64) / n_bytes;
    memset(run->bitmap, 0, 8);

    if (free_block->n_pages > 1) {
        /* create volatile and nonvolatile run objects at the end of the free block */
        run->nvm_run = (nvm_run_header_t*) ((uintptr_t)free_block->nvm_block + (free_block->n_pages - 1) * BLOCK_SIZE);
        nvm_run = run->nvm_run;
        memset(nvm_run, 0, sizeof(nvm_run_header_t));
        nvm_run->state = USAGE_RUN | STATE_INITIALIZED;
        nvm_run->n_bytes = n_bytes;
        nvm_run->vdata = run;
        nvm_run->bit_idx = -1;
        nvm_run->arena_id = arena->id;
        nvm_run->version = current_version;
        PERSIST(nvm_run);

        /* shrink the free block and reinsert into tree */
        free_block->n_pages -= 1;
        free_block->nvm_block->n_pages = free_block->n_pages;
        PERSIST(free_block->nvm_block);
        tree_add(&free_block->link, block_node_compare, &arena->free_pageruns);

        /* now we can release the lock */
        pthread_mutex_unlock(&arena->mtx);
    } else {
        /* block is the size we want, lock can be released right away in this case */
        pthread_mutex_unlock(&arena->mtx);

        run->nvm_run = (nvm_run_header_t*) free_block->nvm_block;
        nvm_run = run->nvm_run;
        free(free_block);

        /* convert free block to run */
        // TODO: check that this is failure safe
        memset(nvm_run, 0, sizeof(nvm_run_header_t));
        nvm_run->vdata = run;
        memset(nvm_run->bitmap, 0, 8);
        nvm_run->bit_idx = -1;
        nvm_run->arena_id = arena->id;
        nvm_run->version = current_version;
        sfence();
        nvm_run->state = USAGE_RUN | STATE_INITIALIZED;
        nvm_run->n_bytes = n_bytes;
        PERSIST(nvm_run);

        assert(nvm_run->state == (USAGE_RUN | STATE_INITIALIZED));
    }

    return run;
}

nvm_block_header_t* arena_create_block(arena_t *arena, uint32_t n_pages) {
    nvm_block_header_t *nvm_block = NULL;
    arena_block_t *free_block = NULL;

    /* what comes next should be protected */
    pthread_mutex_lock(&arena->mtx);

    /* find a free block for the specified number of pages */
    if ((free_block = tree_upper_bound(n_pages, arena->free_pageruns)) == NULL) {
        if ((free_block = arena_add_chunk(arena)) == NULL) {
            return NULL;
        }
    } else {
        assert(free_block->n_pages >= n_pages);
        tree_del(&free_block->link, &arena->free_pageruns);
    }

    if (free_block->n_pages > n_pages) {
        /* create volatile and nonvolatile block objects at the end of the free block */
        nvm_block = (nvm_block_header_t*) ((uintptr_t)free_block->nvm_block + (free_block->n_pages - n_pages) * BLOCK_SIZE);
        nvm_block->state = USAGE_FREE | STATE_INITIALIZED;
        nvm_block->n_pages = n_pages;
        nvm_block->arena_id = arena->id;
        PERSIST(nvm_block);

        /* shrink the free block and reinsert into tree */
        free_block->n_pages -= n_pages;
        free_block->nvm_block->n_pages -= n_pages;
        assert(free_block->nvm_block->n_pages > 0);
        PERSIST(free_block->nvm_block);
        tree_add(&free_block->link, block_node_compare, &arena->free_pageruns);

        /* now we can release the lock */
        pthread_mutex_unlock(&arena->mtx);
    } else {
        /* block is the size we want, lock can be released right away in this case */
        pthread_mutex_unlock(&arena->mtx);
        nvm_block = free_block->nvm_block;
    }

    return nvm_block;
}

arena_block_t* arena_add_chunk(arena_t *arena) {
    nvm_chunk_header_t *chunk = NULL;
    nvm_block_header_t *nvm_block = NULL;
    arena_block_t *free_block = NULL;

    if ((chunk = (nvm_chunk_header_t*) activate_more_chunks(1)) == NULL) {
        return NULL;
    }
    nvm_block = (nvm_block_header_t*) ((uintptr_t)chunk + BLOCK_SIZE);

    /* first initialize the chunk */
    memset(chunk->object_table, 0, 63*sizeof(nvm_object_table_entry_t));
    chunk->state = USAGE_ARENA | STATE_INITIALIZING;
    chunk->next_ot_chunk = (uintptr_t)NULL;
    strncpy(chunk->signature, NVM_CHUNK_SIGNATURE, 47);
    chunk->signature[46] = '\0';
    PERSIST_RANGE(chunk, BLOCK_SIZE);

    /* create initial free block */
    free_block = (arena_block_t*) malloc(sizeof(arena_block_t));
    free_block->nvm_block = nvm_block;
    free_block->n_pages = CHUNK_SIZE / BLOCK_SIZE - 1;
    free_block->arena = arena;

    memset(nvm_block->on, 0, 2*sizeof(nvm_ptrset_t));
    nvm_block->state = USAGE_FREE | STATE_INITIALIZED; /* no need to worry, as long as chunk header is still in INITIALIZING */
    nvm_block->n_pages = CHUNK_SIZE / BLOCK_SIZE - 1;
    PERSIST(nvm_block);

    /* set chunk's status to initialized */
    chunk->state = USAGE_ARENA | STATE_INITIALIZED;
    PERSIST(chunk);

    return free_block;
}

arena_run_t* arena_create_run_header(nvm_run_header_t *nvm_run) {
    int i=0;
    arena_t *arena = arenas[nvm_run->arena_id];
    arena_run_t *run = (arena_run_t*) malloc(sizeof(arena_run_t));

    memcpy(run->bitmap, nvm_run->bitmap, 8);
    run->nvm_run = nvm_run;
    run->bin = &arena->bins[nvm_run->n_bytes/64 - 1];
    run->elem_size = nvm_run->n_bytes;
    run->n_free = 0;
    run->n_max = (BLOCK_SIZE-64) / run->elem_size;
    for (i=0; i<run->n_max; ++i) {
        if ((run->bitmap[i/8] & 1<<(i%8)) == 0) {
            ++run->n_free;
        }
    }
    run->next = NULL;

    return run;
}

arena_block_t* arena_create_block_header(nvm_block_header_t *nvm_block) {
    arena_block_t *block = (arena_block_t*) malloc(sizeof(arena_block_t));
    block->nvm_block = nvm_block;
    block->n_pages = nvm_block->n_pages;
    block->arena = arenas[nvm_block->arena_id];
    return block;
}

void arena_teardown(arena_t *arena) {
    arena_block_t *node = NULL, *tmp = NULL;
    uint8_t i = 0;
    arena_bin_t *bin = NULL;
    arena_run_t *run = NULL;

    /* free all elements of the free block tree */
    tree_for_each_entry_safe(node, tmp, arena->free_pageruns, link) {
        tree_del(&node->link, &arena->free_pageruns);
        free(node);
    }
    /* iterate through bins and delete all run headers */
    for (i=0; i<31; ++i) {
        bin = &arena->bins[i];
        if (bin->current_run) {
            free(bin->current_run);
            bin->current_run = NULL;
        }
        while (bin->runs) {
            run = bin->runs;
            bin->runs = bin->runs->next;
            free(run);
        }
    }
    /* free arena object itself */
    free(arena);
}
