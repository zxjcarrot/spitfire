/* Copyright (c) 2014 Tim Berning */

#include "nvm_malloc.h"

#include <assert.h>
#include <pthread.h>
#include <string.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <unistd.h>

#include "arena.h"
#include "chunk.h"
#include "object_table.h"
#include "util.h"

#include <ulib/hash_chain_prot.h>
#include <ulib/util_algo.h>

void nvm_initialize_empty();
void nvm_initialize_recovered(uint64_t n_chunks_recovered);
void* nvm_recovery_thread();
nvm_huge_header_t* nvm_reserve_huge(uint64_t n_chunks);
void log_activate(void *ptr);

/* comparison function for a the free chunk tree - sort by number of chunks */
int chunk_node_compare(const void *_a, const void *_b) {
    const huge_t *a = tree_entry(_a, huge_t, link);
    const huge_t *b = tree_entry(_b, huge_t, link);
    return generic_compare((uintptr_t)a->n_chunks, (uintptr_t)b->n_chunks);
}

static huge_t* tree_upper_bound(uint32_t req_chunks, struct tree_root *root) {
    huge_t *entry;
    huge_t *last_larger = NULL;
    while (root) {
        entry = tree_entry(root, huge_t, link);
        if (entry->n_chunks == req_chunks) {
            return entry;
        } else if (entry->n_chunks < req_chunks){
            root = root->right;
        } else {
            last_larger = entry;
            root = root->left;
        }
    }
    return last_larger;
}

/* start of mapped NVM space */
void *nvm_start = NULL;

/* meta information */
extern void *meta_info;
uint64_t current_version = 0;
uint64_t next_log_entry = 0;
uint64_t max_log_entries = 127;
uintptr_t *log_start = (uintptr_t*) NULL;

/* global free chunk tree */
node_t *free_chunks = NULL;
pthread_mutex_t chunk_mtx = PTHREAD_MUTEX_INITIALIZER;

/* thread -> arena mapping as hash table */
DEFINE_CHAINHASH(tmap,
                 pid_t,
                 arena_t*,
                 1,
                 chainhash_hashfn,
                 chainhash_equalfn,
                 chainhash_cmpfn);
arena_t **arenas=NULL;
static uint32_t next_arena=0;
static chainhash_t(tmap) *tidmap = NULL;

void* nvm_initialize(const char *workspace_path, int recover_if_possible) {
    uint64_t n_chunks_recovered = 0;

    if (nvm_start != NULL) {
        return nvm_start;
    }
    nvm_start = initalize_nvm_space(workspace_path, MAX_NVM_CHUNKS);

    tidmap = chainhash_init(tmap, INITIAL_ARENAS);

    if (!recover_if_possible || (n_chunks_recovered = recover_chunks()) == 0) {
        /* no chunks were recovered, this is a fresh start so initialize */
        nvm_initialize_empty();
        log_start = (uintptr_t*)meta_info + 1;
        ot_init(nvm_start);
    } else {
        /* chunks were recovered, perform cleanup and consistency check */
        current_version = (*(uint64_t*) meta_info)++;
        PERSIST(meta_info);
        log_start = (uintptr_t*)meta_info + 1;
        nvm_initialize_recovered(n_chunks_recovered);
        ot_init(nvm_start);
        ot_recover(nvm_start);
    }

    return nvm_start;
}

void* nvm_reserve(uint64_t n_bytes) {
    void *mem = NULL;
    arena_t *arena = NULL;
    huge_t *huge = NULL;
    nvm_huge_header_t *nvm_huge=NULL;
    pid_t tid;
    uint64_t n_chunks, next_arena_num;

    if (n_bytes <= SCLASS_LARGE_MAX) {
        /* determine arena for calling thread */
        tid = (uint64_t) syscall(SYS_gettid);
        chainhash_itr_t(tmap) it = chainhash_get(tmap, tidmap, tid);
        if (chainhash_end(it)) {
            next_arena_num = __sync_fetch_and_add(&next_arena, 1) % INITIAL_ARENAS;
            arena = arenas[next_arena_num];
            it = chainhash_set(tmap, tidmap, tid);
            chainhash_value(tmap, it) = arena;
        } else {
            arena = chainhash_value(tmap, it);
        }

        /* let thread's arena handle allocation */
        mem = arena_allocate(arena, n_bytes);
    } else {
        /* round n_bytes to multiple of chunk size */
        n_chunks = (n_bytes + sizeof(nvm_huge_header_t) + CHUNK_SIZE) / CHUNK_SIZE;

        pthread_mutex_lock(&chunk_mtx);
        huge = tree_upper_bound(n_chunks, free_chunks);

        if (huge == NULL) {
            pthread_mutex_unlock(&chunk_mtx);
            nvm_huge = nvm_reserve_huge(n_chunks);
        } else {
            tree_del(&huge->link, &free_chunks);
            pthread_mutex_unlock(&chunk_mtx);

            if (huge->n_chunks > n_chunks) {
                /* got too many chunks, split and insert rest */
                nvm_huge = (nvm_huge_header_t*) ((uintptr_t)huge->nvm_chunk + (huge->n_chunks - n_chunks)*CHUNK_SIZE);
                nvm_huge->state = USAGE_FREE | STATE_INITIALIZED;
                nvm_huge->n_chunks = n_chunks;
                PERSIST(nvm_huge);

                huge->nvm_chunk->n_chunks -= n_chunks;
                huge->n_chunks -= n_chunks;

                pthread_mutex_lock(&chunk_mtx);
                tree_add(&huge->link, chunk_node_compare, &free_chunks);
                pthread_mutex_unlock(&chunk_mtx);
            } else {
                nvm_huge = huge->nvm_chunk;
                free(huge);
            }
        }
        mem = (void*) (nvm_huge+1);
    }

    return mem;
}

void* nvm_reserve_id(const char *id, uint64_t n_bytes) {
    void *mem = NULL;

    /* check that id is not in use yet */
    if (ot_get(id) != NULL) {
        return NULL;
    }

    if ((mem = nvm_reserve(n_bytes)) == NULL) {
        return NULL;
    }

    ot_insert(id, mem);

    return mem;
}

void nvm_activate(void *ptr, void **link_ptr1, void *target1, void **link_ptr2, void *target2) {
    nvm_huge_header_t *nvm_huge = NULL;
    nvm_block_header_t *nvm_block = NULL;
    nvm_run_header_t *nvm_run = NULL;
    uintptr_t rel_ptr = __NVM_ABS_TO_REL(ptr);
    uint16_t run_idx;

    log_activate(ptr);

    /* determine whether we are activating a small, large or huge object */
    if (rel_ptr % CHUNK_SIZE == sizeof(nvm_huge_header_t)) {
        /* ptr is 64 bytes into a chunk --> huge block */
        nvm_huge = (nvm_huge_header_t*) ((uintptr_t)ptr - sizeof(nvm_huge_header_t));

        /* store link pointers in header */
        if (link_ptr1) {
            nvm_huge->on[0].ptr = __NVM_ABS_TO_REL(link_ptr1);
            nvm_huge->on[0].value = __NVM_ABS_TO_REL_WITH_NULL(target1);
            if (link_ptr2) {
                nvm_huge->on[1].ptr = __NVM_ABS_TO_REL(link_ptr2);
                nvm_huge->on[1].value = __NVM_ABS_TO_REL_WITH_NULL(target2);
            }

            sfence();
            nvm_huge->state = USAGE_HUGE | STATE_ACTIVATING;
            sfence();

            *link_ptr1 = (void*) __NVM_ABS_TO_REL_WITH_NULL(target1);
            PERSIST(*link_ptr1);
            if (link_ptr2) {
                *link_ptr2 = (void*) __NVM_ABS_TO_REL_WITH_NULL(target2);
                PERSIST(*link_ptr2);
            }
        }

        nvm_huge->state = USAGE_HUGE | STATE_INITIALIZED;
        sfence();
        memset(nvm_huge->on, 0, 2*sizeof(nvm_ptrset_t));
        PERSIST(nvm_huge);
    } else {
        nvm_block = (nvm_block_header_t*) ((uintptr_t)ptr & ~(BLOCK_SIZE-1));
        if (GET_USAGE(nvm_block->state) == USAGE_FREE) {
            /* large block */

            /* store link pointers in header */
            if (link_ptr1) {
                nvm_block->on[0].ptr = __NVM_ABS_TO_REL(link_ptr1);
                nvm_block->on[0].value = __NVM_ABS_TO_REL_WITH_NULL(target1);
                if (link_ptr2) {
                    nvm_block->on[1].ptr = __NVM_ABS_TO_REL(link_ptr2);
                    nvm_block->on[1].value = __NVM_ABS_TO_REL_WITH_NULL(target2);
                }

                sfence();
                nvm_block->state = USAGE_BLOCK | STATE_ACTIVATING;
                sfence();

                *link_ptr1 = (void*) __NVM_ABS_TO_REL_WITH_NULL(target1);
                PERSIST(*link_ptr1);
                if (link_ptr2) {
                    *link_ptr2 = (void*) __NVM_ABS_TO_REL_WITH_NULL(target2);
                    PERSIST(*link_ptr2);
                }
            }

            nvm_block->state = USAGE_BLOCK | STATE_INITIALIZED;
            sfence();
            memset(nvm_block->on, 0, 2*sizeof(nvm_ptrset_t));
            PERSIST(nvm_block);
        } else {
            /* small block */
            nvm_run = (nvm_run_header_t*) nvm_block;
            run_idx = ((uintptr_t)ptr - (uintptr_t)(nvm_run+1)) / nvm_run->n_bytes;

            /* make sure no concurrent activations are performed on the same run */
            while (!__sync_bool_compare_and_swap(&nvm_run->state, (USAGE_RUN | STATE_INITIALIZED), (USAGE_RUN | STATE_PREACTIVATE))) {}

            /* save the bit to be changed */
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
                nvm_run->state = USAGE_RUN | STATE_ACTIVATING;
                sfence();

                *link_ptr1 = (void*) __NVM_ABS_TO_REL_WITH_NULL(target1);
                PERSIST(*link_ptr1);
                if (link_ptr2) {
                    *link_ptr2 = (void*) __NVM_ABS_TO_REL_WITH_NULL(target2);
                    PERSIST(*link_ptr2);
                }
            }

            /* mark slot as used on NVM */
            sfence();
            nvm_run->bitmap[run_idx/8] |= (1<<(run_idx%8));
            sfence();
            nvm_run->state = USAGE_RUN | STATE_INITIALIZED;
            sfence();
            nvm_run->bit_idx = -1;
            memset(nvm_run->on, 0, 2*sizeof(nvm_ptrset_t));
            PERSIST(nvm_run);
        }
    }
}

void nvm_activate_id(const char *id) {
    object_table_entry_t *ot_entry = NULL;
    nvm_object_table_entry_t *nvm_ot_entry = NULL;

    if ((ot_entry = ot_get(id)) == NULL) {
        return;
    }
    nvm_ot_entry = ot_entry->nvm_entry;

    /* step 1 - persist id in INITIALIZING state */
    nvm_ot_entry->state = STATE_NONE;
    sfence();
    strncpy(nvm_ot_entry->id, id, MAX_ID_LENGTH);
    nvm_ot_entry->id[54] = '\0';
    nvm_ot_entry->ptr = __NVM_ABS_TO_REL(ot_entry->data_ptr);
    sfence();
    nvm_ot_entry->state = STATE_INITIALIZING;
    PERSIST(nvm_ot_entry);

    /* step 2 - activate data normally */
    nvm_activate(ot_entry->data_ptr, NULL, NULL, NULL, NULL);

    /* step 3 - activate NVM object table entry */
    nvm_ot_entry->state = STATE_INITIALIZED;
    PERSIST(nvm_ot_entry);
}

void* nvm_get_id(const char *id) {
    object_table_entry_t *ot_entry = NULL;

    if ((ot_entry = ot_get(id)) == NULL) {
        return NULL;
    }

    return ot_entry->data_ptr;
}

void nvm_free(void *ptr, void **link_ptr1, void *target1, void **link_ptr2, void *target2) {
    nvm_huge_header_t *nvm_huge = NULL;
    huge_t *huge = NULL;
    uintptr_t rel_ptr = __NVM_ABS_TO_REL(ptr);

    if (rel_ptr % CHUNK_SIZE == sizeof(nvm_huge_header_t)) {
        /* ptr is 64 bytes into a chunk --> huge block */
        nvm_huge = (nvm_huge_header_t*) (ptr - sizeof(nvm_huge_header_t));
        huge = (huge_t*) malloc(sizeof(huge_t));
        huge->nvm_chunk = nvm_huge;
        huge->n_chunks = nvm_huge->n_chunks;

        /* store link pointers in header */
        if (link_ptr1) {
            nvm_huge->on[0].ptr = __NVM_ABS_TO_REL(link_ptr1);
            nvm_huge->on[0].value = __NVM_ABS_TO_REL_WITH_NULL(target1);
            if (link_ptr2) {
                nvm_huge->on[1].ptr = __NVM_ABS_TO_REL(link_ptr2);
                nvm_huge->on[1].value = __NVM_ABS_TO_REL_WITH_NULL(target2);
            }

            sfence();
            nvm_huge->state = USAGE_HUGE | STATE_FREEING;
            sfence();

            *link_ptr1 = (void*) __NVM_ABS_TO_REL_WITH_NULL(target1);
            PERSIST(*link_ptr1);
            if (link_ptr2) {
                *link_ptr2 = (void*) __NVM_ABS_TO_REL_WITH_NULL(target2);
                PERSIST(*link_ptr2);
            }
        }

        nvm_huge->state = USAGE_FREE | STATE_INITIALIZED;
        sfence();
        memset(nvm_huge->on, 0, 2*sizeof(nvm_ptrset_t));
        PERSIST(nvm_huge);

        pthread_mutex_lock(&chunk_mtx);
        tree_add(&huge->link, chunk_node_compare, &free_chunks);
        pthread_mutex_unlock(&chunk_mtx);
    } else {
        /* otherwise must be a run or block --> let arena handle */
        arena_free(ptr, link_ptr1, target1, link_ptr2, target2);
    }
}

void nvm_free_id(const char *id) {
    object_table_entry_t *ot_entry = NULL;
    nvm_object_table_entry_t *nvm_entry = NULL;

    if ((ot_entry = ot_get(id)) == NULL) {
        return;
    }

    nvm_entry = ot_entry->nvm_entry;
    nvm_entry->state = STATE_FREEING; // TODO: maybe not do this and instead do sanity check on startup?
    PERSIST(nvm_entry);

    nvm_free(ot_entry->data_ptr, NULL, NULL, NULL, NULL);

    ot_remove(id);
}

extern void nvm_persist(const void *ptr, uint64_t n_bytes) {
    PERSIST_RANGE(ptr, n_bytes);
}

void* nvm_abs(void *rel_ptr) {
    assert(nvm_start != NULL);
    return __NVM_REL_TO_ABS_WITH_NULL(rel_ptr);
}

void* nvm_rel(void *abs_ptr) {
    assert(nvm_start != NULL);
    return (void*) __NVM_ABS_TO_REL_WITH_NULL(abs_ptr);
}

/* internal functions */
/* ------------------ */

void nvm_initialize_empty() {
    uint32_t i;
    nvm_chunk_header_t *chunk_hdr=NULL;
    nvm_block_header_t *block_hdr=NULL;
    arena_t *arena=NULL;

    /* perform initialization for chunks when not recovering */
    initialize_chunks();
    *(uint64_t*) meta_info = 1;
    PERSIST(meta_info);
    current_version = 0;

    /* allocate chunks for the initial arena setup */
    activate_more_chunks(INITIAL_ARENAS);

    /* perform initial chunk setup */
    for (i=0; i<INITIAL_ARENAS; ++i) {
        /* initialize the chunk header */
        chunk_hdr = (nvm_chunk_header_t*)__NVM_REL_TO_ABS(i*CHUNK_SIZE);
        chunk_hdr->state = STATE_INITIALIZING | USAGE_ARENA;
        strncpy(chunk_hdr->signature, NVM_CHUNK_SIGNATURE, 47);
        chunk_hdr->next_ot_chunk = i < INITIAL_ARENAS-1 ? (uintptr_t)((i+1)*CHUNK_SIZE) : (uintptr_t)NULL;
        memset((void*)chunk_hdr->object_table, 0, 4032);
        PERSIST_RANGE((void*)chunk_hdr, sizeof(nvm_chunk_header_t));

        /* initialize the chunk content */
        block_hdr = (nvm_block_header_t*)(chunk_hdr+1);
        block_hdr->state = STATE_INITIALIZING | USAGE_FREE;
        block_hdr->n_pages = (CHUNK_SIZE - sizeof(nvm_chunk_header_t) - sizeof(nvm_block_header_t)) / BLOCK_SIZE;
        memset((void*)((uintptr_t)block_hdr + 5), 0, 59);
        PERSIST((void*)block_hdr);
    }
    /* mark the chunks as initialized */
    for (i=0; i<INITIAL_ARENAS; ++i) {
        chunk_hdr = (nvm_chunk_header_t*)__NVM_REL_TO_ABS(i*CHUNK_SIZE);
        chunk_hdr->state = STATE_INITIALIZED | USAGE_ARENA;
        PERSIST((void*)chunk_hdr);
    }

    /* create arenas on chunks */
    arenas = (arena_t**) malloc(INITIAL_ARENAS * sizeof(arena_t*));
    for (i=0; i<INITIAL_ARENAS; ++i) {
        arena = (arena_t*) malloc(sizeof(arena_t));
        arena_init(arena, i, __NVM_REL_TO_ABS(i*CHUNK_SIZE), 1);
        arenas[i] = arena;
    }
}

void* nvm_recovery_thread(void *chunk_count) {
    nvm_chunk_header_t *nvm_chunk = NULL;
    nvm_huge_header_t *nvm_huge = NULL;
    nvm_block_header_t *nvm_block = NULL;
    nvm_run_header_t *nvm_run = NULL;
    arena_block_t *block = NULL;
    arena_run_t *run = NULL, *tmp_run = NULL;
    char usage = 0;
    uint64_t n_chunks = (uint64_t) chunk_count, i = 0, j = 0;

    while (i < n_chunks) {
        nvm_chunk = (nvm_chunk_header_t*) (nvm_start + i*CHUNK_SIZE);
        if (GET_USAGE(nvm_chunk->state) == USAGE_ARENA) {
            /* only process arena chunks since we're looking for runs */
            j = 1;
            while (j < CHUNK_SIZE/BLOCK_SIZE) {
                nvm_block = (nvm_block_header_t*) ((uintptr_t)nvm_chunk + j*BLOCK_SIZE);
                usage = GET_USAGE(nvm_block->state);
                if (usage == USAGE_FREE) {
                    /* free block, add to arenas fee pageruns tree */
                    block = arena_create_block_header(nvm_block);
                    pthread_mutex_lock(&block->arena->mtx);
                    tree_add(&block->link, block_node_compare, &block->arena->free_pageruns);
                    pthread_mutex_unlock(&block->arena->mtx);
                    j += nvm_block->n_pages;
                } else if (usage == USAGE_RUN) {
                    /* run, check if version is up-to-date and otherwise create VHeader */
                    nvm_run = (nvm_run_header_t*) nvm_block;
                    tmp_run = nvm_run->vdata;
                    if (nvm_run->version < current_version) {
                        run = arena_create_run_header(nvm_run);
                        pthread_mutex_lock(&run->bin->mtx);
                        /* after locking, make sure nobody else has created a VHeader yet */
                        if (tmp_run == nvm_run->vdata) {
                            nvm_run->vdata = run;
                            sfence(); /* need to guarantee that vdata is set before version */
                            nvm_run->version = current_version;
                            PERSIST(nvm_run);
                        } else {
                            /* VHeader was just created by a concurrent deallocation */
                            free(run);
                        }
                        pthread_mutex_unlock(&run->bin->mtx);
                    }
                    ++j;
                } else {
                    /* block in use, skip */
                    uint64_t oldj = j;
                    j += nvm_block->n_pages;
                    if (j == oldj) {
                    }
                }
            }
            ++i;
        } else {
            /* must be a huge allocation then, so skip it */
            nvm_huge = (nvm_huge_header_t*) nvm_chunk;
            i += nvm_huge->n_chunks;
        }
    }


    return NULL;
}

void nvm_initialize_recovered(uint64_t n_chunks_recovered) {
    uint64_t i;
    uintptr_t rel_ptr = 0;
    void *ptr = NULL, **target = NULL;
    arena_t *arena = NULL;
    nvm_huge_header_t *nvm_huge = NULL;
    nvm_block_header_t *nvm_block = NULL;
    nvm_run_header_t *nvm_run = NULL;
    huge_t *huge = NULL;
    arena_block_t *block = NULL;
    arena_run_t *run = NULL;
    char usage = 0;
    char state = 0;
    pthread_t recovery_thread;

    /* create the arenas */
    arenas = (arena_t**) malloc(INITIAL_ARENAS * sizeof(arena_t*));
    for (i=0; i<INITIAL_ARENAS; ++i) {
        arena = (arena_t*) malloc(sizeof(arena_t));
        arena_init(arena, i, NULL, 0);
        arenas[i] = arena;
    }

    /* process the log to identify potentially inconsistent entries */
    for (i=0; i<max_log_entries; ++i) {
        rel_ptr = log_start[i];
        if (rel_ptr == 0)
            continue;
        ptr = __NVM_REL_TO_ABS(rel_ptr);
        usage = identify_usage(ptr);

        if (usage == USAGE_HUGE) {
            nvm_huge = (nvm_huge_header_t*) ((uintptr_t)ptr - sizeof(nvm_huge_header_t));
            state = GET_STATE(nvm_huge->state);
            if (state == STATE_PREFREE) {
                /* before committed to freeing, rollback */
                memset(nvm_huge->on, 0, 2*sizeof(nvm_ptrset_t));
                nvm_huge->state = USAGE_HUGE | STATE_INITIALIZED;
                PERSIST(nvm_huge);
            } else if (state == STATE_FREEING) {
                /* committed to freeing, replay */
                if (nvm_huge->on[0].ptr) {
                    target = (void**)__NVM_REL_TO_ABS(nvm_huge->on[0].ptr);
                    *target = __NVM_REL_TO_ABS_WITH_NULL(nvm_huge->on[0].value);
                    PERSIST(target);
                    if (nvm_huge->on[1].ptr) {
                        target = (void**)__NVM_REL_TO_ABS(nvm_huge->on[1].ptr);
                        *target = __NVM_REL_TO_ABS_WITH_NULL(nvm_huge->on[1].value);
                        PERSIST(target);
                    }
                }
                memset(nvm_huge->on, 0, 2*sizeof(nvm_ptrset_t));
                nvm_huge->state = USAGE_FREE | STATE_INITIALIZED;
                PERSIST(nvm_huge);
                huge = (huge_t*) malloc(sizeof(huge_t));
                huge->nvm_chunk = nvm_huge;
                huge->n_chunks = nvm_huge->n_chunks;
                tree_add(&huge->link, chunk_node_compare, &free_chunks);
            } else if (state == STATE_PREACTIVATE) {
                /* before committed to activation, rollback */
                memset(nvm_huge->on, 0, 2*sizeof(nvm_ptrset_t));
                nvm_huge->state = USAGE_FREE | STATE_INITIALIZED;
                PERSIST(nvm_huge);
                huge = (huge_t*) malloc(sizeof(huge_t));
                huge->nvm_chunk = nvm_huge;
                huge->n_chunks = nvm_huge->n_chunks;
                tree_add(&huge->link, chunk_node_compare, &free_chunks);
            } else if (state == STATE_ACTIVATING) {
                /* committed to activation, replay */
                if (nvm_huge->on[0].ptr) {
                    target = (void**)__NVM_REL_TO_ABS(nvm_huge->on[0].ptr);
                    *target = __NVM_REL_TO_ABS_WITH_NULL(nvm_huge->on[0].value);
                    PERSIST(target);
                    if (nvm_huge->on[1].ptr) {
                        target = (void**)__NVM_REL_TO_ABS(nvm_huge->on[1].ptr);
                        *target = __NVM_REL_TO_ABS_WITH_NULL(nvm_huge->on[1].value);
                        PERSIST(target);
                    }
                }
                memset(nvm_huge->on, 0, 2*sizeof(nvm_ptrset_t));
                nvm_huge->state = USAGE_HUGE | STATE_INITIALIZED;
                PERSIST(nvm_huge);
            } else {
                assert(state == STATE_INITIALIZED);
            }

        } else if (usage == USAGE_BLOCK) {
            nvm_block = (nvm_block_header_t*) ((uintptr_t)ptr & ~(BLOCK_SIZE-1));
            state = GET_STATE(nvm_block->state);
            if (state == STATE_PREFREE) {
                /* before committed to freeing, rollback */
                memset(nvm_block->on, 0, 2*sizeof(nvm_ptrset_t));
                nvm_block->state = USAGE_BLOCK | STATE_INITIALIZED;
                PERSIST(nvm_block);
            } else if (state == STATE_FREEING) {
                /* committed to freeing, replay */
                if (nvm_block->on[0].ptr) {
                    target = (void**)__NVM_REL_TO_ABS(nvm_block->on[0].ptr);
                    *target = __NVM_REL_TO_ABS_WITH_NULL(nvm_block->on[0].value);
                    PERSIST(target);
                    if (nvm_block->on[1].ptr) {
                        target = (void**)__NVM_REL_TO_ABS(nvm_block->on[1].ptr);
                        *target = __NVM_REL_TO_ABS_WITH_NULL(nvm_block->on[1].value);
                        PERSIST(target);
                    }
                }
                memset(nvm_block->on, 0, 2*sizeof(nvm_ptrset_t));
                nvm_block->state = USAGE_FREE | STATE_INITIALIZED;
                PERSIST(nvm_block);
                block = (arena_block_t*) malloc(sizeof(arena_block_t));
                block->nvm_block = nvm_block;
                block->n_pages = nvm_block->n_pages;
                block->arena = arenas[nvm_block->arena_id];
                tree_add(&block->link, block_node_compare, &block->arena->free_pageruns);
            } else if (state == STATE_PREACTIVATE) {
                /* before committed to activation, rollback */
                memset(nvm_block->on, 0, 2*sizeof(nvm_ptrset_t));
                nvm_block->state = USAGE_FREE | STATE_INITIALIZED;
                PERSIST(nvm_block);
                block = (arena_block_t*) malloc(sizeof(arena_block_t));
                block->nvm_block = nvm_block;
                block->n_pages = nvm_block->n_pages;
                block->arena = arenas[nvm_block->arena_id];
                tree_add(&block->link, block_node_compare, &block->arena->free_pageruns);
            } else if (state == STATE_ACTIVATING) {
                /* committed to activation, replay */
                if (nvm_block->on[0].ptr) {
                    target = (void**)__NVM_REL_TO_ABS(nvm_block->on[0].ptr);
                    *target = __NVM_REL_TO_ABS_WITH_NULL(nvm_block->on[0].value);
                    PERSIST(target);
                    if (nvm_block->on[1].ptr) {
                        target = (void**)__NVM_REL_TO_ABS(nvm_block->on[1].ptr);
                        *target = __NVM_REL_TO_ABS_WITH_NULL(nvm_block->on[1].value);
                        PERSIST(target);
                    }
                }
                memset(nvm_block->on, 0, 2*sizeof(nvm_ptrset_t));
                nvm_block->state = USAGE_BLOCK | STATE_INITIALIZED;
                PERSIST(nvm_block);
            } else {
                assert(state == STATE_INITIALIZED);
            }

        } else if (usage == USAGE_RUN) {
            nvm_run = (nvm_run_header_t*) ((uintptr_t)ptr & ~(BLOCK_SIZE-1));
            state = GET_STATE(nvm_run->state);
            if (state == STATE_PREFREE) {
                /* before committed to freeing, rollback */
            } else if (state == STATE_FREEING) {
                /* committed to freeing, replay */
                if (nvm_run->on[0].ptr) {
                    target = (void**)__NVM_REL_TO_ABS(nvm_run->on[0].ptr);
                    *target = __NVM_REL_TO_ABS_WITH_NULL(nvm_run->on[0].value);
                    PERSIST(target);
                    if (nvm_run->on[1].ptr) {
                        target = (void**)__NVM_REL_TO_ABS(nvm_run->on[1].ptr);
                        *target = __NVM_REL_TO_ABS_WITH_NULL(nvm_run->on[1].value);
                        PERSIST(target);
                    }
                }
                nvm_run->bitmap[nvm_run->bit_idx/8] &= ~(1 << (nvm_run->bit_idx % 8));
            } else if (state == STATE_PREACTIVATE) {
                /* before committed to activation, rollback */
            } else if (state == STATE_ACTIVATING) {
                /* committed to activation, replay */
                if (nvm_run->on[0].ptr) {
                    target = (void**)__NVM_REL_TO_ABS(nvm_run->on[0].ptr);
                    *target = __NVM_REL_TO_ABS_WITH_NULL(nvm_run->on[0].value);
                    PERSIST(target);
                    if (nvm_run->on[1].ptr) {
                        target = (void**)__NVM_REL_TO_ABS(nvm_run->on[1].ptr);
                        *target = __NVM_REL_TO_ABS_WITH_NULL(nvm_run->on[1].value);
                        PERSIST(target);
                    }
                }
                nvm_run->bitmap[nvm_run->bit_idx/8] |= 1 << (nvm_run->bit_idx % 8);
            } else {
                assert(state == STATE_INITIALIZED);
            }
            /* create the VHeader and reset fields either way */
            run = arena_create_run_header(nvm_run);
            run->next = run->bin->runs;
            run->bin->runs = run;
            memset(nvm_run->on, 0, 2*sizeof(nvm_ptrset_t));
            nvm_run->version = current_version;
            nvm_run->vdata = run;
            nvm_run->bit_idx = -1;
            nvm_run->state = USAGE_RUN | STATE_INITIALIZED;
            PERSIST(nvm_run);

        } else {
            /* error case, this should not happen! */
        }
        log_start[i] = (uintptr_t) NULL;
    }

    pthread_create(&recovery_thread, NULL, nvm_recovery_thread, (void*)n_chunks_recovered);
    pthread_detach(recovery_thread);
}

nvm_huge_header_t* nvm_reserve_huge(uint64_t n_chunks) {
    nvm_huge_header_t *nvm_huge = NULL;

    /* create new chunks for the request */
    nvm_huge = activate_more_chunks(n_chunks);
    nvm_huge->state = USAGE_HUGE | STATE_INITIALIZING;
    nvm_huge->n_chunks = n_chunks;
    memset(nvm_huge->on, 0, sizeof(nvm_huge->on));
    PERSIST(nvm_huge);

    return nvm_huge;
}

void log_activate(void *ptr) {
    uint64_t slot_index = __sync_fetch_and_add(&next_log_entry, 1);
    uintptr_t *slot = log_start + (slot_index % max_log_entries);
    *slot = __NVM_ABS_TO_REL(ptr);
    PERSIST(slot);
}

void nvm_teardown() {
    /* WARNING: this method is NOT thread safe! Make sure all nvm_malloc operations
       are finished before calling this method. */
    huge_t *node = NULL, *tmp = NULL;
    uint8_t i = 0;

    if (nvm_start == NULL) {
        return;
    }

    /* teardown chunk system */
    teardown_nvm_space();

    /* free all global free chunk headers */
    tree_for_each_entry_safe(node, tmp, free_chunks, link) {
        tree_del(&node->link, &free_chunks);
        free(node);
    }

    /* deconstruct all arenas */
    for (i=0; i<INITIAL_ARENAS; ++i) {
        arena_teardown(arenas[i]);
        arenas[i] = NULL;
    }
    free(arenas);

    /* deconstruct object table */
    ot_teardown();

    /* destroy thread->arena mapping (no explicit deallocations necessary) */
    chainhash_destroy(tmap, tidmap);
    tidmap = NULL;

    /* zero some global values */
    nvm_start = NULL;
    current_version = 0;
    next_log_entry = 0;
    log_start = (uintptr_t*) NULL;
}
