/* Copyright (c) 2014 Tim Berning */

#include "chunk.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <stddef.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <unistd.h>

#include "types.h"

#ifdef __APPLE__
#define MAP_ANONYMOUS MAP_ANON
#endif

static inline void error_and_exit(char *msg, ...) {
    va_list args;
    va_start(args, msg);
    vfprintf(stderr, msg, args);
    va_end(args);
    exit(-1);
}

static void            *chunk_region_start = NULL;
       void            *meta_info = NULL; /* must be non-static as it is exported to other translation units */
static uint64_t        max_chunks = 0;
static int             backing_file_fd = -1;
static char            *backing_file_path = NULL;
static int             meta_file_fd = -1;
static char            *meta_file_path = NULL;
static uint64_t        next_chunk = 0;
static pthread_mutex_t chunk_mtx = PTHREAD_MUTEX_INITIALIZER;

inline int nvm_fallocate(int fd, off_t offset, off_t len) {
#ifdef __linux
    return posix_fallocate(fd, offset, len);
#elif __APPLE__
    fstore_t s = {F_ALLOCATECONTIG, F_PEOFPOSMODE, 0, len};
    if (fcntl(fd, F_PREALLOCATE, &s) == -1) {
        s.fst_flags = F_ALLOCATEALL;
        if (fcntl(fd, F_PREALLOCATE, &s) == -1) {
            return -1;
        }
    }
    if (ftruncate(fd, len) != 0) {
        return -1;
    }
    return 0;
#endif
}

inline int open_existing_file(char *path) {
    int fd=-1;
    struct stat stbuf;
    if (stat(path, &stbuf) < 0) {
        return -1;
    } else if ((fd = open(path, O_RDWR)) < 0) {
        error_and_exit("unable to open file %s\n", path);
    }
    return fd;
}

inline int open_empty_or_create_file(char *path) {
    int fd=-1;
    struct stat stbuf;
    if (stat(path, &stbuf) < 0) {
        if ((fd = open(path, O_RDWR|O_CREAT, 0666)) < 0)
            error_and_exit("unable to create file %s\n", path);
    } else if ((fd = open(path, O_RDWR|O_TRUNC)) < 0) {
            error_and_exit("unable to open file %s\n", path);
    }
    return fd;
}

inline uint64_t get_file_size(char *path) {
    struct stat stbuf;
    if (stat(path, &stbuf) != 0) {
        return 0;
    }
    return (uint64_t) stbuf.st_size;
}

void* initalize_nvm_space(const char *workspace_path, uint64_t max_num_chunks) {
    uint64_t base_path_length = 0;
    max_chunks = max_num_chunks;

    // perform initial request for large memory block, CHUNK_SIZE *must* be a multiple of 2mb
    if ((chunk_region_start = mmap(NULL, max_chunks*CHUNK_SIZE, PROT_NONE, MAP_SHARED|MAP_ANONYMOUS|MAP_NORESERVE, -1, 0)) == MAP_FAILED) {
        error_and_exit("Unable to mmap initial block of %lu chunks\n", max_chunks);
    }

    base_path_length = strlen(workspace_path);
    backing_file_path = (char*) malloc(base_path_length + 1 + 7 + 1); /* <workspace_path> + '/' + 'backing' + '\0' */
    meta_file_path    = (char*) malloc(base_path_length + 1 + 4 + 1); /* <workspace_path> + '/' + 'meta' + '\0' */
    sprintf(backing_file_path, "%s/backing", workspace_path);
    sprintf(meta_file_path, "%s/meta", workspace_path);

    return chunk_region_start;
}

void initialize_chunks() {
    backing_file_fd = open_empty_or_create_file(backing_file_path);
    /* >>>> HACK begin: call nvm_fallocate with 1MB first to prevent PMFS from switching to huge pages */
    if (nvm_fallocate(backing_file_fd, 0, 1024*1024) != 0)
        error_and_exit("unable to ensure file size of %s", backing_file_path);
    /* <<<< HACK end */

    /* open new meta file */
    meta_file_fd = open_empty_or_create_file(meta_file_path);
    if (nvm_fallocate(meta_file_fd, 0, BLOCK_SIZE) != 0)
        error_and_exit("unable to ensure file size of %s", meta_file_path);
    if ((meta_info = mmap(NULL, BLOCK_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_NORESERVE, meta_file_fd, 0)) == MAP_FAILED)
        error_and_exit("error mapping meta info\n");
}

uint64_t recover_chunks() {
    void *next_chunk_addr = (void*) ((char*)chunk_region_start + next_chunk*CHUNK_SIZE);
    uint64_t n_bytes = get_file_size(backing_file_path);
    if (n_bytes == 0) {
        return 0;
    }

    next_chunk = n_bytes / CHUNK_SIZE;
    backing_file_fd = open_existing_file(backing_file_path);
    if (mmap(next_chunk_addr, n_bytes, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_NORESERVE|MAP_FIXED, backing_file_fd, 0) == MAP_FAILED) {
        error_and_exit("unable to mmap %s\n", backing_file_path);
    }

    /* open existing meta file */
    meta_file_fd = open_existing_file(meta_file_path);
    if ((meta_info = mmap(NULL, BLOCK_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_NORESERVE, meta_file_fd, 0)) == MAP_FAILED)
        error_and_exit("error mapping meta info\n");

    return next_chunk;
}

void* activate_more_chunks(uint64_t n_chunks) {
    void *next_chunk_addr=NULL;

    pthread_mutex_lock(&chunk_mtx);

    /* first check if we would overflow our max with the request */
    if (next_chunk + n_chunks > max_chunks) {
        pthread_mutex_unlock(&chunk_mtx);
        error_and_exit("Requested too many chunks\n");
    }

    next_chunk_addr = (void*) ((uintptr_t)chunk_region_start + next_chunk*CHUNK_SIZE);

    if (nvm_fallocate(backing_file_fd, next_chunk*CHUNK_SIZE, n_chunks*CHUNK_SIZE) != 0)
        error_and_exit("unable to increase file size of %s", backing_file_path);
    if (mmap(next_chunk_addr, n_chunks*CHUNK_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_NORESERVE|MAP_FIXED, backing_file_fd, next_chunk*CHUNK_SIZE) == MAP_FAILED)
        error_and_exit("error mapping chunks");

    next_chunk += n_chunks;

    pthread_mutex_unlock(&chunk_mtx);

    return next_chunk_addr;
}

void teardown_nvm_space() {
    munmap(chunk_region_start, max_chunks*CHUNK_SIZE);
    chunk_region_start = NULL;
    munmap(meta_info, BLOCK_SIZE);
    meta_info = NULL;
    close(backing_file_fd);
    backing_file_fd = -1;
    close(meta_file_fd);
    meta_file_fd = -1;
    free(backing_file_path);
    backing_file_path = NULL;
    free(meta_file_path);
    meta_file_path = NULL;
    max_chunks = 0;
    next_chunk = 0;
}
