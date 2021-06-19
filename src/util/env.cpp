//
// Created by zxjcarrot on 2019-12-21.
//

#include <dirent.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <queue>
#include <set>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>

#include <immintrin.h>

#include "util/env.h"
namespace spitfire {

Status PosixError(const std::string& context, int error_number){
    if (error_number == ENOENT) {
        return Status::NotFound(context, ::strerror(error_number));
    } else {
        return Status::IOError(context, ::strerror(error_number));
    }
}

bool PosixEnv::FileExists(const std::string &filename) {
    return ::access(filename.c_str(), F_OK) == 0;
}

Status PosixEnv::GetChildren(const std::string &directory_path,
                             std::vector<std::string> *result) {
    result->clear();
    ::DIR *dir = ::opendir(directory_path.c_str());
    if (dir == nullptr) {
        return PosixError(directory_path, errno);
    }
    struct ::dirent *entry;
    while ((entry = ::readdir(dir)) != nullptr) {
        result->emplace_back(entry->d_name);
    }
    ::closedir(dir);
    return Status::OK();
}

Status PosixEnv::DeleteFile(const std::string &filename) {
    if (::unlink(filename.c_str()) != 0) {
        return PosixError(filename, errno);
    }
    return Status::OK();
}

Status PosixEnv::CreateDir(const std::string &dirname) {
    if (::mkdir(dirname.c_str(), 0755) != 0) {
        return PosixError(dirname, errno);
    }
    return Status::OK();
}

Status PosixEnv::DeleteDir(const std::string &dirname) {
    if (::rmdir(dirname.c_str()) != 0) {
        return PosixError(dirname, errno);
    }
    return Status::OK();
}

Status PosixEnv::GetFileSize(const std::string &filename, uint64_t *size) {
    struct ::stat file_stat;
    if (::stat(filename.c_str(), &file_stat) != 0) {
        *size = 0;
        return PosixError(filename, errno);
    }
    *size = file_stat.st_size;
    return Status::OK();
}

Status PosixEnv::RenameFile(const std::string &from, const std::string &to) {
    if (std::rename(from.c_str(), to.c_str()) != 0) {
        return PosixError(from, errno);
    }
    return Status::OK();
}


Status PosixEnv::Fallocate(int fd, off_t offset, off_t len) {
    int err = posix_fallocate(fd, offset, len);
    if (err != 0)
        return PosixError("fd: " + std::to_string(fd), err);
    return Status::OK();
}


Status PosixEnv::PWrite(int fd, off_t off, const void * buf, size_t size) {
    int written = 0;
    while (written < size) {
        int ret = pwrite(fd, (char*)buf + written, size - written, off + written);
        if (ret < 0) {
            if (errno == EINTR)
                continue;
            else
                return PosixError("fd: " + std::to_string(fd), errno);
        }
        written += ret;
    }
    return Status::OK();
}


Status PosixEnv::PRead(int fd, off_t off, void * buf, size_t size) {
    int nread = 0;
    while (nread < size) {
        int ret = pread(fd, (char*)buf + nread, size - nread, off + nread);
        if (ret < 0) {
            if (errno == EINTR)
                continue;
            else
                return PosixError("fd: " + std::to_string(fd), errno);
        }
        nread += ret;
    }
    return Status::OK();
}


Status PosixEnv::OpenRWFile(const std::string & filepath, int & fd, bool direct_io) {
    auto flags = O_RDWR;
    if (direct_io == true) {
        flags |= O_DIRECT;
    }
    int fd1 = open(filepath.c_str(), flags);
    if (fd1 < 0)
        return PosixError(filepath, errno);
    fd = fd1;
    return Status::OK();
}


Status PosixEnv::CreateRWFile(const std::string & filepath, int & fd, bool direct_io) {
    auto flags = O_TRUNC | O_RDWR | O_CREAT;
    if (direct_io) {
        flags |= O_DIRECT;
    }

    int fd1 = open(filepath.c_str(), flags, 0644);
    if (fd1 < 0)
        return PosixError(filepath, errno);
    fd = fd1;
    return Status::OK();
}

static inline void *
pmem_map(int fd, size_t len) {
    void *base;

    if ((base = mmap((caddr_t) 0, len, PROT_READ | PROT_WRITE,
                     MAP_SHARED, fd, 0)) == MAP_FAILED)
        return NULL;

    return base;
}

Status PosixEnv::MMapNVMFile(const std::string & filepath, void * & mmap_addr, size_t filesize) {
    std::string ctx;
    void *pmp;
    int err;
    int fd = -1;
    struct stat stbuf;

    if (stat(filepath.c_str(), &stbuf) < 0) {

        if ((fd = open(filepath.c_str(), O_CREAT | O_RDWR, 0666)) < 0) {
            ctx = "open";
            goto out;
        }


        if ((errno = posix_fallocate(fd, 0, filesize)) != 0) {
            ctx = "posix_fallocate";
            goto out;
        }
    } else {

        if ((fd = open(filepath.c_str(), O_RDWR)) < 0) {
            ctx = "open";
            goto out;
        }

        if (stbuf.st_size < filesize) {
            if ((errno = posix_fallocate(fd, 0, filesize)) != 0) {
                ctx = "posix_fallocate";
                goto out;
            }
        } else if (stbuf.st_size > filesize){
            if (errno = ftruncate(fd, filesize)) {
                ctx = "posix_fallocate";
                goto out;
            }
        }
    }

    /*
     * map the file
     */
    if ((pmp = pmem_map(fd, filesize)) == NULL) {
        perror("mapping failed ");
        goto out;
    }

    mmap_addr = pmp;
    return Status::OK();

    out: err = errno;
    if (fd != -1)
        close(fd);
    errno = err;
    return PosixError(ctx, errno);
}

Status PosixEnv::MUNMapNVMFile(void *mmap_addr, size_t length) {
    if (munmap(mmap_addr, length) != 0) {
        return Status::IOError(std::strerror(errno));
    }
    return Status::OK();
}


#define ALIGN(addr, alignment) ((char *)((unsigned long)(addr) & ~((alignment) - 1)))
#define CACHELINE_ALIGN(addr) ALIGN(addr, 64)


#if defined(__i386__)

static __inline__ unsigned long long rdtsc(void)
{
    unsigned long long int x;
    __asm__ volatile (".byte 0x0f, 0x31" : "=A" (x));
    return x;
}

#elif defined(__x86_64__)

static __inline__ unsigned long long rdtsc(void)
{
    unsigned hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    return ( (unsigned long long)lo)|( ((unsigned long long)hi)<<32 );
}

#endif

void NVMUtilities::persist(char * addr, size_t size) {
    volatile char *ptr = CACHELINE_ALIGN(addr);
    for (; ptr < addr + size; ptr += 64)
    {
        asm volatile("clwb %0"
        : "+m"(*ptr));
    }
    asm volatile ("sfence" ::: "memory");
}

unsigned long long cycles_now() {
    _mm_lfence();
    unsigned long long v = rdtsc();
    _mm_lfence();
    return v;
}
}