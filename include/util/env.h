//
// Created by zxjcarrot on 2019-12-21.
//

#ifndef SPITFIRE_ENV_H
#define SPITFIRE_ENV_H

#include <string>
#include <string.h>
#include <mutex>
#include <functional>
#include <vector>
#include "status.h"

// Following helper functions are partly adopted from the LevelDB project.

namespace spitfire {

Status PosixError(const std::string& context, int error_number);

class PosixEnv {
public:
    static bool FileExists(const std::string &filename);

    static Status GetChildren(const std::string &directory_path,
                              std::vector<std::string> *result);

    static Status DeleteFile(const std::string &filename);

    static Status CreateDir(const std::string &dirname);

    static Status DeleteDir(const std::string &dirname);

    static Status GetFileSize(const std::string &filename, uint64_t *size);

    static Status RenameFile(const std::string &from, const std::string &to);

    static Status Fallocate(int fd, off_t offset, off_t len);

    static Status PWrite(int fd, off_t off, const void *buf, size_t size);

    static Status PRead(int fd, off_t off, void *buf, size_t size);

    static Status OpenRWFile(const std::string & filepath, int & fd, bool direct_io = false);

    static Status CreateRWFile(const std::string & filepath, int & fd, bool direct_io = false);

    static Status MMapNVMFile(const std::string & filepath, void * & mmap_addr, size_t filesize);

    static Status MUNMapNVMFile(void *mmap_addr, size_t length);
};


class NVMUtilities {
public:
    // Persist a range of bytes to the underlying NVM device
    static void persist(char * addr, size_t);
};

class LockGuard {
private:
    std::mutex * m;
public:

    LockGuard(std::mutex * m): m(m) {
        Lock();
    }

    ~LockGuard() {
        Unlock();
    }

    void Lock() {
        if (m)
            m->lock();
    }
    void Unlock() {
        if (m) {
            m->unlock();
            m = nullptr;
        }
    }
};

class TryLockGuard {
private:
    std::mutex * m;
public:

    TryLockGuard(): m(nullptr) {}

    bool TryLock(std::mutex * m) {
        this->m = m;
        if (m)
            return m->try_lock();
        return false;
    }

    ~TryLockGuard() {
        Unlock();
    }

    void Unlock() {
        if (m) {
            m->unlock();
            m = nullptr;
        }
    }
};

unsigned long long cycles_now();

class ScopedTimer {
public:
    ScopedTimer(std::function<void(unsigned long long)> acc) : accumulator(acc) {
        cycles = cycles_now();
    }
    ~ScopedTimer() {
        unsigned long long diff = cycles_now() - cycles;
        accumulator(diff);
    }
private:
    std::function<void(unsigned long long)> accumulator;
    unsigned long long cycles;
};

}

#endif //DPTREE_ENV_H
