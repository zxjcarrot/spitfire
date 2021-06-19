//
// Created by zxjcarrot on 2019-12-28.
//

#ifndef SPITFIRE_SYNC_H
#define SPITFIRE_SYNC_H

#include <vector>
#include <queue>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <future>
#include <functional>
#include <stdexcept>
#include <cassert>
#include <cstring>

namespace spitfire {
#define COMPILER_MEMORY_FENCE asm volatile("" ::: "memory")

class ThreadRefHolder;

class RefManager {
public:
    void AddThreadRef(ThreadRefHolder *te) {
        int i = idx.fetch_add(1);
        assert(i < max_num_threads);
        thread_refs[i] = te;
    }

    void DeleteThreadRef(ThreadRefHolder *te) {
        assert(idx <= max_num_threads);
        restart:
        for (int i = 0; i < thread_refs.size(); ++i) {
            if (thread_refs[i] == te) {
                thread_refs[i] = nullptr;
                break;
            }
        }
    }

    void IterateThreadRefs(std::function<void(ThreadRefHolder *)> processor) {
        assert(idx <= max_num_threads);
        size_t count = idx.load();
        for (int i = 0; i < count; ++i) {
            ThreadRefHolder *te = thread_refs[i];
            if (thread_refs[i] != nullptr)
                processor(thread_refs[i]);
        }
    }

    RefManager(size_t max_num_threads) : thread_refs(max_num_threads, nullptr), max_num_threads(max_num_threads),
                                         idx(0) {}

private:
    std::vector<ThreadRefHolder *> thread_refs;
    const size_t max_num_threads;
    std::atomic<int> idx;
};

class ThreadRefHolder {
public:
    ThreadRefHolder() : v(0), registered(false), manager(nullptr) {}

    uintptr_t SetValue(uintptr_t new_v) {
        v = new_v | Active();
        return new_v;
    }

    uintptr_t GetValue() { return v & (~1); }

    inline void Register(RefManager *manager) {
        if (registered)
            return;
        this->manager = manager;
        manager->AddThreadRef(this);
        registered = true;
    }

    ~ThreadRefHolder() {
        if (manager) {
            manager->DeleteThreadRef(this);
            manager = nullptr;
        }
    }

    void Enter() {
        v = v | 1;
    }

    void Leave() { v = 0; }

    bool Active() { return v & 1; }

private:
    uintptr_t v;
    bool registered;
    RefManager *manager;
};

// RAII-style Enter and Leave
class ThreadRefGuard {
public:
    ThreadRefGuard(ThreadRefHolder &ref) : thread_ref(ref) {
        ref.Enter();
        asm volatile("mfence"::
        : "memory");
    }

    ~ThreadRefGuard() { thread_ref.Leave(); }

private:
    ThreadRefHolder &thread_ref;
};

static void WaitUntilNoRefs(RefManager &manager, uintptr_t V) {
    while (true) {
        bool might_have_refs = false;
        manager.IterateThreadRefs(
                [&might_have_refs,
                        V](ThreadRefHolder *te) {
                    auto tev = te->GetValue();
                    if (te->Active() && (tev == 0 || te->GetValue() == V)) {
                        might_have_refs = true;
                    }
                });
        if (might_have_refs == false)
            break;
        std::this_thread::yield();
    }
}

static bool WaitUntilNoRefsNonBlocking(RefManager &manager, uintptr_t V, int wait_times) {
    while (wait_times--) {
        bool might_have_refs = false;
        manager.IterateThreadRefs(
                [&might_have_refs,
                        V](ThreadRefHolder *te) {
                    auto tev = te->GetValue();
                    if (te->Active() && (tev == 0 || te->GetValue() == V)) {
                        might_have_refs = true;
                    }
                });
        if (might_have_refs == false) {
            return true;
        }

        std::this_thread::yield();
    }
    return false;
}

class ThreadPool {
public:
    ThreadPool(size_t);

    template<class F, class... Args>
    auto enqueue(F &&f, Args &&... args)
    -> std::future<typename std::result_of<F(Args...)>::type>;

    ~ThreadPool();

    size_t ShowTaskCount() {
        std::unique_lock<std::mutex> lock(this->queue_mutex);
        return tasks.size();
    }

private:
    // need to keep track of threads so we can join them
    std::vector<std::thread> workers;
    // the task queue
    std::queue<std::function<void()> > tasks;

    // synchronization
    std::mutex queue_mutex;
    std::condition_variable condition;
    bool stop;
};

// the constructor just launches some amount of workers
inline ThreadPool::ThreadPool(size_t threads)
        : stop(false) {
    for (size_t i = 0; i < threads; ++i)
        workers.emplace_back(
                [this] {
                    for (;;) {
                        std::function<void()> task;

                        {
                            std::unique_lock<std::mutex> lock(this->queue_mutex);
                            this->condition.wait(lock,
                                                 [this] { return this->stop || !this->tasks.empty(); });
                            if (this->stop && this->tasks.empty())
                                return;
                            task = std::move(this->tasks.front());
                            this->tasks.pop();
                        }

                        task();
                    }
                }
        );
}

// add new work item to the pool
template<class F, class... Args>
auto ThreadPool::enqueue(F &&f, Args &&... args)
-> std::future<typename std::result_of<F(Args...)>::type> {
    using return_type = typename std::result_of<F(Args...)>::type;

    auto task = std::make_shared<std::packaged_task<return_type()> >(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...)
    );

    std::future<return_type> res = task->get_future();
    {
        std::unique_lock<std::mutex> lock(queue_mutex);

        // don't allow enqueueing after stopping the pool
        if (stop)
            throw std::runtime_error("enqueue on stopped ThreadPool");

        tasks.emplace([task]() { (*task)(); });
    }
    condition.notify_one();
    return res;
}

// the destructor joins all threads
inline ThreadPool::~ThreadPool() {
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        stop = true;
    }
    condition.notify_all();
    for (std::thread &worker: workers)
        worker.join();
}

class CountDownLatch {
public:
    explicit CountDownLatch(const unsigned int count) : count(count) {}

    virtual ~CountDownLatch() = default;

    void Await(void) {
        std::unique_lock<std::mutex> lock(mtx);
        if (count > 0) {
            cv.wait(lock, [this]() { return count == 0; });
        }
    }

    void CountDown(void) {
        std::unique_lock<std::mutex> lock(mtx);
        if (count > 0) {
            count--;
            cv.notify_all();
        }
    }

    unsigned int GetCount(void) {
        std::unique_lock<std::mutex> lock(mtx);
        return count;
    }

private:
    std::mutex mtx;
    std::condition_variable cv;
    unsigned int count = 0;
};

class DeferCode {
public:
    DeferCode(std::function<void()> code) : code(code) {}

    ~DeferCode() { code(); }

private:
    std::function<void()> code;
};

#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE  64 // 64 byte cache line on x86-64
#endif

struct PadInt {
    PadInt() { data = 0; }

    union {
        uint64_t data;
        char padding[CACHE_LINE_SIZE];
    };
};


template<int buckets>
class DistributedCounter {
public:
    static_assert(buckets == 0 || (buckets & (buckets - 1)) == 0, "buckets must be a multiple of 2");

    DistributedCounter(int initVal = 0) {
        for (int i = 0; i < buckets; ++i) {
            countArray[i].data = 0;
        }
        increment(initVal);
    }

    void operator+=(int by) { increment(by); }

    void operator-=(int by) { decrement(by); }

    void operator++() { *this += 1; }

    void operator++(int) { *this += 1; }

    void operator--() { *this -= 1; }

    void operator--(int) { *this -= 1; }

    int64_t load() const { return get(); }

    void store(int64_t v) {
        for (int i = 0; i < buckets; ++i) {
            countArray[i].data = 0;
        }
        countArray[0].data = v;
    }

    inline void increment(int v = 1) {
        int idx =  arrayIndex();
        assert(idx>=0);
        assert(idx < buckets);
        __atomic_add_fetch(&countArray[idx].data, v, __ATOMIC_RELAXED);
    }

    inline void decrement(int v = 1) {
        int idx =  arrayIndex();
        assert(idx>=0);
        assert(idx < buckets);
        __atomic_sub_fetch(&countArray[idx].data, v, __ATOMIC_RELAXED);
    }

    int64_t get() const {
        int64_t val = 0;
        for (int i = 0; i < buckets; ++i) {
            val += __atomic_load_n(&countArray[i].data, __ATOMIC_RELAXED);
        }
        return val;
    }

private:
    inline uint64_t getCPUId() {
        return cpuId;
    }

    inline int arrayIndex() {
        return getCPUId() & (buckets - 1);
    }

    static thread_local uint64_t cpuId;
    PadInt countArray[buckets];
};

template<int buckets>
thread_local uint64_t DistributedCounter<buckets>::cpuId = (uint64_t) std::hash<std::thread::id>{}(std::this_thread::get_id());

}
#endif //SPITFIRE_SYNC_H
