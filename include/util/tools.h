//
// Created by zxjcarrot on 2020-01-27.
//
#ifndef SPITFIRE_TOOLS_H
#define SPITFIRE_TOOLS_H
#include <iostream>
#include <chrono>

#define EXPECT_EQ(A,B) assert((A) == (B))
#define EXPECT_TRUE(A) assert(A)

namespace spitfire {

class Timer {
public:
    void Start() {
        m_StartTime = std::chrono::system_clock::now();
        m_bRunning = true;
    }

    void Stop() {
        m_EndTime = std::chrono::system_clock::now();
        m_bRunning = false;
    }

    double ElapsedMilliseconds() {
        std::chrono::time_point<std::chrono::system_clock> endTime;

        if (m_bRunning) {
            endTime = std::chrono::system_clock::now();
        } else {
            endTime = m_EndTime;
        }

        return std::chrono::duration_cast<std::chrono::milliseconds>(endTime - m_StartTime).count();
    }

    double ElapsedSeconds() {
        return ElapsedMilliseconds() / 1000.0;
    }

private:
    std::chrono::time_point<std::chrono::system_clock> m_StartTime;
    std::chrono::time_point<std::chrono::system_clock> m_EndTime;
    bool m_bRunning = false;
};

class TimedThroughputReporter {
private:
    Timer timer;
    size_t nops;
public:
    TimedThroughputReporter(size_t n) : nops(n) {
        timer.Start();
    }

    ~TimedThroughputReporter() {
        timer.Stop();
        std::cout << "Elapsed " << timer.ElapsedSeconds() << " secs, " << (int) (nops / timer.ElapsedSeconds())
                  << " op/s"
                  << std::endl;
    }
};


}
#endif //SPITFIRE_TOOLS_H
