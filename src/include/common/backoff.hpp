#pragma once

#include <ctime>

#include <sched.h>

namespace ipc {

inline void cpu_relax() {
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#elif defined(__aarch64__)
    asm volatile("yield");
#else
    asm volatile("" ::: "memory");
#endif
}

struct Backoff {
    unsigned n = 0;
    void reset() { n = 0; }
    void wait() {
        ++n;
        if (n < 64) {
            cpu_relax();
        } else if (n < 4096) {
            sched_yield();
        } else {
            timespec ts{0, 100'000};
            nanosleep(&ts, nullptr);
        }
    }
};

}
