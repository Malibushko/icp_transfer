#pragma once

#include <cstdint>
#include <ctime>

namespace ipc {

inline uint64_t now_ns() {
    timespec ts{};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ull +
           static_cast<uint64_t>(ts.tv_nsec);
}

inline void sleep_ms(long ms) {
    timespec ts{ms / 1000, (ms % 1000) * 1'000'000L};
    nanosleep(&ts, nullptr);
}

}
