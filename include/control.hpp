#pragma once

#include <csignal>
#include <cstdint>

#include <poll.h>
#include <sched.h>
#include <termios.h>
#include <time.h>
#include <unistd.h>

namespace ipc {

inline volatile std::sig_atomic_t g_stop = 0;
inline volatile std::sig_atomic_t g_pause = 0;

inline void install_signal_handlers() {
    struct sigaction sa{};
    sa.sa_handler = [](int) { g_stop = 1; };
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
    sa.sa_handler = [](int) { g_pause = !g_pause; };
    sigaction(SIGUSR1, &sa, nullptr);
}

inline uint64_t now_ns() {
    timespec ts{};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ull +
           static_cast<uint64_t>(ts.tv_nsec);
}

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

class RawTerminal {
public:
    RawTerminal() {
        if (!isatty(STDIN_FILENO)) return;
        if (tcgetattr(STDIN_FILENO, &saved_) != 0) return;
        termios raw = saved_;
        raw.c_lflag &= ~static_cast<tcflag_t>(ICANON | ECHO);
        raw.c_cc[VMIN] = 0;
        raw.c_cc[VTIME] = 0;
        if (tcsetattr(STDIN_FILENO, TCSANOW, &raw) == 0) active_ = true;
    }
    ~RawTerminal() {
        if (active_) tcsetattr(STDIN_FILENO, TCSANOW, &saved_);
    }
    RawTerminal(const RawTerminal&) = delete;
    RawTerminal& operator=(const RawTerminal&) = delete;

    int poll_key() const {
        if (!active_) return -1;
        pollfd pfd{STDIN_FILENO, POLLIN, 0};
        if (poll(&pfd, 1, 0) > 0 && (pfd.revents & POLLIN)) {
            unsigned char c;
            if (read(STDIN_FILENO, &c, 1) == 1) return c;
        }
        return -1;
    }

private:
    termios saved_{};
    bool active_ = false;
};

inline void handle_key(const RawTerminal& term) {
    const int key = term.poll_key();
    if (key < 0) return;
    if (key == 'q' || key == 'Q') {
        g_stop = 1;
    } else {
        g_pause = !g_pause;
    }
}

}
