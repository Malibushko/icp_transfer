#pragma once

#include <csignal>

namespace ipc {

inline volatile std::sig_atomic_t g_stop = 0;
inline volatile std::sig_atomic_t g_pause = 0;

inline void install_signal_handlers() {
    struct sigaction sa{};
    sigemptyset(&sa.sa_mask);
    sa.sa_handler = [](int) { g_stop = 1; };
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
    sa.sa_handler = [](int) { g_pause = !g_pause; };
    sigaction(SIGUSR1, &sa, nullptr);
}

}
