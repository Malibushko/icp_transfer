#pragma once

#include <poll.h>
#include <termios.h>
#include <unistd.h>

#include "control.hpp"

namespace ipc {

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

    [[nodiscard]] int poll_key() const {
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
