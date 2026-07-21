#pragma once

#include <cstdlib>
#include <exception>
#include <string>

#include "control.hpp"
#include "ipc_ring_buffer.hpp"
#include "logging.hpp"
#include "terminal.hpp"

namespace consumer {

inline ipc::RingBuffer open_ring(const std::string& shm_name) {
    spdlog::info("waiting for producer on '{}', pid {}", shm_name, getpid());
    spdlog::info("SIGUSR1 or any key: pause/resume, 'q' or Ctrl+C: quit");
    try {
        return ipc::RingBuffer::open(shm_name, &ipc::g_stop);
    } catch (const std::exception& e) {
        spdlog::critical("{}", e.what());
        std::exit(1);
    }
}

void run_consumer_loop(ipc::RingBuffer& ring, const ipc::RawTerminal& term);

}
