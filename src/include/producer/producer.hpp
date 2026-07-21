#pragma once

#include <cstddef>
#include <cstdlib>
#include <string>

#include "ipc_ring_buffer.hpp"
#include "logging.hpp"
#include "protocol.hpp"
#include "terminal.hpp"

namespace producer {

constexpr size_t kMiB = 1024 * 1024;

struct Options {
    size_t payload_size = 0;
    std::string shm_name = "/pkt_ring";
    size_t ring_mib = 64;
};

inline ipc::RingBuffer create_ring(const Options& opt) {
    const size_t capacity = opt.ring_mib * kMiB;
    if (opt.payload_size >= ipc::kWrapMarker) {
        spdlog::error("payload_bytes must be < {}", ipc::kWrapMarker);
        std::exit(2);
    }
    if (ipc::record_size(opt.payload_size) > capacity / 2) {
        spdlog::error("payload too large for a {} MiB ring (max ~{} bytes)",
                      opt.ring_mib, capacity / 2 - sizeof(ipc::RecordHeader));
        std::exit(2);
    }
    return ipc::RingBuffer::create(opt.shm_name, capacity);
}

void run_producer_loop(ipc::RingBuffer& ring, const ipc::RawTerminal& term, const Options& opt);

}
