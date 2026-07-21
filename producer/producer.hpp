#pragma once

#include <cstddef>
#include <cstdlib>
#include <string>

#include "control.hpp"
#include "ipc_ring_buffer.hpp"
#include "logging.hpp"
#include "protocol.hpp"

namespace producer {

constexpr size_t kMiB = 1024 * 1024;

struct Options {
    size_t payload_size = 0;
    std::string shm_name = "/pkt_ring";
    size_t ring_mib = 64;
};

inline ipc::RingBuffer create_ring(const Options& opt) {
    ipc::RingBuffer ring = ipc::RingBuffer::create(opt.shm_name, opt.ring_mib * kMiB);
    if (ipc::record_size(opt.payload_size) > ring.max_record_size()) {
        spdlog::error("payload too large for a {} MiB ring (max ~{} bytes)",
                      opt.ring_mib, ring.max_record_size() - sizeof(ipc::RecordHeader));
        std::exit(2);
    }
    return ring;
}

void run_producer_loop(ipc::RingBuffer& ring, const ipc::RawTerminal& term, const Options& opt);

}
