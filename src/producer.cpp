#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <random>
#include <string>
#include <vector>

#include "control.hpp"
#include "logging.hpp"
#include "protocol.hpp"
#include "ipc_ring_buffer.hpp"

#include <crc32c/crc32c.h>

namespace {

void usage(const char* argv0) {
    std::fprintf(stderr,
                 "Usage: %s <payload_bytes> [shm_name] [ring_mib]\n"
                 "  payload_bytes  payload size of each packet (>= 1)\n"
                 "  shm_name       shared memory name (default /pkt_ring)\n"
                 "  ring_mib       ring capacity in MiB, power of two (default 64)\n",
                 argv0);
}

uint64_t parse_u64(const char* s, const char* what) {
    char* end = nullptr;
    errno = 0;
    const unsigned long long v = std::strtoull(s, &end, 10);
    if (errno != 0 || end == s || *end != '\0') {
        spdlog::error("invalid {}: '{}'", what, s);
        std::exit(2);
    }
    return v;
}

}

int main(int argc, char** argv) {
    ipc::init_logging("producer");

    if (argc < 2 || argc > 4) {
        usage(argv[0]);
        return 2;
    }
    const size_t payload_size = parse_u64(argv[1], "payload size");
    const std::string shm_name = argc > 2 ? argv[2] : "/pkt_ring";
    const size_t ring_mib = argc > 3 ? parse_u64(argv[3], "ring size") : 64;
    const size_t capacity = ring_mib * 1024 * 1024;

    if (payload_size < 1) {
        spdlog::error("payload size must be >= 1");
        return 2;
    }

    ipc::install_signal_handlers();
    ipc::RawTerminal term;

    ipc::RingBuffer ring = ipc::RingBuffer::create(shm_name, capacity);
    if (ipc::record_size(payload_size) > ring.max_record_size()) {
        spdlog::error("payload too large for a {} MiB ring (max ~{} bytes)",
                      ring_mib, ring.max_record_size() - sizeof(ipc::RecordHeader));
        return 2;
    }

    spdlog::info("ring '{}' ({} MiB), payload {} B, pid {}",
                 shm_name, ring_mib, payload_size, getpid());
    spdlog::info("SIGUSR1 or any key: pause/resume, 'q' or Ctrl+C: quit");

    std::vector<uint8_t> payload(payload_size);
    std::mt19937_64 rng(0xC0FFEE);
    for (auto& b : payload) b = static_cast<uint8_t>(rng());

    uint64_t seq = 0;
    bool announced_pause = false;
    ipc::Backoff backoff;
    const uint64_t start_ns = ipc::now_ns();

    while (!ipc::g_stop) {
        if ((seq & 0x3FF) == 0) ipc::handle_key(term);

        if (ipc::g_pause) {
            if (!announced_pause) {
                spdlog::warn("paused at seq={}", seq);
                announced_pause = true;
            }
            ipc::handle_key(term);
            timespec ts{0, 20'000'000};
            nanosleep(&ts, nullptr);
            continue;
        }
        if (announced_pause) {
            spdlog::info("resumed");
            announced_pause = false;
        }

        std::memcpy(payload.data(), &seq, std::min(payload_size, sizeof(seq)));

        ipc::RecordHeader hdr{};
        hdr.payload_size = static_cast<uint32_t>(payload_size);
        hdr.seq = seq;
        hdr.timestamp_ns = ipc::now_ns();
        hdr.crc = crc32c::Crc32c(payload.data(), payload_size);

        backoff.reset();
        bool pushed = ring.try_push(hdr, payload.data());
        while (!pushed && !ipc::g_stop && !ipc::g_pause) {
            backoff.wait();
            if ((backoff.n & 0xFFF) == 0) ipc::handle_key(term);
            pushed = ring.try_push(hdr, payload.data());
        }
        if (pushed) ++seq;
    }

    const double secs = (ipc::now_ns() - start_ns) * 1e-9;
    spdlog::info("done: {} packets, {:.1f} MB payload in {:.1f} s",
                 seq, seq * static_cast<double>(payload_size) / 1e6, secs);
    return 0;
}
