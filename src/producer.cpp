// Producer: generates packets of a given payload size, adds metadata
// (sequence number, monotonic timestamp, CRC32C) and pushes them into a
// shared-memory SPSC ring for the consumer process.
//
// Usage: producer <payload_bytes> [shm_name] [ring_mib]
//
// Pause/resume: SIGUSR1 or any key. Quit: 'q', Ctrl+C or SIGTERM.
// When the ring is full (consumer paused or slower), the producer applies
// backpressure: it waits for free space, no packet is ever dropped.

#include <algorithm>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <random>
#include <string>
#include <vector>

#include "control.hpp"
#include "crc32c.hpp"
#include "ring_buffer.hpp"

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
        std::fprintf(stderr, "invalid %s: '%s'\n", what, s);
        std::exit(2);
    }
    return v;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2 || argc > 4) {
        usage(argv[0]);
        return 2;
    }
    const size_t payload_size = parse_u64(argv[1], "payload size");
    const std::string shm_name = argc > 2 ? argv[2] : "/pkt_ring";
    const size_t ring_mib = argc > 3 ? parse_u64(argv[3], "ring size") : 64;
    const size_t capacity = ring_mib * 1024 * 1024;

    if (payload_size < 1) {
        std::fprintf(stderr, "payload size must be >= 1\n");
        return 2;
    }

    ipc::install_signal_handlers();
    ipc::RawTerminal term;

    ipc::SpscRing ring = ipc::SpscRing::create(shm_name, capacity);
    if (ipc::record_size(payload_size) > ring.max_record_size()) {
        std::fprintf(stderr, "payload too large for a %zu MiB ring (max ~%zu bytes)\n",
                     ring_mib, ring.max_record_size() - sizeof(ipc::RecordHeader));
        return 2;
    }

    std::fprintf(stderr,
                 "[producer] ring '%s' (%zu MiB), payload %zu B, pid %d\n"
                 "[producer] SIGUSR1 or any key: pause/resume, 'q' or Ctrl+C: quit\n",
                 shm_name.c_str(), ring_mib, payload_size, getpid());

    // Payload template: random bytes generated once. Generating fresh random
    // data per packet would benchmark the RNG, not the transport. The
    // sequence number is stamped into the head of the payload so both the
    // content and the CRC differ from packet to packet.
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
                std::fprintf(stderr, "[producer] paused at seq=%" PRIu64 "\n", seq);
                announced_pause = true;
            }
            ipc::handle_key(term);
            timespec ts{0, 20'000'000};
            nanosleep(&ts, nullptr);
            continue;
        }
        if (announced_pause) {
            std::fprintf(stderr, "[producer] resumed\n");
            announced_pause = false;
        }

        std::memcpy(payload.data(), &seq, std::min(payload_size, sizeof(seq)));

        ipc::RecordHeader hdr{};
        hdr.payload_size = static_cast<uint32_t>(payload_size);
        hdr.seq = seq;
        hdr.timestamp_ns = ipc::now_ns();
        hdr.crc = ipc::crc32c(payload.data(), payload_size);

        // Backpressure: if the ring is full, wait for the consumer. A pause
        // or stop request interrupts the wait; the packet is retried with
        // the same seq after resume, so nothing is lost or duplicated.
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
    std::fprintf(stderr,
                 "[producer] done: %" PRIu64 " packets, %.1f MB payload in %.1f s\n",
                 seq, seq * static_cast<double>(payload_size) / 1e6, secs);
    return 0;
}
