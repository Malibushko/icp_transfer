#include "producer.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <random>
#include <vector>

#include "backoff.hpp"
#include "clock.hpp"
#include "control.hpp"
#include "logging.hpp"
#include "protocol.hpp"
#include "terminal.hpp"

#include <crc32c/crc32c.h>

namespace producer {

namespace {

std::vector<uint8_t> make_payload(size_t size) {
    static std::random_device rd;

    std::vector<uint8_t> payload(size);
    std::mt19937_64 rng(rd());
    for (auto& b : payload) {
        b = static_cast<uint8_t>(rng());
    }
    return payload;
}

}

void run_producer_loop(ipc::RingBuffer& ring, const ipc::RawTerminal& term, const Options& opt) {
    spdlog::info("ring '{}' ({} MiB), payload {} B, pid {}",
                 opt.shm_name, opt.ring_mib, opt.payload_size, getpid());
    spdlog::info("SIGUSR1 or any key: pause/resume, 'q' or Ctrl+C: quit");

    std::vector<uint8_t> payload = make_payload(opt.payload_size);
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

        std::memcpy(payload.data(), &seq, std::min(opt.payload_size, sizeof(seq)));

        ipc::RecordHeader hdr{};
        hdr.payload_size = static_cast<uint32_t>(opt.payload_size);
        hdr.seq = seq;
        hdr.timestamp_ns = ipc::now_ns();
        hdr.crc = crc32c::Crc32c(payload.data(), opt.payload_size);

        backoff.reset();
        bool pushed = ring.try_push(hdr, payload.data());
        while (!pushed && !ipc::g_stop && !ipc::g_pause) {
            backoff.wait();
            if ((backoff.n & 0xFFF) == 0) ipc::handle_key(term);
            pushed = ring.try_push(hdr, payload.data());
        }
        if (pushed) ++seq;
    }

    const double secs = static_cast<double>((ipc::now_ns() - start_ns)) * 1e-9;
    spdlog::info("done: {} packets, {:.1f} MB payload in {:.1f} s",
                 seq, static_cast<double>(seq) * static_cast<double>(opt.payload_size) / 1e6, secs);
}

}
