#include <algorithm>
#include <cstring>
#include <random>
#include <string>
#include <vector>

#include "control.hpp"
#include "logging.hpp"
#include "protocol.hpp"
#include "ipc_ring_buffer.hpp"

#include <CLI/CLI.hpp>
#include <crc32c/crc32c.h>

int main(int argc, char** argv) {
    ipc::init_logging("producer");

    CLI::App app{"IPC ring buffer producer"};
    size_t payload_size = 0;
    std::string shm_name = "/pkt_ring";
    size_t ring_mib = 64;
    app.add_option("payload_bytes", payload_size, "payload size of each packet (>= 1)")
        ->required();
    app.add_option("shm_name", shm_name, "shared memory name")
        ->capture_default_str();
    app.add_option("ring_mib", ring_mib, "ring capacity in MiB, power of two")
        ->capture_default_str();
    CLI11_PARSE(app, argc, argv);

    if (payload_size < 1) {
        spdlog::error("payload_bytes must be >= 1");
        return 2;
    }
    if (ring_mib == 0 || (ring_mib & (ring_mib - 1)) != 0) {
        spdlog::error("ring_mib must be a power of two");
        return 2;
    }
    const size_t capacity = ring_mib * 1024 * 1024;

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
