#include <string>

#include "control.hpp"
#include "logging.hpp"
#include "ipc_ring_buffer.hpp"

#include <crc32c/crc32c.h>

namespace {

struct Stats {
    uint64_t total_packets = 0;
    uint64_t total_bytes = 0;
    uint64_t crc_errors = 0;
    uint64_t seq_gaps = 0;
    uint64_t window_packets = 0;
    uint64_t window_bytes = 0;
    uint64_t last_report_ns = 0;

    void report_if_due(uint64_t now) {
        if (now - last_report_ns < 1'000'000'000ull) return;
        const double dt = (now - last_report_ns) * 1e-9;
        const double pps = static_cast<double>(window_packets) / dt;
        const double bps = static_cast<double>(window_bytes) / dt;
        spdlog::info(
            "total={:12} | {:11.0f} pkt/s | {:9.2f} MB/s ({:6.3f} GB/s)"
            " | crc_err={} | seq_gap={}{}",
            total_packets, pps, bps / 1e6, bps / 1e9, crc_errors, seq_gaps,
            ipc::g_pause ? " [PAUSED]" : "");
        window_packets = 0;
        window_bytes = 0;
        last_report_ns = now;
    }
};

}

int main(int argc, char** argv) {
    const std::string shm_name = argc > 1 ? argv[1] : "/pkt_ring";

    ipc::init_logging("consumer");
    ipc::install_signal_handlers();
    ipc::RawTerminal term;

    spdlog::info("waiting for producer on '{}', pid {}", shm_name, getpid());
    spdlog::info("SIGUSR1 or any key: pause/resume, 'q' or Ctrl+C: quit");
    ipc::RingBuffer ring = [&] {
        try {
            return ipc::RingBuffer::open(shm_name, &ipc::g_stop);
        } catch (const std::exception& e) {
            spdlog::critical("{}", e.what());
            std::exit(1);
        }
    }();

    Stats stats;
    stats.last_report_ns = ipc::now_ns();
    uint64_t expected_seq = 0;
    bool have_seq = false;
    bool announced_pause = false;
    ipc::Backoff backoff;
    uint64_t iter = 0;

    while (!ipc::g_stop) {
        if ((iter++ & 0x3FF) == 0) {
            ipc::handle_key(term);
            stats.report_if_due(ipc::now_ns());
        }

        if (ipc::g_pause) {
            if (!announced_pause) {
                spdlog::warn("paused (ring will fill, producer will block)");
                announced_pause = true;
            }
            ipc::handle_key(term);
            stats.report_if_due(ipc::now_ns());
            timespec ts{0, 20'000'000};
            nanosleep(&ts, nullptr);
            continue;
        }
        if (announced_pause) {
            spdlog::info("resumed");
            announced_pause = false;
        }

        const ipc::RecordHeader* hdr = ring.try_pop_begin();
        if (!hdr) {
            backoff.wait();
            ipc::handle_key(term);
            stats.report_if_due(ipc::now_ns());
            continue;
        }
        backoff.reset();

        const uint8_t* payload =
            reinterpret_cast<const uint8_t*>(hdr) + sizeof(ipc::RecordHeader);
        if (crc32c::Crc32c(payload, hdr->payload_size) != hdr->crc) {
            ++stats.crc_errors;
        }
        if (have_seq && hdr->seq != expected_seq) {
            ++stats.seq_gaps;
        }
        expected_seq = hdr->seq + 1;
        have_seq = true;

        ++stats.total_packets;
        ++stats.window_packets;
        stats.total_bytes += hdr->payload_size;
        stats.window_bytes += hdr->payload_size;
        ring.pop_end();
    }

    spdlog::info("done: total={} packets, {:.1f} MB payload, crc_err={}, seq_gap={}",
                 stats.total_packets, static_cast<double>(stats.total_bytes) / 1e6,
                 stats.crc_errors, stats.seq_gaps);
    return 0;
}
