#include "consumer.hpp"

#include "backoff.hpp"
#include "clock.hpp"
#include "control.hpp"
#include "logging.hpp"
#include "protocol.hpp"
#include "stats.hpp"
#include "terminal.hpp"

#include <crc32c/crc32c.h>

namespace consumer {

void run_consumer_loop(ipc::RingBuffer& ring, const ipc::RawTerminal& term) {
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
            report_if_due(stats, ipc::now_ns());
        }

        if (ipc::g_pause) {
            if (!announced_pause) {
                spdlog::warn("paused (ring will fill, producer will block)");
                announced_pause = true;
            }
            ipc::handle_key(term);
            report_if_due(stats, ipc::now_ns());
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
            report_if_due(stats, ipc::now_ns());
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

    log_summary(stats);
}

}
