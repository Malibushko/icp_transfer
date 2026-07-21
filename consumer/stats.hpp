#pragma once

#include <cstdint>

#include "control.hpp"
#include "logging.hpp"

namespace consumer {

struct Stats {
    uint64_t total_packets = 0;
    uint64_t total_bytes = 0;
    uint64_t crc_errors = 0;
    uint64_t seq_gaps = 0;
    uint64_t window_packets = 0;
    uint64_t window_bytes = 0;
    uint64_t last_report_ns = 0;
};

namespace detail {

inline void log_throughput(const Stats& s, uint64_t now) {
    const double dt = static_cast<double>(now - s.last_report_ns) * 1e-9;
    const double pps = static_cast<double>(s.window_packets) / dt;
    const double bps = static_cast<double>(s.window_bytes) / dt;
    spdlog::info(
        "total={:12} | {:11.0f} pkt/s | {:9.2f} MB/s ({:6.3f} GB/s)"
        " | crc_err={} | seq_gap={}{}",
        s.total_packets, pps, bps / 1e6, bps / 1e9, s.crc_errors, s.seq_gaps,
        ipc::g_pause ? " [PAUSED]" : "");
}

}

inline void report_if_due(Stats& s, uint64_t now) {
    if (now - s.last_report_ns < 1'000'000'000ull) return;
    detail::log_throughput(s, now);
    s.window_packets = 0;
    s.window_bytes = 0;
    s.last_report_ns = now;
}

inline void log_summary(const Stats& s) {
    spdlog::info("done: total={} packets, {:.1f} MB payload, crc_err={}, seq_gap={}",
                 s.total_packets, static_cast<double>(s.total_bytes) / 1e6,
                 s.crc_errors, s.seq_gaps);
}

}
