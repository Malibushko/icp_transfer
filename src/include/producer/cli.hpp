#pragma once

#include <cstddef>
#include <cstdlib>

#include "logging.hpp"
#include "producer.hpp"

#include <CLI/CLI.hpp>

namespace producer {

namespace detail {

inline bool is_power_of_two(size_t v) {
    return v != 0 && (v & (v - 1)) == 0;
}

}

inline Options parse_args(int argc, char** argv) {
    CLI::App app{"IPC ring buffer producer"};
    Options opt;
    app.add_option("payload_bytes", opt.payload_size, "payload size of each packet (>= 1)")
        ->required();
    app.add_option("shm_name", opt.shm_name, "shared memory name")
        ->capture_default_str();
    app.add_option("ring_mib", opt.ring_mib, "ring capacity in MiB, power of two")
        ->capture_default_str();

    try {
        app.parse(argc, argv);
    } catch (const CLI::ParseError& e) {
        std::exit(app.exit(e));
    }
    if (opt.payload_size < 1) {
        spdlog::error("payload_bytes must be >= 1");
        std::exit(2);
    }
    if (!detail::is_power_of_two(opt.ring_mib)) {
        spdlog::error("ring_mib must be a power of two");
        std::exit(2);
    }
    return opt;
}

}
