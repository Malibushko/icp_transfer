#pragma once

#include <cstdlib>
#include <string>

#include <CLI/CLI.hpp>

namespace consumer {

inline std::string parse_args(int argc, char** argv) {
    CLI::App app{"IPC ring buffer consumer"};
    std::string shm_name = "/pkt_ring";
    app.add_option("shm_name", shm_name, "shared memory name")
        ->capture_default_str();
    try {
        app.parse(argc, argv);
    } catch (const CLI::ParseError& e) {
        std::exit(app.exit(e));
    }
    return shm_name;
}

}
