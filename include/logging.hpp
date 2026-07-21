#pragma once

#include <string>
#include <utility>

#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>

namespace ipc {

inline void init_logging(const std::string& name) {
    auto logger = spdlog::stderr_color_mt(name);
    logger->set_pattern("[%H:%M:%S.%e] [%^%l%$] [%n] %v");
    logger->flush_on(spdlog::level::info);
    spdlog::set_default_logger(std::move(logger));
}

}
