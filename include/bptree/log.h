#pragma once

#include <iostream>
#include <string>

#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/spdlog.h"

#define BPTREE_LOG_DEBUG(...) spdlog::debug(__VA_ARGS__);
#define BPTREE_LOG_INFO(...) spdlog::info(__VA_ARGS__);
#define BPTREE_LOG_WARN(...) spdlog::warn(__VA_ARGS__);
#define BPTREE_LOG_ERROR(...) spdlog::error(__VA_ARGS__);

namespace bptree {
inline void LogInit(const std::string& filename = "") {
  if (filename != "") {
    auto logger = spdlog::rotating_logger_mt("file_logger", filename, 1024 * 1024 * 10, 3);
    spdlog::set_default_logger(logger);
  }
  spdlog::set_level(spdlog::level::debug);
}
}  // namespace bptree
