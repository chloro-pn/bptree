#pragma once

#include <filesystem>
#include <string>

namespace bptree {
namespace util {

inline bool FileNotExist(const std::string& filename) {
  return !std::filesystem::exists(std::filesystem::path(filename));
}

}  // namespace util
}  // namespace bptree