#pragma once

#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <memory>
#include <numeric>
#include <string>
#include <thread>

#include "bptree/exception.h"

#define BPTREE_INTERFACE

namespace bptree {
namespace util {

inline bool FileNotExist(const std::string& filename) {
  return !std::filesystem::exists(std::filesystem::path(filename));
}

inline void DeleteFile(const std::string& filename) { std::filesystem::remove(std::filesystem::path(filename)); }

inline bool CreateDir(const std::string& dir) { return std::filesystem::create_directory(std::filesystem::path(dir)); }

template <typename T>
inline void StringAppender(std::string& str, const T& t) {
  str.append((const char*)&t, sizeof(t));
}

inline void StringAppender(std::string& str, const std::string& t) {
  uint32_t length = static_cast<uint32_t>(t.size());
  str.append((const char*)&length, sizeof(uint32_t));
  str.append(t);
}

template <typename T>
inline T StringParser(const std::string& str, size_t& offset) {
  assert(offset + sizeof(T) <= str.size());
  T t;
  memcpy(&t, &str[offset], sizeof(T));
  offset += sizeof(T);
  return t;
}

inline std::string StringParser(const std::string& str, size_t& offset) {
  assert(offset + sizeof(uint32_t) <= str.size());
  uint32_t length = 0;
  memcpy(&length, &str[offset], sizeof(uint32_t));
  offset += sizeof(uint32_t);
  assert(offset + length <= str.size());
  std::string tmp;
  tmp.resize(length);
  memcpy(&tmp[0], &str[offset], length);
  offset += length;
  return tmp;
}

// helper function

// tested
inline uint32_t StringToUInt32t(const std::string& value) noexcept {
  return static_cast<uint32_t>(atol(value.c_str()));
}

// tested
inline uint32_t StringToUInt32t(const std::string_view& value) noexcept {
  return static_cast<uint32_t>(atol(value.data()));
}

// tested
inline std::string ConstructIndexByNum(uint32_t n) noexcept {
  std::string result((const char*)&n, sizeof(uint32_t));
  return result;
}

template <typename T>
concept NotString = !std::is_same_v<std::decay_t<T>, std::string>;

// tested
template <typename T>
requires NotString<T> inline size_t AppendToBuf(char* buf, const T& t, size_t start_point) noexcept {
  memcpy((void*)&buf[start_point], &t, sizeof(T));
  start_point += sizeof(T);
  return start_point;
}

// tested
inline size_t AppendStrToBuf(const char* buf, const std::string& str, size_t start_point) noexcept {
  uint32_t len = str.size();
  memcpy((void*)&buf[start_point], &len, sizeof(len));
  start_point += sizeof(len);
  memcpy((void*)&buf[start_point], str.data(), len);
  start_point += len;
  return start_point;
}

// tested
template <typename T>
requires NotString<T> inline size_t ParseFromBuf(const char* buf, T& t, size_t start_point) noexcept {
  memcpy(&t, &buf[start_point], sizeof(T));
  start_point += sizeof(T);
  return start_point;
}

// tested
inline size_t ParseStrFromBuf(const char* buf, std::string& t, size_t start_point) {
  uint32_t len = 0;
  memcpy(&len, &buf[start_point], sizeof(len));
  start_point += sizeof(len);
  t.clear();
  if (len == 0) {
    return start_point;
  }
  char* ptr = new char[len];
  memcpy(ptr, &buf[start_point], len);
  start_point += len;
  t.append(ptr, len);
  delete[] ptr;
  return start_point;
}

// end helper function

}  // namespace util

constexpr uint64_t no_wal_sequence = std::numeric_limits<uint64_t>::max();

}  // namespace bptree