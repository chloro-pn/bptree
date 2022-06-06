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
#include "bptree/queue.h"

#define BPTREE_INTERFACE

namespace bptree {
namespace util {

inline bool FileNotExist(const std::string& filename) {
  return !std::filesystem::exists(std::filesystem::path(filename));
}

inline void DeleteFile(const std::string& filename) { std::filesystem::remove(std::filesystem::path(filename)); }

inline bool CreateDir(const std::string& dir) { return std::filesystem::create_directory(std::filesystem::path(dir)); }

// 打开一个已经存在的文件，如果不存在则抛出异常
inline FILE* OpenFile(const std::string& file_name) {
  FILE* f = fopen(file_name.c_str(), "rb+");
  if (f == nullptr) {
    std::string e_str = "open file " + file_name + " fail : " + std::string(strerror(errno));
    throw BptreeExecption(e_str);
  }
  return f;
}

inline FILE* CreateFile(const std::string& file_name) {
  FILE* f = fopen(file_name.c_str(), "wb+");
  if (f == nullptr) {
    std::string e_str = "create file " + file_name + " fail : " + std::string(strerror(errno));
    throw BptreeExecption(e_str);
  }
  return f;
}

inline void FileAppend(FILE* f, const char* str, size_t size) {
  assert(f != nullptr);
  int ret = fwrite(str, size, 1, f);
  if (ret != 1) {
    std::string e_str = "fappend fail : " + std::string(strerror(errno));
    throw BptreeExecption(e_str);
  }
}

inline void FileWriteAt(FILE* f, const char* str, size_t size, size_t offset) {
  assert(f != nullptr);
  int ret = fseek(f, offset, SEEK_SET);
  if (ret != 0) {
    throw BptreeExecption("fseek error : " + std::string(strerror(errno)));
  }
  ret = fwrite(str, size, 1, f);
  if (ret != 1) {
    std::string e_str = "fwrite fail : " + std::string(strerror(errno));
    throw BptreeExecption(e_str);
  }
}

// 从当前位置读size字节的数据，如果有任何错误抛出异常
inline void FileRead(FILE* f, void* buf, size_t size) {
  assert(f != nullptr);
  int ret = fread(buf, size, 1, f);
  if (ret != 1) {
    throw BptreeExecption("fread fail : " + std::string(strerror(errno)));
  }
}

inline void FileReadAt(FILE* f, void* buf, size_t size, size_t offset) {
  assert(f != nullptr);
  int ret = fseek(f, offset, SEEK_SET);
  if (ret != 0) {
    throw BptreeExecption("fseek error : " + std::string(strerror(errno)));
  }
  ret = fread(buf, size, 1, f);
  if (ret != 1) {
    throw BptreeExecption("fread fail : " + std::string(strerror(errno)));
  }
}

inline bool FEof(FILE* f) { return feof(f) != 0; }

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
template <size_t n, typename T>
requires NotString<T> inline size_t AppendToBuf(char (&buf)[n], const T& t, size_t start_point) noexcept {
  memcpy((void*)&buf[start_point], &t, sizeof(T));
  start_point += sizeof(T);
  assert(start_point <= n);
  return start_point;
}

// tested
template <size_t n>
inline size_t AppendStrToBuf(char (&buf)[n], const std::string& str, size_t start_point) noexcept {
  uint32_t len = str.size();
  memcpy((void*)&buf[start_point], &len, sizeof(len));
  start_point += sizeof(len);
  memcpy((void*)&buf[start_point], str.data(), len);
  start_point += len;
  assert(start_point <= n);
  return start_point;
}

// tested
template <size_t n, typename T>
requires NotString<T> inline size_t ParseFromBuf(char (&buf)[n], T& t, size_t start_point) noexcept {
  memcpy(&t, &buf[start_point], sizeof(T));
  start_point += sizeof(T);
  assert(start_point <= n);
  return start_point;
}

// tested
template <size_t n>
inline size_t ParseStrFromBuf(char (&buf)[n], std::string& t, size_t start_point) {
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
  assert(start_point <= n);
  t.append(ptr, len);
  delete[] ptr;
  return start_point;
}

// end helper function

}  // namespace util

constexpr uint64_t no_wal_sequence = std::numeric_limits<uint64_t>::max();

enum class OperationType {
  Begin,
  End,
  RollBack,
  Get,
  GetRange,
  Insert,
  Update,
  Delete,
};

struct Operation {
  OperationType type;
  std::string key;
  std::string value;
  std::shared_ptr<Queue<Operation>> notify_queue_;
  uint64_t sequence;

  std::unique_ptr<Operation> CopyWithoutQueue() const {
    std::unique_ptr<Operation> result(new Operation());
    result->type = type;
    result->key = key;
    result->value = value;
    result->sequence = sequence;
    return result;
  }
};

inline void sleep(std::chrono::milliseconds ms) { std::this_thread::sleep_for(ms); }

}  // namespace bptree