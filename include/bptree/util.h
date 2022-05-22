#pragma once

#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <string>

#include "bptree/exception.h"

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

}  // namespace util
}  // namespace bptree