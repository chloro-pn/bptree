#pragma once

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

}  // namespace util
}  // namespace bptree