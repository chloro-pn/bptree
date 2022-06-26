#pragma once

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <string>

#include "bptree/block.h"
#include "bptree/exception.h"
#include "bptree/file.h"
#include "bptree/util.h"

namespace bptree {

class DoubleWrite {
 public:
  explicit DoubleWrite(const std::string& file_name) : file_name_(file_name), f_() {}

  void OpenFile() {
    if (util::FileNotExist(file_name_)) {
      f_ = FileHandler::CreateFile(file_name_, FileType::DIRECT_AND_SYNC);
    } else {
      f_ = FileHandler::OpenFile(file_name_, FileType::DIRECT_AND_SYNC);
    }
  }

  void WriteBlock(const BlockBase& block) {
    f_.Write(block.GetBuf(), block_size, 0); 
    }

  void ReadBlock(char* buf) { f_.Read(buf, block_size, 0); }

  void Close() { f_.Close(); }

  ~DoubleWrite() { Close(); }

 private:
  std::string file_name_;
  FileHandler f_;
};
}  // namespace bptree