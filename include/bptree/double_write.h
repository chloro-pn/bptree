#pragma once

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <string>

#include "bptree/block.h"
#include "bptree/exception.h"
#include "bptree/util.h"

namespace bptree {

class DoubleWrite {
 public:
  explicit DoubleWrite(const std::string& file_name) : file_name_(file_name), f_(nullptr) {}

  void OpenFile() {
    if (util::FileNotExist(file_name_)) {
      f_ = util::CreateFile(file_name_);
    } else {
      f_ = util::OpenFile(file_name_);
    }
  }

  void WriteBlock(BlockBase* block) {
    util::FileWriteAt(f_, block->GetBuf(), block_size, 0);
    fflush(f_);
  }

  void ReadBlock(BlockBase* block) {
    block->BufInit();
    fseek(f_, 0, SEEK_SET);
    int ret = fread(block->GetBuf(), block_size, 1, f_);
    if (ret != 1) {
      throw BptreeExecption("dw file read fail : {}", strerror(errno));
    }
  }

  void Close() {
    if (f_ == nullptr) {
      return;
    }
    fflush(f_);
    fclose(f_);
    f_ = nullptr;
  }

  ~DoubleWrite() { Close(); }

 private:
  std::string file_name_;
  FILE* f_;
};
}  // namespace bptree