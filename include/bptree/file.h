#pragma once

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <cstring>
#include <string>

#include "bptree/exception.h"
#include "bptree/log.h"

namespace bptree {

enum class OS {
  LINUX,
  WIN,
  APPLE,
};

enum class FileType {
  NORMAL,
  DIRECT,
  DIRECT_AND_SYNC,
};

template <OS os>
class FileHandlerImpl;

template <>
class FileHandlerImpl<OS::LINUX> {
 public:
  /**
   * @brief 打开文件名称为filename的文件，具有读写权限，文件必须存在
   * @param filename 文件名称
   * @param sync 是否以同步模式打开文件
   * @note 如果不满足权限或者文件不存在，抛出异常
   */
  static FileHandlerImpl OpenFile(const std::string& filename, FileType type = FileType::NORMAL) {
    int flag = O_RDWR;
    if (type == FileType::DIRECT) {
      flag |= O_DIRECT;
    } else if (type == FileType::DIRECT_AND_SYNC) {
      flag = flag | O_DIRECT | O_SYNC;
    }
    int fd = open(filename.c_str(), flag, 0666);
    if (fd == -1) {
      throw BptreeExecption("open file {} error : {}", filename, strerror(errno));
    }
    return FileHandlerImpl(fd, filename);
  }

  /**
   * @brief 创建文件名称为filename的文件，具有读写权限，文件必须不存在
   * @param filename 文件名称
   * @param sync 是否以同步模式打开文件
   * @note 如果不满足权限或者文件已存在，抛出异常
   */
  static FileHandlerImpl CreateFile(const std::string& filename, FileType type = FileType::NORMAL) {
    int flag = O_RDWR | O_CREAT | O_EXCL;
    if (type == FileType::DIRECT) {
      flag |= O_DIRECT;
    } else if (type == FileType::DIRECT_AND_SYNC) {
      flag = flag | O_DIRECT | O_SYNC;
    }
    int fd = open(filename.c_str(), flag, 0666);
    if (fd == -1) {
      throw BptreeExecption("create file {} error : {}", filename, strerror(errno));
    }
    return FileHandlerImpl(fd, filename);
  }

  FileHandlerImpl(int fd, const std::string& filename) : fd_(fd), file_name_(filename) {
    BPTREE_LOG_DEBUG("file {} fd {}", file_name_, fd_);
  }

  FileHandlerImpl() : fd_(-1) {}

  FileHandlerImpl(const FileHandlerImpl&) = delete;
  FileHandlerImpl& operator=(const FileHandlerImpl&) = delete;

  FileHandlerImpl(FileHandlerImpl&& other) : fd_(other.fd_), file_name_(std::move(other.file_name_)) { other.fd_ = -1; }

  FileHandlerImpl& operator=(FileHandlerImpl&& other) {
    Close();
    fd_ = other.fd_;
    file_name_ = std::move(other.file_name_);
    other.fd_ = -1;
    return *this;
  }

  /**
   * @brief 定位写，将buf开始的连续nbyte个字节写入偏移量为offset的位置
   * @param buf 写入数据首地址
   * @param nbyte 写入数据长度
   * @param offset 文件偏移量
   * @return
   *     - true 写入成功
   *     - false 写入失败，错误信息参考errno值
   * @note
   *     - linux平台下使用direct io，因此对buf和offset都有要求（512字节对齐）
   *     - learn from : https://github.com/siying/rocksdb/blob/816b0baf7aa5e4b2280cd2c596f12bc4a50401e2/env/io_posix.cc
   */
  bool WriteWithoutException(const char* buf, size_t nbyte, size_t offset) noexcept {
    const size_t kLimit1Gb = 1UL << 30;
    const char* src = buf;
    size_t left = nbyte;
    while (left != 0) {
      size_t bytes_to_write = std::min(left, kLimit1Gb);
      ssize_t done = pwrite(fd_, src, bytes_to_write, static_cast<off_t>(offset));
      if (done < 0) {
        if (errno == EINTR) {
          continue;
        }
        return false;
      }
      left -= done;
      offset += done;
      src += done;
    }
    return true;
  }

  /**
   * @brief 追加写，将buf开始的连续nbyte个字节写入文件
   * @param buf 写入数据首地址
   * @param nbyte 写入数据长度
   * @return
   *     - true 写入成功
   *     - false 写入失败，错误信息参考errno值
   * @note
   *     - linux平台下使用direct io，因此对buf和offset都有要求（512字节对齐）
   *     - learn from : https://github.com/siying/rocksdb/blob/816b0baf7aa5e4b2280cd2c596f12bc4a50401e2/env/io_posix.cc
   */
  bool WriteWithoutException(const char* buf, size_t nbyte) noexcept {
    const size_t kLimit1Gb = 1UL << 30;
    const char* src = buf;
    size_t left = nbyte;
    while (left != 0) {
      size_t bytes_to_write = std::min(left, kLimit1Gb);
      ssize_t done = write(fd_, src, bytes_to_write);
      if (done < 0) {
        if (errno == EINTR) {
          continue;
        }
        return false;
      }
      left -= done;
      src += done;
    }
    return true;
  }

  void Write(const char* buf, size_t nbyte, size_t offset) {
    bool succ = WriteWithoutException(buf, nbyte, offset);
    if (succ == false) {
      throw BptreeExecption("file {}. Write error : {}", file_name_, strerror(errno));
    }
  }

  void Write(const char* buf, size_t nbyte) {
    bool succ = WriteWithoutException(buf, nbyte);
    if (succ == false) {
      throw BptreeExecption("file {}. Write error : {}", file_name_, strerror(errno));
    }
  }

  /**
   * @brief 定位读，从偏移量offset开始读取连续的nbyte个字节存储buf中
   * @param buf buf首地址
   * @param nbyte buf长度
   * @param offset 文件偏移量
   * @param eof 向调用方传递本次是否读到了文件尾
   * @return
   *    - true 读成功
   *    - false 读失败，错误信息参考errno值或者eof
   * @note
   *     - linux平台下使用direct io，因此对buf和offset都有要求（512字节对齐）
   */
  bool ReadWithoutException(char* buf, size_t nbyte, size_t offset, bool& eof) noexcept {
    eof = false;
    ssize_t ret = -1;
    size_t left = nbyte;
    char* ptr = buf;
    while (left > 0) {
      ret = pread(fd_, ptr, left, static_cast<off_t>(offset));
      if (ret <= 0) {
        if (ret == -1 && errno == EINTR) {
          continue;
        }
        if (ret == 0) {
          eof = true;
        }
        break;
      }
      ptr += ret;
      offset += ret;
      left -= ret;
    }
    return left == 0;
  }

  /**
   * @brief 当前读，读取连续的nbyte个字节存储buf中
   * @param buf buf首地址
   * @param nbyte buf长度
   * @param eof 向调用方传递本次是否读到了文件尾
   * @return
   *    - true 读成功
   *    - false 读失败，错误信息参考errno值或者eof
   * @note
   *     - linux平台下使用direct io，因此对buf和offset都有要求（512字节对齐）
   */
  bool ReadWithoutException(char* buf, size_t nbyte, bool& eof) noexcept {
    eof = false;
    ssize_t ret = -1;
    size_t left = nbyte;
    char* ptr = buf;
    while (left > 0) {
      ret = read(fd_, ptr, left);
      if (ret <= 0) {
        if (ret == -1 && errno == EINTR) {
          continue;
        }
        if (ret == 0) {
          eof = true;
        }
        break;
      }
      ptr += ret;
      left -= ret;
    }
    return left == 0;
  }

  void Read(char* buf, size_t nbyte, size_t offset) {
    bool eof = false;
    bool succ = ReadWithoutException(buf, nbyte, offset, eof);
    if (succ == false) {
      std::string err_msg;
      if (eof == true) {
        err_msg = "end_of_file";
      } else {
        err_msg = strerror(errno);
      }
      throw BptreeExecption("file {}. Read error : {}", file_name_, err_msg);
    }
  }

  void Read(char* buf, size_t nbyte) {
    bool eof = false;
    bool succ = ReadWithoutException(buf, nbyte, eof);
    if (succ == false) {
      std::string err_msg;
      if (eof == true) {
        err_msg = "end_of_file";
      } else {
        err_msg = strerror(errno);
      }
      throw BptreeExecption("file {}. Read error : {}", file_name_, err_msg);
    }
  }

  void Flush() { fsync(fd_); }

  void Close() {
    if (fd_ != -1) {
      close(fd_);
      fd_ = -1;
    }
  }

  bool Closed() const { return fd_ == -1; }

  ~FileHandlerImpl() { Close(); }

 private:
  int fd_;
  std::string file_name_;
};

#ifdef __linux__
using FileHandler = FileHandlerImpl<OS::LINUX>;
#endif

}  // namespace bptree