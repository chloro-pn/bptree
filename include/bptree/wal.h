#pragma once

#include <cassert>
#include <cstdio>
#include <functional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "bptree/exception.h"
#include "bptree/log.h"
#include "bptree/util.h"
#include "crc/crc32.h"

namespace bptree {

/*
 * 该类的作用是为block_manager提供事务日志的支持，通过将多个block的修改写入同一条wal中，
 * 如果在全部将block刷盘之前进程crach或者主机直接down掉，可以通过回放wal日志维护多个修改的一致性。
 * （即要么都修改，要么都不修改）
 * wal的作用：bptree内部使用，当产生split/merge时涉及多个block上的修改，通过wal保证一致性；提供给用户使用，支持单机事务功能
 * 每条日志的格式：length + sequence + type + log + crc32
 * type标志以下几个类型之一：事务开始日志、事务结束日志、事务放弃日志、数据日志
 */
class WriteAheadLog {
 public:
  struct LogEntry {
    // type == 0 事务开始, type == 1 事务结束, type == 2 事务放弃, type == 3 数据日志
    uint8_t type;
    uint64_t sequence;
    std::string msg;
    uint32_t crc;
  };

  enum class LogType : uint8_t {
    TxBegin,
    TxEnd,
    TxAbort,
    Data,
  };

  WriteAheadLog(const std::string& file_name, const std::function<void(uint64_t, std::string)>& handler)
      : next_wal_sequence_(0), file_name_(file_name), f_(nullptr), log_handler_(handler) {
    if (util::FileNotExist(file_name_)) {
      f_ = fopen(file_name_.c_str(), "wb+");
    } else {
      f_ = fopen(file_name_.c_str(), "rb+");
    }
    if (f_ == nullptr) {
      throw BptreeExecption("file " + file_name_ + " open fail");
    }
    recover(f_);
  }

  ~WriteAheadLog() { Close(); }

  // 申请一个新的事务编号，并将事务开始标志写入wal文件，之后在事务结束标志被写入前，所有使用该编号写入的
  // 数据日志都被认为是在一个事务内，wal保证这些数据日志要么都被提交，要么都会通过调用log_handler_进行回滚
  uint64_t Begin() {
    uint64_t result = next_wal_sequence_;
    next_wal_sequence_ += 1;
    assert(writing_wal_.count(result) == 0);
    writing_wal_.insert(result);
    // write
    WriteBeginLog(result);
    return result;
  }

  void WriteLog(uint64_t sequence, const std::string& log, LogType etype = LogType::Data) {
    uint8_t type = logTypeToUint8(etype);
    uint32_t length = sizeof(sequence) + sizeof(type) + log.size() + sizeof(uint32_t);
    std::string result;
    result.append((const char*)&length, sizeof(length));
    result.append((const char*)&sequence, sizeof(sequence));
    result.append((const char*)&type, sizeof(type));
    result.append(log);
    uint32_t crc = crc32(&result[sizeof(length)], result.size() - sizeof(length));
    result.append((const char*)&crc, sizeof(crc));
    assert(f_ != nullptr);
    int ret = fwrite(result.data(), result.size(), 1, f_);
    if (ret != 1) {
      throw BptreeExecption("wal fwrite error");
    }
  }

  void End(uint64_t sequence) {
    assert(writing_wal_.count(sequence) == 1);
    writing_wal_.erase(sequence);
    WriteEndLog(sequence);
  }

 private:
  uint64_t next_wal_sequence_;
  std::string file_name_;
  std::unordered_set<uint64_t> writing_wal_;
  // 使用者注册本回调函数，当恢复过程中发现某些事务没有被提交，则会将属于该事务的日志按照写入顺序依次调用本回调函数
  // 使用者应该在日志中记录旧值，使用旧值擦除可能写入or写入一半的新值
  // 注意，这一操作应该是幂等的，因为这时的wal并不能确定新值是否写入；这一操作并不能依赖修改部分是有意义并完整的，因为
  // 这时的wal并不能确定新值写入一半时是否发生了掉电故障
  // 举例，日志中可以记录每个block改动前[offset, size]处的二进制值，回放过程中直接覆盖掉现在的数据即可。
  std::function<void(uint64_t, const std::string&)> log_handler_;
  FILE* f_;

  constexpr uint8_t logTypeToUint8(LogType type) const { return static_cast<uint8_t>(type); }

  /*
   * 读取wal文件，恢复next_wal_sequence_，对所有没有结束日志标志的写入日志进行回滚操作，回滚之后在日志中追加abort(sequence)
   */
  void recover(FILE* f) {
    assert(f != nullptr);
    std::unordered_map<uint64_t, std::vector<std::string>> pending_txs;
    while (true) {
      bool eof = false;
      bool crc_error = false;
      LogEntry entry = ReadNextLogFromFile(eof, crc_error);
      if (eof == true || crc_error == true) {
        break;
      }
      BPTREE_LOG_DEBUG("read entry from wal, sequence = {}, type = {}, msg = {}", entry.sequence, entry.type, entry.msg);
      // 更新sequence
      if (next_wal_sequence_ <= entry.sequence) {
        next_wal_sequence_ = entry.sequence + 1;
      }
      if (entry.type == logTypeToUint8(LogType::TxBegin)) {
        assert(pending_txs.count(entry.sequence) == 0);
        pending_txs[entry.sequence] = {};
      } else if (entry.type == logTypeToUint8(LogType::TxEnd)) {
        assert(pending_txs.count(entry.sequence) == 1);
        pending_txs.erase(entry.sequence);
      } else if (entry.type == logTypeToUint8(LogType::TxAbort)) {
        // 这个事务内的日志需要回滚
      } else if (entry.type == logTypeToUint8(LogType::Data)) {
        assert(pending_txs.count(entry.sequence) == 1);
        pending_txs[entry.sequence].push_back(entry.msg);
      } else {
        // 错误的日志
        BPTREE_LOG_ERROR("wal recover read a wrong type log");
        break;
      }
    }
    // 此时，pending_txs记录的都是需要回滚的事务
    for (auto& each : pending_txs) {
      uint64_t seq = each.first;
      for (auto& eachlog : each.second) {
        log_handler_(seq, eachlog);
      }
      BPTREE_LOG_INFO("tx {} rollback complete", seq);
    }
    BPTREE_LOG_INFO("wal recover complete, next_sequence is {}", next_wal_sequence_);
  }

  LogEntry ReadNextLogFromFile(bool& eof, bool& crc_error) {
    uint32_t length = 0;
    int ret = fread(&length, sizeof(length), 1, f_);
    if (feof(f_) != 0) {
      eof = true;
      return LogEntry();
    }
    if (ret != 1) {
      throw BptreeExecption("wal fread fail");
    }
    std::string buf;
    buf.resize(length);
    ret = fread(buf.data(), 1, length, f_);
    if (ret != length) {
      throw BptreeExecption("wal fread fail");
    }
    uint32_t non_msg_length = sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint64_t);
    if (buf.size() <= non_msg_length) {
      crc_error = true;
      return LogEntry();
    }
    LogEntry entry;
    uint32_t crc = crc32(buf.data(), buf.size() - sizeof(uint32_t));
    uint32_t old_crc = 0;
    memcpy(&old_crc, &buf[buf.size() - sizeof(uint32_t)], sizeof(uint32_t));
    if (crc != old_crc) {
      crc_error = true;
      return entry;
    }
    entry.crc = crc;
    memcpy(&entry.sequence, &buf[0], sizeof(entry.sequence));
    memcpy(&entry.type, &buf[sizeof(uint64_t)], sizeof(entry.type));
    entry.msg = std::string(&buf[sizeof(uint64_t) + sizeof(uint8_t)], buf.size() - non_msg_length);
    return entry;
  }

  void WriteBeginLog(uint64_t sequence) { WriteLog(sequence, "tx begin", LogType::TxBegin); }

  void WriteEndLog(uint64_t sequence) { WriteLog(sequence, "tx end", LogType::TxEnd); }

  void Close() {
    if (f_ != nullptr) {
      fflush(f_);
      fclose(f_);
      f_ = nullptr;
    }
  }
};
}  // namespace bptree