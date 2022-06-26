#pragma once

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <functional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "bptree/exception.h"
#include "bptree/file.h"
#include "bptree/log.h"
#include "bptree/util.h"
#include "crc32.h"

namespace bptree {

enum class MsgType : uint8_t {
  Redo,
  Undo,
};

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
    std::string redo_log;
    std::string undo_log;
    // 这一项用来区分每条日志的写入先后顺序，undo阶段使用其进行逆序排序
    uint64_t log_number;
    uint32_t crc;

    LogEntry() = default;
    LogEntry(const LogEntry&) = default;
    LogEntry(LogEntry&&) = default;
    LogEntry& operator=(const LogEntry&) = default;
    LogEntry& operator=(LogEntry&&) = default;
  };

  enum class LogType : uint8_t {
    TxBegin,
    TxEnd,
    Data,
  };

  WriteAheadLog(const std::string& file_name)
      : next_wal_sequence_(0),
        next_log_number_(0),
        current_flush_number_(0),
        last_write_number_(0),
        file_name_(file_name) {}

  void OpenFile() {
    if (util::FileNotExist(file_name_)) {
      f_ = FileHandler::CreateFile(file_name_, FileType::NORMAL);
    } else {
      f_ = FileHandler::OpenFile(file_name_, FileType::NORMAL);
    }
  }

  void RegisterLogHandler(const std::function<void(uint64_t, MsgType, std::string)>& handler) {
    log_handler_ = handler;
  }

  // 日志编号为log_number及其之前的日志确保持久化到磁盘中
  void EnsureLogFlush(uint64_t log_number) {
    // 请求不应该请求比最近写入日志编号还要大的编号
    assert(last_write_number_ >= log_number);
    if (current_flush_number_ < log_number) {
      f_.Flush();

      current_flush_number_ = last_write_number_;
    }
  }

  void Flush() {
    if (current_flush_number_ < last_write_number_) {
      current_flush_number_ = last_write_number_;
      f_.Flush();
    }
  }

  void Recover() {
    BPTREE_LOG_INFO("begin to recover");
    recover();
  }

  ~WriteAheadLog() { Close(); }

  // 申请一个新的事务编号，并将事务开始标志写入wal文件，之后在事务结束标志被写入前，所有使用该编号写入的
  // 数据日志都被认为是在一个事务内，wal保证这些数据日志要么都被提交，要么都会通过调用log_handler_进行回滚
  // todo : 这里需要是线程安全的
  void Begin(uint64_t seq) {
    assert(writing_wal_.count(seq) == 0);
    writing_wal_.insert(seq);
    // write
    WriteBeginLog(seq);
  }

  uint64_t RequestSeq() {
    uint64_t seq = next_log_number_;
    next_wal_sequence_ += 1;
    return seq;
  }

  uint64_t WriteLog(uint64_t sequence, const std::string& redo_log, const std::string& undo_log,
                    LogType etype = LogType::Data) {
    if (sequence == no_wal_sequence) {
      return no_wal_sequence;
    }
    uint8_t type = logTypeToUint8(etype);
    uint64_t log_number = GetNextLogNum();
    uint32_t length = sizeof(sequence) + sizeof(type) + redo_log.size() + undo_log.size() + 2 * sizeof(uint32_t) +
                      sizeof(log_number) + sizeof(uint32_t);
    std::string result;
    util::StringAppender(result, length);
    util::StringAppender(result, sequence);
    util::StringAppender(result, type);
    util::StringAppender(result, redo_log);
    util::StringAppender(result, undo_log);
    util::StringAppender(result, log_number);
    uint32_t crc = crc32(&result[sizeof(length)], result.size() - sizeof(length));
    util::StringAppender(result, crc);
    f_.Write(result.data(), result.size());
    last_write_number_ = log_number;
    return log_number;
  }

  void End(uint64_t sequence) {
    if (sequence == no_wal_sequence) {
      return;
    }
    assert(writing_wal_.count(sequence) == 1);
    writing_wal_.erase(sequence);
    WriteEndLog(sequence);
  }

  // 调用方首先保证所有写入wal的日志对应的操作都已经写入磁盘，然后调用本函数
  // 本函数会清空之前写入的所有wal日志，每次调用本函数可视为提交了一条check point日志
  // 作用：防止wal日志无限增加，占用过多存储空间，并且恢复时间也很长
  void ResetLogFile() {
    util::DeleteFile(file_name_);
    f_ = FileHandler::CreateFile(file_name_, FileType::NORMAL);
  }

 private:
  // 每个操作唯一的编号，由多个日志共享
  uint64_t next_wal_sequence_;
  // 下一个待分配的日志编号
  uint64_t next_log_number_;
  // 最新的确保持久化到磁盘的最后一条日志编号
  uint64_t current_flush_number_;
  // 最后写入（可能在缓存中）的日志编号
  uint64_t last_write_number_;
  std::string file_name_;
  std::unordered_set<uint64_t> writing_wal_;
  // 使用者注册本回调函数，当恢复过程中首先对checkpoint点后的日志按照写入顺序执行redo操作，然后将所有未提交的事务日志按照
  // 逆序执行undo操作
  // 使用者应该在日志中记录新值和旧值
  // 注意，这一操作应该是幂等的，因为redo和undo日志并不保证只执行一次；这一操作并不能依赖修改部分是有意义并完整的，因为
  // 之前的写入并不保证时完整的
  // 举例，日志中可以记录每个block改动前后[offset, size]处的二进制值，回放过程中直接用新值/旧值覆盖掉现在的数据即可。
  std::function<void(uint64_t, MsgType type, const std::string&)> log_handler_;
  FileHandler f_;

  uint64_t GetNextLogNum() {
    uint64_t result = next_log_number_;
    next_log_number_ += 1;
    return result;
  }

  constexpr uint8_t logTypeToUint8(LogType type) const { return static_cast<uint8_t>(type); }

  /*
   * 读取wal文件，恢复next_wal_sequence_，对完成的操作进行redo，对没有完成的操作undo
   */
  void recover() {
    assert(f_.Closed() == false);
    if (!log_handler_) {
      throw BptreeExecption("invalid log handler");
    }
    bool seq_end = true;
    uint64_t current_seq = 0;
    std::vector<LogEntry> current_wal;
    while (true) {
      bool read_error = false;
      bool crc_error = false;
      LogEntry entry = ReadNextLogFromFile(read_error, crc_error);
      if (read_error == true || crc_error == true) {
        break;
      }
      BPTREE_LOG_DEBUG("read entry from wal, sequence = {}, type = {}, redo.size() = {}, undo.size() = {}",
                       entry.sequence, entry.type, entry.redo_log.size(), entry.undo_log.size());
      // 更新sequence
      if (next_wal_sequence_ <= entry.sequence) {
        next_wal_sequence_ = entry.sequence + 1;
      }
      // 更新log_number
      if (next_log_number_ <= entry.log_number) {
        next_log_number_ = entry.log_number + 1;
      }
      if (entry.type == logTypeToUint8(LogType::TxBegin)) {
        assert(seq_end == true);
        current_seq = entry.sequence;
        seq_end = false;
      } else if (entry.type == logTypeToUint8(LogType::TxEnd)) {
        assert(seq_end == false);
        current_seq = 0;
        current_wal.clear();
        seq_end = true;
      } else if (entry.type == logTypeToUint8(LogType::Data)) {
        assert(seq_end == false);
        // redo
        current_wal.push_back(entry);
        log_handler_(entry.sequence, MsgType::Redo, entry.redo_log);
      } else {
        // 错误的日志
        BPTREE_LOG_ERROR("wal recover read a wrong type log");
        break;
      }
    }
    if (current_wal.empty() == false) {
      BPTREE_LOG_DEBUG("remain {} logs to undo", current_wal.size());
    }
    // 此时没有完成的wal操作逆序排序，并执行undo
    std::sort(current_wal.begin(), current_wal.end(),
              [](const LogEntry& e1, const LogEntry& e2) -> bool { return e1.log_number > e2.log_number; });
    for (auto& each : current_wal) {
      log_handler_(each.sequence, MsgType::Undo, each.undo_log);
    }

    BPTREE_LOG_INFO("wal recover complete, next_sequence is {}, next_log_number is {}", next_wal_sequence_,
                    next_log_number_);
  }

  LogEntry ReadNextLogFromFile(bool& error, bool& crc_error) {
    uint32_t length = 0;
    bool eof = false;
    bool succ = f_.ReadWithoutException((char*)&length, sizeof(length), eof);
    if (succ == false) {
      error = true;
      return LogEntry();
    }
    std::string buf;
    buf.resize(length);
    succ = f_.ReadWithoutException(buf.data(), length, eof);
    if (succ == false) {
      error = true;
      return LogEntry{};
    }
    LogEntry entry;
    uint32_t crc = crc32(buf.data(), buf.size() - sizeof(uint32_t));
    uint32_t old_crc = 0;
    memcpy(&old_crc, &buf[buf.size() - sizeof(uint32_t)], sizeof(uint32_t));
    if (crc != old_crc) {
      crc_error = true;
      BPTREE_LOG_ERROR("crc check error, {} != {}", crc, old_crc);
      return entry;
    }
    entry.crc = crc;
    size_t offset = 0;
    entry.sequence = util::StringParser<uint64_t>(buf, offset);
    entry.type = util::StringParser<uint8_t>(buf, offset);
    entry.redo_log = util::StringParser(buf, offset);
    entry.undo_log = util::StringParser(buf, offset);
    entry.log_number = util::StringParser<uint64_t>(buf, offset);
    assert(offset + sizeof(uint32_t) == length);
    return entry;
  }

  void WriteBeginLog(uint64_t sequence) { WriteLog(sequence, "tx begin", "", LogType::TxBegin); }

  void WriteEndLog(uint64_t sequence) { WriteLog(sequence, "tx end", "", LogType::TxEnd); }

  void Close() { f_.Close(); }
};
}  // namespace bptree