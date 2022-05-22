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
#include "bptree/log.h"
#include "bptree/util.h"
#include "crc/crc32.h"

namespace bptree {

constexpr uint64_t no_wal_sequence = std::numeric_limits<uint64_t>::max();

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
    TxAbort,
    Data,
  };

  WriteAheadLog(const std::string& file_name, const std::function<void(uint64_t, MsgType, std::string)>& handler)
      : next_wal_sequence_(0), next_log_number_(0), file_name_(file_name), f_(nullptr), log_handler_(handler) {}

  void OpenFile() {
    if (util::FileNotExist(file_name_)) {
      f_ = util::CreateFile(file_name_);
    } else {
      f_ = util::OpenFile(file_name_);
    }
  }

  void Recover() { recover(f_); }

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

  void WriteLog(uint64_t sequence, const std::string& redo_log, const std::string& undo_log,
                LogType etype = LogType::Data) {
    if (sequence == no_wal_sequence) {
      return;
    }
    uint8_t type = logTypeToUint8(etype);
    uint64_t log_number = GetNextLogNum();
    uint32_t length = sizeof(sequence) + sizeof(type) + redo_log.size() + undo_log.size() + 2 * sizeof(uint32_t) +
                      sizeof(log_number) + sizeof(uint32_t);
    std::string result;
    result.append((const char*)&length, sizeof(length));
    result.append((const char*)&sequence, sizeof(sequence));
    result.append((const char*)&type, sizeof(type));
    uint32_t log_length = redo_log.size();
    result.append((const char*)&log_length, sizeof(log_length));
    result.append(redo_log);
    log_length = undo_log.size();
    result.append((const char*)&log_length, sizeof(log_length));
    result.append(undo_log);
    result.append((const char*)&log_number, sizeof(log_number));
    uint32_t crc = crc32(&result[sizeof(length)], result.size() - sizeof(length));
    result.append((const char*)&crc, sizeof(crc));
    util::FileAppend(f_, result.data(), result.size());
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
    f_ = util::CreateFile(file_name_);
  }

 private:
  uint64_t next_wal_sequence_;
  uint64_t next_log_number_;
  std::string file_name_;
  std::unordered_set<uint64_t> writing_wal_;
  // 使用者注册本回调函数，当恢复过程中首先对checkpoint点后的日志按照写入顺序执行redo操作，然后将所有未提交的事务日志按照
  // 逆序执行undo操作
  // 使用者应该在日志中记录新值和旧值
  // 注意，这一操作应该是幂等的，因为redo和undo日志并不保证只执行一次；这一操作并不能依赖修改部分是有意义并完整的，因为
  // 之前的写入并不保证时完整的
  // 举例，日志中可以记录每个block改动前后[offset, size]处的二进制值，回放过程中直接用新值/旧值覆盖掉现在的数据即可。
  std::function<void(uint64_t, MsgType type, const std::string&)> log_handler_;
  FILE* f_;

  uint64_t GetNextLogNum() {
    uint64_t result = next_log_number_;
    next_log_number_ += 1;
    return result;
  }

  constexpr uint8_t logTypeToUint8(LogType type) const { return static_cast<uint8_t>(type); }

  /*
   * 读取wal文件，恢复next_wal_sequence_，找到checkpoint点，对其后的日志执行redo操作，然后找到所有未提交的事务日志逆序undo
   */
  void recover(FILE* f) {
    assert(f != nullptr);
    std::unordered_map<uint64_t, std::vector<LogEntry>> undo_list;
    while (true) {
      bool read_error = false;
      bool crc_error = false;
      LogEntry entry = ReadNextLogFromFile(read_error, crc_error);
      if (read_error == true || crc_error == true) {
        break;
      }
      BPTREE_LOG_DEBUG("read entry from wal, sequence = {}, type = {}, redo = {}, undo = {}", entry.sequence,
                       entry.type, entry.redo_log, entry.undo_log);
      // 更新sequence
      if (next_wal_sequence_ <= entry.sequence) {
        next_wal_sequence_ = entry.sequence + 1;
      }
      // 更新log_number
      if (next_log_number_ <= entry.log_number) {
        next_log_number_ = entry.log_number + 1;
      }
      if (entry.type == logTypeToUint8(LogType::TxBegin)) {
        assert(undo_list.count(entry.sequence) == 0);
        undo_list[entry.sequence] = {};
      } else if (entry.type == logTypeToUint8(LogType::TxEnd)) {
        assert(undo_list.count(entry.sequence) == 1);
        undo_list.erase(entry.sequence);
      } else if (entry.type == logTypeToUint8(LogType::TxAbort)) {
        // 这个事务内的日志需要回滚，这里我们没有采用《数据库系统概念》一书中的在恢复过程中对
        // 没有commit的日志写入redo-only的方法，目前复杂度有点高，暂且先不考虑，而是每次都undo abort的日志
      } else if (entry.type == logTypeToUint8(LogType::Data)) {
        assert(undo_list.count(entry.sequence) == 1);
        // redo
        log_handler_(entry.sequence, MsgType::Redo, entry.redo_log);
        undo_list[entry.sequence].push_back(entry);
      } else {
        // 错误的日志
        BPTREE_LOG_ERROR("wal recover read a wrong type log");
        break;
      }
    }
    // 此时，pending_txs记录的都是需要回滚的事务
    // undo操作需要逆序执行
    std::vector<LogEntry> undo_entry;
    for (auto& each : undo_list) {
      for (auto& each_entry : each.second) {
        undo_entry.push_back(std::move(each_entry));
      }
    }
    std::sort(undo_entry.begin(), undo_entry.end(),
              [](const LogEntry& e1, const LogEntry& e2) -> bool { return e1.log_number > e2.log_number; });
    BPTREE_LOG_INFO("undo log size {}", undo_entry.size());
    for (auto& each : undo_entry) {
      log_handler_(each.sequence, MsgType::Undo, each.undo_log);
    }
    BPTREE_LOG_INFO("wal recover complete, next_sequence is {}, next_log_number is {}", next_wal_sequence_,
                    next_log_number_);
    if (util::FEof(f_) == false) {
      BPTREE_LOG_WARN("wal recover read error before read eof");
    }
  }

  LogEntry ReadNextLogFromFile(bool& error, bool& crc_error) {
    uint32_t length = 0;
    int ret = fread(&length, sizeof(length), 1, f_);
    if (ret != 1) {
      error = true;
      return LogEntry();
    }
    std::string buf;
    buf.resize(length);
    ret = fread(buf.data(), 1, length, f_);
    if (ret != length) {
      error = true;
      return LogEntry();
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
    memcpy(&entry.sequence, &buf[offset], sizeof(entry.sequence));
    offset += sizeof(entry.sequence);
    memcpy(&entry.type, &buf[offset], sizeof(entry.type));
    offset += sizeof(entry.type);
    uint32_t log_length = 0;
    memcpy(&log_length, &buf[offset], sizeof(log_length));
    offset += sizeof(log_length);
    entry.redo_log = std::string(&buf[offset], log_length);
    offset += log_length;

    memcpy(&log_length, &buf[offset], sizeof(log_length));
    offset += sizeof(log_length);
    entry.undo_log = std::string(&buf[offset], log_length);
    offset += log_length;

    memcpy(&entry.log_number, &buf[offset], sizeof(entry.log_number));
    offset += sizeof(entry.log_number);
    assert(offset + sizeof(uint32_t) == length);
    return entry;
  }

  void WriteBeginLog(uint64_t sequence) { WriteLog(sequence, "tx begin", "", LogType::TxBegin); }

  void WriteEndLog(uint64_t sequence) { WriteLog(sequence, "tx end", "", LogType::TxEnd); }

  void Close() {
    if (f_ != nullptr) {
      fflush(f_);
      fclose(f_);
      f_ = nullptr;
    }
  }
};
}  // namespace bptree