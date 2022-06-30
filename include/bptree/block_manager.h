#pragma once

#include <atomic>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "bptree/block.h"
#include "bptree/cache.h"
#include "bptree/double_write.h"
#include "bptree/exception.h"
#include "bptree/fault_injection.h"
#include "bptree/file.h"
#include "bptree/key_comparator.h"
#include "bptree/log.h"
#include "bptree/metric/metric.h"
#include "bptree/metric/metric_set.h"
#include "bptree/unused_block.h"
#include "bptree/util.h"
#include "bptree/wal.h"

namespace bptree {

enum class GetRangeOption {
  SKIP,
  SELECT,
  STOP,
};

enum class Mode {
  R,
  W,
  WR,
};

inline const char* ModeStr(Mode mode) {
  if (mode == Mode::R) {
    return "R";
  } else if (mode == Mode::W) {
    return "W";
  } else if (mode == Mode::WR) {
    return "WR";
  }
  return nullptr;
}

enum class NotExistFlag {
  CREATE,
  ERROR,
};

enum class ExistFlag {
  SUCC,
  ERROR,
};

struct BlockManagerOption {
  // 指定db的名字
  std::string db_name = "";

  // 指定权限，只读、只写或者读写，不满足权限的操作会导致抛出BptreeException异常
  Mode mode = Mode::R;

  // 如果db不存在，根据neflag决定是创建一个新的db或者抛出异常
  NotExistFlag neflag = NotExistFlag::ERROR;
  // 如果db存在，根据eflag决定是成功返回或者抛出异常
  ExistFlag eflag = ExistFlag::SUCC;

  // 以下两个参数指定了key和value的大小，note: 本库只支持固定大小的kv存储
  // 如果key和value的大小之和过大，导致单个block中无法存储1个kv对，则会抛出异常
  uint32_t key_size = 0;
  uint32_t value_size = 0;

  // 指定lru使用的block数量
  size_t cache_size = 1024;

  // 指定多少个写操作之后生成一个check point
  size_t create_check_point_per_ops = 4096;

  // 指定是否每个写操作之后同步wal日志
  bool sync_per_write = false;

  // 指定是否关闭double write写（计划通过wal日志记录block最近版本，避免double write写）
  bool double_write_turn_off = false;

  // 可选的自定义cmp，db中会按照该cmp指定的顺序对key-value按序存储
  std::shared_ptr<Comparator> cmp = std::make_shared<Comparator>();
};

inline std::string CreateWalNameByDB(const std::string& db_name) { return db_name + "/" + db_name + "_wal.log"; }

inline std::string CreateDWfileNameByDB(const std::string& db_name) {
  return db_name + "/" + db_name + "_double_write.log";
}

inline std::string CreateDbFileNameByDB(const std::string& db_name) { return db_name + "/" + db_name + ".db"; }

namespace detail {

enum class LogType : uint8_t {
  SUPER_META,
  BLOCK_META,
  BLOCK_DATA,
  BLOCK_ALLO,
  BLOCK_RESET,
  BLOCK_VIEW,
};

inline constexpr uint8_t LogTypeToUint8T(LogType type) { return static_cast<uint8_t>(type); }

}  // namespace detail

class BlockManager {
 public:
  friend class Block;
  friend class BlockBase;
  friend class SuperBlock;

  /**
   * @brief 构造或者打开db，根据option参数决定
   * @param option 见BlockManagerOption注释
   */
  BPTREE_INTERFACE explicit BlockManager(BlockManagerOption option)
      : mode_(option.mode),
        comparator_(option.cmp),
        block_cache_(option.cache_size),
        db_name_(option.db_name),
        super_block_(*this, option.key_size, option.value_size),
        wal_(CreateWalNameByDB(db_name_)),
        dw_(CreateDWfileNameByDB(db_name_)),
        create_checkpoint_per_op_(option.create_check_point_per_ops),
        sync_per_write_(option.sync_per_write),
        unused_blocks_() {
    if (db_name_.empty() == true) {
      throw BptreeExecption("please specify the db's name");
    }
    block_cache_.SetFreeNotify([this](const uint32_t& key, Block& value) -> void { this->OnCacheDelete(key, value); });
    wal_.RegisterLogHandler(
        [this](uint64_t seq, MsgType type, const std::string log) -> void { this->HandleWal(seq, type, log); });
    RegisterMetrics();
    if (util::FileNotExist(db_name_)) {
      if (option.neflag == NotExistFlag::ERROR) {
        throw BptreeExecption("db {} not exist", db_name_);
      }
      if (option.key_size == 0 || option.value_size == 0) {
        throw BptreeExecption(
            "block manager construct error, key_size and value_size should not "
            "be 0");
      }
      util::CreateDir(db_name_);
      f_ = FileHandler::CreateFile(CreateDbFileNameByDB(db_name_), FileType::NORMAL);
      wal_.OpenFile();
      dw_.OpenFile();
      if (option.double_write_turn_off == true) {
        dw_.TurnOff();
      }
      // 新建root block并写入wal文件
      auto root_block = std::unique_ptr<Block>(
          new Block(*this, super_block_.root_index_, 1, super_block_.key_size_, super_block_.value_size_));
      GetMetricSet().GetAs<Gauge>("dirty_block_count")->Add();
      block_cache_.Insert(super_block_.root_index_, std::move(root_block));

      uint64_t seq = wal_.RequestSeq();
      wal_.Begin(seq);
      std::string redo_log =
          CreateAllocBlockWalLog(super_block_.root_index_, 1, super_block_.key_size_, super_block_.value_size_);
      std::string undo_log = "";
      wal_.WriteLog(seq, redo_log, undo_log);
      wal_.End(seq);
      wal_.Flush();
      FlushSuperBlockToFile();
      BPTREE_LOG_INFO("create db {} succ", db_name_);
    } else {
      if (option.eflag == ExistFlag::ERROR) {
        throw BptreeExecption("db {} already exists", db_name_);
      }
      f_ = FileHandler::OpenFile(CreateDbFileNameByDB(db_name_), FileType::NORMAL);
      wal_.OpenFile();
      dw_.OpenFile();
      if (option.double_write_turn_off == true) {
        dw_.TurnOff();
      }
      ParseSuperBlockFromFile();
      wal_.Recover();
      // 这个时候cache中的block buf都已经通过wal日志恢复，但是kvview还没有变更，因此需要对cache中的block更新kvview
      block_cache_.ForeachValueInCache([](const uint32_t& index, Block& block) { block.UpdateKvViewByBuf(); });
      // 生成一个快照
      CreateCheckPoint();
      BPTREE_LOG_INFO("open db {} succ", db_name_);
    }
  }

  BPTREE_INTERFACE ~BlockManager() { FlushToFile(); }

  /**
   * @brief 接口函数，根据key在db中查询value
   * @param key 用户指定的key
   * @return
   *      - 空字符串 指定的key在db中不存在
   *      - 非空字符串，为key对应的value
   * @note 用户需要有读权限，key的大小需要和构造时指定的key_size一致，否则抛出异常
   */
  BPTREE_INTERFACE std::string Get(const std::string& key) {
    if (mode_ != Mode::R && mode_ != Mode::WR) {
      throw BptreeExecption("Permission denied");
    }
    if (key.size() != super_block_.key_size_) {
      throw BptreeExecption("wrong key length");
    }
    GetMetricSet().GetAs<Counter>("get_count")->Add();
    auto block = GetBlock(super_block_.root_index_);
    std::string result = block.Get().Get(key);
    return result;
  }

  /**
   * @brief 接口函数，范围查找，key为需要查找的起始位置，对后续的每个key-value调用functor，
   根据返回值决定结束查找 or 跳过这个key-value or 选择这个key-value并继续

   * @param key 范围查找的key
   * @param functor 选择functor
   * @return 查找到的key-value对
   * @note 用户需要有读权限，key的大小需要和构造时指定的key_size一致，否则抛出异常
   */
  BPTREE_INTERFACE std::vector<std::pair<std::string, std::string>> GetRange(
      const std::string& key, std::function<GetRangeOption(const Entry& entry)> functor) {
    if (mode_ != Mode::R && mode_ != Mode::WR) {
      throw BptreeExecption("Permission denied");
    }
    if (key.size() != super_block_.key_size_) {
      throw BptreeExecption("wrong key length");
    }
    GetMetricSet().GetAs<Counter>("get_range_count")->Add();
    auto location = GetBlock(super_block_.root_index_).Get().GetBlockIndexContainKey(key);
    BPTREE_LOG_DEBUG("get range, key == {}, find the location : {}, {}", key, location.first, location.second);
    if (location.first == 0) {
      return {};
    }
    Counter scan("scan count");
    std::vector<std::pair<std::string, std::string>> result;
    uint32_t block_index = location.first;
    uint32_t view_index = location.second;
    while (block_index != 0) {
      auto block = GetBlock(block_index);
      for (size_t i = view_index; i < block.Get().GetKVView().size(); ++i) {
        const Entry& entry = block.Get().GetViewByIndex(i);
        GetRangeOption state = functor(entry);
        if (state == GetRangeOption::SKIP) {
          continue;
        } else if (state == GetRangeOption::SELECT) {
          result.push_back({std::string(entry.key_view), std::string(entry.value_view)});
        } else {
          return result;
        }
      }
      // 下一个block
      block_index = block.Get().GetNext();
      view_index = 0;
      scan.Add();
    }
    BPTREE_LOG_DEBUG("get range, key == {}, scans {} blocks", key, scan.GetValue());
    return result;
  }

  /**
   * @brief 接口函数，将key-value插入db
   * @param key 用户指定的key
   * @param value 用户指定的value
   * @param seq 事务编号，用于将多个读写操作组合成单个事务进行wal记录，用户使用默认值即可
   * @return
   *      - true 插入成功
   *      - false key已经在db中存在，插入失败
   * @note 用户需要有写权限，key和value的大小需要和构造时指定的key_size和value_size一致，否则抛出异常
   */
  BPTREE_INTERFACE bool Insert(const std::string& key, const std::string& value, uint64_t seq = no_wal_sequence) {
    if (mode_ != Mode::W && mode_ != Mode::WR) {
      throw BptreeExecption("Permission denied");
    }
    if (key.size() != super_block_.key_size_ || value.size() != super_block_.value_size_) {
      throw BptreeExecption("wrong kv length");
    }
    GetMetricSet().GetAs<Counter>("insert_count")->Add();
    uint64_t sequence = seq;
    if (sequence == no_wal_sequence) {
      sequence = wal_.RequestSeq();
      wal_.Begin(sequence);
    }
    InsertInfo info = GetBlock(super_block_.root_index_).Get().Insert(key, value, sequence);
    bool result = true;
    if (info.state_ == InsertInfo::State::Invalid) {
      result = false;
    } else if (info.state_ == InsertInfo::State::Split) {
      // 根节点的分裂
      SplitTheRootBlock(info.key_, info.value_, sequence);
      BPTREE_LOG_DEBUG("the insert operation(key = {}, value = {}) caused the root block to split", key, value);
    } else {
      assert(InsertInfo::State::Ok == info.state_);
    }
    if (seq == no_wal_sequence) {
      wal_.End(sequence);
      AfterCommitTx();
    }
    return result;
  }

  /**
   * @brief 接口函数，删除key指定的kv对
   * @param key 用户指定的key
   * @param seq 事务编号，用于将多个读写操作组合成单个事务进行wal记录，用户使用默认值即可
   * @return
   *      - 空字符串 指定的key在db中不存在
   *      - 非空字符串，删除掉的value值
   * @note 用户需要有写权限，key的大小需要和构造时指定的key_size一致，否则抛出异常
   */
  BPTREE_INTERFACE std::string Delete(const std::string& key, uint64_t seq = no_wal_sequence) {
    if (mode_ != Mode::W && mode_ != Mode::WR) {
      throw BptreeExecption("Permission denied");
    }
    if (key.size() != super_block_.key_size_) {
      throw BptreeExecption("wrong key length");
    }
    uint64_t sequence = seq;
    if (sequence == no_wal_sequence) {
      sequence = wal_.RequestSeq();
      wal_.Begin(sequence);
    }
    GetMetricSet().GetAs<Counter>("delete_count")->Add();
    // 根节点的merge信息不处理
    auto ret = GetBlock(super_block_.root_index_).Get().Delete(key, sequence);
    if (seq == no_wal_sequence) {
      wal_.End(sequence);
      AfterCommitTx();
    }
    return ret.old_v_;
  }

  /**
   * @brief 接口函数，将db中的key对应的值更新为value
   * @param key 用户指定的key
   * @param value 用户指定的value
   * @param seq 事务编号，用于将多个读写操作组合成单个事务进行wal记录，用户使用默认值即可
   * @return
   *      - 空字符串 指定的key在db中不存在
   *      - 非空字符串，更新前的value值
   * @note 用户需要有写权限，key和value的大小需要和构造时指定的key_size和value_size一致，否则抛出异常
   */
  BPTREE_INTERFACE std::string Update(const std::string& key, const std::string& value,
                                      uint64_t seq = no_wal_sequence) {
    if (mode_ != Mode::W && mode_ != Mode::WR) {
      throw BptreeExecption("Permission denied");
    }
    if (key.size() != super_block_.key_size_) {
      throw BptreeExecption("wrong key length");
    }
    uint64_t sequence = seq;
    if (sequence == no_wal_sequence) {
      sequence = wal_.RequestSeq();
      wal_.Begin(sequence);
    }
    GetMetricSet().GetAs<Counter>("update_count")->Add();
    auto ret = GetBlock(super_block_.root_index_).Get().Update(key, value, sequence);
    if (seq == no_wal_sequence) {
      wal_.End(sequence);
      AfterCommitTx();
    }
    return ret.old_v_;
  }

  BPTREE_INTERFACE void PrintOption() const {
    BPTREE_LOG_INFO("db name                  : {}", db_name_);
    BPTREE_LOG_INFO("mode                     : {}", ModeStr(mode_));
    BPTREE_LOG_INFO("cache size               : {}", block_cache_.GetCapacity());
    BPTREE_LOG_INFO("key size                 : {}", super_block_.key_size_);
    BPTREE_LOG_INFO("value size               : {}", super_block_.value_size_);
    BPTREE_LOG_INFO("create checkpoint per op : {}", create_checkpoint_per_op_);
    BPTREE_LOG_INFO("sync per write           : {}", sync_per_write_ ? "true" : "false");
  }

  BPTREE_INTERFACE void PrintRootBlock() {
    auto root_block = GetBlock(super_block_.root_index_);
    root_block.Get().Print();
  }

  BPTREE_INTERFACE void PrintBlockByIndex(uint32_t index) {
    if (index == 0) {
      PrintSuperBlockInfo();
      return;
    }
    if (super_block_.current_max_block_index_ < index) {
      throw BptreeExecption("request block's index invalid : {}", index);
    }
    auto block = GetBlock(index);
    block.Get().Print();
  }

  BPTREE_INTERFACE void PrintCacheInfo() { block_cache_.PrintInfo(); }

  BPTREE_INTERFACE void PrintMetricSet() { metric_set_.Print(); }

  BPTREE_INTERFACE void PrintSuperBlockInfo() {
    BPTREE_LOG_INFO("-----begin super block print-----");
    BPTREE_LOG_INFO("root_index : {}", super_block_.root_index_);
    BPTREE_LOG_INFO("key size and value size : {} {}", super_block_.key_size_, super_block_.value_size_);
    BPTREE_LOG_INFO("free block size : {}", super_block_.free_block_size_);
    BPTREE_LOG_INFO("total block size : {}", super_block_.current_max_block_index_ + 1);
    double tmp = double(super_block_.free_block_size_) / double(super_block_.current_max_block_index_ + 1);
    BPTREE_LOG_INFO("free_block_size / total_block_size : {}", tmp);
    BPTREE_LOG_INFO("------end super block print------");
  }

  FaultInjection& GetFaultInjection() { return fj_; }

  WriteAheadLog& GetWal() { return wal_; }

  MetricSet& GetMetricSet() { return metric_set_; }

  const Comparator& GetComparator() { return *comparator_.get(); }

  typename LRUCache<uint32_t, Block>::Wrapper GetBlock(uint32_t index) {
    auto wrapper = block_cache_.Get(index);
    if (wrapper.Exist() == false) {
      GetMetricSet().GetAs<Counter>("load_block_count")->Add();
      auto block = LoadBlock(index);
      block_cache_.Insert(index, std::move(block));
      return block_cache_.Get(index);
    }
    return wrapper;
  }

  uint32_t GetRootIndex() const noexcept { return super_block_.root_index_; }

  uint32_t GetMaxBlockIndex() const noexcept { return super_block_.current_max_block_index_; }

 private:
  std::pair<uint32_t, uint32_t> BlockSplit(const Block* block, uint64_t sequence) {
    BPTREE_LOG_DEBUG("block split begin");
    GetMetricSet().GetAs<Counter>("block_split_count")->Add();
    uint32_t new_block_1_index = AllocNewBlock(block->GetHeight(), sequence);
    uint32_t new_block_2_index = AllocNewBlock(block->GetHeight(), sequence);
    auto new_block_1 = GetBlock(new_block_1_index);
    auto new_block_2 = GetBlock(new_block_2_index);
    // 在插入之前记录旧的数据快照, 作为undo log
    std::string block_1_undo, block_2_undo;
    if (sequence != no_wal_sequence) {
      block_1_undo = CreateResetBlockWalLog(new_block_1_index, new_block_1.Get().GetHeight(), super_block_.key_size_,
                                            super_block_.value_size_);
      block_2_undo = CreateResetBlockWalLog(new_block_2_index, new_block_2.Get().GetHeight(), super_block_.key_size_,
                                            super_block_.value_size_);
    }
    size_t half_count = block->GetKVView().size() / 2;
    for (size_t i = 0; i < half_count; ++i) {
      bool succ = new_block_1.Get().AppendKv(block->GetViewByIndex(i).key_view, block->GetViewByIndex(i).value_view,
                                             no_wal_sequence);
      if (succ == false) {
        throw BptreeExecption("block broken (spliting)", i);
      }
    }
    for (int i = half_count; i < block->GetKVView().size(); ++i) {
      bool succ = new_block_2.Get().AppendKv(block->GetViewByIndex(i).key_view, block->GetViewByIndex(i).value_view,
                                             no_wal_sequence);
      if (succ == false) {
        throw BptreeExecption("block broken (spliting)", i);
      }
    }
    // 插入之后记录新的数据快照，作为redo log
    if (sequence != no_wal_sequence) {
      std::string block_1_redo = CreateBlockViewWalLog(new_block_1_index, new_block_1.Get().CreateDataView());
      std::string block_2_redo = CreateBlockViewWalLog(new_block_2_index, new_block_2.Get().CreateDataView());
      auto log_num_1 = wal_.WriteLog(sequence, block_1_redo, block_1_undo);
      auto log_num_2 = wal_.WriteLog(sequence, block_2_redo, block_2_undo);
      new_block_1.Get().UpdateLogNumber(log_num_1);
      new_block_2.Get().UpdateLogNumber(log_num_2);
    }
    BPTREE_LOG_DEBUG("block split, from {} to {} and {}", block->GetIndex(), new_block_1_index, new_block_2_index);
    return {new_block_1_index, new_block_2_index};
  }

  void SplitTheRootBlock(const std::string& key, const std::string& value, uint64_t sequence) {
    GetMetricSet().GetAs<Counter>("root_block_split_count")->Add();
    // 根节点的分裂
    uint32_t old_root_index = super_block_.root_index_;
    auto old_root = GetBlock(super_block_.root_index_);
    uint32_t old_root_height = old_root.Get().GetHeight();
    auto new_blocks = BlockSplit(&old_root.Get(), sequence);

    auto left_block = GetBlock(new_blocks.first);
    auto right_block = GetBlock(new_blocks.second);
    // update link
    left_block.Get().SetNext(new_blocks.second, sequence);
    right_block.Get().SetPrev(new_blocks.first, sequence);
    // insert
    if (key <= left_block.Get().GetMaxKey()) {
      auto ret = left_block.Get().InsertKv(key, value, sequence);
    } else {
      right_block.Get().InsertKv(key, value, sequence);
    }
    // update root
    old_root.Get().Clear(sequence);
    old_root.Get().SetHeight(old_root_height + 1, sequence);
    old_root.Get().AppendKv(left_block.Get().GetMaxKey(), util::ConstructIndexByNum(new_blocks.first), sequence);
    old_root.Get().AppendKv(right_block.Get().GetMaxKey(), util::ConstructIndexByNum(new_blocks.second), sequence);
  }

  uint32_t BlockMerge(const Block* b1, const Block* b2, uint64_t sequence) {
    GetMetricSet().GetAs<Counter>("block_merge_count")->Add();
    uint32_t new_block_index = AllocNewBlock(b1->GetHeight(), sequence);
    auto new_block = GetBlock(new_block_index);
    std::string block_undo;
    if (sequence != no_wal_sequence) {
      block_undo = CreateBlockViewWalLog(new_block_index, new_block.Get().CreateDataView());
    }
    for (size_t i = 0; i < b1->GetKVView().size(); ++i) {
      bool succ =
          new_block.Get().AppendKv(b1->GetViewByIndex(i).key_view, b1->GetViewByIndex(i).value_view, no_wal_sequence);
      if (succ == false) {
        throw BptreeExecption("block broken (merging)");
      }
    }
    for (size_t i = 0; i < b2->GetKVView().size(); ++i) {
      bool succ =
          new_block.Get().AppendKv(b2->GetViewByIndex(i).key_view, b2->GetViewByIndex(i).value_view, no_wal_sequence);
      if (succ == false) {
        throw BptreeExecption("block broken (merging)");
      }
    }
    if (sequence != no_wal_sequence) {
      std::string block_redo = CreateBlockViewWalLog(new_block_index, new_block.Get().CreateDataView());
      auto log_num = wal_.WriteLog(sequence, block_redo, block_undo);
      new_block.Get().UpdateLogNumber(log_num);
    }
    BPTREE_LOG_DEBUG("block merge, from {} and {} to {}", b1->GetIndex(), b2->GetIndex(), new_block_index);
    return new_block_index;
  }

  std::string CreateAllocBlockWalLog(uint32_t index, uint32_t height, uint32_t key_size, uint32_t value_size) {
    std::string result;
    util::StringAppender(result, detail::LogTypeToUint8T(detail::LogType::BLOCK_ALLO));
    util::StringAppender(result, index);
    util::StringAppender(result, height);
    util::StringAppender(result, key_size);
    util::StringAppender(result, value_size);
    return result;
  }

  std::string CreateResetBlockWalLog(uint32_t index, uint32_t height, uint32_t key_size, uint32_t value_size) {
    std::string result;
    util::StringAppender(result, detail::LogTypeToUint8T(detail::LogType::BLOCK_RESET));
    util::StringAppender(result, index);
    util::StringAppender(result, height);
    util::StringAppender(result, key_size);
    util::StringAppender(result, value_size);
    return result;
  }

  std::string CreateBlockViewWalLog(uint32_t index, const std::string& view) {
    std::string result;
    util::StringAppender(result, detail::LogTypeToUint8T(detail::LogType::BLOCK_VIEW));
    util::StringAppender(result, index);
    util::StringAppender(result, view);
    return result;
  }

  // 申请一个新的Block
  uint32_t AllocNewBlock(uint32_t height, uint64_t sequence) {
    GetMetricSet().GetAs<Counter>("alloc_block_count")->Add();
    uint32_t result = 0;
    if (super_block_.free_block_head_ == 0) {
      super_block_.SetCurrentMaxBlockIndex(super_block_.current_max_block_index_ + 1, sequence);
      result = super_block_.current_max_block_index_;
      BPTREE_LOG_DEBUG("extend max block index to {}", result);
      auto new_block =
          std::unique_ptr<Block>(new Block(*this, result, height, super_block_.key_size_, super_block_.value_size_));
      if (sequence != no_wal_sequence) {
        std::string redo_log = CreateAllocBlockWalLog(result, height, super_block_.key_size_, super_block_.value_size_);
        std::string undo_log = "";
        auto log_number = wal_.WriteLog(sequence, redo_log, undo_log);
        new_block->UpdateLogNumber(log_number);
      }
      BPTREE_LOG_DEBUG("alloc new block {}", result);
      GetMetricSet().GetAs<Gauge>("dirty_block_count")->Add();
      block_cache_.Insert(result, std::move(new_block));
      return result;
    } else {
      return ReuseFreeBlock(height, sequence);
    }
  }

  uint32_t ReuseFreeBlock(uint32_t height, uint64_t sequence) {
    assert(super_block_.free_block_head_ != 0);
    BPTREE_LOG_DEBUG("get free block head {}", super_block_.free_block_head_);
    auto block = unused_blocks_.Get(super_block_.free_block_head_);
    if (block == nullptr) {
      block = LoadBlock(super_block_.free_block_head_);
    }
    super_block_.SetFreeBlockHead(block->GetNextFreeIndex(), sequence);
    uint32_t result = block->GetIndex();
    BPTREE_LOG_DEBUG("reuse block index {}", result);
    assert(super_block_.free_block_size_ > 0);
    super_block_.SetFreeBlockSize(super_block_.free_block_size_ - 1, sequence);

    std::string redo_log, undo_log;
    if (sequence != no_wal_sequence) {
      redo_log = CreateResetBlockWalLog(result, height, super_block_.key_size_, super_block_.value_size_);
      undo_log = CreateBlockViewWalLog(result, block->CreateDataView());
    }

    block->SetClean();
    block.reset(new Block(*this, result, height, super_block_.key_size_, super_block_.value_size_));

    if (sequence != no_wal_sequence) {
      auto log_number = wal_.WriteLog(sequence, redo_log, undo_log);
      block->UpdateLogNumber(log_number);
    }
    GetMetricSet().GetAs<Gauge>("dirty_block_count")->Add();
    block_cache_.Insert(result, std::move(block));
    return result;
  }

  void DeallocBlock(uint32_t index, uint64_t sequence, bool update_link_relation = true) {
    GetMetricSet().GetAs<Counter>("dealloc_block_count")->Add();
    auto block = GetBlock(index);
    if (update_link_relation == true) {
      uint32_t next = block.Get().GetNext();
      uint32_t prev = block.Get().GetPrev();
      if (next != 0) {
        auto next_block = GetBlock(next);
        next_block.Get().SetPrev(prev, sequence);
      }
      if (prev != 0) {
        auto prev_block = GetBlock(prev);
        prev_block.Get().SetNext(next, sequence);
      }
    }
    // 在wal层面，不需要有block删除的概念，因为block的删除只是把元数据字段中的next free index设置为新值而已
    block.Get().SetNextFreeIndex(super_block_.free_block_head_, sequence);
    block.UnBind();
    super_block_.SetFreeBlockHead(index, sequence);
    super_block_.SetFreeBlockSize(super_block_.free_block_size_ + 1, sequence);
    auto unused_block = block_cache_.Move(index);
    GetMetricSet().GetAs<Gauge>("dirty_block_count")->Sub();
    unused_blocks_.Push(std::move(unused_block));
    BPTREE_LOG_DEBUG("dealloc block {}", index);
  }

  char* ReadBlockFromFile(uint32_t index) {
    char* buf = new ((std::align_val_t)linux_alignment) char[block_size];
    f_.Read(buf, block_size, index * block_size);
    return buf;
  }

  void ReadBlockFromFile(SuperBlock& sb) { f_.Read(sb.GetBuf(), block_size, sb.GetIndex() * block_size); }

  void FlushToFile() {
    if (f_.Closed()) {
      return;
    }
    wal_.Flush();
    FlushSuperBlockToFile();
    bool succ = block_cache_.Clear();
    assert(succ == true);
    FlushUnusedBlockToFile();
    f_.Close();
    dw_.Close();
    auto& cond = GetFaultInjection().GetTheLastCheckPointFailCondition();
    if (cond && cond() == true) {
      BPTREE_LOG_WARN("fault injection : the last check point fail");
      std::exit(-1);
    }
    // 正常关闭的情况下执行到这里，所有block都刷到了磁盘，所以可以安全的删除wal日志
    wal_.ResetLogFile();
  }

  void OnCacheDelete(const uint32_t& index, Block& block) {
    bool dirty = block.Flush();
    if (dirty == true) {
      GetMetricSet().GetAs<Counter>("flush_block_count")->Add();
      BPTREE_LOG_DEBUG("block {} flush to disk, dirty", index);
      // 确保block上所有修改操作对应的日志已经持久化到磁盘
      this->wal_.EnsureLogFlush(block.GetLogNumber());
      this->dw_.WriteBlock(block);
      this->FlushBlockToFile(block);
    } else {
      BPTREE_LOG_DEBUG("block {} don't flush to disk, clean", index);
    }
  }

  /*
   * 支持故障注入，可选择的写入部分数据，模拟断电过程中partial write情况
   */
  void FlushBlockToFile(const BlockBase& block) {
    uint32_t index = block.GetIndex();
    auto& condition = GetFaultInjection().GetPartialWriteCondition();
    if (condition) {
      bool ret = condition(index);
      if (ret == true) {
        FlushBlockToFilePartialWriteAndExit(index, block);
        return;
      }
    }
    f_.Write(block.GetBuf(), block_size, index * block_size);
  }

  void FlushBlockToFilePartialWriteAndExit(uint32_t index, const BlockBase& block) {
    f_.Write(block.GetBuf(), block_size / 2, index * block_size);
    std::exit(-1);
  }

  void FlushSuperBlockToFile() {
    super_block_.SetDirty(false);
    super_block_.Flush(false);
    wal_.EnsureLogFlush(super_block_.GetLogNumber());
    dw_.WriteBlock(super_block_);
    FlushBlockToFile(super_block_);
  }

  void FlushUnusedBlockToFile() {
    auto blocks = unused_blocks_.GetAll();
    for (auto& each : blocks) {
      OnCacheDelete(each->GetIndex(), *each);
    }
  }

  void ParseSuperBlockFromFile() {
    bool succ = true;
    try {
      ReadBlockFromFile(super_block_);
    } catch (const BptreeExecption& e) {
      succ = false;
    }
    if (succ == true) {
      super_block_.NeedToParse();
      succ = super_block_.Parse();
    }
    if (succ == false) {
      BPTREE_LOG_WARN("super block crc32 check fail, try to recover from double_write file");
      // crc校验失败，尝试从double write文件中恢复，并写回db文件
      dw_.ReadBlock(super_block_.GetBuf());
      super_block_.NeedToParse();
      succ = super_block_.Parse();
      if (succ == false || super_block_.GetIndex() != 0) {
        throw BptreeExecption("inner error, can't recover super block from double_write file");
      }
      // 如果成功，立刻覆盖掉错误的数据
      BPTREE_LOG_INFO("recover super block succ, flush to db immediately");
      super_block_.SetDirty(false);
      super_block_.Flush(false);
      FlushBlockToFile(super_block_);
    }
  }

  // 读到后进行crc校验，如果失败则从dw文件中恢复
  std::unique_ptr<Block> LoadBlock(uint32_t index) {
    char* buf = ReadBlockFromFile(index);
    std::unique_ptr<Block> new_block = std::unique_ptr<Block>(new Block(*this, buf));
    bool succ = new_block->Parse();
    if (succ == false) {
      BPTREE_LOG_WARN("parse block {} error : crc32 check fail, try to recover from double_write file", index);
      dw_.ReadBlock(buf);
      new_block->NeedToParse();
      succ = new_block->Parse();
      if (succ == false || new_block->GetIndex() != index) {
        throw BptreeExecption("inner error, can't recover block from double_write file : {}", index);
      }
    }
    BPTREE_LOG_DEBUG("load block {} from disk succ", index);
    return new_block;
  }

  void HandleWal(uint64_t sequence, MsgType type, const std::string& log) {
    if (log.empty() == true) {
      return;
    }
    size_t offset = 0;
    uint8_t wal_type = util::StringParser<uint8_t>(log, offset);
    switch (wal_type) {
      case detail::LogTypeToUint8T(detail::LogType::SUPER_META): {
        BPTREE_LOG_DEBUG("handle super meta log");
        uint32_t index = util::StringParser<uint32_t>(log, offset);
        std::string meta_name = util::StringParser(log, offset);
        uint32_t value = util::StringParser<uint32_t>(log, offset);
        assert(offset == log.size());
        super_block_.HandleWAL(meta_name, value);
        break;
      }
      case detail::LogTypeToUint8T(detail::LogType::BLOCK_META): {
        BPTREE_LOG_DEBUG("handle block meta log");
        uint32_t index = util::StringParser<uint32_t>(log, offset);
        std::string meta_name = util::StringParser(log, offset);
        uint32_t value = util::StringParser<uint32_t>(log, offset);
        assert(offset == log.size());
        HandleBlockMetaUpdateWal(sequence, index, meta_name, value);
        break;
      }
      case detail::LogTypeToUint8T(detail::LogType::BLOCK_DATA): {
        BPTREE_LOG_DEBUG("handle block data log");
        uint32_t index = util::StringParser<uint32_t>(log, offset);
        uint32_t region_offset = util::StringParser<uint32_t>(log, offset);
        std::string region = util::StringParser(log, offset);
        assert(offset == log.size());
        HandleBlockDataUpdateWal(sequence, index, region_offset, region);
        break;
      }
      case detail::LogTypeToUint8T(detail::LogType::BLOCK_ALLO): {
        BPTREE_LOG_DEBUG("handle block_alloc log");
        uint32_t index = util::StringParser<uint32_t>(log, offset);
        uint32_t height = util::StringParser<uint32_t>(log, offset);
        uint32_t key_size = util::StringParser<uint32_t>(log, offset);
        uint32_t value_size = util::StringParser<uint32_t>(log, offset);
        assert(offset == log.size());
        HandleBlockAllocWal(sequence, index, height, key_size, value_size);
        break;
      }
      case detail::LogTypeToUint8T(detail::LogType::BLOCK_RESET): {
        BPTREE_LOG_DEBUG("handle block_reset log");
        uint32_t index = util::StringParser<uint32_t>(log, offset);
        uint32_t height = util::StringParser<uint32_t>(log, offset);
        uint32_t key_size = util::StringParser<uint32_t>(log, offset);
        uint32_t value_size = util::StringParser<uint32_t>(log, offset);
        assert(offset == log.size());
        HandleBlockResetWal(sequence, index, height, key_size, value_size);
        break;
      }
      case detail::LogTypeToUint8T(detail::LogType::BLOCK_VIEW): {
        BPTREE_LOG_DEBUG("handle block view log");
        uint32_t index = util::StringParser<uint32_t>(log, offset);
        std::string view = util::StringParser(log, offset);
        assert(offset == log.size());
        HandleBlockViewWal(sequence, index, view);
        break;
      }
      default: {
        throw BptreeExecption("invalid wal type : {}", wal_type);
      }
    }
  }

  void HandleBlockAllocWal(uint64_t sequence, uint32_t index, uint32_t height, uint32_t key_size, uint32_t value_size) {
    assert(key_size == super_block_.key_size_ && value_size == super_block_.value_size_);
    auto block = std::unique_ptr<Block>(new Block(*this, index, height, key_size, value_size));
    // index block之前就不存在，因此不可能在cache中，直接插入
    assert(block_cache_.Get(index).Exist() == false);
    block_cache_.Insert(index, std::move(block));
  }

  void HandleBlockResetWal(uint64_t sequence, uint32_t index, uint32_t height, uint32_t key_size, uint32_t value_size) {
    assert(key_size == super_block_.key_size_ && value_size == super_block_.value_size_);
    auto block = std::unique_ptr<Block>(new Block(*this, index, height, key_size, value_size));
    // 直接将之前的版本删除，新建一个新的block代替即可。
    auto tmp = block_cache_.Get(index);
    if (tmp.Exist() == true) {
      tmp.Get().SetClean();
    }
    tmp.UnBind();
    block_cache_.Delete(index, false);
    block_cache_.Insert(index, std::move(block));
  }

  void HandleBlockMetaUpdateWal(uint64_t sequence, uint32_t index, const std::string& name, uint32_t value) {
    auto wrapper = GetBlock(index);
    wrapper.Get().HandleMetaUpdateWal(name, value);
  }

  void HandleBlockDataUpdateWal(uint64_t sequence, uint32_t index, uint32_t offset, const std::string& region) {
    auto wrapper = GetBlock(index);
    wrapper.Get().HandleDataUpdateWal(offset, region);
  }

  void HandleBlockViewWal(uint64_t sequence, uint32_t index, const std::string& view) {
    auto wrapper = GetBlock(index);
    wrapper.Get().HandleViewWal(view);
  }

  void AfterCommitTx() {
    if (sync_per_write_ == true) {
      wal_.Flush();
    }
    static uint64_t tx_count = 0;
    tx_count += 1;
    if (tx_count % create_checkpoint_per_op_ == 0) {
      CreateCheckPoint();
    }
  }

  // 为当前存储内容创建一个快照
  void CreateCheckPoint() {
    BPTREE_LOG_INFO("begin to create chcek point");
    GetMetricSet().GetAs<Counter>("create_checkpoint_count")->Add();
    // 所有wal日志刷盘
    wal_.Flush();
    FlushSuperBlockToFile();
    block_cache_.ForeachValueInCache([this](const uint32_t& key, Block& block) {
      bool dirty = block.Flush();
      if (dirty == true) {
        this->dw_.WriteBlock(block);
        this->FlushBlockToFile(block);
      }
    });
    unused_blocks_.ForeachUnusedBlocks([this](uint32_t key, Block& block) {
      bool dirty = block.Flush(false);
      if (dirty == true) {
        this->dw_.WriteBlock(block);
        this->FlushBlockToFile(block);
      }
    });
    // 所有block刷盘
    f_.Flush();
    // 重置wal文件
    wal_.ResetLogFile();
    BPTREE_LOG_DEBUG("create check point succ");
  }

  void RegisterMetrics() {
    // 接口计数
    metric_set_.CreateMetric<Counter>("get_count");
    metric_set_.CreateMetric<Counter>("get_range_count");
    metric_set_.CreateMetric<Counter>("insert_count");
    metric_set_.CreateMetric<Counter>("update_count");
    metric_set_.CreateMetric<Counter>("delete_count");
    // 从文件中读取block的数量
    metric_set_.CreateMetric<Counter>("load_block_count");
    //
    metric_set_.CreateMetric<Counter>("flush_block_count");
    // 生成check_point的数量
    metric_set_.CreateMetric<Counter>("create_checkpoint_count");
    metric_set_.CreateMetric<Counter>("block_split_count");
    metric_set_.CreateMetric<Counter>("root_block_split_count");
    metric_set_.CreateMetric<Counter>("block_merge_count");
    metric_set_.CreateMetric<Counter>("alloc_block_count");
    metric_set_.CreateMetric<Counter>("dealloc_block_count");
    metric_set_.CreateMetric<Gauge>("dirty_block_count");
  }

 private:
  Mode mode_;
  std::shared_ptr<Comparator> comparator_;
  LRUCache<uint32_t, Block> block_cache_;
  std::string db_name_;
  SuperBlock super_block_;
  FileHandler f_;
  WriteAheadLog wal_;
  DoubleWrite dw_;
  FaultInjection fj_;
  // 记录运行过程中各项指标信息
  MetricSet metric_set_;
  size_t create_checkpoint_per_op_;
  bool sync_per_write_;
  UnusedBlocks unused_blocks_;
};
}  // namespace bptree