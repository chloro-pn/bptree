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
#include "bptree/key_comparator.h"
#include "bptree/log.h"
#include "bptree/metric/metric.h"
#include "bptree/metric/metric_set.h"
#include "bptree/queue.h"
#include "bptree/transaction.h"
#include "bptree/util.h"
#include "bptree/wal.h"

namespace bptree {

constexpr uint32_t bit_map_size = 1024;
constexpr size_t operation_queue_size = 4096;

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

enum class NotExistFlag {
  CREATE,
  ERROR,
};

enum class ExistFlag {
  SUCC,
  ERROR,
};

struct BlockManagerOption {
  std::string db_name;
  Mode mode;
  NotExistFlag neflag;
  ExistFlag eflag;
  uint32_t key_size;
  uint32_t value_size;
  std::shared_ptr<Comparator> cmp = std::make_shared<Comparator>();
  bool no_reset_wal = false;
};

inline std::string CreateWalNameByDB(const std::string& db_name) {
  return db_name + "/" + db_name + "_wal.log";
}

inline std::string CreateDWfileNameByDB(const std::string& db_name) {
  return db_name + "/" + db_name + "_double_write.log";
}

inline std::string CreateDbFileNameByDB(const std::string& db_name) {
  return db_name + "/" + db_name + ".db";
}

namespace detail {

enum class LogType : uint8_t {
  SUPER_META,
  BLOCK_META,
  BLOCK_DATA,
  BLOCK_ALLO,
  BLOCK_DEAL,
  BLOCK_VIEW,
};

inline constexpr uint8_t LogTypeToUint8T(LogType type) { return static_cast<uint8_t>(type); }

}  // namespace detail

class BlockManager {
 public:
  friend class Block;
  friend class BlockBase;
  friend class SuperBlock;

  BPTREE_INTERFACE explicit BlockManager(BlockManagerOption option)
      : mode_(option.mode),
        comparator_(option.cmp),
        block_cache_(1024),
        db_name_(option.db_name),
        super_block_(*this, option.key_size, option.value_size),
        wal_(CreateWalNameByDB(db_name_)),
        dw_(CreateDWfileNameByDB(db_name_)),
        no_reset_wal_(option.no_reset_wal),
        queue_(operation_queue_size),
        stop_(false) {
    block_cache_.SetFreeNotify([this](const uint32_t& key, Block& value) -> void {
      this->OnCacheDelete(key, value);
    });
    wal_.RegisterLogHandler([this](uint64_t seq, MsgType type, const std::string log) -> void {
      this->HandleWal(seq, type, log);
    });
    RegisterMetrics();
    if (util::FileNotExist(db_name_)) {
      if (option.neflag == NotExistFlag::ERROR) {
        throw BptreeExecption("db ", db_name_, " not exist");
      }
      if (option.key_size == 0 || option.value_size == 0) {
        throw BptreeExecption(
            "block manager construct error, key_size and value_size should not "
            "be 0");
      }
      // 新建的b+树，初始化super block和其他信息即可
      // 不需要将这些信息写入wal日志，因为如果db存在的话会解析super block，解析失败则抛出异常
      super_block_.root_index_ = 1;
      super_block_.free_block_head_ = 0;
      super_block_.current_max_block_index_ = 1;
      util::CreateDir(db_name_);
      f_ = util::CreateFile(CreateDbFileNameByDB(db_name_));
      wal_.OpenFile();
      dw_.OpenFile();
      auto root_block = std::unique_ptr<Block>(
          new Block(*this, super_block_.root_index_, 1, super_block_.key_size_, super_block_.value_size_));
      block_cache_.Insert(super_block_.root_index_, std::move(root_block));

      FlushSuperBlockToFile(f_);

      uint64_t seq = wal_.RequestSeq();
      wal_.Begin(seq);
      std::string redo_log =
          CreateAllocBlockWalLog(super_block_.root_index_, 1, super_block_.key_size_, super_block_.value_size_);
      std::string undo_log = "";
      wal_.WriteLog(seq, redo_log, undo_log);
      wal_.End(seq);
      BPTREE_LOG_INFO("create db {} succ", db_name_);
    } else {
      if (option.eflag == ExistFlag::ERROR) {
        throw BptreeExecption("db ", db_name_, " already exists");
      }
      f_ = util::OpenFile(CreateDbFileNameByDB(db_name_));
      wal_.OpenFile();
      dw_.OpenFile();
      ParseSuperBlockFromFile();
      wal_.Recover();
      // 这个时候cache中的block buf都已经通过wal日志恢复，但是kvview还没有变更，因此需要对cache中的block更新kvview
      block_cache_.ForeachValueInCache([](const uint32_t& index, Block& block) { block.UpdateKvViewByBuf(); });
      BPTREE_LOG_INFO("open db {} succ", db_name_);
    }
  }

  BPTREE_INTERFACE ~BlockManager() { FlushToFile(); }

  // 单点查找，如果db中存在key，返回对应的value，否则返回""
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

  // 范围查找，key为需要查找的起始点，对后续的每个kv调用functor，根据返回值确定结束范围查找 or 跳过 or 选择
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

  // 插入，如果key已经存在返回false，并不做任何操作，否则执行插入并返回true
  BPTREE_INTERFACE bool Insert(const std::string& key, const std::string& value, uint64_t seq = no_wal_sequence) {
    if (mode_ != Mode::W && mode_ != Mode::WR) {
      throw BptreeExecption("Permission denied");
    }
    if (key.size() != super_block_.key_size_ || value.size() != super_block_.value_size_) {
      throw BptreeExecption("wrong kv length");
    }

    uint64_t sequence = seq;
    if (sequence == no_wal_sequence) {
      sequence = wal_.RequestSeq();
      wal_.Begin(sequence);
    }
    GetMetricSet().GetAs<Counter>("insert_count")->Add();
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

  // 删除, 返回被删除的value，如果没有找到，则返回空字符串
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

  // 更新，返回更新前的旧值，如果key不存在返回空字符串
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
      throw BptreeExecption("request block's index invalid : ", std::to_string(index));
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
      auto block = LoadBlock(index, false);
      block_cache_.Insert(index, std::move(block));
      return block_cache_.Get(index);
    }
    return wrapper;
  }

  uint32_t GetRootIndex() const noexcept { return super_block_.root_index_; }

  uint32_t GetMaxBlockIndex() const noexcept { return super_block_.current_max_block_index_; }

  Queue<Operation>& GetQueue() { return queue_; }

  void Stop() { stop_.store(true); }

  void Run() {
    // 记录了每个事务的历史记录，用于回滚
    std::unordered_map<uint64_t, std::vector<std::unique_ptr<Operation>>> txs_;
    while (stop_.load() == false) {
      std::vector<std::unique_ptr<Operation>> ops = queue_.TryPop();
      for (auto& each : ops) {
        std::unique_ptr<Operation> result(new Operation());
        result->sequence = each->sequence;
        result->type = each->type;
        auto notify_queue = each->notify_queue_;
        uint64_t seq = each->sequence;
        if (each->type == OperationType::Begin) {
          wal_.Begin(seq);
          assert(txs_.count(seq) == 0);
          txs_[seq].push_back(result->CopyWithoutQueue());
        } else if (each->type == OperationType::End) {
          wal_.End(seq);
          txs_.erase(seq);
        } else if (each->type == OperationType::Get) {
          auto value = Get(each->key);
          result->key = each->key;
          result->value = value;
          assert(txs_.count(seq) == 1);
          txs_[seq].push_back(result->CopyWithoutQueue());
        } else if (each->type == OperationType::Insert) {
          bool succ = Insert(each->key, each->value, seq);
          result->key = each->key;
          result->value = succ ? each->value : "";
          assert(txs_.count(seq) == 1);
          txs_[seq].push_back(result->CopyWithoutQueue());
        } else if (each->type == OperationType::Delete) {
          auto old_v = Delete(each->key, seq);
          result->key = each->key;
          result->value = old_v;
          assert(txs_.count(seq) == 1);
          txs_[seq].push_back(result->CopyWithoutQueue());
        } else if (each->type == OperationType::Update) {
          auto old_v = Update(each->key, each->value, seq);
          result->key = each->key;
          result->value = old_v;
          assert(txs_.count(seq) == 1);
          txs_[seq].push_back(result->CopyWithoutQueue());
        } else if (each->type == OperationType::RollBack) {
          // todo rollback
          RollBack(txs_[seq], *this, seq);
          txs_.erase(seq);
        }
        notify_queue->Push(std::move(result));
      }
      sleep(std::chrono::milliseconds(10));
    }
  }

 private:
  std::pair<uint32_t, uint32_t> BlockSplit(const Block* block, uint64_t sequence) {
    GetMetricSet().GetAs<Counter>("block_split_count")->Add();
    uint32_t new_block_1_index = AllocNewBlock(block->GetHeight(), sequence);
    uint32_t new_block_2_index = AllocNewBlock(block->GetHeight(), sequence);
    auto new_block_1 = GetBlock(new_block_1_index);
    auto new_block_2 = GetBlock(new_block_2_index);
    // 在插入之前记录旧的数据快照, 作为undo log
    std::string block_1_undo = CreateBlockViewWalLog(new_block_1_index, new_block_1.Get().CreateDataView());
    std::string block_2_undo = CreateBlockViewWalLog(new_block_2_index, new_block_2.Get().CreateDataView());
    size_t half_count = block->GetKVView().size() / 2;
    for (size_t i = 0; i < half_count; ++i) {
      bool succ = new_block_1.Get().AppendKv(block->GetViewByIndex(i).key_view, block->GetViewByIndex(i).value_view,
                                             no_wal_sequence);
      if (succ == false) {
        throw BptreeExecption("block broken, insert nth kv : ", std::to_string(i));
      }
    }
    for (int i = half_count; i < block->GetKVView().size(); ++i) {
      bool succ = new_block_2.Get().AppendKv(block->GetViewByIndex(i).key_view, block->GetViewByIndex(i).value_view,
                                             no_wal_sequence);
      if (succ == false) {
        throw BptreeExecption("block broken, insert nth kv : ", std::to_string(i));
      }
    }
    // 插入之后记录新的数据快照，作为redo log
    std::string block_1_redo = CreateBlockViewWalLog(new_block_1_index, new_block_1.Get().CreateDataView());
    std::string block_2_redo = CreateBlockViewWalLog(new_block_2_index, new_block_2.Get().CreateDataView());
    wal_.WriteLog(sequence, block_1_redo, block_1_undo);
    wal_.WriteLog(sequence, block_2_redo, block_2_undo);
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
    std::string block_undo = CreateBlockViewWalLog(new_block_index, new_block.Get().CreateDataView());
    for (size_t i = 0; i < b1->GetKVView().size(); ++i) {
      bool succ =
          new_block.Get().AppendKv(b1->GetViewByIndex(i).key_view, b1->GetViewByIndex(i).value_view, no_wal_sequence);
      if (succ == false) {
        throw BptreeExecption("block broken");
      }
    }
    for (size_t i = 0; i < b2->GetKVView().size(); ++i) {
      bool succ =
          new_block.Get().AppendKv(b2->GetViewByIndex(i).key_view, b2->GetViewByIndex(i).value_view, no_wal_sequence);
      if (succ == false) {
        throw BptreeExecption("block broken");
      }
    }
    std::string block_redo = CreateBlockViewWalLog(new_block_index, new_block.Get().CreateDataView());
    wal_.WriteLog(sequence, block_redo, block_undo);
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

  std::string CreateDeallocBlockWalLog(uint32_t index) {
    std::string result;
    util::StringAppender(result, detail::LogTypeToUint8T(detail::LogType::BLOCK_DEAL));
    util::StringAppender(result, index);
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
      if (sequence != no_wal_sequence) {
        std::string redo_log = CreateAllocBlockWalLog(result, height, super_block_.key_size_, super_block_.value_size_);
        std::string undo_log = "";
        wal_.WriteLog(sequence, redo_log, undo_log);
      }
    } else {
      auto block = GetBlock(super_block_.free_block_head_);
      super_block_.SetFreeBlockHead(block.Get().GetNextFreeIndex(), sequence);
      result = block.Get().GetIndex();
      assert(super_block_.free_block_size_ > 0);
      super_block_.SetFreeBlockSize(super_block_.free_block_size_ - 1, sequence);
      // 这个wal日志的意义是，在回滚的时候可能已经成功分配了新的block并刷盘，因此需要 确保这个块被回收
      if (sequence != no_wal_sequence) {
        std::string redo_log =
            CreateAllocBlockWalLog(result, height, super_block_.key_size_, super_block_.value_size_);
        std::string undo_log = block.Get().CreateMetaChangeWalLog("next_free_index", super_block_.free_block_head_);
        wal_.WriteLog(sequence, redo_log, undo_log);
      }
      block.UnBind();
      // result这个block读出来只是为了获取下一个free block的index，不会有change操作，因此从cache中删除时不用刷盘
      bool succ = block_cache_.Delete(result, false);
      assert(succ == true);
    }
    auto new_block =
        std::unique_ptr<Block>(new Block(*this, result, height, super_block_.key_size_, super_block_.value_size_));
    block_cache_.Insert(result, std::move(new_block));
    BPTREE_LOG_DEBUG("alloc new block {}", result);
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
    block.Get().Clear(sequence);
    block.Get().SetNextFreeIndex(super_block_.free_block_head_, sequence);
    block.UnBind();
    super_block_.SetFreeBlockHead(index, sequence);
    super_block_.SetFreeBlockSize(super_block_.free_block_size_ + 1, sequence);
    bool succ = block_cache_.Delete(index, true);
    assert(succ == true);
    if (sequence != no_wal_sequence) {
      std::string redo_log = CreateDeallocBlockWalLog(index);
      std::string undo_log = "";
      wal_.WriteLog(sequence, redo_log, undo_log);
    }
    BPTREE_LOG_DEBUG("dealloc block {}", index);
  }

  void ReadBlockFromFile(BlockBase* block, uint32_t index) {
    block->BufInit();
    util::FileReadAt(f_, block->GetBuf(), block_size, index * block_size);
  }

  void FlushToFile() {
    if (f_ == nullptr) {
      return;
    }
    FlushSuperBlockToFile(f_);
    bool succ = block_cache_.Clear();
    assert(succ == true);
    fclose(f_);
    f_ = nullptr;
    dw_.Close();
    auto& cond = GetFaultInjection().GetTheLastCheckPointFailCondition();
    if (cond && cond() == true) {
      BPTREE_LOG_WARN("fault injection : the last check point fail");
      std::exit(-1);
    }
    // 正常关闭的情况下执行到这里，所有block都刷到了磁盘，所以可以安全的删除wal日志
    if (no_reset_wal_ == false) {
      wal_.ResetLogFile();
    }
  }

  void OnCacheDelete(const uint32_t& index, Block& block) {
    bool dirty = block.Flush();
    if (dirty == true) {
      BPTREE_LOG_DEBUG("block {} flush to disk, dirty", index);
      this->dw_.WriteBlock(&block);
      this->FlushBlockToFile(this->f_, block.GetIndex(), &block);
    } else {
      BPTREE_LOG_DEBUG("block {} don't flush to disk, clean", index);
    }
  }

  /*
   * 支持故障注入，可选择的写入部分数据，模拟断电过程中partial write情况
   */
  void FlushBlockToFile(FILE* f, uint32_t index, BlockBase* block) {
    auto& condition = GetFaultInjection().GetPartialWriteCondition();
    if (condition) {
      bool ret = condition(index);
      if (ret == true) {
        FlushBlockToFilePartialWriteAndExit(f, index, block);
        return;
      }
    }
    util::FileWriteAt(f, block->GetBuf(), block_size, index * block_size);
    fflush(f);
  }

  void FlushBlockToFilePartialWriteAndExit(FILE* f, uint32_t index, BlockBase* block) {
    util::FileWriteAt(f, block->GetBuf(), block_size / 2, index * block_size);
    fflush(f);
    std::exit(-1);
  }

  void FlushSuperBlockToFile(FILE* f) {
    super_block_.SetDirty();
    super_block_.Flush();
    dw_.WriteBlock(&super_block_);
    FlushBlockToFile(f, 0, &super_block_);
  }

  void ParseSuperBlockFromFile() {
    bool succ = true;
    try {
      ReadBlockFromFile(&super_block_, 0);
    } catch (const BptreeExecption& e) {
      succ = false;
    }
    if (succ == true) {
      succ = super_block_.Parse();
    }
    if (succ == false) {
      BPTREE_LOG_WARN("super block crc32 check fail, try to recover from double_write file");
      // crc校验失败，尝试从double write文件中恢复，并写回db文件
      dw_.ReadBlock(&super_block_);
      succ = super_block_.Parse();
      if (succ == false || super_block_.GetIndex() != 0) {
        throw BptreeExecption("inner error, can't recover super block from double_write file");
      }
      // 如果成功，立刻覆盖掉错误的数据
      BPTREE_LOG_INFO("recover super block succ, flush to db immediately");
      super_block_.SetDirty();
      super_block_.Flush();
      FlushBlockToFile(f_, 0, &super_block_);
    }
  }

  // 读到后进行crc校验，如果失败则从dw文件中恢复
  std::unique_ptr<Block> LoadBlock(uint32_t index, bool recovering) {
    std::unique_ptr<Block> new_block = std::unique_ptr<Block>(new Block(*this));
    if (recovering == false) {
      ReadBlockFromFile(new_block.get(), index);
      bool succ = new_block->Parse();
      if (succ == false) {
        throw BptreeExecption("load block, crc32 check error");
      }
      return new_block;
    }

    ReadBlockFromFile(new_block.get(), index);
    bool succ = new_block->Parse();
    if (succ == false) {
      BPTREE_LOG_WARN("parse block {} error : crc32 check fail, try to recover from double_write file", index);
      dw_.ReadBlock(new_block.get());
      succ = new_block->Parse();
      if (succ == false || new_block->GetIndex() != index) {
        throw BptreeExecption("inner error, can't recover block from double_write file : " + std::to_string(index));
      }
    }
    BPTREE_LOG_DEBUG("load block {} from disk succ", index);
    return new_block;
  }

  void HandleWal(uint64_t sequence, MsgType type, const std::string& log) {
    // 五种类型的日志：
    // superblock meta update
    // block alloc
    // block dealloc
    // block meta update
    // block data update
    size_t offset = 0;
    uint8_t wal_type = util::StringParser<uint8_t>(log, offset);
    switch (wal_type) {
      case detail::LogTypeToUint8T(detail::LogType::SUPER_META): {
        uint32_t index = util::StringParser<uint32_t>(log, offset);
        std::string meta_name = util::StringParser(log, offset);
        uint32_t value = util::StringParser<uint32_t>(log, offset);
        assert(offset == log.size());
        super_block_.HandleWAL(meta_name, value);
        break;
      }
      case detail::LogTypeToUint8T(detail::LogType::BLOCK_META): {
        uint32_t index = util::StringParser<uint32_t>(log, offset);
        std::string meta_name = util::StringParser(log, offset);
        uint32_t value = util::StringParser<uint32_t>(log, offset);
        assert(offset == log.size());
        HandleBlockMetaUpdateWal(sequence, index, meta_name, value);
        break;
      }
      case detail::LogTypeToUint8T(detail::LogType::BLOCK_DATA): {
        uint32_t index = util::StringParser<uint32_t>(log, offset);
        uint32_t region_offset = util::StringParser<uint32_t>(log, offset);
        std::string region = util::StringParser(log, offset);
        assert(offset == log.size());
        HandleBlockDataUpdateWal(sequence, index, region_offset, region);
        break;
      }
      case detail::LogTypeToUint8T(detail::LogType::BLOCK_ALLO): {
        uint32_t index = util::StringParser<uint32_t>(log, offset);
        uint32_t height = util::StringParser<uint32_t>(log, offset);
        uint32_t key_size = util::StringParser<uint32_t>(log, offset);
        uint32_t value_size = util::StringParser<uint32_t>(log, offset);
        assert(offset == log.size());
        HandleBlockAllocWal(sequence, index, height, key_size, value_size);
        break;
      }
      case detail::LogTypeToUint8T(detail::LogType::BLOCK_DEAL): {
        uint32_t index = util::StringParser<uint32_t>(log, offset);
        HandleBlockDeallocWal(sequence, index);
        break;
      }
      case detail::LogTypeToUint8T(detail::LogType::BLOCK_VIEW): {
        uint32_t index = util::StringParser<uint32_t>(log, offset);
        std::string view = util::StringParser(log, offset);
        assert(offset == log.size());
        HandleBlockViewWal(sequence, index, view);
        break;
      }
      default: {
        throw BptreeExecption("invalid wal type ", std::to_string(wal_type));
      }
    }
  }

  void HandleBlockAllocWal(uint64_t sequence, uint32_t index, uint32_t height, uint32_t key_size, uint32_t value_size) {
    assert(key_size == super_block_.key_size_ && value_size == super_block_.value_size_);
    auto block = std::unique_ptr<Block>(new Block(*this, index, height, key_size, value_size));
    auto wrapper = block_cache_.Get(index);
    if (wrapper.Exist() == true) {
      throw BptreeExecption("inner error, duplicate block " + std::to_string(index));
    }
    block_cache_.Insert(index, std::move(block));
  }

  void HandleBlockDeallocWal(uint64_t sequence, uint32_t index) { block_cache_.Delete(index, true); }

  void HandleBlockMetaUpdateWal(uint64_t sequence, uint32_t index, const std::string& name, uint32_t value) {
    auto wrapper = block_cache_.Get(index);
    if (wrapper.Exist() == false) {
      BPTREE_LOG_DEBUG("block meta update load block {}", index);
      auto block = LoadBlock(index, true);
      block_cache_.Insert(index, std::move(block));
      wrapper = block_cache_.Get(index);
    }
    wrapper.Get().HandleMetaUpdateWal(name, value);
  }

  void HandleBlockDataUpdateWal(uint64_t sequence, uint32_t index, uint32_t offset, const std::string& region) {
    auto wrapper = block_cache_.Get(index);
    if (wrapper.Exist() == false) {
      BPTREE_LOG_DEBUG("block data update load block {}", index);
      auto block = LoadBlock(index, true);
      block_cache_.Insert(index, std::move(block));
      wrapper = block_cache_.Get(index);
    }
    wrapper.Get().HandleDataUpdateWal(offset, region);
  }

  void HandleBlockViewWal(uint64_t sequence, uint32_t index, const std::string& view) {
    auto wrapper = block_cache_.Get(index);
    if (wrapper.Exist() == false) {
      BPTREE_LOG_DEBUG("block view load block {}", index);
      auto block = LoadBlock(index, true);
      block_cache_.Insert(index, std::move(block));
      wrapper = block_cache_.Get(index);
    }
    wrapper.Get().HandleViewWal(view);
  }

  // 每隔一段时间，刷新cache中的部分脏页到磁盘，必要的时候创建快照
  void AfterCommitTx() {
    uint64_t tx_count = GetMetricSet().GetAs<Counter>("insert_count")->GetValue();
    tx_count += GetMetricSet().GetAs<Counter>("update_count")->GetValue();
    tx_count += GetMetricSet().GetAs<Counter>("delete_count")->GetValue();
    size_t total_block_count = block_cache_.GetEntrySize();
    // 如果cache中block的数量本来就很小，则不进行刷盘处理
    bool too_small = total_block_count <= block_cache_.GetCapacity() * 0.2;
    if (tx_count % 128 == 0 && tx_count % 2048 != 0 && !too_small) {
      double dirty_block_count = GetMetricSet().GetAs<Gauge>("dirty_block_count")->GetValue();
      double rate = dirty_block_count / total_block_count;
      if (rate >= 0.4) {
        size_t flush_count = dirty_block_count * 0.2;
        BPTREE_LOG_DEBUG("flush {} blocks to disk", flush_count);
        // todo, 将部分脏页刷盘，按照lru列表中的倒序顺序进行，因为排到后面的页面有更大的可能不被修改
        size_t current = 0;
        block_cache_.ForeachValueInTheReverseOrderOfLRUList(
            [this, flush_count, &current](const uint32_t& key, Block& block) -> bool {
              if (current == flush_count) {
                return false;
              }
              bool dirty = block.Flush();
              if (dirty == true) {
                current += 1;
                BPTREE_LOG_DEBUG("block {} flush to disk, dirty", key);
                this->dw_.WriteBlock(&block);
                this->FlushBlockToFile(this->f_, block.GetIndex(), &block);
              } else {
                BPTREE_LOG_DEBUG("block {} don't flush to disk, clean", key);
              }
              return true;
            });
      }
      return;
    }
    // 固定的每隔2048个事务生成一个checkpoint
    if (tx_count % 2048 == 0 && no_reset_wal_ == false) {
      CreateCheckPoint();
    }
  }

  // 为当前存储内容创建一个快照，注意：不能有未完成的事务，原因：创建完快照后之前的日志全部删除，
  // 此时如果有执行到一半的事务，则快照点前的部分的日志已经消失，但是事务不确定是提交的，如果之后由于故障导致事务回滚，则会导致快照点之前的日志丢失而无法回滚
  // 1. 将所有cache中的block刷回磁盘(但是并不从cache中delete)
  // 2. 调用wal_.ResetLogFile()函数
  void CreateCheckPoint() {
    GetMetricSet().GetAs<Counter>("create_checkpoint_count")->Add();
    FlushSuperBlockToFile(f_);
    Counter flush_block_count("flush_block_count");
    block_cache_.ForeachValueInCache([this, &flush_block_count](const uint32_t& key, Block& block) {
      bool dirty = block.Flush();
      if (dirty == true) {
        flush_block_count.Add();
        BPTREE_LOG_DEBUG("block {} flush to disk, dirty", key);
        this->dw_.WriteBlock(&block);
        this->FlushBlockToFile(this->f_, block.GetIndex(), &block);
      } else {
        BPTREE_LOG_DEBUG("block {} don't flush to disk, clean");
      }
    });
    BPTREE_LOG_DEBUG("create check point : flush {} blocks to disk", flush_block_count.GetValue());
    // 此时所有修改操作都已经刷入磁盘，可以安全的删除wal日志
    wal_.ResetLogFile();
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
  FILE* f_;
  WriteAheadLog wal_;
  DoubleWrite dw_;
  FaultInjection fj_;
  // 记录运行过程中各项指标信息
  MetricSet metric_set_;
  // 关闭清空wal日志操作
  bool no_reset_wal_;
  Queue<Operation> queue_;
  std::atomic<bool> stop_;
};
}  // namespace bptree