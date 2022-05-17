#pragma once

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
#include "bptree/log.h"
#include "bptree/util.h"
#include "bptree/wal.h"

namespace bptree {

constexpr uint32_t bit_map_size = 1024;

enum class GetRangeOption {
  SKIP,
  SELECT,
  STOP,
};

struct BlockManagerOption {
  std::string file_name;
  bool create;
  uint32_t key_size;
  uint32_t value_size;
};

class BlockManager {
 public:
  friend class Block;

  explicit BlockManager(BlockManagerOption option)
      : block_cache_(1024),
        file_name_(option.file_name),
        super_block_(*this, option.key_size, option.value_size),
        wal_(file_name_ + "_wal.log",
             [this](uint64_t seq, MsgType type, const std::string log) -> void { this->HandleWal(seq, type, log); }),
        dw_(file_name_ + "_double_write.log") {
    block_cache_.SetFreeNotify([this](const uint32_t& key, Block& value) -> void {
      bool dirty = value.Flush();
      if (dirty == true) {
        BPTREE_LOG_DEBUG("block {} flush to disk, dirty", key);
        this->dw_.WriteBlock(&value);
        this->FlushBlockToFile(this->f_, value.GetIndex(), &value);
      } else {
        BPTREE_LOG_DEBUG("block {} don't flush to disk, clean");
      }
    });
    if (util::FileNotExist(file_name_)) {
      if (option.create == false) {
        throw BptreeExecption("file ", file_name_, " not exist");
      }
      if (option.key_size == 0 || option.value_size == 0) {
        throw BptreeExecption(
            "block manager construct error, key_size and value_size should not "
            "be 0");
      }
      // 新建的b+树，初始化super block和其他信息即可
      super_block_.root_index_ = 1;
      // super block
      super_block_.free_block_head_ = 0;
      super_block_.current_max_block_index_ = 1;
      f_ = util::CreateFile(file_name_);
      auto root_block = std::unique_ptr<Block>(
          new Block(*this, super_block_.root_index_, 1, super_block_.key_size_, super_block_.value_size_));
      block_cache_.Insert(super_block_.root_index_, std::move(root_block));

      uint64_t seq = wal_.Begin();
      std::string redo_log =
          CreateAllocBlockWalLog(super_block_.root_index_, 1, super_block_.key_size_, super_block_.value_size_);
      std::string undo_log = "";
      wal_.WriteLog(seq, redo_log, undo_log);
      wal_.End(seq);
      BPTREE_LOG_INFO("create db {} succ", file_name_);
    } else {
      if (option.create == true) {
        throw BptreeExecption("file ", file_name_, " already exists");
      }
      f_ = util::OpenFile(file_name_);
      ParseSuperBlockFromFile();
      wal_.Recover();
      // 这个时候cache中的block buf都已经通过wal日志恢复，但是kvview还没有变更，因此需要对cache中的block更新kvview
      block_cache_.ForeachValueInCache([](const uint32_t& index, Block& block) { block.UpdateKvViewByBuf(); });
      BPTREE_LOG_INFO("open db {} succ", file_name_);
    }
  }

  ~BlockManager() { FlushToFile(); }

  typename LRUCache<uint32_t, Block>::Wrapper GetBlock(uint32_t index) {
    auto wrapper = block_cache_.Get(index);
    if (wrapper.Exist() == false) {
      auto block = LoadBlock(index, false);
      block_cache_.Insert(index, std::move(block));
      return block_cache_.Get(index);
    }
    return wrapper;
  }

  uint32_t GetRootIndex() const noexcept { return super_block_.root_index_; }

  // 单点查找，如果db中存在key，返回对应的value，否则返回""
  std::string Get(const std::string& key) {
    if (key.size() != super_block_.key_size_) {
      throw BptreeExecption("wrong key length");
    }
    auto block = GetBlock(super_block_.root_index_);
    std::string result = block.Get().Get(key);
    return result;
  }

  // 范围查找，key为需要查找的起始点，对后续的每个kv调用functor，根据返回值确定结束范围查找 or 跳过 or 选择
  std::vector<std::pair<std::string, std::string>> GetRange(const std::string& key,
                                                            std::function<GetRangeOption(const Entry& entry)> functor) {
    if (key.size() != super_block_.key_size_) {
      throw BptreeExecption("wrong key length");
    }
    auto location = GetBlock(super_block_.root_index_).Get().GetBlockIndexContainKey(key);
    if (location.first == 0) {
      return {};
    }
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
    }
    return result;
  }

  // 插入，如果key已经存在于db中，执行更新操作
  // insert流程中任何涉及到数据修改的地方都需要使用wal日志记录，包括：
  // BlockSplit、修改链接关系、InsertKv操作、对old_root的刷新操作
  void Insert(const std::string& key, const std::string& value) {
    if (key.size() != super_block_.key_size_ || value.size() != super_block_.value_size_) {
      throw BptreeExecption("wrong kv length");
    }
    uint64_t sequence = wal_.Begin();
    InsertInfo info = GetBlock(super_block_.root_index_).Get().Insert(key, value, sequence);
    if (info.state_ == InsertInfo::State::Split) {
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
      if (info.key_ <= left_block.Get().GetMaxKey()) {
        left_block.Get().InsertKv(info.key_, info.value_, sequence);
      } else {
        right_block.Get().InsertKv(info.key_, info.value_, sequence);
      }
      // update root
      old_root.Get().Clear(sequence);
      old_root.Get().SetHeight(old_root_height + 1, sequence);
      old_root.Get().InsertKv(left_block.Get().GetMaxKey(), ConstructIndexByNum(new_blocks.first), sequence);
      old_root.Get().InsertKv(right_block.Get().GetMaxKey(), ConstructIndexByNum(new_blocks.second), sequence);
    }
    wal_.End(sequence);
    return;
  }

  // 删除
  void Delete(const std::string& key) {
    uint64_t sequence = wal_.Begin();
    if (key.size() != super_block_.key_size_) {
      throw BptreeExecption("wrong key length");
    }
    // 根节点的merge信息不处理
    GetBlock(super_block_.root_index_).Get().Delete(key, sequence);
    wal_.End(sequence);
  }

  // 更新，如果key不在db中，直接返回，否则会将对应的value在内存中的缓存传递给updator，由update执行更新，bptree负责将数据重新刷回硬盘
  bool Update(const std::string& key, const std::function<void(char* const ptr, size_t len)>& updator) {
    uint64_t sequence = wal_.Begin();
    if (key.size() != super_block_.key_size_) {
      throw BptreeExecption("wrong key length");
    }
    bool succ = GetBlock(super_block_.root_index_).Get().Update(key, updator, sequence);
    wal_.End(sequence);
    return succ;
  }

  void PrintRootBlock() {
    auto root_block = GetBlock(super_block_.root_index_);
    root_block.Get().Print();
  }

  void PrintBlockByIndex(uint32_t index) {
    if (index == 0) {
      throw BptreeExecption("should not print super block");
    }
    if (super_block_.current_max_block_index_ < index) {
      throw BptreeExecption("request block's index invalid : ", std::to_string(index));
    }
    auto block = GetBlock(index);
    block.Get().Print();
  }

  void PrintCacheInfo() { block_cache_.PrintInfo(); }

  void PrintSuperBlockInfo() {
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

 private:
  std::pair<uint32_t, uint32_t> BlockSplit(const Block* block, uint64_t sequence) {
    uint32_t new_block_1_index = AllocNewBlock(block->GetHeight(), sequence);
    uint32_t new_block_2_index = AllocNewBlock(block->GetHeight(), sequence);
    auto new_block_1 = GetBlock(new_block_1_index);
    auto new_block_2 = GetBlock(new_block_2_index);
    size_t half_count = block->GetKVView().size() / 2;
    for (size_t i = 0; i < half_count; ++i) {
      bool succ =
          new_block_1.Get().InsertKv(block->GetViewByIndex(i).key_view, block->GetViewByIndex(i).value_view, sequence);
      if (succ == false) {
        throw BptreeExecption("block broken, insert nth kv : ", std::to_string(i));
      }
    }
    for (int i = half_count; i < block->GetKVView().size(); ++i) {
      bool succ =
          new_block_2.Get().InsertKv(block->GetViewByIndex(i).key_view, block->GetViewByIndex(i).value_view, sequence);
      if (succ == false) {
        throw BptreeExecption("block broken, insert nth kv : ", std::to_string(i));
      }
    }
    return {new_block_1_index, new_block_2_index};
  }

  uint32_t BlockMerge(const Block* b1, const Block* b2, uint64_t sequence) {
    uint32_t new_block_index = AllocNewBlock(b1->GetHeight(), sequence);
    auto new_block = GetBlock(new_block_index);
    for (size_t i = 0; i < b1->GetKVView().size(); ++i) {
      bool succ = new_block.Get().InsertKv(b1->GetViewByIndex(i).key_view, b1->GetViewByIndex(i).value_view, sequence);
      if (succ == false) {
        throw BptreeExecption("block broken");
      }
    }
    for (size_t i = 0; i < b2->GetKVView().size(); ++i) {
      bool succ = new_block.Get().InsertKv(b2->GetViewByIndex(i).key_view, b2->GetViewByIndex(i).value_view, sequence);
      if (succ == false) {
        throw BptreeExecption("block broken");
      }
    }
    return new_block_index;
  }

  std::string CreateAllocBlockWalLog(uint32_t index, uint32_t height, uint32_t key_size, uint32_t value_size) {
    std::string result;
    uint8_t type = 3;
    result.append((const char*)&type, sizeof(type));
    result.append((const char*)&index, sizeof(index));
    result.append((const char*)&height, sizeof(height));
    result.append((const char*)&key_size, sizeof(key_size));
    result.append((const char*)&value_size, sizeof(value_size));
    return result;
  }

  std::string CreateDeallocBlockWalLog(uint32_t index) {
    std::string result;
    uint8_t type = 4;
    result.append((const char*)&type, sizeof(type));
    result.append((const char*)&index, sizeof(index));
    return result;
  }

  // 申请一个新的Block
  uint32_t AllocNewBlock(uint32_t height, uint64_t sequence) {
    uint32_t result = 0;
    if (super_block_.free_block_head_ == 0) {
      super_block_.SetCurrentMaxBlockIndex(super_block_.current_max_block_index_ + 1, sequence);
      result = super_block_.current_max_block_index_;

      std::string redo_log = CreateAllocBlockWalLog(result, height, super_block_.key_size_, super_block_.value_size_);
      std::string undo_log = "";
      wal_.WriteLog(sequence, redo_log, undo_log);
    } else {
      {
        auto block = GetBlock(super_block_.free_block_head_);
        super_block_.SetFreeBlockHead(block.Get().GetNextFreeIndex(), sequence);
        result = block.Get().GetIndex();
        assert(super_block_.free_block_size_ > 0);
        super_block_.SetFreeBlockSize(super_block_.free_block_size_ - 1, sequence);
        // 这个wal日志的意义是，在回滚的时候可能已经成功分配了新的block并刷盘，因此需要 确保这个块被回收
        std::string redo_log = CreateAllocBlockWalLog(result, height, super_block_.key_size_, super_block_.value_size_);
        std::string undo_log = block.Get().CreateMetaChangeWalLog("next_free_index", super_block_.free_block_head_);
        wal_.WriteLog(sequence, redo_log, undo_log);
      }
      // result这个block读出来只是为了获取下一个free block的index，不会有change操作，因此从cache中删除时不用刷盘
      bool succ = block_cache_.Delete(result, false);
      assert(succ == true);
    }
    auto new_block =
        std::unique_ptr<Block>(new Block(*this, result, height, super_block_.key_size_, super_block_.value_size_));
    block_cache_.Insert(result, std::move(new_block));
    return result;
  }

  void DeallocBlock(uint32_t index, uint64_t sequence, bool update_link_relation = true) {
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
    std::string redo_log = CreateDeallocBlockWalLog(index);
    std::string undo_log = "";
    wal_.WriteLog(sequence, redo_log, undo_log);
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
    dw_.Close();
    f_ = nullptr;
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
      BPTREE_LOG_WARN("parse block {} error, crc32 check fail, try to recover from double_write file", index);
      dw_.ReadBlock(new_block.get());
      succ = new_block->Parse();
      if (succ == false || new_block->GetIndex() != index) {
        throw BptreeExecption("inner error, can't recover block from double_write file : " + std::to_string(index));
      }
    }
    return new_block;
  }

  void HandleWal(uint64_t sequence, MsgType type, const std::string& log) {
    uint8_t wal_type = 0;
    // 五种类型的日志：
    // 1. superblock meta update
    // 2. block alloc
    // 3. block dealloc
    // 4. block meta update
    // 5. block data update
    size_t offset = 0;
    memcpy(&wal_type, &log[offset], sizeof(uint8_t));
    offset += sizeof(uint8_t);
    if (wal_type == 0) {
      uint32_t index = 0;
      uint32_t length = 0;
      std::string meta_name;
      memcpy(&index, &log[offset], sizeof(index));
      offset += sizeof(index);
      assert(index == 0);
      memcpy(&length, &log[offset], sizeof(length));
      offset += sizeof(length);
      meta_name = std::string(&log[offset], length);
      offset += length;
      uint32_t value = 0;
      memcpy(&value, &log[offset], sizeof(value));
      offset += sizeof(value);
      assert(offset == log.size());
      super_block_.HandleWAL(meta_name, value);
    } else if (wal_type == 1) {
      uint32_t index = 0;
      uint32_t length = 0;
      std::string meta_name;
      memcpy(&index, &log[offset], sizeof(index));
      offset += sizeof(index);
      memcpy(&length, &log[offset], sizeof(length));
      offset += sizeof(length);
      meta_name = std::string(&log[offset], length);
      offset += length;
      uint32_t value = 0;
      memcpy(&value, &log[offset], sizeof(value));
      offset += sizeof(value);
      assert(offset == log.size());
      HandleBlockMetaUpdateWal(sequence, index, meta_name, value);
    } else if (wal_type == 2) {
      uint32_t index = 0;
      uint32_t region_offset = 0;
      uint32_t length = 0;
      std::string region;
      memcpy(&index, &log[offset], sizeof(uint32_t));
      offset += sizeof(uint32_t);
      memcpy(&region_offset, &log[offset], sizeof(uint32_t));
      offset += sizeof(uint32_t);
      memcpy(&length, &log[offset], sizeof(uint32_t));
      offset += sizeof(uint32_t);
      region = std::string(&log[offset], length);
      offset += length;
      assert(offset == log.size());
      HandleBlockDataUpdateWal(sequence, index, region_offset, region);
    } else if (wal_type == 3) {
      uint32_t index = 0;
      uint32_t height = 0;
      uint32_t key_size = 0;
      uint32_t value_size = 0;
      memcpy(&index, &log[offset], sizeof(uint32_t));
      offset += sizeof(uint32_t);
      memcpy(&height, &log[offset], sizeof(uint32_t));
      offset += sizeof(uint32_t);
      memcpy(&key_size, &log[offset], sizeof(uint32_t));
      offset += sizeof(uint32_t);
      memcpy(&value_size, &log[offset], sizeof(uint32_t));
      offset += sizeof(uint32_t);
      HandleBlockAllocWal(sequence, index, height, key_size, value_size);
    } else if (wal_type == 4) {
      uint32_t index = 0;
      memcpy(&index, &log[offset], sizeof(uint32_t));
      offset += sizeof(uint32_t);
      HandleBlockDeallocWal(sequence, index);
    } else {
      throw BptreeExecption("invalid wal type ", std::to_string(wal_type));
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
      BPTREE_LOG_INFO("block meta update load block {}", index);
      auto block = LoadBlock(index, true);
      block_cache_.Insert(index, std::move(block));
      wrapper = block_cache_.Get(index);
    }
    wrapper.Get().HandleMetaUpdateWal(name, value);
  }

  void HandleBlockDataUpdateWal(uint64_t sequence, uint32_t index, uint32_t offset, const std::string& region) {
    auto wrapper = block_cache_.Get(index);
    if (wrapper.Exist() == false) {
      BPTREE_LOG_INFO("block data update load block {}", index);
      auto block = LoadBlock(index, true);
      block_cache_.Insert(index, std::move(block));
      wrapper = block_cache_.Get(index);
    }
    wrapper.Get().HandleDataUpdateWal(offset, region);
  }

 private:
  LRUCache<uint32_t, Block> block_cache_;
  std::string file_name_;
  SuperBlock super_block_;
  FILE* f_;
  WriteAheadLog wal_;
  DoubleWrite dw_;
  FaultInjection fj_;
};
}  // namespace bptree