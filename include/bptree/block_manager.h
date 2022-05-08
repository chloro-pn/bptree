#pragma once

#include <cassert>
#include <cstdio>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "bptree/block.h"
#include "bptree/cache.h"
#include "bptree/exception.h"
#include "bptree/util.h"

namespace bptree {

constexpr uint32_t bit_map_size = 1024;

enum class GetRangeOption {
  SKIP,
  SELECT,
  STOP,
};

class BlockManager {
 public:  
  explicit BlockManager(const std::string& file_name, uint32_t key_size = 0,
                        uint32_t value_size = 0)
      : block_cache_(1024,
                     [this](const uint32_t key) -> std::unique_ptr<Block> {
                       return this->LoadBlock(key);
                     }),
        file_name_(file_name),
        super_block_(key_size, value_size) {
    block_cache_.SetFreeNotify([this](const uint32_t& key, Block& value) -> void {
      bool dirty = value.Flush();
      if (dirty == true) {
          this->FlushBlockToFile(this->f_, value.GetIndex(), &value);
      }
    });
    // 从文件中读取super_block_，填充root_index_。
    if (util::FileNotExist(file_name)) {
      if (key_size == 0 || value_size == 0) {
        throw BptreeExecption(
            "block manager construct error, key_size and value_size should not "
            "be 0");
      }
      // 新建的b+树，初始化super block和其他信息即可
      super_block_.root_index_ = 1;
      // super block
      super_block_.free_block_head_ = 0;
      super_block_.current_max_block_index_ = 1;
      // root block
      auto root_block = std::unique_ptr<Block>(
          new Block(*this, super_block_.root_index_, 1, key_size, value_size));
      block_cache_.Get(super_block_.root_index_, std::move(root_block));
      f_ = fopen(file_name_.c_str(), "wb+");
      if (f_ == nullptr) {
        throw BptreeExecption("file " + file_name + " open fail");
      }
    } else {
      f_ = fopen(file_name.c_str(), "rb+");
      if (f_ == nullptr) {
        throw BptreeExecption("file " + file_name + " open fail");
      }
      ParseSuperBlockFromFile();
    }
  }

  ~BlockManager() { FlushToFile(); }

  Block& GetBlock(uint32_t index) { return block_cache_.Get(index); }

  std::pair<uint32_t, uint32_t> BlockSplit(const Block* block) {
    uint32_t new_block_1_index = AllocNewBlock(block->GetHeight());
    uint32_t new_block_2_index = AllocNewBlock(block->GetHeight());
    Block& new_block_1 = block_cache_.Get(new_block_1_index);
    Block& new_block_2 = block_cache_.Get(new_block_2_index);
    size_t half_count = block->GetKVView().size() / 2;
    for (size_t i = 0; i < half_count; ++i) {
      bool succ = new_block_1.InsertKv(block->GetViewByIndex(i).key_view,
                                       block->GetViewByIndex(i).value_view);
      if (succ == false) {
        throw BptreeExecption("block broken");
      }
    }
    for (int i = half_count; i < block->GetKVView().size(); ++i) {
      bool succ = new_block_2.InsertKv(block->GetViewByIndex(i).key_view,
                                       block->GetViewByIndex(i).value_view);
      if (succ == false) {
        throw BptreeExecption("block broken");
      }
    }
    return {new_block_1_index, new_block_2_index};
  }

  uint32_t BlockMerge(const Block* b1, const Block* b2) {
    uint32_t new_block_index = AllocNewBlock(b1->GetHeight());
    Block& new_block = block_cache_.Get(new_block_index);
    for (size_t i = 0; i < b1->GetKVView().size(); ++i) {
      bool succ = new_block.InsertKv(b1->GetViewByIndex(i).key_view,
                                     b1->GetViewByIndex(i).value_view);
      if (succ == false) {
        throw BptreeExecption("block broken");
      }
    }
    for (size_t i = 0; i < b2->GetKVView().size(); ++i) {
      bool succ = new_block.InsertKv(b2->GetViewByIndex(i).key_view,
                                     b2->GetViewByIndex(i).value_view);
      if (succ == false) {
        throw BptreeExecption("block broken");
      }
    }
    return new_block_index;
  }

  std::string Get(const std::string& key) {
    if (key.size() != super_block_.key_size_) {
      throw BptreeExecption("wrong key length");
    }
    return block_cache_.Get(super_block_.root_index_).Get(key);
  }

  // 范围查找
  std::vector<std::pair<std::string, std::string>> GetRange(
      const std::string& key,
      std::function<GetRangeOption(const Entry& entry)> functor) {
    if (key.size() != super_block_.key_size_) {
      throw BptreeExecption("wrong key length");
    }
    auto location =
        block_cache_.Get(super_block_.root_index_).GetBlockIndexContainKey(key);
    if (location.first == 0) {
      return {};
    }
    std::vector<std::pair<std::string, std::string>> result;
    uint32_t block_index = location.first;
    uint32_t view_index = location.second;
    while (block_index != 0) {
      Block& block = block_cache_.Get(block_index);
      for (size_t i = view_index; i < block.GetKVView().size(); ++i) {
        const Entry& entry = block.GetViewByIndex(i);
        GetRangeOption state = functor(entry);
        if (state == GetRangeOption::SKIP) {
          continue;
        } else if (state == GetRangeOption::SELECT) {
          result.push_back(
              {std::string(entry.key_view), std::string(entry.value_view)});
        } else {
          return result;
        }
      }
      // 下一个block
      block_index = block.GetNext();
      view_index = 0;
    }
    return result;
  }

  void Insert(const std::string& key, const std::string& value) {
    if (key.size() != super_block_.key_size_ ||
        value.size() != super_block_.value_size_) {
      throw BptreeExecption("wrong kv length");
    }
    InsertInfo info =
        block_cache_.Get(super_block_.root_index_).Insert(key, value);
    if (info.state_ == InsertInfo::State::Split) {
      // 根节点的分裂
      uint32_t old_root_index = super_block_.root_index_;
      Block& old_root = block_cache_.Get(super_block_.root_index_);
      uint32_t old_root_height = old_root.GetHeight();
      auto new_blocks = BlockSplit(&old_root);

      Block& left_block = block_cache_.Get(new_blocks.first);
      Block& right_block = block_cache_.Get(new_blocks.second);
      // update link
      left_block.SetNext(new_blocks.second);
      right_block.SetPrev(new_blocks.first);
      // insert
      if (info.key_ <= left_block.GetMaxKey()) {
        left_block.InsertKv(info.key_, info.value_);
      } else {
        right_block.InsertKv(info.key_, info.value_);
      }
      // update root
      old_root.Clear();
      old_root.SetHeight(old_root_height + 1);
      old_root.InsertKv(left_block.GetMaxKey(),
                        ConstructIndexByNum(new_blocks.first));
      old_root.InsertKv(right_block.GetMaxKey(),
                        ConstructIndexByNum(new_blocks.second));
    }
    return;
  }

  void Delete(const std::string& key) {
    if (key.size() != super_block_.key_size_) {
      throw BptreeExecption("wrong key length");
    }
    // 根节点的merge信息不处理
    block_cache_.Get(super_block_.root_index_).Delete(key);
  }

  // 申请一个新的Block
  uint32_t AllocNewBlock(uint32_t height) {
    uint32_t result = 0;
    if (super_block_.free_block_head_ == 0) {
      ++super_block_.current_max_block_index_;
      result = super_block_.current_max_block_index_;
    } else {
      Block& block = block_cache_.Get(super_block_.free_block_head_);
      super_block_.free_block_head_ = block.GetNextFreeIndex();
      result = block.GetIndex();
      block_cache_.Delete(result, false);
    }
    auto new_block = std::unique_ptr<Block>(
        new Block(*this, result, height, super_block_.key_size_,
                  super_block_.value_size_));
    block_cache_.Get(result, std::move(new_block));
    return result;
  }

  void DeallocBlock(uint32_t index, bool update_link_relation = true) {
    Block& block = block_cache_.Get(index);
    if (update_link_relation == true) {
      uint32_t next = block.GetNext();
      uint32_t prev = block.GetPrev();
      if (next != 0) {
        Block& next_block = block_cache_.Get(next);
        next_block.SetPrev(prev);
      }
      if (prev != 0) {
        Block& prev_block = block_cache_.Get(prev);
        prev_block.SetNext(next);
      }
    }
    block.Clear();
    block.SetNextFreeIndex(super_block_.free_block_head_);
    super_block_.free_block_head_ = index;
    // cache中也要删除
    block_cache_.Delete(index, true);
  }

  void ReadBlockFromFile(BlockBase* block, uint32_t index) {
    block->BufInit();
    int ret = fseek(f_, index * block_size, SEEK_SET);
    if (ret != 0) {
      throw BptreeExecption("fseek fail");
    }
    size_t read_ret = fread(block->GetBuf(), block_size, 1, f_);
    if (read_ret != 1) {
      throw BptreeExecption("fread fail, index == " + std::to_string(index));
    }
  }

  void FlushToFile() {
    if (f_ == nullptr) {
      return;
    }
    FlushSuperBlockToFile(f_);
    block_cache_.Clear();
    fclose(f_);
    f_ = nullptr;
  }

  bool FlushBlockToFile(FILE* f, uint32_t index, BlockBase* block) {
    uint32_t offset = index * block_size;
    int ret = fseek(f, static_cast<long>(offset), SEEK_SET);
    if (ret != 0) {
      throw BptreeExecption("fseek error");
    }
    size_t n = fwrite(block->GetBuf(), block_size, 1, f);
    if (n != 1) {
      throw BptreeExecption("fwrite error");
    }
  }

  void OnBlockDestructor(Block& block) { block.Flush(); }

  // 重构
  void FlushSuperBlockToFile(FILE* f) {
    super_block_.SetDirty();
    super_block_.Flush();
    FlushBlockToFile(f, 0, &super_block_);
  }

  void ParseSuperBlockFromFile() {
    ReadBlockFromFile(&super_block_, 0);
    super_block_.Parse();
    GetBlock(super_block_.root_index_);
  }

 private:
  LRUCache<uint32_t, Block> block_cache_;
  std::string file_name_;
  SuperBlock super_block_;
  FILE* f_;

  std::unique_ptr<Block> LoadBlock(uint32_t index) {
    std::unique_ptr<Block> new_block = std::unique_ptr<Block>(new Block(*this));
    ReadBlockFromFile(new_block.get(), index);
    new_block->Parse();
    return new_block;
  }
};
}  // namespace bptree