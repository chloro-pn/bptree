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
#include "bptree/log.h"
#include "bptree/util.h"

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
  explicit BlockManager(BlockManagerOption option)
      : block_cache_(1024, [this](const uint32_t key) -> std::unique_ptr<Block> { return this->LoadBlock(key); }),
        file_name_(option.file_name),
        super_block_(option.key_size, option.value_size) {
    block_cache_.SetFreeNotify([this](const uint32_t& key, Block& value) -> void {
      bool dirty = value.Flush();
      if (dirty == true) {
        this->FlushBlockToFile(this->f_, value.GetIndex(), &value);
      }
    });
    // 从文件中读取super_block_，填充root_index_。
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
      // root block
      auto root_block =
          std::unique_ptr<Block>(new Block(*this, super_block_.root_index_, 1, option.key_size, option.value_size));
      block_cache_.Get(super_block_.root_index_, std::move(root_block));
      f_ = fopen(file_name_.c_str(), "wb+");
      if (f_ == nullptr) {
        throw BptreeExecption("file " + file_name_ + " open fail");
      }
      BPTREE_LOG_INFO("create db {} succ", file_name_);
    } else {
      if (option.create == true) {
        throw BptreeExecption("file ", file_name_, " already exists");
      }
      f_ = fopen(file_name_.c_str(), "rb+");
      if (f_ == nullptr) {
        throw BptreeExecption("file " + file_name_ + " open fail");
      }
      ParseSuperBlockFromFile();
      BPTREE_LOG_INFO("open db {} succ", file_name_);
    }
  }

  ~BlockManager() { FlushToFile(); }

  typename LRUCache<uint32_t, Block>::Wrapper GetBlock(uint32_t index) { return block_cache_.Get(index); }

  uint32_t GetRootIndex() const noexcept { return super_block_.root_index_; }

  std::pair<uint32_t, uint32_t> BlockSplit(const Block* block) {
    uint32_t new_block_1_index = AllocNewBlock(block->GetHeight());
    uint32_t new_block_2_index = AllocNewBlock(block->GetHeight());
    auto new_block_1 = block_cache_.Get(new_block_1_index);
    auto new_block_2 = block_cache_.Get(new_block_2_index);
    size_t half_count = block->GetKVView().size() / 2;
    for (size_t i = 0; i < half_count; ++i) {
      bool succ = new_block_1.Get().InsertKv(block->GetViewByIndex(i).key_view, block->GetViewByIndex(i).value_view);
      if (succ == false) {
        throw BptreeExecption("block broken, insert nth kv : ", std::to_string(i));
      }
    }
    for (int i = half_count; i < block->GetKVView().size(); ++i) {
      bool succ = new_block_2.Get().InsertKv(block->GetViewByIndex(i).key_view, block->GetViewByIndex(i).value_view);
      if (succ == false) {
        throw BptreeExecption("block broken, insert nth kv : ", std::to_string(i));
      }
    }
    return {new_block_1_index, new_block_2_index};
  }

  uint32_t BlockMerge(const Block* b1, const Block* b2) {
    uint32_t new_block_index = AllocNewBlock(b1->GetHeight());
    auto new_block = block_cache_.Get(new_block_index);
    for (size_t i = 0; i < b1->GetKVView().size(); ++i) {
      bool succ = new_block.Get().InsertKv(b1->GetViewByIndex(i).key_view, b1->GetViewByIndex(i).value_view);
      if (succ == false) {
        throw BptreeExecption("block broken");
      }
    }
    for (size_t i = 0; i < b2->GetKVView().size(); ++i) {
      bool succ = new_block.Get().InsertKv(b2->GetViewByIndex(i).key_view, b2->GetViewByIndex(i).value_view);
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
    auto block = block_cache_.Get(super_block_.root_index_);
    std::string result = block.Get().Get(key);
    return result;
  }

  // 范围查找
  std::vector<std::pair<std::string, std::string>> GetRange(const std::string& key,
                                                            std::function<GetRangeOption(const Entry& entry)> functor) {
    if (key.size() != super_block_.key_size_) {
      throw BptreeExecption("wrong key length");
    }
    auto location = block_cache_.Get(super_block_.root_index_).Get().GetBlockIndexContainKey(key);
    if (location.first == 0) {
      return {};
    }
    std::vector<std::pair<std::string, std::string>> result;
    uint32_t block_index = location.first;
    uint32_t view_index = location.second;
    while (block_index != 0) {
      auto block = block_cache_.Get(block_index);
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

  void Insert(const std::string& key, const std::string& value) {
    if (key.size() != super_block_.key_size_ || value.size() != super_block_.value_size_) {
      throw BptreeExecption("wrong kv length");
    }
    InsertInfo info = block_cache_.Get(super_block_.root_index_).Get().Insert(key, value);
    if (info.state_ == InsertInfo::State::Split) {
      // 根节点的分裂
      uint32_t old_root_index = super_block_.root_index_;
      auto old_root = block_cache_.Get(super_block_.root_index_);
      uint32_t old_root_height = old_root.Get().GetHeight();
      auto new_blocks = BlockSplit(&old_root.Get());

      auto left_block = block_cache_.Get(new_blocks.first);
      auto right_block = block_cache_.Get(new_blocks.second);
      // update link
      left_block.Get().SetNext(new_blocks.second);
      right_block.Get().SetPrev(new_blocks.first);
      // insert
      if (info.key_ <= left_block.Get().GetMaxKey()) {
        left_block.Get().InsertKv(info.key_, info.value_);
      } else {
        right_block.Get().InsertKv(info.key_, info.value_);
      }
      // update root
      old_root.Get().Clear();
      old_root.Get().SetHeight(old_root_height + 1);
      old_root.Get().InsertKv(left_block.Get().GetMaxKey(), ConstructIndexByNum(new_blocks.first));
      old_root.Get().InsertKv(right_block.Get().GetMaxKey(), ConstructIndexByNum(new_blocks.second));
    }
    return;
  }

  void Delete(const std::string& key) {
    if (key.size() != super_block_.key_size_) {
      throw BptreeExecption("wrong key length");
    }
    // 根节点的merge信息不处理
    block_cache_.Get(super_block_.root_index_).Get().Delete(key);
  }

  // 申请一个新的Block
  uint32_t AllocNewBlock(uint32_t height) {
    uint32_t result = 0;
    if (super_block_.free_block_head_ == 0) {
      ++super_block_.current_max_block_index_;
      result = super_block_.current_max_block_index_;
    } else {
      {
        auto block = block_cache_.Get(super_block_.free_block_head_);
        super_block_.free_block_head_ = block.Get().GetNextFreeIndex();
        result = block.Get().GetIndex();
      }
      bool succ = block_cache_.Delete(result, false);
      assert(succ == true);
    }
    auto new_block =
        std::unique_ptr<Block>(new Block(*this, result, height, super_block_.key_size_, super_block_.value_size_));
    block_cache_.Get(result, std::move(new_block));
    return result;
  }

  void DeallocBlock(uint32_t index, bool update_link_relation = true) {
    auto block = block_cache_.Get(index);
    if (update_link_relation == true) {
      uint32_t next = block.Get().GetNext();
      uint32_t prev = block.Get().GetPrev();
      if (next != 0) {
        auto next_block = block_cache_.Get(next);
        next_block.Get().SetPrev(prev);
      }
      if (prev != 0) {
        auto prev_block = block_cache_.Get(prev);
        prev_block.Get().SetNext(next);
      }
    }
    block.Get().Clear();
    block.Get().SetNextFreeIndex(super_block_.free_block_head_);
    block.UnBind();
    super_block_.free_block_head_ = index;
    // cache中也要删除
    bool succ = block_cache_.Delete(index, true);
    assert(succ == true);
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
    bool succ = block_cache_.Clear();
    assert(succ == true);
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

  void PrintRootBlock() {
    auto root_block = block_cache_.Get(super_block_.root_index_);
    root_block.Get().Print();
  }

  void PrintBlockByIndex(uint32_t index) {
    if (index == 0) {
      throw BptreeExecption("should not print super block");
    }
    if (super_block_.current_max_block_index_ < index) {
      throw BptreeExecption("request block's index invalid : ", std::to_string(index));
    }
    auto block = block_cache_.Get(index);
    block.Get().Print();
  }

  void PrintCacheInfo() { block_cache_.PrintInfo(); }

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