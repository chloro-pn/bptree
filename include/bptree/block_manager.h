#pragma once

#include "bptree/block.h"
#include "bptree/bitmap.h"
#include "bptree/util.h"

#include <unordered_map>
#include <string>
#include <memory>
#include <vector>
#include <cstdio>
#include <cassert>

#include <iostream>

namespace bptree {

constexpr uint32_t bit_map_size = 1024;

class BlockManager {
 public:
  BlockManager(const std::string& file_name, uint32_t key_size, uint32_t value_size) : 
    file_name_(file_name),
    super_block_(key_size, value_size) {
    // 从文件中读取super_block_，填充root_index_。
    if (util::FileNotExist(file_name)) {
      assert(key_size > 0 && value_size > 0);
      // 新建的b+树，初始化super block和其他信息即可
      super_block_.bit_map_.Init(bit_map_size);
      super_block_.root_index_ = 1;
      // super block
      super_block_.bit_map_.SetUse(0);
      // root block
      super_block_.bit_map_.SetUse(1);
      cache_[1] = std::unique_ptr<Block>(new Block(*this, super_block_.root_index_, 1, key_size, value_size));
      f_ = fopen(file_name_.c_str(), "wb+");
      assert(f_ != nullptr);
    } else {
      f_ = fopen(file_name.c_str(), "rb+");
      assert(f_ != nullptr);
      ParseSuperBlockFromFile();
      assert(super_block_.key_size_ == key_size && super_block_.value_size_ == value_size);
    }
  }

  std::string Get(const std::string& key) {
    if (key.size() != super_block_.key_size_) {
      return "";
    }
    return cache_[super_block_.root_index_]->Get(key);
  }

  bool Insert(const std::string& key, const std::string& value) {
    if (key.size() != super_block_.key_size_ || value.size() != super_block_.value_size_) {
      return false;
    }
    InsertInfo info = cache_[super_block_.root_index_]->Insert(key, value);
    if (info.state_ == InsertInfo::State::Split) {
      // 根节点的分裂
      uint32_t old_root_height = LoadBlock(super_block_.root_index_)->GetHeight();
      uint32_t new_block_index = AllocNewBlock(old_root_height);
      uint32_t old_root_index = super_block_.root_index_;
      Block* old_root = LoadBlock(super_block_.root_index_);
      Block* new_block = LoadBlock(new_block_index);
      Block::MoveHalfElementsFromTo(old_root, new_block);
      // update link
      old_root->SetNext(new_block_index);
      new_block->SetPrev(old_root_index);
      super_block_.root_index_ = AllocNewBlock(old_root_height + 1);
      Block* new_root = LoadBlock(super_block_.root_index_);
      new_root->InsertKv(old_root->GetMaxKey(), std::to_string(old_root_index));
      new_root->InsertKv(new_block->GetMaxKey(), std::to_string(new_block_index));
    }
    return true;
  }

  void Delete(const std::string& key) {
    if (key.size() != super_block_.key_size_) {
      return;
    }
    // 根节点的merge信息不处理
    cache_[super_block_.root_index_]->Delete(key);
  }

  Block* LoadBlock(uint32_t index) {
    if (cache_.count(index) == 0) {
      std::unique_ptr<Block> new_block = std::unique_ptr<Block>(new Block(*this));
      ReadBlockFromFile(new_block.get(), index);
      new_block->ParseFromBuf();
      cache_[index] = std::move(new_block);
    }
    return cache_[index].get();
  }

  // 申请一个新的Block
  uint32_t AllocNewBlock(uint32_t height) {
    uint32_t result = super_block_.bit_map_.GetFirstFreeAndSet();
    cache_[result] = std::unique_ptr<Block>(new Block(*this, result, height, super_block_.key_size_, super_block_.value_size_));
    return result;
  }

  void DeallocBlock(uint32_t index) {
    Block* block = LoadBlock(index);
    uint32_t next = block->next_;
    uint32_t prev = block->prev_;
    if (next != 0) {
      Block* next_block = LoadBlock(next);
      next_block->SetPrev(prev);
    }
    if (prev != 0) {
      Block* prev_block = LoadBlock(prev);
      prev_block->SetNext(next);
    }
    super_block_.bit_map_.SetFree(index);
    cache_.erase(index);
  }

  uint32_t GetFirstLeafBlockIndex() {
    uint32_t result = 0;
    Block* block = cache_[super_block_.root_index_].get();
    while(block->GetHeight() > 0) {
      if (block->kvs_.size() == 0) {
        return 0;
      }
      uint32_t first_child_index = block->GetChildIndex(0);
      block = LoadBlock(first_child_index);
      result = first_child_index;
    }
    return result;
  }

  void ReadBlockFromFile(BlockBase* block, uint32_t index) {
    block->BufInit();
    int ret = fseek(f_, index * block_size, SEEK_SET);
    assert(ret == 0);
    size_t read_ret = fread(block->buf_, block_size, 1, f_);
    assert(read_ret == 1);
  }

  void PrintBpTree() {
    for(auto& each : cache_) {
      std::cout << " index : " << each.first << std::endl;
      each.second->Print();
    }
  }

  void PrintLeafBlockIndexOrdered() {
    uint32_t first = GetFirstLeafBlockIndex();
    while(first != 0) {
      std::cout << first << " ";
      first = LoadBlock(first)->next_;
    }
    std::cout << std::endl;
  }

  void FlushToFile() {
    if (f_ == nullptr) {
      return;
    }
    FlushSuperBlockToFile(f_);
    for(auto& each : cache_) {
      bool dirty_block = each.second->FlushToBuf();
      if (dirty_block == true) {
        FlushBlockToFile(f_, each.first, each.second.get());
      }
    }
    fclose(f_);
    f_ = nullptr;
  }

  bool FlushBlockToFile(FILE* f, uint32_t index, BlockBase* block) {
    uint32_t offset = index * block_size;
    int ret = fseek(f, static_cast<long>(offset), SEEK_SET);
    assert(ret == 0);
    size_t n = fwrite(block->buf_, block_size, 1, f);
    return n == static_cast<size_t>(block_size);
  }

  // 重构
  void FlushSuperBlockToFile(FILE* f) {
    super_block_.FlushToBuf();
    FlushBlockToFile(f, 0, &super_block_);
  }

  void ParseSuperBlockFromFile() {
    ReadBlockFromFile(&super_block_, 0);
    super_block_.ParseFromBuf();
    LoadBlock(super_block_.root_index_);
  }

 private:
  std::unordered_map<uint32_t, std::unique_ptr<Block>> cache_;
  std::string file_name_;
  SuperBlock super_block_;
  FILE* f_;
};
}