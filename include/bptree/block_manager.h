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

constexpr uint32_t super_height = std::numeric_limits<uint32_t>::max();

class BlockManager {
 public:
  BlockManager(const std::string& file_name) : file_name_(file_name) {
    // 从文件中读取super_block_，填充root_index_。
    if (util::FileNotExist(file_name)) {
      // 新建的b+树，初始化super block和其他信息即可
      bit_map_.Init(bit_map_size);
      root_index_ = 1;
      // super block
      bit_map_.SetUse(0);
      // root block
      bit_map_.SetUse(root_index_);
      cache_[root_index_] = std::unique_ptr<Block>(new Block(*this, 1));
      super_block_.reset(new Block(*this, super_height));
      f_ = fopen(file_name_.c_str(), "wb+");
    } else {
      f_ = fopen(file_name.c_str(), "rb+");
      assert(f_ != nullptr);
      ParseSuperBlockFromFile();
    }
  }

  std::string Get(const std::string& key) {
    return cache_[root_index_]->Get(key);
  }

  void Insert(const std::string& key, const std::string& value) {
    InsertInfo info = cache_[root_index_]->Insert(key, value);
    if (info.state_ == InsertInfo::State::Split) {
      // 根节点的分裂
      uint32_t old_root_height = LoadBlock(root_index_)->GetHeight();
      uint32_t new_block_index = AllocNewBlock(old_root_height);
      uint32_t old_root_index = root_index_;
      Block* old_root = LoadBlock(root_index_);
      Block* new_block = LoadBlock(new_block_index);
      Block::MoveHalfElementsFromTo(old_root, new_block);
      // update link
      old_root->SetNext(new_block_index);
      new_block->SetPrev(old_root_index);
      root_index_ = AllocNewBlock(old_root_height + 1);
      Block* new_root = LoadBlock(root_index_);
      new_root->InsertKv(old_root->GetMaxKey(), std::to_string(old_root_index));
      new_root->InsertKv(new_block->GetMaxKey(), std::to_string(new_block_index));
    }
  }

  void Delete(const std::string& key) {
    // 根节点的merge信息不处理
    cache_[root_index_]->Delete(key);
  }

  Block* LoadBlock(uint32_t index) {
    if (cache_.count(index) == 0) {
      uint8_t buf[block_size];
      ReadBlockFromFile(buf, index);
      cache_[index] = std::unique_ptr<Block>(new Block(*this, buf));
    }
    return cache_[index].get();
  }

  // 申请一个新的Block
  uint32_t AllocNewBlock(uint32_t height) {
    uint32_t result = bit_map_.GetFirstFreeAndSet();
    cache_[result] = std::unique_ptr<Block>(new Block(*this, height));
    cache_[result]->height_ = height;
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
    bit_map_.SetFree(index);
    cache_.erase(index);
  }

  uint32_t GetFirstLeafBlockIndex() {
    uint32_t result = 0;
    Block* block = cache_[root_index_].get();
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

  void ReadBlockFromFile(uint8_t(&buf)[block_size], uint32_t index) {
    int ret = fseek(f_, index * block_size, SEEK_SET);
    assert(ret == 0);
    size_t read_ret = fread(buf, block_size, 1, f_);
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

  bool FlushBlockToFile(FILE* f, uint32_t index, Block* block) {
    uint32_t offset = index * block_size;
    int ret = fseek(f, static_cast<long>(offset), SEEK_SET);
    assert(ret == 0);
    size_t n = fwrite(block->buf_, block_size, 1, f);
    return n == static_cast<size_t>(block_size);
  }

  // 重构
  void FlushSuperBlockToFile(FILE* f) {
    // 将元信息flush到super_block_中，然后写入文件
    assert(super_block_ != nullptr);
    size_t offset = 0;
    super_block_->kvs_.clear();
    super_block_->InsertKv("root_index", std::to_string(root_index_));
    super_block_->InsertKv("bitmap", std::string((const char*)bit_map_.ptr(), bit_map_.len()));
    std::cout << "flush super, bitmap size == " << bit_map_.len() << std::endl;
    super_block_->FlushToBuf();
    FlushBlockToFile(f, 0, super_block_.get());
  }

  void ParseSuperBlockFromFile() {
    assert(super_block_ == nullptr);
    uint8_t buf[block_size];
    ReadBlockFromFile(buf, 0);
    super_block_ = std::unique_ptr<Block>(new Block(*this, buf));
    super_block_->Print();
    auto root_index_str = super_block_->Get("root_index");
    assert(root_index_str.empty() == false);
    root_index_ = StringToUInt32t(root_index_str);
    auto bit_map_str = super_block_->Get("bitmap");
    assert(bit_map_str.size() != 0);
    bit_map_.Init((uint8_t*)(bit_map_str.data()), bit_map_str.size());

    LoadBlock(root_index_);
  }

 private:
  std::unordered_map<uint32_t, std::unique_ptr<Block>> cache_;
  std::string file_name_;
  std::unique_ptr<Block> super_block_;
  uint32_t root_index_;
  // bitmap
  Bitmap bit_map_;
  FILE* f_;
};
}