#pragma once

#include <memory>
#include <unordered_map>
#include <vector>
#include <cassert>
#include <functional>

#include "bptree/log.h"
#include "bptree/block.h"

namespace bptree {

class UnusedBlocks {
 public:
  UnusedBlocks() {

  }

  void Push(std::unique_ptr<Block>&& block) {
    assert(block->GetNextFreeIndex() != not_free_flag);
    uint32_t index = block->GetIndex();
    assert(blocks_.count(index) == 0);
    blocks_[index] = std::move(block);
  }

  std::unique_ptr<Block> Get(uint32_t index) {
    if (blocks_.count(index) == 0) {
      return nullptr;
    }
    auto result = std::move(blocks_[index]);
    blocks_.erase(index);
    return result;
  }

  std::vector<std::unique_ptr<Block>> GetAll() {
    std::vector<std::unique_ptr<Block>> result;
    for(auto& each : blocks_) {
      result.push_back(std::move(each.second));
    }
    blocks_.clear();
    return result;
  }

  void ForeachUnusedBlocks(const std::function<void(uint32_t index, Block& block)>& func) {
    for(auto& each : blocks_) {
      func(each.first, *each.second);
    }
  }

  ~UnusedBlocks() {
    if(blocks_.empty() == false) {
      BPTREE_LOG_ERROR("unused block should be flush disk!");
    }
  }

 private:
  std::unordered_map<uint32_t, std::unique_ptr<Block>> blocks_;
};

}