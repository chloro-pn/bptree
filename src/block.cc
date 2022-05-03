#include "bptree/block.h"
#include "bptree/block_manager.h"

#include <string>
#include <algorithm>
#include <cassert>
#include <iostream>

// todo 
// super block的格式问题

namespace bptree {

void Block::MoveHalfElementsFromTo(Block* block, Block* new_block) {
  size_t split_num = block->kvs_.size() / 2;
  for(int i = 0; i < split_num; ++i) {
    size_t index = block->kvs_.size() - split_num + i;
    new_block->InsertKv(block->kvs_[index]);
  }
  for(int i = 0; i < split_num; ++i) {
    block->PopBack();
  }
}

std::string Block::Get(const std::string& key) {
  if (height_ > 0 && height_ != super_height) {
    for(int i = 0; i < kvs_.size(); ++i) {
      if (kvs_[i].first >= key) {
        uint32_t child_index = GetChildIndex(i);
        return manager_.LoadBlock(child_index)->Get(key);
      }
    }
    return "";
  } else {
    for(int i = 0; i < kvs_.size(); ++i) {
      if (kvs_[i].first == key) {
        return kvs_[i].second;
      }
    }
  }
  assert(height_ != super_height);
  return "";
}

InsertInfo Block::Insert(const std::string& key, const std::string& value) {
  if (height_ > 0 && height_ != super_height) {
    if (kvs_.empty() == true) {
      uint32_t child_block_index = manager_.AllocNewBlock(height_ - 1);
      manager_.LoadBlock(child_block_index)->Insert(key, value);
      InsertKv(key, std::to_string(child_block_index));
      return InsertInfo::Ok();
    }
    int32_t child_index = -1;
    for(int i = 0; i < kvs_.size(); ++i) {
      if (kvs_[i].first >= key) {
        child_index = i;
        break;
      }
    }
    if (child_index == -1) {
      child_index = kvs_.size() - 1;
      // 假设key的改变不会影响使用的存储空间的改变
      kvs_.back().first = key;
      dirty_ = true;
    }
    uint32_t child_block_index = GetChildIndex(child_index);
    InsertInfo info = manager_.LoadBlock(child_block_index)->Insert(key, value);
    if (info.state_ == InsertInfo::State::Ok) {
      return info;
    }
    return DoSplit(child_index);
  } else {
    auto it = std::find_if(kvs_.begin(), kvs_.end(), [&](const std::pair<std::string, std::string>& n) -> bool {
      return n.first == key;
    });
    if (it != kvs_.end()) {
      it->second = value;
      return InsertInfo::Ok();
    }
    InsertKv(key, value);
    ReSort();
    if (CheckIfOverflow()) {
      return InsertInfo::Split();
    } else {
      return InsertInfo::Ok();
    }
  }
  InsertInfo obj;
  obj.state_ = InsertInfo::State::Invalid;
  return obj;
}

DeleteInfo Block::Delete(const std::string& key) {
  if (height_ > 0 && height_ != super_height) {
    for(int i = 0; i < kvs_.size(); ++i) {
      if (kvs_[i].first >= key) {
        Block* block = manager_.LoadBlock(GetChildIndex(i));
        DeleteInfo info = block->Delete(key);
        assert(info.state_ != DeleteInfo::State::Invalid);
        if (kvs_[i].first == key && block->kvs_.size() != 0) {
          // 更新maxkey的记录，如果子节点block的kv为空不需要处理，因为后面会在DoMerge中删除这个节点
          kvs_[i].first = block->GetMaxKey();
          dirty_ = true;
        }
        if (info.state_ == DeleteInfo::State::Ok) {
          return info;
        } else {
          // do merge.
          return DoMerge(i);
        }
      }
    }
    // 不存在这个key
    return DeleteInfo::Ok();
  } else {
    for(auto it = kvs_.begin(); it != kvs_.end(); ++it) {
      if (it->first == key) {
        used_bytes_ = used_bytes_ - (2 * sizeof(uint32_t) + it->first.size() + it->second.size());
        dirty_ = true;
        kvs_.erase(it);
        break;
      }
    }
    if (CheckIfNeedToMerge() == true) {
      return DeleteInfo::Merge();
    } else {
      return DeleteInfo::Ok();
    }
  }
  DeleteInfo obj;
  obj.state_ = DeleteInfo::State::Invalid;
  return obj;
}

void Block::InsertKv(const std::string& key, const std::string& value) {
  kvs_.push_back({key, value});
  used_bytes_ = used_bytes_ + key.size() + value.size() + 2 * sizeof(uint32_t); // 分别记录key和value的长度
  dirty_ = true;
}

void Block::PopBack() {
  if (kvs_.empty() == true) {
    return;
  }
  auto& tmp = kvs_.back();
  uint32_t unused_size = (2 * sizeof(uint32_t) + tmp.first.size() + tmp.second.size());
  assert(used_bytes_ > unused_size);
  used_bytes_ -= unused_size;
  kvs_.pop_back();
  dirty_ = true;
}

void Block::UpdateBlockPrevIndex(uint32_t block_index, uint32_t prev) {
  Block* block = manager_.LoadBlock(block_index);
  block->SetPrev(prev);
}

void Block::Print() {
  std::cout << "block height " << GetHeight() << std::endl;
  std::cout << "prev and next " << prev_ << " " << next_ << std::endl;
  for(int i = 0; i < kvs_.size(); ++i) {
    std::cout << i << " th kv : " << kvs_[i].first << " " << kvs_[i].second << std::endl;
  }
}

void Block::MoveAllElementsTo(Block* other) {
  for(int i = 0; i < kvs_.size(); ++i) {
    other->InsertKv(kvs_[i]);
  }
  kvs_.clear();
  dirty_ = true;
  // 本节点即将被dealloc，因此可以不更新used_bytes_
}

void Block::MoveFirstElementTo(Block* other) {
  assert(kvs_.empty() == false);
  other->InsertKv(kvs_.front());
  auto it = kvs_.begin();
  dirty_ = true;
  used_bytes_ = used_bytes_ - (2 * sizeof(uint32_t) + it->first.size() + it->second.size());
  kvs_.erase(it);
}

void Block::MoveLastElementTo(Block* other) {
  assert(kvs_.empty() == false);
  auto other_begin = other->kvs_.begin();
  other->InsertKv(kvs_.back());
  other->ReSort();
  auto it = kvs_.end();
  dirty_ = true;
  used_bytes_ = used_bytes_ - (2 * sizeof(uint32_t) + it->first.size() + it->second.size());
  kvs_.pop_back();
}

InsertInfo Block::DoSplit(uint32_t child_index) {
  // 只有非叶子节点才会调用这里
  assert(height_ > 0);
  uint32_t block_index = GetChildIndex(child_index);
  Block* block = manager_.LoadBlock(block_index);
  uint32_t new_block_index = manager_.AllocNewBlock(block->GetHeight());
  Block* new_block = manager_.LoadBlock(new_block_index);
  // 从child_index代表的block中转移一半kv到new_block中，更新，并检测更新后的本节点是否需要继续split。
  MoveHalfElementsFromTo(block, new_block);
  // update link 
  // block -> (new_block ->) (block old next)
  // block <- (new_block <-) (block old next)
  uint32_t next_index = block->next_;
  block->SetNext(new_block_index);
  new_block->SetNext(next_index);
  new_block->SetPrev(block_index);
  if (next_index != 0) {
    UpdateBlockPrevIndex(next_index, new_block_index);
  }
  // update max key
  std::string block_max_key = block->GetMaxKey();
  std::string new_block_max_key = new_block->GetMaxKey();
  kvs_[child_index].first = block_max_key;
  InsertKv(new_block_max_key, std::to_string(new_block_index));
  // resort
  ReSort();
  // 检查由于child的split，本节点是否需要split
  if (CheckIfOverflow()) {
    return InsertInfo::Split();
  }
  return InsertInfo::Ok();
}

DeleteInfo Block::DoMerge(uint32_t child_index) {
  assert(height_ > 0);
  uint32_t child_block_index = GetChildIndex(child_index);
  Block* child = manager_.LoadBlock(child_block_index);
  // 特殊情况，只有这个节点并且这个节点已经空了，则直接删除并返回继续merge
  if (child->kvs_.size() == 0 && kvs_.size() == 1) {
    used_bytes_ = used_bytes_ - (2*sizeof(uint32_t) + kvs_[0].first.size() + kvs_[0].second.size());
    kvs_.clear();
    dirty_ = true;
    manager_.DeallocBlock(child_block_index);
    return DeleteInfo::Merge();
  }
  // 分别向相邻节点借child过来，如果相邻节点都不能借，则合并节点。
  uint32_t left_child_index = 0;
  uint32_t right_child_index = 0;
  if (child_index > 0) {
    left_child_index = child_index - 1;
    right_child_index = child_index;
  } else if (child_index + 1 < kvs_.size()) {
    left_child_index = child_index;
    right_child_index = child_index + 1;
  } else {
    // 只有一个子节点, 直接返回
    return DeleteInfo::Ok();
  }
  Block* left_child = manager_.LoadBlock(GetChildIndex(left_child_index));
  Block* right_child = manager_.LoadBlock(GetChildIndex(right_child_index));
  if (CheckCanMerge(left_child, right_child)) {
    right_child->MoveAllElementsTo(left_child);
    manager_.DeallocBlock(GetChildIndex(right_child_index));
    // kvs中删除right索引
    RemoveByIndex(right_child_index);
  } else {
    // 不能合并，两个节点中的一个必然含有较多的项，rebalance一下即可。
    if (left_child->CheckIfNeedToMerge()) {
      right_child->MoveFirstElementTo(left_child);
    } else {
      left_child->MoveLastElementTo(right_child);
    }
    kvs_[left_child_index].first = left_child->GetMaxKey();
    dirty_ = true;
  }

  // 判断经过删除后，本节点是否需要merge，并将判断情况交给父节点处理
  if (CheckIfNeedToMerge()) {
    return DeleteInfo::Merge();
  } else {
    return DeleteInfo::Ok();
  }
}
}