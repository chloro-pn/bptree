#include "bptree/block.h"

#include <algorithm>
#include <cassert>
#include <iostream>
#include <string>

#include "bptree/block_manager.h"

// todo
// super block的格式问题

namespace bptree {

// todo 使用二分查找优化
// 假设key + value占用100字节，一个block大约容纳40个kv对，
// 这个量级下遍历or二分差别不大。
std::string Block::Get(const std::string& key) {
  assert(height_ != super_height);
  if (height_ > 0) {
    for (size_t i = 0; i < kv_view_.size(); ++i) {
      if (kv_view_[i].key_view >= key) {
        uint32_t child_index = GetChildIndex(i);
        return manager_.LoadBlock(child_index)->Get(key);
      }
    }
    return "";
  } else {
    for (size_t i = 0; i < kv_view_.size(); ++i) {
      if (kv_view_[i].key_view == key) {
        uint32_t offset = GetOffsetByEntryIndex(kv_view_[i].index);
        return std::string(GetEntryValueView(offset));
      }
    }
  }
  return "";
}

InsertInfo Block::Insert(const std::string& key, const std::string& value) {
  assert(height_ != super_height);
  if (height_ > 0) {
    if (kv_view_.empty() == true) {
      uint32_t child_block_index = manager_.AllocNewBlock(height_ - 1);
      manager_.LoadBlock(child_block_index)->Insert(key, value);
      bool succ = InsertKv(key, ConstructIndexByNum(child_block_index));
      // 只插入一个元素，不应该失败
      assert(succ == true);
      return InsertInfo::Ok();
    }
    int32_t child_index = -1;
    for (size_t i = 0; i < kv_view_.size(); ++i) {
      if (kv_view_[i].key_view >= key) {
        child_index = i;
        break;
      }
    }
    if (child_index == -1) {
      child_index = kv_view_.size() - 1;
      UpdateEntryKey(kv_view_.back().index, key);
    }
    uint32_t child_block_index = GetChildIndex(child_index);
    InsertInfo info = manager_.LoadBlock(child_block_index)->Insert(key, value);
    if (info.state_ == InsertInfo::State::Ok) {
      return info;
    }
    return DoSplit(child_index, info.key_, info.value_);
  } else {
    auto it =
        std::find_if(kv_view_.begin(), kv_view_.end(),
                     [&](const Entry& n) -> bool { return n.key_view == key; });
    if (it != kv_view_.end()) {
      UpdateEntryValue(it->index, value);
      return InsertInfo::Ok();
    }
    bool succ = InsertKv(key, value);
    if (succ == false) {
      return InsertInfo::Split(key, value);
    } else {
      return InsertInfo::Ok();
    }
  }
  InsertInfo obj;
  obj.state_ = InsertInfo::State::Invalid;
  return obj;
}

DeleteInfo Block::Delete(const std::string& key) {
  assert(height_ != super_height);
  if (height_ > 0) {
    for (size_t i = 0; i < kv_view_.size(); ++i) {
      if (kv_view_[i].key_view >= key) {
        Block* block = manager_.LoadBlock(GetChildIndex(i));
        DeleteInfo info = block->Delete(key);
        assert(info.state_ != DeleteInfo::State::Invalid);
        if (kv_view_[i].key_view == key && block->kv_view_.size() != 0) {
          // 更新maxkey的记录，如果子节点block的kv为空不需要处理，因为后面会在DoMerge中删除这个节点
          UpdateEntryKey(kv_view_[i].index, block->GetMaxKey());
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
    for (auto it = kv_view_.begin(); it != kv_view_.end(); ++it) {
      if (it->key_view == key) {
        RemoveEntry(it->index, it == kv_view_.begin() ? 0 : (it - 1)->index);
        kv_view_.erase(it);
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

bool Block::InsertKv(const std::string_view& key,
                     const std::string_view& value) noexcept {
  // just for test
  if (kv_view_.size() >= 10) {
    return false;
  }
  uint32_t prev_index = std::numeric_limits<uint32_t>::max();
  for (size_t i = 0; i < kv_view_.size(); ++i) {
    if (kv_view_[i].key_view == key) {
      UpdateEntryValue(kv_view_[i].index, std::string(value));
      return true;
    } else if (kv_view_[i].key_view > key) {
      break;
    } else {
      prev_index = i;
    }
  }
  bool full = false;
  Entry entry;
  if (prev_index == std::numeric_limits<uint32_t>::max()) {
    entry = InsertEntry(0, std::string(key), std::string(value), full);
  } else {
    entry = InsertEntry(kv_view_[prev_index].index, std::string(key),
                        std::string(value), full);
  }
  if (full == true) {
    return false;
  }
  // todo 优化
  kv_view_.push_back(entry);
  std::sort(kv_view_.begin(), kv_view_.end(),
            [](const Entry& n1, const Entry& n2) -> bool {
              return n1.key_view < n2.key_view;
            });
  return true;
}

void Block::UpdateBlockPrevIndex(uint32_t block_index, uint32_t prev) {
  Block* block = manager_.LoadBlock(block_index);
  block->SetPrev(prev);
}

void Block::UpdateBlockNextIndex(uint32_t block_index, uint32_t next) {
  Block* block = manager_.LoadBlock(block_index);
  block->SetNext(next);
}

void Block::Print() {
  std::cout << "block height " << GetHeight() << std::endl;
  if (next_free_index_ != not_free_flag) {
    std::cout << "free block " << std::endl;
    return;
  }
  std::cout << "prev and next " << prev_ << " " << next_ << std::endl;
  for (size_t i = 0; i < kv_view_.size(); ++i) {
    std::cout << i << " th kv : " << kv_view_[i].key_view << " "
              << kv_view_[i].value_view << std::endl;
  }
}

void Block::MoveFirstElementTo(Block* other) {
  assert(kv_view_.empty() == false);
  uint32_t move_index = kv_view_[0].index;
  other->InsertKv(kv_view_[0].key_view, kv_view_[0].value_view);
  RemoveEntry(move_index, 0);
  kv_view_.erase(kv_view_.begin());
}

void Block::MoveLastElementTo(Block* other) {
  assert(kv_view_.empty() == false);
  uint32_t size = kv_view_.size();
  uint32_t move_index = kv_view_.back().index;
  other->InsertKv(kv_view_.back().key_view, kv_view_.back().value_view);
  RemoveEntry(move_index, size == 1 ? 0 : kv_view_[size - 2].index);
  kv_view_.pop_back();
}

InsertInfo Block::DoSplit(uint32_t child_index, const std::string& key,
                          const std::string& value) {
  // 只有非叶子节点才会调用这里
  assert(height_ > 0);
  uint32_t block_index = GetChildIndex(child_index);
  Block* block = manager_.LoadBlock(block_index);
  auto new_blocks = manager_.BlockSplit(block);

  uint32_t new_block_1_index = new_blocks.first;
  uint32_t new_block_2_index = new_blocks.second;

  Block* new_block_1 = manager_.LoadBlock(new_block_1_index);
  Block* new_block_2 = manager_.LoadBlock(new_block_2_index);

  // update link
  uint32_t block_prev = block->prev_;
  uint32_t block_next = block->next_;
  new_block_1->SetPrev(block_prev);
  new_block_2->SetPrev(new_block_1_index);
  if (block_next != 0) {
    UpdateBlockPrevIndex(block_next, new_block_2_index);
  }
  new_block_1->SetNext(new_block_2_index);
  new_block_2->SetNext(block_next);
  if (block_prev != 0) {
    UpdateBlockNextIndex(block_prev, new_block_1_index);
  }
  // 删除过时节点
  manager_.DeallocBlock(block_index, false);
  // 将key和value插入到新的节点中
  if (key <= new_block_1->GetMaxKey()) {
    bool succ = new_block_1->InsertKv(key, value);
    assert(succ == true);
  } else {
    bool succ = new_block_2->InsertKv(key, value);
    assert(succ == true);
  }
  std::string block_1_max_key = new_block_1->GetMaxKey();
  std::string block_2_max_key = new_block_2->GetMaxKey();
  // 更新本节点的索引
  UpdateByIndex(child_index, block_1_max_key,
                ConstructIndexByNum(new_block_1_index));
  bool succ = InsertKv(block_2_max_key, ConstructIndexByNum(new_block_2_index));
  if (succ == false) {
    return InsertInfo::Split(block_2_max_key,
                             ConstructIndexByNum(new_block_2_index));
  }
  return InsertInfo::Ok();
}

DeleteInfo Block::DoMerge(uint32_t child_index) {
  assert(height_ > 0);
  uint32_t child_block_index = GetChildIndex(child_index);
  Block* child = manager_.LoadBlock(child_block_index);
  // 特殊情况，只有这个节点并且这个节点已经空了，则直接删除并返回继续merge
  if (child->kv_view_.size() == 0 && kv_view_.size() == 1) {
    assert(head_entry_ == kv_view_[0].index);
    RemoveEntry(kv_view_[0].index, 0);
    kv_view_.clear();
    manager_.DeallocBlock(child_block_index);
    return DeleteInfo::Merge();
  }
  // 分别向相邻节点借child过来，如果相邻节点都不能借，则合并节点。
  uint32_t left_child_index = 0;
  uint32_t right_child_index = 0;
  if (child_index > 0) {
    left_child_index = child_index - 1;
    right_child_index = child_index;
  } else if (child_index + 1 < kv_view_.size()) {
    left_child_index = child_index;
    right_child_index = child_index + 1;
  } else {
    // 只有一个子节点, 直接返回
    return DeleteInfo::Ok();
  }
  uint32_t left_block_index = GetChildIndex(left_child_index);
  uint32_t right_block_index = GetChildIndex(right_child_index);
  Block* left_child = manager_.LoadBlock(left_block_index);
  Block* right_child = manager_.LoadBlock(right_block_index);
  if (CheckCanMerge(left_child, right_child)) {
    uint32_t new_block_index = manager_.BlockMerge(left_child, right_child);
    Block* new_block = manager_.LoadBlock(new_block_index);
    // update link
    uint32_t prev_index = left_child->prev_;
    uint32_t next_index = right_child->next_;
    if (prev_index != 0) {
      UpdateBlockNextIndex(prev_index, new_block_index);
    }
    if (next_index != 0) {
      UpdateBlockPrevIndex(next_index, new_block_index);
    }
    // 删除过时节点
    UpdateByIndex(left_child_index, new_block->GetMaxKey(),
                  ConstructIndexByNum(new_block_index));
    DeleteKvByIndex(right_child_index);
    manager_.DeallocBlock(left_block_index);
    manager_.DeallocBlock(right_block_index);
  } else {
    // 不能合并，两个节点中的一个必然含有较多的项，rebalance一下即可。
    if (left_child->CheckIfNeedToMerge()) {
      right_child->MoveFirstElementTo(left_child);
    } else {
      left_child->MoveLastElementTo(right_child);
    }
    auto key_view = UpdateEntryKey(kv_view_[left_child_index].index,
                                   left_child->GetMaxKey());
    kv_view_[left_child_index].key_view = key_view;
  }

  // 判断经过删除后，本节点是否需要merge，并将判断情况交给父节点处理
  if (CheckIfNeedToMerge()) {
    return DeleteInfo::Merge();
  } else {
    return DeleteInfo::Ok();
  }
}
}  // namespace bptree