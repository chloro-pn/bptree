#pragma once

#include <cstdint>
#include <cstring>
#include <vector>
#include <cassert>
#include <limits>
#include <algorithm>
#include <string>
#include <new>
#include <type_traits>
#include <string_view>

#include "bptree/bitmap.h"

namespace bptree {

constexpr uint32_t block_size = 4 * 1024;

constexpr uint32_t super_height = std::numeric_limits<uint32_t>::max();

constexpr uint32_t max_index_length = 10;

// helper function

inline uint32_t StringToUInt32t(const std::string& value) {
  return static_cast<uint32_t>(atol(value.c_str()));
}

inline uint32_t StringToUInt32t(const std::string_view& value) {
  return static_cast<uint32_t>(atol(value.data()));
}

inline std::string ConstructIndexByNum(uint32_t n) {
  std::string result = std::to_string(n);
  assert(result.size() <= 10);
  if (result.size() < 10) {
    std::string tmp;
    for(int i = 0; i < (10 - result.size()); ++i) {
      tmp.push_back('0');
    }
    return tmp + result;
  }
  return result;
}

template <size_t n, typename T, typename std::enable_if<!std::is_same<T, std::string>::value, int>::type = 0>
inline size_t AppendToBuf(uint8_t(&buf)[n], const T& t, size_t start_point) {
  memcpy((void*)&buf[start_point], &t, sizeof(T));
  start_point += sizeof(T);
  assert(start_point <= n);
  return start_point;
}

template <size_t n>
inline size_t AppendStrToBuf(uint8_t(&buf)[n], const std::string& str, size_t start_point) {
  uint32_t len = str.size();
  memcpy((void*)&buf[start_point], &len, sizeof(len));
  start_point += sizeof(len);
  memcpy((void*)&buf[start_point], str.data(), len);
  start_point += len;
  assert(start_point <= n);
  return start_point;
}

template <size_t n, typename T, typename std::enable_if<!std::is_same<T, std::string>::value, int>::type = 0>
inline size_t ParseFromBuf(uint8_t(&buf)[n], T& t, size_t start_point) {
  memcpy(&t, &buf[start_point], sizeof(T));
  start_point += sizeof(T);
  assert(start_point <= n);
  return start_point;
}

template <size_t n>
inline size_t ParseStrFromBuf(uint8_t(&buf)[n], std::string& t, size_t start_point) {
  uint32_t len = 0;
  memcpy(&len, &buf[start_point], sizeof(len));
  start_point += sizeof(len);
  t.clear();
  if (len == 0) {
    return start_point;
  }
  char* ptr = new (std::nothrow) char[len];
  assert(ptr != nullptr);
  memcpy(ptr, &buf[start_point], len);
  start_point += len;
  assert(start_point <= n);
  t.append(ptr, len);
  delete[] ptr;
  return start_point;
}

// end helper function

class BlockManager;

struct InsertInfo {
  enum class State { Ok, Split, Invalid };
  State state_;
  std::string key_;
  std::string value_;

  static InsertInfo Ok() {
    InsertInfo obj;
    obj.state_ = State::Ok;
    return obj;
  }

  static InsertInfo Split(const std::string& key, const std::string& value) {
    InsertInfo obj;
    obj.state_ = State::Split;
    obj.key_ = key;
    obj.value_ = value;
    return obj;
  }
};

struct DeleteInfo {
  enum class State { Ok, Merge, Invalid };
  State state_;

  static DeleteInfo Ok() {
    DeleteInfo obj;
    obj.state_ = State::Ok;
    return obj;
  }

  static DeleteInfo Merge() {
    DeleteInfo obj;
    obj.state_ = State::Merge;
    return obj;
  }
};

class BlockBase {
 public:
  friend class BlockManager;
  BlockBase() : index_(0), height_(0), buf_init_(false) {}
  BlockBase(uint32_t index, uint32_t height) : index_(index), height_(height), buf_init_(false) {}

  virtual bool FlushToBuf() = 0;
  virtual void ParseFromBuf() = 0;

  void BufInit() {
    assert(buf_init_ == false);
    buf_init_ = true;
  }

  bool BufInited() const {
    return buf_init_ == true;
  }

 protected:
  uint8_t buf_[block_size];
  uint32_t index_;
  uint32_t height_;
 
 private:
  bool buf_init_;
};

struct Entry {
 std::string_view key_view;
 std::string_view value_view;
 uint32_t index;
};

class Block : public BlockBase {
 public:
  friend class BlockManager;

  // 新建的block构造函数
  Block(BlockManager& manager, uint32_t index, uint32_t height, uint32_t key_size, uint32_t value_size) : 
    BlockBase(index, height), 
    manager_(manager), 
    prev_(0), 
    next_(0), 
    key_size_(key_size),
    value_size_(value_size),
    head_entry_(0),
    free_list_(1) {
      if (height_ != 0) {
        value_size_ = max_index_length;
      }
      InitEmptyEntrys();
  }

  // 从文件中读取
  explicit Block(BlockManager& manager) : BlockBase(), manager_(manager) {
    
  }

  uint32_t GetHeight() const {
    return height_;
  }

  std::string GetMaxKey() const {
    assert(kv_view_.empty() == false);
    return std::string(kv_view_.back().key_view);
  }

  std::string Get(const std::string& key);

  InsertInfo Insert(const std::string& key, const std::string& value);

  DeleteInfo Delete(const std::string& key);

  void Print();

  bool FlushToBuf() override {
    size_t offset = 0;
    offset = AppendToBuf(buf_, index_, offset);
    offset = AppendToBuf(buf_, height_, offset);
    offset = AppendToBuf(buf_, prev_, offset);
    offset = AppendToBuf(buf_, next_, offset);
    offset = AppendToBuf(buf_, key_size_, offset);
    offset = AppendToBuf(buf_, value_size_, offset);
    offset = AppendToBuf(buf_, head_entry_, offset);
    offset = AppendToBuf(buf_, free_list_, offset);
    assert(offset == 32);
    
    return true;
  }

  void ParseFromBuf() override {
  assert(BufInited() == true);
  size_t offset = 0;
  offset = ::bptree::ParseFromBuf(buf_, index_, offset);
  offset = ::bptree::ParseFromBuf(buf_, height_, offset);
  offset = ::bptree::ParseFromBuf(buf_, prev_, offset);
  offset = ::bptree::ParseFromBuf(buf_, next_, offset);
  offset = ::bptree::ParseFromBuf(buf_, key_size_, offset);
  offset = ::bptree::ParseFromBuf(buf_, value_size_, offset);
  offset = ::bptree::ParseFromBuf(buf_, head_entry_, offset);
  offset = ::bptree::ParseFromBuf(buf_, free_list_, offset);

  uint32_t entry_index = head_entry_;
  while(entry_index != 0) {
    Entry entry;
    entry.index = entry_index;
    entry_index = ParseEntry(entry_index, entry.key_view, entry.value_view);
    kv_view_.push_back(entry);
  }
}

 private:
  BlockManager& manager_;
  uint32_t prev_;
  uint32_t next_;
  uint32_t key_size_;
  uint32_t value_size_;
  uint32_t free_list_;
  uint32_t head_entry_;

  std::vector<Entry> kv_view_;

  void SetPrev(uint32_t prev) {
    prev_ = prev;
  }

  void SetNext(uint32_t next) {
    next_ = next;
  }

  // 索引值从1开始计数
  uint32_t GetOffsetByEntryIndex(uint32_t index) {
    assert(index > 0);
    uint32_t result = 8 * sizeof(uint32_t) + (index - 1) * GetEntrySize();
    return result;
  }

  // 解析索引值为index的entry，并返回下一个entry的索引值 or 0 （0 代表本entry为最后一个）
  uint32_t ParseEntry(uint32_t index, std::string_view& key, std::string_view& value) {
    assert(index != 0);
    uint32_t offset = GetOffsetByEntryIndex(index);
    uint32_t next = GetEntryNext(offset);
    key = GetEntryKeyView(offset);
    value = GetEntryValueView(offset);
    return next;
  }

  void InitEmptyEntrys() {
    uint32_t free_index = 1;
    uint32_t offset = GetOffsetByEntryIndex(free_index);
    while (offset + GetEntrySize() <= block_size) {
      SetEntryNext(free_index, free_index + 1);
      free_index += 1;
      offset += GetEntrySize();
    }
    assert(free_index >= 1);
    // 最后一个entry的next改为0，标志结尾。
    SetEntryNext(free_index - 1, 0);
  }

  void SetEntryNext(uint32_t index, uint32_t next) {
    uint32_t offset = GetOffsetByEntryIndex(index);
    assert(offset + sizeof(next) <= block_size);
    memcpy(&buf_[offset], &next, sizeof(next));
  }

  uint32_t GetEntryNext(uint32_t offset) const {
    uint32_t result = 0;
    memcpy(&result, &buf_[offset], sizeof(result));
    return result;
  }

  std::string_view SetEntryKey(uint32_t offset, const std::string& key) {
    assert(key.size() == key_size_);
    memcpy(&buf_[offset + sizeof(uint32_t)], key.data(), key_size_);
    return std::string_view((const char*)&buf_[offset + sizeof(uint32_t)], static_cast<size_t>(key_size_));
  }

  std::string_view GetEntryKeyView(uint32_t offset) const {
    return std::string_view((const char*)&buf_[offset + sizeof(uint32_t)], static_cast<size_t>(key_size_));
  }

  std::string_view SetEntryValue(uint32_t offset, const std::string& value) {
    assert(value.size() == value_size_);
    memcpy(&buf_[offset + sizeof(uint32_t) + key_size_], value.data(), value_size_);
    return std::string_view((const char*)&buf_[offset + sizeof(uint32_t) + key_size_], static_cast<size_t>(value_size_));
  }

  std::string_view GetEntryValueView(uint32_t offset) const {
    return std::string_view((const char*)&buf_[offset + sizeof(uint32_t) + key_size_], static_cast<size_t>(value_size_));
  }

  uint32_t GetEntrySize() const {
    return sizeof(uint32_t) + key_size_ + value_size_;
  }

  void RemoveEntry(uint32_t index, uint32_t prev_index) {
    uint32_t offset = GetOffsetByEntryIndex(index);
    uint32_t next = GetEntryNext(offset);
    if (prev_index != 0) {
      uint32_t prev_offset = GetOffsetByEntryIndex(prev_index);
      assert(GetEntryNext(prev_offset) == index);
      SetEntryNext(prev_index, next);
    } else {
      head_entry_ = next;
    }
    // 将index的entry加入freelist中
    SetEntryNext(index, free_list_);
    free_list_ = index;
  }

  Entry InsertEntry(uint32_t prev_index, const std::string& key, const std::string& value, bool& full) {
    if (free_list_ == 0) {
      full = true;
      return Entry();
    }
    uint32_t new_index = free_list_;
    uint32_t new_offset = GetOffsetByEntryIndex(free_list_);
    free_list_ = GetEntryNext(new_offset);
    if (prev_index == 0) {
      // 插入到头部的情况
      SetEntryNext(new_index, head_entry_);
      head_entry_ = new_index;
    } else {
      uint32_t prev_offset = GetOffsetByEntryIndex(prev_index);
      uint32_t prev_next = GetEntryNext(prev_offset);
      SetEntryNext(prev_index, new_index);
      SetEntryNext(new_index, prev_next);
    }
    Entry entry;
    entry.index = new_index;
    entry.key_view = SetEntryKey(new_offset, key);
    entry.value_view = SetEntryValue(new_offset, value);
    return entry;
  }

  std::string_view UpdateEntryKey(uint32_t index, const std::string& key) {
    uint32_t offset = GetOffsetByEntryIndex(index);
    SetEntryKey(offset, key);
    return GetEntryKeyView(offset);
  }

  std::string_view UpdateEntryValue(uint32_t index, const std::string& value) {
    uint32_t offset = GetOffsetByEntryIndex(index);
    SetEntryValue(offset, value);
    return GetEntryValueView(offset);
  }

  bool InsertKv(const std::string_view& key, const std::string_view& value);

  bool InsertKv(const std::string& key, const std::string& value) {
    InsertKv(std::string_view(key), std::string_view(value));
  }

  bool InsertKv(const std::pair<std::string, std::string>& kv) {
    InsertKv(kv.first, kv.second);
  }

  void DeleteKvByIndex(uint32_t index) {
    assert(index < kv_view_.size());
    uint32_t block_index = kv_view_[index].index;
    if (index == 0) {
      RemoveEntry(block_index, 0);
    } else {
      RemoveEntry(block_index, kv_view_[index - 1].index);
    }
    auto it = kv_view_.begin();
    std::advance(it, index);
    kv_view_.erase(it);
  }

  void Clear() {
    head_entry_ = 0;
    free_list_ = 1;
    InitEmptyEntrys();
    kv_view_.clear();
  }

  uint32_t GetChildIndex(size_t child_index) const {
    return StringToUInt32t(kv_view_[child_index].value_view);
  }

  void RemoveByIndex(size_t child_index) {
    assert(kv_view_.size() > child_index);
    uint32_t remove_index = kv_view_[child_index].index;
    if (child_index == 0) {
      RemoveEntry(remove_index, 0);
    } else {
      RemoveEntry(remove_index, kv_view_[child_index - 1].index);
    }
    auto it = kv_view_.begin();
    std::advance(it, child_index);
    kv_view_.erase(it);
  }

  void UpdateByIndex(size_t child_index, const std::string& key, const std::string& value) {
    assert(kv_view_.size() > child_index);
    uint32_t update_index = kv_view_[child_index].index;
    auto key_view = UpdateEntryKey(update_index, key);
    auto value_view = UpdateEntryValue(update_index, value);
    kv_view_[child_index].key_view = key_view;
    kv_view_[child_index].value_view = value_view;
  }

  bool CheckIfNeedToMerge() {
    return kv_view_.size() < 5;
    //return used_bytes_ * 2 < block_size;
  }

  bool CheckCanMerge(Block* b1, Block* b2) {
    return b1->kv_view_.size() + b2->kv_view_.size() <= 10;
    //return b1->used_bytes_ + b2->used_bytes_ <= block_size;
  }

  void UpdateBlockPrevIndex(uint32_t block_index, uint32_t prev);

  void UpdateBlockNextIndex(uint32_t block_index, uint32_t next);

  void MoveFirstElementTo(Block* other);

  void MoveLastElementTo(Block* other);

  InsertInfo DoSplit(uint32_t child_index, const std::string& key, const std::string& value);

  DeleteInfo DoMerge(uint32_t child_index);
};

class SuperBlock : public BlockBase {
 public:
  SuperBlock(uint32_t key_size, uint32_t value_size) : BlockBase(0, super_height), root_index_(0), key_size_(key_size), value_size_(value_size) {

  }

  bool FlushToBuf() override {
    uint32_t offset = 0;
    offset = AppendToBuf(buf_, index_, offset);
    offset = AppendToBuf(buf_, height_, offset);
    offset = AppendToBuf(buf_, root_index_, offset);
    offset = AppendToBuf(buf_, key_size_, offset);
    offset = AppendToBuf(buf_, value_size_, offset);
    std::string bit_map_str((const char*)bit_map_.ptr(), bit_map_.len());
    AppendStrToBuf(buf_, bit_map_str, offset);
  }

  void ParseFromBuf() override {
    assert(BufInited() == true);
    uint32_t offset = 0;
    offset = ::bptree::ParseFromBuf(buf_, index_, offset);
    offset = ::bptree::ParseFromBuf(buf_, height_, offset);
    offset = ::bptree::ParseFromBuf(buf_, root_index_, offset);
    offset = ::bptree::ParseFromBuf(buf_, key_size_, offset);
    offset = ::bptree::ParseFromBuf(buf_, value_size_, offset);
    std::string bit_map_str;
    ParseStrFromBuf(buf_, bit_map_str, offset);
    bit_map_.Init((uint8_t*)bit_map_str.data(), bit_map_str.size());
  }

  uint32_t root_index_;
  uint32_t key_size_;
  uint32_t value_size_;
  Bitmap bit_map_;
};

}