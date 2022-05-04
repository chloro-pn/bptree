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

#include "bptree/bitmap.h"

namespace bptree {

constexpr uint32_t block_size = 4 * 1024;

constexpr uint32_t super_height = std::numeric_limits<uint32_t>::max();

inline uint32_t StringToUInt32t(const std::string& value) {
  return static_cast<uint32_t>(atol(value.c_str()));
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

class BlockManager;

struct InsertInfo {
  enum class State { Ok, Split, Invalid };
  State state_;

  static InsertInfo Ok() {
    InsertInfo obj;
    obj.state_ = State::Ok;
    return obj;
  }

  static InsertInfo Split() {
    InsertInfo obj;
    obj.state_ = State::Split;
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

class Block : public BlockBase {
 public:
  friend class BlockManager;
  static void MoveHalfElementsFromTo(Block* block, Block* new_block);

  // 新建的block构造函数
  Block(BlockManager& manager, uint32_t index, uint32_t height, uint32_t key_size, uint32_t value_size) : 
    BlockBase(index, height), 
    manager_(manager), 
    prev_(0), 
    next_(0), 
    key_size_(key_size),
    value_size_(value_size),
    dirty_(true) {
    used_bytes_ = 7 * sizeof(uint32_t);
  }

  // 从文件中读取
  explicit Block(BlockManager& manager) : BlockBase(), manager_(manager), dirty_(false) {
    used_bytes_ = 0;
  }

  uint32_t GetHeight() const {
    return height_;
  }

  std::string GetMaxKey() const {
    assert(kvs_.empty() == false);
    return kvs_.back().first;
  }

  std::string Get(const std::string& key);

  InsertInfo Insert(const std::string& key, const std::string& value);

  DeleteInfo Delete(const std::string& key);

  void Print();

  bool FlushToBuf() override {
    if (dirty_ == false) {
      return false;
    }
    size_t offset = 0;
    offset = AppendToBuf(buf_, index_, offset);
    offset = AppendToBuf(buf_, height_, offset);
    offset = AppendToBuf(buf_, prev_, offset);
    offset = AppendToBuf(buf_, next_, offset);
    offset = AppendToBuf(buf_, key_size_, offset);
    offset = AppendToBuf(buf_, value_size_, offset);

    uint32_t kv_count = kvs_.size();
    offset = AppendToBuf(buf_, kv_count, offset);

    for(auto& each : kvs_) {
      offset = AppendStrToBuf(buf_, each.first, offset);
      offset = AppendStrToBuf(buf_, each.second, offset);
    }
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

    uint32_t kv_count = 0;
    offset = ::bptree::ParseFromBuf(buf_, kv_count, offset);
    used_bytes_ = 7 * sizeof(uint32_t);
    for(int i = 0; i < kv_count; ++i) {
      std::string key, value;
      offset = ParseStrFromBuf(buf_, key, offset);
      offset = ParseStrFromBuf(buf_, value, offset);
      used_bytes_ = used_bytes_ + key.size() + value.size();
      kvs_.push_back({std::move(key), std::move(value)});
    }
    dirty_ = false;
  }

 private:
  BlockManager& manager_;
  uint32_t prev_;
  uint32_t next_;
  std::vector<std::pair<std::string, std::string>> kvs_;
  uint32_t key_size_;
  uint32_t value_size_;
  uint32_t used_bytes_;
  bool dirty_;

  void SetPrev(uint32_t prev) {
    prev_ = prev;
    dirty_ = true;
  }

  void SetNext(uint32_t next) {
    next_ = next;
    dirty_ = true;
  }

  void InsertKv(const std::string& key, const std::string& value);

  void InsertKv(const std::pair<std::string, std::string>& kv) {
    InsertKv(kv.first, kv.second);
  }

  void PopBack();

  uint32_t GetChildIndex(size_t child_index) const {
    return StringToUInt32t(kvs_[child_index].second);
  }

  void RemoveByIndex(size_t child_index) {
    assert(kvs_.size() > child_index);
    used_bytes_ = used_bytes_ - (kvs_[child_index].first.size() + kvs_[child_index].second.size());
    auto it = kvs_.begin();
    std::advance(it, child_index);
    kvs_.erase(it);
    dirty_ = true;
  }

  void ReSort() {
    std::sort(kvs_.begin(), kvs_.end(), [](const std::pair<std::string, std::string>& n1, const std::pair<std::string, std::string>& n2) -> bool {
      return n1.first < n2.first;
    });
    dirty_ = true;
  }

  bool CheckIfOverflow() {
    // just for test
    return kvs_.size() > 4;
    //return used_bytes_ > block_size;
  }

  bool CheckIfNeedToMerge() {
    return kvs_.size() < 2;
    //return used_bytes_ * 2 < block_size;
  }

  bool CheckCanMerge(Block* b1, Block* b2) {
    return b1->kvs_.size() + b2->kvs_.size() <= 4;
    //return b1->used_bytes_ + b2->used_bytes_ <= block_size;
  }

  void UpdateBlockPrevIndex(uint32_t block_index, uint32_t prev);

  void MoveAllElementsTo(Block* other);

  void MoveFirstElementTo(Block* other);

  void MoveLastElementTo(Block* other);

  InsertInfo DoSplit(uint32_t child_index);

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