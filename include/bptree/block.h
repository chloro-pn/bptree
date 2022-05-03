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

namespace bptree {

constexpr uint32_t block_size = 4 * 1024;

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

class Block {
 public:
  friend class BlockManager;
  static void MoveHalfElementsFromTo(Block* block, Block* new_block);

  // 新建的block构造函数
  Block(BlockManager& manager, uint32_t height) : manager_(manager), height_(height), prev_(0), next_(0), dirty_(true) {
    used_bytes_ = 4 * sizeof(uint32_t);
  }

  void SetPrev(uint32_t prev) {
    prev_ = prev;
    dirty_ = true;
  }

  void SetNext(uint32_t next) {
    next_ = next;
    dirty_ = true;
  }

  // 从文件中读取
  Block(BlockManager& manager, uint8_t(&buf)[block_size]) : manager_(manager), dirty_(false) {
    memcpy(buf_, buf, block_size);
    ParseFromBuf();
  }

  uint32_t GetHeight() const {
    return height_;
  }

  std::string Get(const std::string& key);

  std::string GetMaxKey() const {
    assert(kvs_.empty() == false);
    return kvs_.back().first;
  }

  InsertInfo Insert(const std::string& key, const std::string& value);

  DeleteInfo Delete(const std::string& key);

  void Print();

  bool FlushToBuf() {
    if (dirty_ == false) {
      return false;
    }
    size_t offset = 0;
    offset = AppendToBuf(buf_, height_, offset);
    offset = AppendToBuf(buf_, prev_, offset);
    offset = AppendToBuf(buf_, next_, offset);

    uint32_t kv_count = kvs_.size();
    offset = AppendToBuf(buf_, kv_count, offset);

    for(auto& each : kvs_) {
      offset = AppendStrToBuf(buf_, each.first, offset);
      offset = AppendStrToBuf(buf_, each.second, offset);
    }
    return true;
  }

  void ParseFromBuf() {
    used_bytes_ = 0;
    size_t offset = 0;
    offset = ::bptree::ParseFromBuf(buf_, height_, offset);
    offset = ::bptree::ParseFromBuf(buf_, prev_, offset);
    offset = ::bptree::ParseFromBuf(buf_, next_, offset);

    uint32_t kv_count = 0;
    offset = ::bptree::ParseFromBuf(buf_, kv_count, offset);
    used_bytes_ = 4 * sizeof(uint32_t);
    for(int i = 0; i < kv_count; ++i) {
      std::string key, value;
      offset = ParseStrFromBuf(buf_, key, offset);
      offset = ParseStrFromBuf(buf_, value, offset);
      used_bytes_ = used_bytes_ + 2 * sizeof(uint32_t) + key.size() + value.size();
      kvs_.push_back({std::move(key), std::move(value)});
    }
    dirty_ = false;
  }

 private:
  BlockManager& manager_;
  uint8_t buf_[block_size];
  uint32_t height_;
  uint32_t prev_;
  uint32_t next_;
  std::vector<std::pair<std::string, std::string>> kvs_;
  uint32_t used_bytes_;
  bool dirty_;

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
    used_bytes_ = used_bytes_ - (2*sizeof(uint32_t) + kvs_[child_index].first.size() + kvs_[child_index].second.size());
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

}