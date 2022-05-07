#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <limits>
#include <new>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "bptree/exception.h"
#include "crc32.h"

namespace bptree {

constexpr uint32_t block_size = 4 * 1024;

constexpr uint32_t super_height = std::numeric_limits<uint32_t>::max();

constexpr uint32_t not_free_flag = std::numeric_limits<uint32_t>::max();

// helper function

// tested
inline uint32_t StringToUInt32t(const std::string& value) noexcept {
  return static_cast<uint32_t>(atol(value.c_str()));
}

// tested
inline uint32_t StringToUInt32t(const std::string_view& value) noexcept {
  return static_cast<uint32_t>(atol(value.data()));
}

// tested
inline std::string ConstructIndexByNum(uint32_t n) noexcept {
  std::string result((const char*)&n, sizeof(uint32_t));
  return result;
}

// tested
template <size_t n, typename T,
          typename std::enable_if<!std::is_same<T, std::string>::value,
                                  int>::type = 0>
inline size_t AppendToBuf(uint8_t (&buf)[n], const T& t,
                          size_t start_point) noexcept {
  memcpy((void*)&buf[start_point], &t, sizeof(T));
  start_point += sizeof(T);
  assert(start_point <= n);
  return start_point;
}

// tested
template <size_t n>
inline size_t AppendStrToBuf(uint8_t (&buf)[n], const std::string& str,
                             size_t start_point) noexcept {
  uint32_t len = str.size();
  memcpy((void*)&buf[start_point], &len, sizeof(len));
  start_point += sizeof(len);
  memcpy((void*)&buf[start_point], str.data(), len);
  start_point += len;
  assert(start_point <= n);
  return start_point;
}

// tested
template <size_t n, typename T,
          typename std::enable_if<!std::is_same<T, std::string>::value,
                                  int>::type = 0>
inline size_t ParseFromBuf(uint8_t (&buf)[n], T& t,
                           size_t start_point) noexcept {
  memcpy(&t, &buf[start_point], sizeof(T));
  start_point += sizeof(T);
  assert(start_point <= n);
  return start_point;
}

// tested
template <size_t n>
inline size_t ParseStrFromBuf(uint8_t (&buf)[n], std::string& t,
                              size_t start_point) {
  uint32_t len = 0;
  memcpy(&len, &buf[start_point], sizeof(len));
  start_point += sizeof(len);
  t.clear();
  if (len == 0) {
    return start_point;
  }
  char* ptr = new char[len];
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

  static InsertInfo Ok() noexcept {
    InsertInfo obj;
    obj.state_ = State::Ok;
    return obj;
  }

  static InsertInfo Split(const std::string& key,
                          const std::string& value) noexcept {
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

  static DeleteInfo Ok() noexcept {
    DeleteInfo obj;
    obj.state_ = State::Ok;
    return obj;
  }

  static DeleteInfo Merge() noexcept {
    DeleteInfo obj;
    obj.state_ = State::Merge;
    return obj;
  }
};

class BlockBase {
 public:
  friend class BlockManager;
  BlockBase() noexcept : crc_(0), index_(0), height_(0), buf_init_(false) {}
  BlockBase(uint32_t index, uint32_t height) noexcept
      : crc_(0), index_(index), height_(height), buf_init_(false) {}

  void Parse() noexcept {
    assert(BufInited() == true);
    uint32_t offset = 0;
    offset = ::bptree::ParseFromBuf(buf_, crc_, offset);
    offset = ::bptree::ParseFromBuf(buf_, index_, offset);
    offset = ::bptree::ParseFromBuf(buf_, height_, offset);
    ParseFromBuf(offset);
    if (CheckForDamage() == true) {
      throw BptreeExecption("block damage");
    }
  }

  bool Flush() noexcept {
    uint32_t offset = 0;
    offset = AppendToBuf(buf_, crc_, offset);
    offset = AppendToBuf(buf_, index_, offset);
    offset = AppendToBuf(buf_, height_, offset);
    bool dirty = FlushToBuf(offset);
    // calculate crc and update.
    crc_ = crc32((const char*)&buf_[sizeof(crc_)], block_size - sizeof(crc_));
    AppendToBuf(buf_, crc_, 0);
    return true;
  }

  uint32_t GetUsedSpace() const noexcept {
    return sizeof(crc_) + sizeof(index_) + sizeof(height_);
  }

  virtual bool FlushToBuf(size_t offset) noexcept = 0;
  virtual void ParseFromBuf(size_t offset) noexcept = 0;

  void BufInit() noexcept {
    assert(buf_init_ == false);
    buf_init_ = true;
  }

  bool BufInited() const noexcept { return buf_init_ == true; }

  bool CheckForDamage() const noexcept {
    uint32_t crc =
        crc32((const char*)&buf_[sizeof(crc_)], block_size - sizeof(crc_));
    if (crc != crc_) {
      return true;
    }
    return false;
  }

 protected:
  uint8_t buf_[block_size];
  uint32_t crc_;
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
  friend class Iterator;

  // 新建的block构造函数
  Block(BlockManager& manager, uint32_t index, uint32_t height,
        uint32_t key_size, uint32_t value_size) noexcept
      : BlockBase(index, height),
        manager_(manager),
        next_free_index_(not_free_flag),
        prev_(0),
        next_(0),
        key_size_(key_size),
        value_size_(value_size),
        head_entry_(0),
        free_list_(1) {
    if (height_ != 0) {
      value_size_ = sizeof(uint32_t);
    }
    InitEmptyEntrys();
  }

  // 从文件中读取
  explicit Block(BlockManager& manager) noexcept
      : BlockBase(), manager_(manager) {}

  uint32_t GetNextFreeIndex() const noexcept {
    assert(next_free_index_ != not_free_flag);
    return next_free_index_;
  }

  void SetNextFreeIndex(uint32_t nfi) noexcept { next_free_index_ = nfi; }

  uint32_t GetHeight() const noexcept { return height_; }

  // tested
  std::string GetMaxKey() const noexcept {
    if (kv_view_.empty() == true) {
      return "";
    }
    return std::string(kv_view_.back().key_view);
  }

  // 查找含有key的leaf block的index以及view_index
  // 如果不存在，返回 {0, 0}
  std::pair<uint32_t, uint32_t> GetBlockIndexContainKey(const std::string& key);

  std::string Get(const std::string& key);

  InsertInfo Insert(const std::string& key, const std::string& value);

  DeleteInfo Delete(const std::string& key);

  void Print();

  bool FlushToBuf(size_t offset) noexcept override {
    offset = AppendToBuf(buf_, next_free_index_, offset);
    offset = AppendToBuf(buf_, prev_, offset);
    offset = AppendToBuf(buf_, next_, offset);
    offset = AppendToBuf(buf_, key_size_, offset);
    offset = AppendToBuf(buf_, value_size_, offset);
    offset = AppendToBuf(buf_, head_entry_, offset);
    offset = AppendToBuf(buf_, free_list_, offset);
    return true;
  }

  void ParseFromBuf(size_t offset) noexcept override {
    offset = ::bptree::ParseFromBuf(buf_, next_free_index_, offset);
    offset = ::bptree::ParseFromBuf(buf_, prev_, offset);
    offset = ::bptree::ParseFromBuf(buf_, next_, offset);
    offset = ::bptree::ParseFromBuf(buf_, key_size_, offset);
    offset = ::bptree::ParseFromBuf(buf_, value_size_, offset);
    offset = ::bptree::ParseFromBuf(buf_, head_entry_, offset);
    offset = ::bptree::ParseFromBuf(buf_, free_list_, offset);

    uint32_t entry_index = head_entry_;
    while (entry_index != 0) {
      Entry entry;
      entry.index = entry_index;
      entry_index = ParseEntry(entry_index, entry.key_view, entry.value_view);
      kv_view_.push_back(entry);
    }
  }

 private:
  BlockManager& manager_;
  uint32_t next_free_index_;
  uint32_t prev_;
  uint32_t next_;
  uint32_t key_size_;
  uint32_t value_size_;
  uint32_t free_list_;
  uint32_t head_entry_;

  std::vector<Entry> kv_view_;

  void SetPrev(uint32_t prev) noexcept { prev_ = prev; }

  void SetNext(uint32_t next) noexcept { next_ = next; }

  uint32_t GetMetaSpace() const noexcept {
    return BlockBase::GetUsedSpace() + sizeof(next_free_index_) +
           sizeof(prev_) + sizeof(next_) + sizeof(key_size_) +
           sizeof(value_size_) + sizeof(head_entry_) + sizeof(free_list_);
  }

  // 索引值从1开始计数
  // tested
  uint32_t GetOffsetByEntryIndex(uint32_t index) noexcept {
    assert(index > 0);
    uint32_t result = GetMetaSpace() + (index - 1) * GetEntrySize();
    return result;
  }

  // 解析索引值为index的entry，并返回下一个entry的索引值 or 0 （0
  // 代表本entry为最后一个）
  // tested
  uint32_t ParseEntry(uint32_t index, std::string_view& key,
                      std::string_view& value) noexcept {
    assert(index != 0);
    uint32_t offset = GetOffsetByEntryIndex(index);
    uint32_t next = GetEntryNext(offset);
    key = GetEntryKeyView(offset);
    value = GetEntryValueView(offset);
    return next;
  }

  void InitEmptyEntrys() noexcept {
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

  void SetEntryNext(uint32_t index, uint32_t next) noexcept {
    uint32_t offset = GetOffsetByEntryIndex(index);
    assert(offset + sizeof(next) <= block_size);
    memcpy(&buf_[offset], &next, sizeof(next));
  }

  // tested
  uint32_t GetEntryNext(uint32_t offset) const noexcept {
    uint32_t result = 0;
    memcpy(&result, &buf_[offset], sizeof(result));
    return result;
  }

  // tested
  std::string_view SetEntryKey(uint32_t offset,
                               const std::string& key) noexcept {
    assert(key.size() == key_size_);
    memcpy(&buf_[offset + sizeof(uint32_t)], key.data(), key_size_);
    return std::string_view((const char*)&buf_[offset + sizeof(uint32_t)],
                            static_cast<size_t>(key_size_));
  }

  // tested
  std::string_view GetEntryKeyView(uint32_t offset) const noexcept {
    return std::string_view((const char*)&buf_[offset + sizeof(uint32_t)],
                            static_cast<size_t>(key_size_));
  }

  // tested
  std::string_view SetEntryValue(uint32_t offset,
                                 const std::string& value) noexcept {
    assert(value.size() == value_size_);
    memcpy(&buf_[offset + sizeof(uint32_t) + key_size_], value.data(),
           value_size_);
    return std::string_view(
        (const char*)&buf_[offset + sizeof(uint32_t) + key_size_],
        static_cast<size_t>(value_size_));
  }

  // tested
  std::string_view GetEntryValueView(uint32_t offset) const noexcept {
    return std::string_view(
        (const char*)&buf_[offset + sizeof(uint32_t) + key_size_],
        static_cast<size_t>(value_size_));
  }

  // tested
  uint32_t GetEntrySize() const noexcept {
    return sizeof(uint32_t) + key_size_ + value_size_;
  }

  // tested
  void RemoveEntry(uint32_t index, uint32_t prev_index) noexcept {
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

  // tested
  Entry InsertEntry(uint32_t prev_index, const std::string& key,
                    const std::string& value, bool& full) noexcept {
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

  // note : 调用者需要保证key的有序性
  // tested
  std::string_view UpdateEntryKey(uint32_t index,
                                  const std::string& key) noexcept {
    uint32_t offset = GetOffsetByEntryIndex(index);
    return SetEntryKey(offset, key);
  }

  // tested
  std::string_view UpdateEntryValue(uint32_t index,
                                    const std::string& value) noexcept {
    uint32_t offset = GetOffsetByEntryIndex(index);
    return SetEntryValue(offset, value);
  }

  /*
   * 以下函数是对Entry系列函数的封装，保证kv_view_和buf内数据的一致性。
   */

  bool InsertKv(const std::string_view& key,
                const std::string_view& value) noexcept;

  bool InsertKv(const std::string& key, const std::string& value) noexcept {
    InsertKv(std::string_view(key), std::string_view(value));
  }

  bool InsertKv(const std::pair<std::string, std::string>& kv) noexcept {
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

  void Clear() noexcept {
    head_entry_ = 0;
    free_list_ = 1;
    InitEmptyEntrys();
    kv_view_.clear();
  }

  uint32_t GetChildIndex(size_t child_index) const noexcept {
    uint32_t result = 0;
    std::string_view index_view = kv_view_[child_index].value_view;
    memcpy(&result, index_view.data(), index_view.size());
    return result;
  }

  // note : 调用者需要保证更新后key的有序性
  // tested
  void UpdateByIndex(size_t child_index, const std::string& key,
                     const std::string& value) noexcept {
    assert(kv_view_.size() > child_index);
    uint32_t update_index = kv_view_[child_index].index;
    auto key_view = UpdateEntryKey(update_index, key);
    auto value_view = UpdateEntryValue(update_index, value);
    kv_view_[child_index].key_view = key_view;
    kv_view_[child_index].value_view = value_view;
  }

  /*
   * 以下函数涉及block的分裂和合并相关操作
   */

  bool CheckIfNeedToMerge() noexcept {
    return kv_view_.size() < 5;
    // return used_bytes_ * 2 < block_size;
  }

  bool CheckCanMerge(Block* b1, Block* b2) noexcept {
    return b1->kv_view_.size() + b2->kv_view_.size() <= 10;
    // return b1->used_bytes_ + b2->used_bytes_ <= block_size;
  }

  void UpdateBlockPrevIndex(uint32_t block_index, uint32_t prev) noexcept;

  void UpdateBlockNextIndex(uint32_t block_index, uint32_t next) noexcept;

  void MoveFirstElementTo(Block* other);

  void MoveLastElementTo(Block* other);

  InsertInfo DoSplit(uint32_t child_index, const std::string& key,
                     const std::string& value);

  DeleteInfo DoMerge(uint32_t child_index);
};

class SuperBlock : public BlockBase {
 public:
  SuperBlock(uint32_t key_size, uint32_t value_size) noexcept
      : BlockBase(0, super_height),
        root_index_(0),
        key_size_(key_size),
        value_size_(value_size),
        free_block_head_(0),
        current_max_block_index_(1) {}

  bool FlushToBuf(size_t offset) noexcept override {
    offset = AppendToBuf(buf_, root_index_, offset);
    offset = AppendToBuf(buf_, key_size_, offset);
    offset = AppendToBuf(buf_, value_size_, offset);
    offset = AppendToBuf(buf_, free_block_head_, offset);
    offset = AppendToBuf(buf_, current_max_block_index_, offset);
  }

  void ParseFromBuf(size_t offset) noexcept override {
    assert(BufInited() == true);
    offset = ::bptree::ParseFromBuf(buf_, root_index_, offset);
    offset = ::bptree::ParseFromBuf(buf_, key_size_, offset);
    offset = ::bptree::ParseFromBuf(buf_, value_size_, offset);
    offset = ::bptree::ParseFromBuf(buf_, free_block_head_, offset);
    offset = ::bptree::ParseFromBuf(buf_, current_max_block_index_, offset);
  }

  uint32_t root_index_;
  uint32_t key_size_;
  uint32_t value_size_;
  // free_block_head_ == 0意味着当前分配的所有block满了，需要分配新的block
  uint32_t free_block_head_;
  uint32_t current_max_block_index_;
};

}  // namespace bptree