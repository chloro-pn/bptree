#pragma once

#include <algorithm>
#include <cassert>
#include <concepts>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <limits>
#include <new>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "bptree/exception.h"
#include "bptree/log.h"
#include "bptree/util.h"
#include "bptree/wal.h"
#include "crc32.h"

namespace bptree {

constexpr uint32_t block_size = 4 * 1024 * 4;

constexpr uint32_t super_height = std::numeric_limits<uint32_t>::max();

constexpr uint32_t not_free_flag = std::numeric_limits<uint32_t>::max();

constexpr uint32_t linux_alignment = 512;

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

  static InsertInfo Exist() noexcept {
    InsertInfo obj;
    obj.state_ = State::Invalid;
    return obj;
  }

  static InsertInfo Split(const std::string& key, const std::string& value) noexcept {
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
  std::string old_v_;

  static DeleteInfo Ok(const std::string& old_v) noexcept {
    DeleteInfo obj;
    obj.state_ = State::Ok;
    obj.old_v_ = old_v;
    return obj;
  }

  static DeleteInfo Merge(const std::string& old_v) noexcept {
    DeleteInfo obj;
    obj.state_ = State::Merge;
    obj.old_v_ = old_v;
    return obj;
  }

  static DeleteInfo Invalid() noexcept {
    DeleteInfo obj;
    obj.state_ = State::Invalid;
    return obj;
  }
};

struct UpdateInfo {
  enum class State { Ok, Invalid };
  State state_;
  std::string old_v_;

  static UpdateInfo Ok(const std::string& old_v) noexcept {
    UpdateInfo obj;
    obj.old_v_ = old_v;
    obj.state_ = State::Ok;
    return obj;
  }

  static UpdateInfo Invalid() noexcept {
    UpdateInfo obj;
    obj.state_ = State::Invalid;
    return obj;
  }
};

class BlockBase {
 public:
  /**
   * @brief BlockBase构造函数，新建一个从磁盘导入的block时调用，index和height等在之后parse时解析得到
   * @param manager BlockManager引用
   * @param buf 存储着磁盘数据的buf，大小需要至少为block_size
   */
  BlockBase(BlockManager& manager, char* buf) noexcept
      : manager_(manager),
        buf_(buf),
        dirty_(true),
        need_to_parse_(true),
        crc_(0),
        index_(0),
        height_(0),
        change_log_number_(0) {}

  /**
   * @brief BlockBase构造函数，新建一个空的block时调用
   * @param manager BlockManager引用
   * @param index 该block的index值
   * @param height 该block的高度
   */
  BlockBase(BlockManager& manager, uint32_t index, uint32_t height) noexcept
      : manager_(manager),
        buf_(nullptr),
        dirty_(true),
        need_to_parse_(false),
        crc_(0),
        index_(index),
        height_(height),
        change_log_number_(0) {
    buf_ = new ((std::align_val_t)linux_alignment) char[block_size];
  }

  void NeedToParse() noexcept { need_to_parse_ = true; }

  /**
   * @brief 从磁盘导入的block当填充buf后，需要调用本函数解析元数据和kv数据
   */
  bool Parse() noexcept {
    // 只有从磁盘导入的block才能Parse
    assert(need_to_parse_ == true);
    uint32_t offset = 0;
    offset = ::bptree::util::ParseFromBuf(buf_, crc_, offset);
    if (CheckForDamage() == true) {
      return false;
    }
    offset = ::bptree::util::ParseFromBuf(buf_, index_, offset);
    offset = ::bptree::util::ParseFromBuf(buf_, height_, offset);
    ParseFromBuf(offset);
    BPTREE_LOG_DEBUG("block {} parse succ", index_);
    // 当Parse之后，磁盘数据和内存数据一致，dirty修改为false
    dirty_ = false;
    need_to_parse_ = false;
    return true;
  }

  /**
   * @brief 根据buf数据更新内存中的元数据，不检测crc
   */
  void UpdateMeta() noexcept {
    uint32_t offset = 0;
    offset = ::bptree::util::ParseFromBuf(buf_, crc_, offset);
    offset = ::bptree::util::ParseFromBuf(buf_, index_, offset);
    offset = ::bptree::util::ParseFromBuf(buf_, height_, offset);
    UpdateMetaData(offset);
  }

  bool Flush(bool update_dirty_block_count = true) noexcept;

  uint32_t GetUsedSpace() const noexcept { return sizeof(crc_) + sizeof(index_) + sizeof(height_); }

  virtual void FlushToBuf(size_t offset) noexcept = 0;
  virtual void ParseFromBuf(size_t offset) noexcept = 0;
  virtual void UpdateMetaData(size_t offset) noexcept {}

  void UpdateLogNumber(uint64_t log_num) {
    if (change_log_number_ < log_num) {
      change_log_number_ = log_num;
    }
  }

  uint64_t GetLogNumber() const noexcept { return change_log_number_; }

  bool CheckForDamage() const noexcept {
    uint32_t crc = crc32((const char*)&buf_[sizeof(crc_)], block_size - sizeof(crc_));
    if (crc != crc_) {
      return true;
    }
    return false;
  }

  const char* GetBuf() const noexcept { return buf_; }

  char* GetBuf() noexcept { return buf_; }

  uint32_t GetIndex() const noexcept { return index_; }

  uint32_t GetHeight() const noexcept { return height_; }

  uint32_t& getHeight() noexcept { return height_; };

  void SetDirty(bool update_dirty_block_count = true);

  void SetClean() { dirty_ = false; }

  virtual ~BlockBase() {
    if (dirty_ == true) {
      std::cerr << "warn : block " << index_ << " destruct in dirty state, maybe throw exception or some inner error!"
                << std::endl;
    }
    delete[] buf_;
  }

 protected:
  BlockManager& manager_;
  char* buf_;
  // 标识从磁盘导入的数据是否被修改过，如果是新建的block，初始化为true。如果是从磁盘导入的block，初始化为false。
  bool dirty_;
  bool need_to_parse_;

 private:
  uint32_t crc_;
  uint32_t index_;
  uint32_t height_;
  uint32_t change_log_number_;
};

struct Entry {
  std::string_view key_view;
  std::string_view value_view;
  uint32_t index;
};

class Block : public BlockBase {
 public:
  // 新建一个空的block的构造函数
  Block(BlockManager& manager, uint32_t index, uint32_t height, uint32_t key_size, uint32_t value_size);

  // 新建一个从磁盘导入的block的构造函数
  explicit Block(BlockManager& manager, char* buf) noexcept : BlockBase(manager, buf) {}

  Block(const Block&) = delete;
  Block(Block&&) = delete;
  Block& operator=(const Block&) = delete;
  Block& operator=(Block&&) = delete;

  ~Block() {}

  uint32_t GetNextFreeIndex() const noexcept {
    assert(next_free_index_ != not_free_flag);
    return next_free_index_;
  }

  void SetNextFreeIndex(uint32_t nfi, uint64_t sequence) noexcept;

  // tested
  std::string GetMaxKey() const {
    if (kv_view_.empty() == true) {
      throw BptreeExecption("get max key from empty block {}", GetIndex());
    }
    return std::string(kv_view_.back().key_view);
  }

  // note: 只有在持有block的wrapper的时候才保证正确，否则可能导致结果指向资源已经被释放的地址
  std::string_view GetMaxKeyAsView() const {
    if (kv_view_.empty() == true) {
      throw BptreeExecption("get max key from empty block {}", GetIndex());
    }
    return kv_view_.back().key_view;
  }

  // 查找含有key的leaf block的index以及view_index
  // 如果不存在，返回 {0, 0}
  std::pair<uint32_t, uint32_t> GetBlockIndexContainKey(const std::string& key);

  std::string Get(const std::string& key);

  InsertInfo Insert(const std::string& key, const std::string& value, uint64_t sequence);

  DeleteInfo Delete(const std::string& key, uint64_t sequence);

  UpdateInfo Update(const std::string& key, const std::string& value, uint64_t sequence);

  void Print();

  void FlushToBuf(size_t offset) noexcept override {
    offset = util::AppendToBuf(buf_, next_free_index_, offset);
    offset = util::AppendToBuf(buf_, prev_, offset);
    offset = util::AppendToBuf(buf_, next_, offset);
    offset = util::AppendToBuf(buf_, key_size_, offset);
    offset = util::AppendToBuf(buf_, value_size_, offset);
    offset = util::AppendToBuf(buf_, head_entry_, offset);
    offset = util::AppendToBuf(buf_, free_list_, offset);
  }

  void ParseFromBuf(size_t offset) noexcept override {
    offset = ::bptree::util::ParseFromBuf(buf_, next_free_index_, offset);
    offset = ::bptree::util::ParseFromBuf(buf_, prev_, offset);
    offset = ::bptree::util::ParseFromBuf(buf_, next_, offset);
    offset = ::bptree::util::ParseFromBuf(buf_, key_size_, offset);
    offset = ::bptree::util::ParseFromBuf(buf_, value_size_, offset);
    offset = ::bptree::util::ParseFromBuf(buf_, head_entry_, offset);
    offset = ::bptree::util::ParseFromBuf(buf_, free_list_, offset);
    if (next_free_index_ == not_free_flag) {
      UpdateKvViewByBuf();
    }
  }

  void UpdateMetaData(size_t offset) noexcept override {
    offset = ::bptree::util::ParseFromBuf(buf_, next_free_index_, offset);
    offset = ::bptree::util::ParseFromBuf(buf_, prev_, offset);
    offset = ::bptree::util::ParseFromBuf(buf_, next_, offset);
    offset = ::bptree::util::ParseFromBuf(buf_, key_size_, offset);
    offset = ::bptree::util::ParseFromBuf(buf_, value_size_, offset);
    offset = ::bptree::util::ParseFromBuf(buf_, head_entry_, offset);
    offset = ::bptree::util::ParseFromBuf(buf_, free_list_, offset);
  }

  /**
   * @brief 根据buf中的数据更新kv_view_的数据
   */
  void UpdateKvViewByBuf() {
    kv_view_.clear();
    uint32_t entry_index = head_entry_;
    while (entry_index != 0) {
      Entry entry;
      entry.index = entry_index;
      entry_index = ParseEntry(entry_index, entry.key_view, entry.value_view);
      kv_view_.push_back(entry);
    }
  }

  void SetPrev(uint32_t prev, uint64_t sequence) noexcept;

  void SetNext(uint32_t next, uint64_t sequence) noexcept;

  void SetHeight(uint32_t height, uint64_t sequence) noexcept;

  void SetHeadEntry(uint32_t entry, uint64_t sequence) noexcept;

  void SetFreeList(uint32_t free_list, uint64_t sequence) noexcept;

  uint32_t GetPrev() const noexcept { return prev_; }

  uint32_t GetNext() const noexcept { return next_; }

  const std::vector<Entry>& GetKVView() const noexcept { return kv_view_; }

  const Entry& GetViewByIndex(size_t i) const noexcept { return kv_view_[i]; }

  std::string CreateMetaChangeWalLog(const std::string& meta_name, uint32_t value);

  std::string CreateDataChangeWalLog(uint32_t offset, const std::string& change_region);

  std::string CreateDataView();

 private:
  uint32_t next_free_index_;
  uint32_t prev_;
  uint32_t next_;
  uint32_t key_size_;
  uint32_t value_size_;
  uint32_t free_list_;
  uint32_t head_entry_;
  std::vector<Entry> kv_view_;

  uint32_t GetMetaSpace() const noexcept {
    return BlockBase::GetUsedSpace() + sizeof(next_free_index_) + sizeof(prev_) + sizeof(next_) + sizeof(key_size_) +
           sizeof(value_size_) + sizeof(head_entry_) + sizeof(free_list_);
  }

  /**
   * @brief 在kv_view_中查找key对应的元素下标
   * @param key 用户指定的key
   * @return
   *      - kv_view_.size() 查找失败
   *      - [0, kv_view_.size()) 查找结果在kv_view_中的下标
   */
  size_t SearchKey(const std::string_view& key) const;

  /**
   * @brief 在kv_view_中查找第一个key大于等于指定key的元素下标
   * @param key 用户指定的key
   * @return
   *      - kv_view_.size() 查找失败
   *      - [0, kv_view_.size()) 查找结果在kv_view_中的下标
   */
  size_t SearchTheFirstGEKey(const std::string_view& key) const;

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
  uint32_t ParseEntry(uint32_t index, std::string_view& key, std::string_view& value) noexcept {
    assert(index != 0);
    uint32_t offset = GetOffsetByEntryIndex(index);
    uint32_t next = GetEntryNext(offset);
    key = GetEntryKeyView(offset);
    value = GetEntryValueView(offset);
    return next;
  }

  // 对于这种操作，可以考虑将整个block记录为redo、undo日志内容，避免多个小改动占用太多空间
  void InitEmptyEntrys(uint64_t sequence) noexcept {
    uint32_t free_index = 1;
    uint32_t offset = GetOffsetByEntryIndex(free_index);
    while (offset + GetEntrySize() <= block_size) {
      SetEntryNext(free_index, free_index + 1, sequence);
      free_index += 1;
      offset += GetEntrySize();
    }
    assert(free_index >= 1);
    // 最后一个entry的next改为0，标志结尾。
    SetEntryNext(free_index - 1, 0, sequence);
  }

  void SetEntryNext(uint32_t index, uint32_t next, uint64_t sequence) noexcept;

  // tested
  uint32_t GetEntryNext(uint32_t offset) const noexcept {
    uint32_t result = 0;
    memcpy(&result, &buf_[offset], sizeof(result));
    return result;
  }

  std::string_view SetEntryKey(uint32_t offset, const std::string& key, uint64_t sequence) noexcept {
    return SetEntryKey(offset, std::string_view(key), sequence);
  }

  // tested
  std::string_view SetEntryKey(uint32_t offset, const std::string_view& key, uint64_t sequence) noexcept;

  // tested
  std::string_view GetEntryKeyView(uint32_t offset) const noexcept {
    return std::string_view((const char*)&buf_[offset + sizeof(uint32_t)], static_cast<size_t>(key_size_));
  }

  std::string_view SetEntryValue(uint32_t offset, const std::string& value, uint64_t sequence) noexcept {
    return SetEntryValue(offset, std::string_view(value), sequence);
  }

  // tested
  std::string_view SetEntryValue(uint32_t offset, const std::string_view& value, uint64_t sequence) noexcept;

  // tested
  std::string_view GetEntryValueView(uint32_t offset) const noexcept {
    return std::string_view((const char*)&buf_[offset + sizeof(uint32_t) + key_size_],
                            static_cast<size_t>(value_size_));
  }

  // tested
  uint32_t GetEntrySize() const noexcept { return sizeof(uint32_t) + key_size_ + value_size_; }

  // tested
  void RemoveEntry(uint32_t index, uint32_t prev_index, uint64_t sequence) noexcept {
    SetDirty();
    uint32_t offset = GetOffsetByEntryIndex(index);
    uint32_t next = GetEntryNext(offset);
    if (prev_index != 0) {
      uint32_t prev_offset = GetOffsetByEntryIndex(prev_index);
      assert(GetEntryNext(prev_offset) == index);
      SetEntryNext(prev_index, next, sequence);
    } else {
      SetHeadEntry(next, sequence);
    }
    // 将index的entry加入freelist中
    SetEntryNext(index, free_list_, sequence);
    SetFreeList(index, sequence);
  }

  Entry InsertEntry(uint32_t prev_index, const std::string& key, const std::string& value, bool& full,
                    uint64_t sequence) noexcept {
    return InsertEntry(prev_index, std::string_view(key), std::string_view(value), full, sequence);
  }

  // tested
  Entry InsertEntry(uint32_t prev_index, const std::string_view& key, const std::string_view& value, bool& full,
                    uint64_t sequence) noexcept {
    if (free_list_ == 0) {
      full = true;
      return Entry();
    }
    SetDirty();
    uint32_t new_index = free_list_;
    uint32_t new_offset = GetOffsetByEntryIndex(free_list_);
    SetFreeList(GetEntryNext(new_offset), sequence);
    if (prev_index == 0) {
      // 插入到头部的情况
      SetEntryNext(new_index, head_entry_, sequence);
      SetHeadEntry(new_index, sequence);
    } else {
      uint32_t prev_offset = GetOffsetByEntryIndex(prev_index);
      uint32_t prev_next = GetEntryNext(prev_offset);
      SetEntryNext(prev_index, new_index, sequence);
      SetEntryNext(new_index, prev_next, sequence);
    }
    Entry entry;
    entry.index = new_index;
    entry.key_view = SetEntryKey(new_offset, key, sequence);
    entry.value_view = SetEntryValue(new_offset, value, sequence);
    return entry;
  }

  // note : 调用者需要保证key的有序性
  // tested
  std::string_view UpdateEntryKey(uint32_t index, const std::string& key, uint64_t sequence) noexcept {
    uint32_t offset = GetOffsetByEntryIndex(index);
    return SetEntryKey(offset, key, sequence);
  }

  // tested
  std::string_view UpdateEntryValue(uint32_t index, const std::string& value, uint64_t sequence) noexcept {
    uint32_t offset = GetOffsetByEntryIndex(index);
    return SetEntryValue(offset, value, sequence);
  }

 public:
  enum class InsertResult {
    FULL,
    EXIST,
    SUCC,
  };

  InsertResult InsertKv(const std::string_view& key, const std::string_view& value, uint64_t sequence) noexcept;

  InsertResult InsertKv(const std::string& key, const std::string& value, uint64_t sequence) noexcept {
    return InsertKv(std::string_view(key), std::string_view(value), sequence);
  }

  InsertResult InsertKv(const std::pair<std::string, std::string>& kv, uint64_t sequence) noexcept {
    return InsertKv(kv.first, kv.second, sequence);
  }

  bool AppendKv(const std::string_view& key, const std::string_view& value, uint64_t sequence) noexcept;

  bool AppendKv(const std::string& key, const std::string& value, uint64_t sequence) noexcept {
    return AppendKv(std::string_view(key), std::string_view(value), sequence);
  }

  bool AppendKv(const std::pair<std::string, std::string>& kv, uint64_t sequence) noexcept {
    return AppendKv(kv.first, kv.second, sequence);
  }

  void DeleteKvByIndex(uint32_t index, uint64_t sequence) {
    assert(index < kv_view_.size());
    uint32_t block_index = kv_view_[index].index;
    if (index == 0) {
      RemoveEntry(block_index, 0, sequence);
    } else {
      RemoveEntry(block_index, kv_view_[index - 1].index, sequence);
    }
    auto it = kv_view_.begin();
    std::advance(it, index);
    kv_view_.erase(it);
  }

  void Clear(uint64_t sequence) noexcept {
    SetHeadEntry(0, sequence);
    SetFreeList(1, sequence);
    InitEmptyEntrys(sequence);
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
  void UpdateByIndex(size_t child_index, const std::string& key, const std::string& value, uint64_t sequence) noexcept {
    assert(kv_view_.size() > child_index);
    uint32_t update_index = kv_view_[child_index].index;
    auto key_view = UpdateEntryKey(update_index, key, sequence);
    auto value_view = UpdateEntryValue(update_index, value, sequence);
    kv_view_[child_index].key_view = key_view;
    kv_view_[child_index].value_view = value_view;
  }

  /*
   * 以下函数涉及block的分裂和合并相关操作
   */

  uint32_t GetMaxEntrySize() const { return (block_size - GetMetaSpace()) / GetEntrySize(); }

  bool CheckIfNeedToMerge() noexcept { return kv_view_.size() * 2 < GetMaxEntrySize(); }

  bool CheckCanMerge(Block* b1, Block* b2) noexcept {
    assert(b1->key_size_ == b2->key_size_ && b1->value_size_ == b2->value_size_);
    return b1->kv_view_.size() + b2->kv_view_.size() <= b1->GetMaxEntrySize();
  }

  void UpdateBlockPrevIndex(uint32_t block_index, uint32_t prev, uint64_t sequence);

  void UpdateBlockNextIndex(uint32_t block_index, uint32_t next, uint64_t sequence);

  void MoveFirstElementTo(Block* other, uint64_t sequence);

  void MoveLastElementTo(Block* other, uint64_t sequence);

  InsertInfo DoSplit(uint32_t child_index, const std::string& key, const std::string& value, uint64_t sequence);

  DeleteInfo DoMerge(uint32_t child_index, uint64_t sequence, const std::string& old_v);

  void HandleMetaUpdateWal(const std::string& meta_name, uint32_t value);

  void HandleDataUpdateWal(uint32_t offset, const std::string& region);

  void HandleViewWal(const std::string& view);
};

class SuperBlock : public BlockBase {
 public:
  friend class BlockManager;
  SuperBlock(BlockManager& manager, uint32_t key_size, uint32_t value_size) noexcept
      : BlockBase(manager, 0, super_height),
        root_index_(0),
        key_size_(key_size),
        value_size_(value_size),
        free_block_head_(0),
        free_block_size_(0),
        current_max_block_index_(1) {}

  void FlushToBuf(size_t offset) noexcept override {
    offset = util::AppendToBuf(buf_, root_index_, offset);
    offset = util::AppendToBuf(buf_, key_size_, offset);
    offset = util::AppendToBuf(buf_, value_size_, offset);
    offset = util::AppendToBuf(buf_, free_block_head_, offset);
    offset = util::AppendToBuf(buf_, free_block_size_, offset);
    offset = util::AppendToBuf(buf_, current_max_block_index_, offset);
  }

  void ParseFromBuf(size_t offset) noexcept override {
    offset = ::bptree::util::ParseFromBuf(buf_, root_index_, offset);
    offset = ::bptree::util::ParseFromBuf(buf_, key_size_, offset);
    offset = ::bptree::util::ParseFromBuf(buf_, value_size_, offset);
    offset = ::bptree::util::ParseFromBuf(buf_, free_block_head_, offset);
    offset = ::bptree::util::ParseFromBuf(buf_, free_block_size_, offset);
    offset = ::bptree::util::ParseFromBuf(buf_, current_max_block_index_, offset);
  }

  std::string CreateMetaChangeWalLog(const std::string& meta_name, uint32_t value);

  void SetCurrentMaxBlockIndex(uint32_t value, uint64_t sequence);

  void SetFreeBlockHead(uint32_t value, uint64_t sequence);

  void SetFreeBlockSize(uint32_t value, uint64_t sequence);

  void HandleWAL(const std::string& meta_name, uint32_t value) {
    if (meta_name == "current_max_block_index") {
      current_max_block_index_ = value;
    } else if (meta_name == "free_block_head") {
      free_block_head_ = value;
    } else if (meta_name == "free_block_size") {
      free_block_size_ = value;
    } else {
      throw BptreeExecption("invalid super meta name : {}", meta_name);
    }
  }

  uint32_t root_index_;
  uint32_t key_size_;
  uint32_t value_size_;
  // free_block_head_ == 0意味着当前分配的所有block满了，需要分配新的block
  uint32_t free_block_head_;
  uint32_t free_block_size_;
  uint32_t current_max_block_index_;
};

}  // namespace bptree