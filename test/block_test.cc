#include "gtest/gtest.h"

#define private public
#define protected public

#include <string>
#include <string_view>

#include "bptree/block.h"
#include "bptree/block_manager.h"

TEST(block, helper) {
  uint32_t result = bptree::util::StringToUInt32t(std::string_view("12138"));
  EXPECT_EQ(result, 12138);
  // 由调用方保证输入的合法性，不合法则返回0
  result = bptree::util::StringToUInt32t(std::string_view("invalid"));
  EXPECT_EQ(result, 0);

  char buf[32];
  size_t offset = bptree::util::AppendToBuf(buf, uint32_t(12138), 0);
  EXPECT_EQ(offset, sizeof(uint32_t));
  EXPECT_EQ(uint32_t(12138), *reinterpret_cast<uint32_t*>(&buf));

  offset = bptree::util::AppendToBuf(buf, uint8_t(1), offset);
  EXPECT_EQ(offset, sizeof(uint32_t) + sizeof(uint8_t));
  EXPECT_EQ(uint8_t(1), *reinterpret_cast<uint8_t*>(&buf[sizeof(uint32_t)]));

  std::string key("bptree_test_key");
  offset = bptree::util::AppendStrToBuf(buf, key, offset);
  EXPECT_EQ(offset, sizeof(uint32_t) + sizeof(uint8_t) + key.size() + sizeof(uint32_t));
  EXPECT_EQ(uint32_t(key.size()), *reinterpret_cast<uint32_t*>(&buf[offset - key.size() - sizeof(uint32_t)]));
  EXPECT_EQ(key, std::string((const char*)&buf[offset - key.size()], key.size()));

  offset = 0;
  uint32_t num = 0;
  offset = bptree::util::ParseFromBuf(buf, num, offset);
  EXPECT_EQ(num, uint32_t(12138));
  EXPECT_EQ(offset, sizeof(uint32_t));

  offset += sizeof(uint8_t);
  uint32_t old_offset = offset;
  std::string new_key;
  offset = bptree::util::ParseStrFromBuf(buf, new_key, offset);
  EXPECT_EQ(key, new_key);
  EXPECT_EQ(offset, old_offset + sizeof(uint32_t) + key.size());
}

TEST(block, constructor) {
  bptree::BlockManagerOption option;
  option.db_name = "test_block";
  option.neflag = bptree::NotExistFlag::CREATE;
  option.eflag = bptree::ExistFlag::ERROR;
  option.mode = bptree::Mode::WR;
  option.key_size = 1;
  option.value_size = 5;
  bptree::BlockManager manager(option);
  bptree::Block block(manager, 2, 0, 1, 5);
  EXPECT_EQ(block.GetHeight(), 0);

  EXPECT_EQ(block.GetEntrySize(), sizeof(uint32_t) + 1 + 5);

  std::string value = block.Get("a");
  EXPECT_EQ(value, "");
  block.Insert("a", "value", bptree::no_wal_sequence);
  EXPECT_EQ("value", block.Get("a"));
  block.Insert("b", "value", bptree::no_wal_sequence);
  EXPECT_EQ(block.GetMaxKey(), "b");
  block.Delete("a", bptree::no_wal_sequence);
  EXPECT_EQ("", block.Get("a"));

  uint32_t offset = block.GetOffsetByEntryIndex(block.head_entry_);
  std::string_view key = block.GetEntryKeyView(offset);
  std::string_view value_v = block.GetEntryValueView(offset);
  uint32_t index = block.GetEntryNext(offset);
  EXPECT_EQ(key, "b");
  EXPECT_EQ(value_v, "value");
  EXPECT_EQ(index, 0);
  key = "";
  value_v = "";
  index = block.ParseEntry(block.head_entry_, key, value_v);
  EXPECT_EQ(key, "b");
  EXPECT_EQ(value_v, "value");
  EXPECT_EQ(index, 0);

  block.UpdateEntryKey(block.head_entry_, "c", bptree::no_wal_sequence);
  EXPECT_EQ(value_v, block.Get("c"));

  value_v = "vvvvv";
  block.UpdateEntryValue(block.head_entry_, std::string(value_v), bptree::no_wal_sequence);
  EXPECT_EQ(value_v, block.Get("c"));

  block.SetEntryKey(offset, std::string("d"), bptree::no_wal_sequence);
  block.SetEntryValue(offset, std::string("newvv"), bptree::no_wal_sequence);
  key = "";
  value_v = "";
  index = block.ParseEntry(block.head_entry_, key, value_v);
  EXPECT_EQ(key, "d");
  EXPECT_EQ(value_v, "newvv");
  EXPECT_EQ(index, 0);

  bool full = false;
  auto entry =
      block.InsertEntry(block.head_entry_, std::string("e"), std::string("value"), full, bptree::no_wal_sequence);
  EXPECT_EQ(full, false);
  EXPECT_EQ(block.GetEntryNext(offset), entry.index);
  EXPECT_EQ(block.GetEntryNext(block.GetOffsetByEntryIndex(entry.index)), 0);
  EXPECT_EQ(entry.key_view, "e");
  EXPECT_EQ(entry.value_view, "value");

  block.UpdateByIndex(0, "f", "ffval", bptree::no_wal_sequence);
  offset = block.GetOffsetByEntryIndex(block.head_entry_);
  EXPECT_EQ(block.GetEntryKeyView(offset), "f");
  EXPECT_EQ(block.GetEntryValueView(offset), "ffval");

  block.RemoveEntry(block.head_entry_, 0, bptree::no_wal_sequence);
  block.RemoveEntry(block.head_entry_, 0, bptree::no_wal_sequence);
  EXPECT_EQ(block.head_entry_, 0);
  EXPECT_EQ(block.free_list_, 1);
  block.SetClean();

  bptree::Block block2(manager, 3, 0, 1, 5);
  block2.Insert("a", "valua", bptree::no_wal_sequence);
  EXPECT_EQ(block2.SearchKey("b"), 1);
  EXPECT_EQ(block2.SearchKey("a"), 0);
  block2.Insert("c", "valuc", bptree::no_wal_sequence);
  block2.Insert("b", "valub", bptree::no_wal_sequence);
  EXPECT_EQ(block2.SearchKey("b"), 1);
  EXPECT_EQ(block2.SearchTheFirstGEKey("b"), 1);
  block2.Insert("e", "value", bptree::no_wal_sequence);
  EXPECT_EQ(block2.SearchTheFirstGEKey("d"), 3);
  block2.SetClean();
}