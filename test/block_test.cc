#include "gtest/gtest.h"

#define private public
#define protected public

#include <string>

#include "bptree/block.h"
#include "bptree/block_manager.h"

TEST(block, helper) {
  uint32_t result = bptree::StringToUInt32t(std::string_view("12138"));
  EXPECT_EQ(result, 12138);
  // 由调用方保证输入的合法性，不合法则返回0
  result = bptree::StringToUInt32t(std::string_view("invalid"));
  EXPECT_EQ(result, 0);

  uint8_t buf[32];
  size_t offset = bptree::AppendToBuf(buf, uint32_t(12138), 0);
  EXPECT_EQ(offset, sizeof(uint32_t));
  EXPECT_EQ(uint32_t(12138), *reinterpret_cast<uint32_t*>(&buf));

  offset = bptree::AppendToBuf(buf, uint8_t(1), offset);
  EXPECT_EQ(offset, sizeof(uint32_t) + sizeof(uint8_t));
  EXPECT_EQ(uint8_t(1), *reinterpret_cast<uint8_t*>(&buf[sizeof(uint32_t)]));

  std::string key("bptree_test_key");
  offset = bptree::AppendStrToBuf(buf, key, offset);
  EXPECT_EQ(offset,
            sizeof(uint32_t) + sizeof(uint8_t) + key.size() + sizeof(uint32_t));
  EXPECT_EQ(uint32_t(key.size()),
            *reinterpret_cast<uint32_t*>(
                &buf[offset - key.size() - sizeof(uint32_t)]));
  EXPECT_EQ(key,
            std::string((const char*)&buf[offset - key.size()], key.size()));

  offset = 0;
  uint32_t num = 0;
  offset = bptree::ParseFromBuf(buf, num, offset);
  EXPECT_EQ(num, uint32_t(12138));
  EXPECT_EQ(offset, sizeof(uint32_t));

  offset += sizeof(uint8_t);
  uint32_t old_offset = offset;
  std::string new_key;
  offset = bptree::ParseStrFromBuf(buf, new_key, offset);
  EXPECT_EQ(key, new_key);
  EXPECT_EQ(offset, old_offset + sizeof(uint32_t) + key.size());
}

TEST(block, constructor) {
  bptree::BlockManager manager("test2.db", 1, 5);
  bptree::Block block(manager, 2, 0, 1, 5);
  EXPECT_EQ(block.GetHeight(), 0);

  EXPECT_EQ(block.GetEntrySize(), sizeof(uint32_t) + 1 + 5);

  std::string value = block.Get("a");
  EXPECT_EQ(value, "");
  block.Insert("a", "value");
  EXPECT_EQ("value", block.Get("a"));
  block.Insert("b", "value");
  EXPECT_EQ(block.GetMaxKey(), "b");
  block.Delete("a");
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

  block.UpdateEntryKey(block.head_entry_, "c");
  EXPECT_EQ(value_v, block.Get("c"));

  value_v = "vvvvv";
  block.UpdateEntryValue(block.head_entry_, std::string(value_v));
  EXPECT_EQ(value_v, block.Get("c"));

  block.SetEntryKey(offset, "d");
  block.SetEntryValue(offset, "newvv");
  key = "";
  value_v = "";
  index = block.ParseEntry(block.head_entry_, key, value_v);
  EXPECT_EQ(key, "d");
  EXPECT_EQ(value_v, "newvv");
  EXPECT_EQ(index, 0);

  bool full = false;
  auto entry = block.InsertEntry(block.head_entry_, "e", "value", full);
  EXPECT_EQ(full, false);
  EXPECT_EQ(block.GetEntryNext(offset), entry.index);
  EXPECT_EQ(block.GetEntryNext(block.GetOffsetByEntryIndex(entry.index)), 0);
  EXPECT_EQ(entry.key_view, "e");
  EXPECT_EQ(entry.value_view, "value");

  block.UpdateByIndex(0, "f", "ffval");
  offset = block.GetOffsetByEntryIndex(block.head_entry_);
  EXPECT_EQ(block.GetEntryKeyView(offset), "f");
  EXPECT_EQ(block.GetEntryValueView(offset), "ffval");

  block.RemoveEntry(block.head_entry_, 0);
  block.RemoveEntry(block.head_entry_, 0);
  EXPECT_EQ(block.head_entry_, 0);
  EXPECT_EQ(block.free_list_, 1);
}