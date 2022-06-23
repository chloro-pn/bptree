#include "bptree/block_manager.h"

#include <thread>

#include "gtest/gtest.h"

TEST(block_manager, base) {
  spdlog::set_level(spdlog::level::debug);
  bptree::BlockManagerOption option;
  option.db_name = "test_base";
  option.neflag = bptree::NotExistFlag::CREATE;
  option.eflag = bptree::ExistFlag::ERROR;
  option.mode = bptree::Mode::WR;
  option.key_size = 1;
  option.value_size = 5;
  bptree::BlockManager manager(option);

  std::string value = manager.Get("a");
  EXPECT_EQ(value, std::string(""));

  bool succ = manager.Insert("a", "value");
  EXPECT_EQ(succ, true);
  succ = manager.Insert("b", "bbbbb");
  EXPECT_EQ(succ, true);
  succ = manager.Insert("a", "vvvvv");
  EXPECT_EQ(succ, false);
  value = manager.Get("c");
  EXPECT_EQ(std::string(""), value);
  EXPECT_EQ("value", manager.Get("a"));
  EXPECT_EQ("bbbbb", manager.Get("b"));

  std::string old_v = manager.Update("c", "valuc");
  EXPECT_EQ(old_v, "");

  std::string old_v2 = manager.Update("a", "valua");
  EXPECT_EQ(old_v2, "value");
  EXPECT_EQ("valua", manager.Get("a"));
}

TEST(block_manager, getrange) {
  bptree::BlockManagerOption option;
  option.db_name = "test_getrange";
  option.neflag = bptree::NotExistFlag::CREATE;
  option.eflag = bptree::ExistFlag::ERROR;
  option.mode = bptree::Mode::WR;
  option.key_size = 1;
  option.value_size = 5;
  bptree::BlockManager manager(option);
  for (int i = 0; i < 20; ++i) {
    std::string key;
    key.push_back('a' + i);
    manager.Insert(key, "value");
  }
  auto kvs = manager.GetRange("a", [](const bptree::Entry& entry) -> bptree::GetRangeOption {
    if (entry.key_view[0] % 5 == 0) {
      return bptree::GetRangeOption::SELECT;
    } else if (entry.key_view[0] > 'a' + 10) {
      return bptree::GetRangeOption::STOP;
    }
    return bptree::GetRangeOption::SKIP;
  });
  std::vector<std::pair<std::string, std::string>> expect_result = {{"d", "value"}, {"i", "value"}};
  EXPECT_EQ(expect_result, kvs);

  kvs = manager.GetRange(
      "a", [](const bptree::Entry& entry) -> bptree::GetRangeOption { return bptree::GetRangeOption::SKIP; });
  expect_result = {};
  EXPECT_EQ(expect_result, kvs);

  kvs = manager.GetRange(
      "c", [](const bptree::Entry& entry) -> bptree::GetRangeOption { return bptree::GetRangeOption::SELECT; });
  expect_result.clear();
  for (char c = 'c'; c < 'a' + 20; ++c) {
    std::string key;
    key.push_back(c);
    expect_result.push_back({key, "value"});
  }
  EXPECT_EQ(expect_result, kvs);
}