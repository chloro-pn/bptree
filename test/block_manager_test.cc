#include "bptree/block_manager.h"

#include "gtest/gtest.h"

TEST(block_manager, base) {
  bptree::BlockManagerOption option;
  option.file_name = "test.db";
  option.create = true;
  option.key_size = 1;
  option.value_size = 5;
  bptree::BlockManager manager(option);

  std::string value = manager.Get("a");
  EXPECT_EQ(value, std::string(""));

  manager.Insert("a", "value");
  manager.Insert("b", "bbbbb");
  value = manager.Get("c");
  EXPECT_EQ(std::string(""), value);
}

TEST(block_manager, getrange) {
  bptree::BlockManagerOption option;
  option.file_name = "test2.db";
  option.create = true;
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
