#include "bptree/block_manager.h"

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

  manager.Insert("a", "value");
  manager.Insert("b", "bbbbb");
  value = manager.Get("c");
  EXPECT_EQ(std::string(""), value);
  EXPECT_EQ("value", manager.Get("a"));
  EXPECT_EQ("bbbbb", manager.Get("b"));

  bool do_update = false;
  bool succ = manager.Update("c", [&do_update](char* const ptr, size_t len) { do_update = true; });
  EXPECT_EQ(do_update, false);
  EXPECT_EQ(succ, false);

  bool succ2 = manager.Update("a", [&do_update](char* const ptr, size_t len) {
    ptr[1] = 'a';
    ptr[len - 1] = 'a';
  });
  EXPECT_TRUE(succ2);
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
