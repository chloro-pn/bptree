#include "gtest/gtest.h"

#include "bptree/block_manager.h"

TEST(block_manager, all) {
  bptree::BlockManager manager("test.db", 1, 5);
  std::string value = manager.Get("a");
  EXPECT_EQ(value, "");
  manager.Insert("a", "value");
  value = manager.Get("a");
  EXPECT_EQ("value", value);

  manager.Delete("a");
  EXPECT_EQ("", manager.Get("a"));
}