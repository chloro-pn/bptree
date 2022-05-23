#include "bptree/key_comparator.h"

#include <string>
#include <string_view>

#include "gtest/gtest.h"

TEST(key_comparator, all) {
  bptree::Comparator cmp;
  EXPECT_TRUE(cmp.Compare(std::string_view("a"), std::string_view("b")) < 0);
  EXPECT_TRUE(cmp.Compare(std::string_view("a"), std::string_view("ab")) < 0);
  EXPECT_TRUE(cmp.Compare(std::string_view("a"), std::string_view("a")) == 0);
  EXPECT_TRUE(cmp.Compare(std::string_view("b"), std::string_view("a")) > 0);
}