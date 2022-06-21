#include <vector>

#include "fmt/core.h"
#include "gtest/gtest.h"

TEST(fmt, all) {
  fmt::print("hello world");
  EXPECT_TRUE(true);
}