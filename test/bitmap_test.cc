#include "gtest/gtest.h"
#include "bptree/bitmap.h"

TEST(bitmap, init) {
  bptree::Bitmap bitmap;
  bitmap.Init(1024);
  EXPECT_TRUE(bitmap.CheckFree(0));
  bitmap.SetUse(0);
  EXPECT_TRUE(!bitmap.CheckFree(0));
  uint32_t n = bitmap.GetFirstFreeAndSet();
  EXPECT_TRUE(n == 1);
  EXPECT_TRUE(!bitmap.CheckFree(1));
}