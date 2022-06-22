#include "bptree/file.h"

#include <new>
#include <vector>

#include "gtest/gtest.h"

TEST(file, all) {
  bptree::FileHandler fh = bptree::FileHandler::CreateFile("test.db");
  char* buf = new ((std::align_val_t)512) char[1024];
  buf[0] = 'h';
  buf[1] = 'w';
  fh.Write(buf, 1024, 0);
  char* buf2 = new ((std::align_val_t)512) char[1024];
  fh.Read(buf2, 1024, 0);
  EXPECT_EQ(buf2[0], 'h');
}