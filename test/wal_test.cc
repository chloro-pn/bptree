#include "bptree/wal.h"

#include "crc32.h"
#include "gtest/gtest.h"

TEST(wal, all) {
  int a = 0;
  int b = 0;
  int c = 0;
  int d = 0;
  auto recover_func = [&](uint64_t seq, std::string msg) -> void {
    assert(msg.size() == 3 && msg[1] == '=');
    if (msg[0] == 'a') {
      a = msg[2] - '0';
    } else if (msg[0] == 'b') {
      b = msg[2] - '0';
    } else if (msg[0] == 'c') {
      c = msg[2] - '0';
    } else if (msg[0] == 'd') {
      d = msg[2] - '0';
    }
  };
  {
    bptree::WriteAheadLog wal("bptree_wal.log", recover_func);
    uint64_t tx_seq = wal.Begin();
    wal.WriteLog(tx_seq, "a=0");
    wal.WriteLog(tx_seq, "b=0");
    a = 3;
    b = 4;
    wal.End(tx_seq);
    tx_seq = wal.Begin();
    wal.WriteLog(tx_seq, "c=0");
    c = 5;
    // 这个时候模拟程序退出，d的wal日志和事务结束日志没有来得及写入
  }
  // 因为a和b的wal日志以及其事务结束日志写入成功，因此a和b的值不会被回滚
  // c和d的wal日志写入了一半，因此c写入的值会被回滚
  bptree::WriteAheadLog wal2("bptree_wal.log", recover_func);
  EXPECT_EQ(a, 3);
  EXPECT_EQ(b, 4);
  EXPECT_EQ(c, 0);
  EXPECT_EQ(d, 0);
}