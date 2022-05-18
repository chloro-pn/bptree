#include "bptree/metric/metric.h"

#include "gtest/gtest.h"

TEST(metric, counter) {
  bptree::Counter counter("test");
  for (int i = 0; i < 100; ++i) {
    size_t count = rand() % 10000;
    for (size_t j = 0; j < count; ++j) {
      counter.Add();
    }
    EXPECT_EQ(counter.GetValue(), count);
    counter.Clear();
  }
  EXPECT_EQ(counter.GetMetricName(), "test");
}