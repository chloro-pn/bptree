#include "gtest/gtest.h"

#include <vector>

#define private public
#include "bptree/cache.h"

TEST(cache, all) {
  std::vector<uint32_t> load_vec;
  std::vector<std::pair<uint32_t, uint32_t>> free_vec;
  bptree::LRUCache<uint32_t, uint32_t> cache(uint32_t(3), [&load_vec](const uint32_t& key) -> std::unique_ptr<uint32_t> {
    load_vec.push_back(key);
    return std::unique_ptr<uint32_t>(new uint32_t(key));
  });

  cache.SetFreeNotify([&free_vec](const uint32_t& key, const uint32_t& value) -> void {
    free_vec.push_back({key, value});
    return;
  });
  // 0
  uint32_t v = cache.Get(0);
  EXPECT_EQ(v, 0);
  // 2 -> 0
  cache.Get(2);
  // 4 -> 2 -> 0
  cache.Get(4);
  // 2 -> 4 -> 0
  cache.Get(2);
  // 3 -> 2 -> 4
  cache.Get(3);
  // 5 -> 3 -> 2
  cache.Get(5);
  // 2 -> 5 -> 3
  cache.Get(2);
  {
    auto expect = std::vector<uint32_t>{0, 2, 4, 3, 5};
    EXPECT_EQ(load_vec, expect);
  }
  {
    auto expect = std::vector<std::pair<uint32_t, uint32_t>>{{0, 0}, {4, 4}};
    EXPECT_EQ(free_vec, expect);
  }
  {
    auto expect = std::list<uint32_t>{2, 5, 3};
    EXPECT_EQ(expect, cache.lru_list_);
  }
}