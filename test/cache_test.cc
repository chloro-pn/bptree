#include <vector>

#include "gtest/gtest.h"

#define private public
#include "bptree/cache.h"

TEST(cache, all) {
  spdlog::set_level(spdlog::level::debug);
  std::vector<std::pair<uint32_t, uint32_t>> free_vec;
  bptree::LRUCache<uint32_t, uint32_t> cache(uint32_t(3));

  cache.SetFreeNotify([&free_vec](const uint32_t& key, const uint32_t& value) -> void {
    free_vec.push_back({key, value});
    return;
  });
  // 0
  cache.Insert(0, std::unique_ptr<uint32_t>(new uint32_t(0)));
  // 2 -> 0
  cache.Insert(2, std::unique_ptr<uint32_t>(new uint32_t(2)));
  // 4 -> 2 -> 0
  cache.Insert(4, std::unique_ptr<uint32_t>(new uint32_t(4)));
  // 2 -> 4 -> 0
  cache.Get(2);
  // 3 -> 2 -> 4
  cache.Insert(3, std::unique_ptr<uint32_t>(new uint32_t(2)));
  // 5 -> 3 -> 2
  cache.Insert(5, std::unique_ptr<uint32_t>(new uint32_t(5)));
  {
    auto expect = std::vector<std::pair<uint32_t, uint32_t>>{{0, 0}, {4, 4}};
    EXPECT_EQ(free_vec, expect);
  }
  {
    auto expect = std::list<uint32_t>{5, 3, 2};
    EXPECT_EQ(expect, cache.lru_list_);
  }
}