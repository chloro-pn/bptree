#include <cstdlib>
#include <filesystem>
#include <string>

#include "bptree/block_manager.h"
#include "spdlog/spdlog.h"

std::string ConstructRandomStr(size_t size) {
  std::string result;
  for (size_t i = 0; i < size; ++i) {
    result.push_back('a' + rand() % 26);
  }
  return result;
}

std::vector<std::pair<std::string, std::string>> ConstructRandomKv(size_t size, size_t key_size, size_t value_size) {
  std::vector<std::pair<std::string, std::string>> result;
  for (size_t i = 0; i < size; ++i) {
    result.push_back({ConstructRandomStr(key_size), ConstructRandomStr(value_size)});
  }
  return result;
}

int main() {
  bptree::LogInit();
  BPTREE_LOG_DEBUG("create db : test.db and insert 10w kvs");
  bptree::BlockManagerOption option;
  option.file_name = "test.db";
  option.create = true;
  option.key_size = 10;
  option.value_size = 20;
  bptree::BlockManager manager(option);
  auto kvs = ConstructRandomKv(100000, 10, 20);
  for (auto& each : kvs) {
    manager.Insert(each.first, each.second);
  }
  BPTREE_LOG_INFO("insert complete");
  for (size_t i = 0; i < 100000; ++i) {
    auto v = manager.Get(kvs[i].first);
    if (v != kvs[i].second) {
      BPTREE_LOG_ERROR("insert-get check fail");
      return -1;
    }
  }
  BPTREE_LOG_INFO("insert-get check succ");

  BPTREE_LOG_INFO("print root block's info : ");
  manager.PrintRootBlock();

  BPTREE_LOG_INFO("print cache's info : ");
  manager.PrintCacheInfo();

  std::sort(kvs.begin(), kvs.end(),
            [](const std::pair<std::string, std::string>& n1, const std::pair<std::string, std::string>& n2) -> bool {
              return n1.first < n2.first;
            });

  BPTREE_LOG_INFO("random delete 10000 kv");
  for (int i = 0; i < 10000; ++i) {
    int delete_index = rand() % kvs.size();
    manager.Delete(kvs[delete_index].first);
    // slow operation
    auto it = kvs.begin();
    std::advance(it, delete_index);
    kvs.erase(it);
  }

  size_t count = 0;
  auto get_kvs = manager.GetRange(kvs[0].first, [&count](const bptree::Entry& entry) -> bptree::GetRangeOption {
    if (count == 1000) {
      return bptree::GetRangeOption::STOP;
    }
    ++count;
    return bptree::GetRangeOption::SELECT;
  });

  BPTREE_LOG_INFO("range get the first 1000 data, begin check");
  for (size_t i = 0; i < get_kvs.size(); ++i) {
    if (kvs[i].first != get_kvs[i].first || kvs[i].second != get_kvs[i].second) {
      BPTREE_LOG_INFO("range_get-check error, {} != {} or {} != {}", kvs[i].first, get_kvs[i].first, kvs[i].second,
                      get_kvs[i].second);
      return -1;
    }
  }
  BPTREE_LOG_INFO("all check succ");
  // std::filesystem::remove(std::filesystem::path("test.db"));
  return 0;
}