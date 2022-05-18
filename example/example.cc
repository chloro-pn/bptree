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

struct Entry {
  std::string key;
  std::string value;
  bool delete_;
};

std::vector<Entry> ConstructRandomKv(size_t size, size_t key_size, size_t value_size) {
  std::vector<Entry> result;
  for (size_t i = 0; i < size; ++i) {
    Entry entry;
    entry.key = ConstructRandomStr(key_size);
    entry.value = ConstructRandomStr(value_size);
    entry.delete_ = false;
    result.push_back(entry);
  }
  return result;
}

int main() {
  //spdlog::set_level(spdlog::level::debug);
  BPTREE_LOG_INFO("create db : test.db and insert 10w kvs");
  bptree::BlockManagerOption option;
  option.file_name = "test.db";
  option.create = true;
  option.key_size = 10;
  option.value_size = 20;
  bptree::BlockManager manager(option);
  auto kvs = ConstructRandomKv(100000, 10, 20);
  for (auto& each : kvs) {
    manager.Insert(each.key, each.value);
  }
  BPTREE_LOG_INFO("insert complete");
  for (size_t i = 0; i < 100000; ++i) {
    auto v = manager.Get(kvs[i].key);
    if (v != kvs[i].value) {
      BPTREE_LOG_ERROR("insert-get check fail");
      return -1;
    }
  }
  BPTREE_LOG_INFO("insert-get check succ");

  BPTREE_LOG_INFO("print root block's info : ");
  manager.PrintRootBlock();

  BPTREE_LOG_INFO("print cache's info : ");
  manager.PrintCacheInfo();

  std::sort(kvs.begin(), kvs.end(), [](const Entry& n1, const Entry& n2) -> bool { return n1.key < n2.key; });

  BPTREE_LOG_INFO("randomly delete 10000 kvs");
  for (int i = 0; i < 10000; ++i) {
    int delete_index = rand() % kvs.size();
    manager.Delete(kvs[delete_index].key);
    kvs[delete_index].delete_ = true;
  }

  std::vector<Entry> kvs_after_delete;
  for (auto& each : kvs) {
    if (each.delete_ == false) {
      kvs_after_delete.push_back(each);
    }
  }

  size_t count = 0;
  auto get_kvs =
      manager.GetRange(kvs_after_delete[0].key, [&count](const bptree::Entry& entry) -> bptree::GetRangeOption {
        if (count == 1000) {
          return bptree::GetRangeOption::STOP;
        }
        ++count;
        return bptree::GetRangeOption::SELECT;
      });

  BPTREE_LOG_INFO("range-get the first 1000 kvs and check them");
  for (size_t i = 0; i < get_kvs.size(); ++i) {
    if (kvs_after_delete[i].key != get_kvs[i].first || kvs_after_delete[i].value != get_kvs[i].second) {
      BPTREE_LOG_INFO("range-get check error, {} != {} or {} != {}", kvs_after_delete[i].key, get_kvs[i].first,
                      kvs_after_delete[i].value, get_kvs[i].second);
      return -1;
    }
  }

  BPTREE_LOG_INFO("all check succ");
  manager.PrintSuperBlockInfo();
  manager.PrintRootBlock();
  // std::filesystem::remove(std::filesystem::path("test.db"));
  return 0;
}