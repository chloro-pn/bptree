#include <string>
#include <filesystem>
#include <vector>
#include "bptree/block_manager.h"

std::string ConstructRandomStr(size_t size) {
  std::string result;
  for(size_t i = 0; i <size; ++i) {
    result.push_back('a' + rand() % 26);
  }
  return result;
}

std::vector<std::pair<std::string, std::string>> ConstructRandomKv(size_t size, size_t key_size, size_t value_size) {
  std::vector<std::pair<std::string, std::string>> result;
  for(size_t i = 0; i < size; ++i) {
    result.push_back({ConstructRandomStr(key_size), ConstructRandomStr(value_size)});
  }
  return result;
}

int main() {
  bptree::BlockManagerOption option;
  option.file_name = "test.db";
  option.create = true;
  option.key_size = 10;
  option.value_size = 20;
  bptree::BlockManager manager(option);
  auto kvs = ConstructRandomKv(100000, 10, 20);
  for(auto& each : kvs) {
    manager.Insert(each.first, each.second);
  }
  std::cout << "insert complete " << std::endl;
  for(size_t i = 0; i < 100000; ++i) {
    auto v = manager.Get(kvs[i].first);
    if (v != kvs[i].second) {
      std::cout << "check fail !" << std::endl;
      return -1;
    }
  }
  std::cout << "check succ !" << std::endl;

  std::cout << "print root block's info" << std::endl;
  manager.PrintRootBlock();

  std::cout << "print cache's info" << std::endl;
  manager.PrintCacheInfo();

  std::sort(kvs.begin(), kvs.end(), [](const std::pair<std::string, std::string>& n1, const std::pair<std::string, std::string>& n2) -> bool {
    return n1.first < n2.first;
  });
  size_t count = 0;
  auto get_kvs = manager.GetRange(kvs[0].first, [&count](const bptree::Entry& entry) -> bptree::GetRangeOption {
    if (count == 1000) {
      return bptree::GetRangeOption::STOP;
    }
    ++count;
    return bptree::GetRangeOption::SELECT;
  });
  std::cout << "range get the first 1000 data, begin check " << std::endl;
  for(int i = 0; i < 1000; ++i) {
    if (kvs[i].first != get_kvs[i].first || kvs[i].second != get_kvs[i].second) {
      std::cout << "check fail, " << kvs[i].first << " != " << get_kvs[i].first << " or " << kvs[i].second << " != " << get_kvs[i].second << std::endl;
      return -1;
    }
  }
  std::cout << "check succ!" << std::endl;
  //std::filesystem::remove(std::filesystem::path("test.db"));
  return 0;
}