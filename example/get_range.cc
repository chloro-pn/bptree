#include <string>

#include "bptree/block_manager.h"

int main() {
  bptree::BlockManager manager("test.db", 1, 5);
  for(int i = 0; i < 20; ++i) {
    std::string key;
    key.push_back('a' + i);
    manager.Insert(key, "value");
  }
  auto kvs = manager.GetRange("a", [](const bptree::Entry& entry) -> bptree::GetRangeOption {
    std::cout << entry.key_view[0] << std::endl;
    if (entry.key_view[0] % 5 == 0) {
      return bptree::GetRangeOption::SELECT;
    } else if (entry.key_view[0] > 'a' + 10) {
      return bptree::GetRangeOption::STOP;
    }
    return bptree::GetRangeOption::SKIP;
  });
  std::vector<std::pair<std::string, std::string>> expect_result = {{"d", "value"}, {"i", "value"}};
  std::cout << kvs.size() << std::endl;
  return 0;
}