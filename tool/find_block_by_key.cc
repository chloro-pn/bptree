#include <iostream>
#include <string>

#include "bptree/block_manager.h"

int main(int argc, char* argv[]) {
  if (argc != 3) {
    std::cerr << "usage : ./find_block_by_key {db name, type:string} {key, type:string}" << std::endl;
    return -1;
  }
  std::string name(argv[1]);
  std::string key(argv[2]);
  try {
    bptree::BlockManagerOption option;
    option.file_name = name;
    option.neflag = bptree::NotExistFlag::ERROR;
    option.eflag = bptree::ExistFlag::SUCC;
    option.mode = bptree::Mode::R;
    bptree::BlockManager manager(option);
    auto position = manager.GetBlock(manager.GetRootIndex()).Get().GetBlockIndexContainKey(key);
    if (position.first == 0) {
      BPTREE_LOG_ERROR("key {} is not in the db", key);
    } else {
      BPTREE_LOG_INFO("the position of the key {} is in the {}th entry of the block {}", key, position.second,
                      position.first);
    }
  } catch (const bptree::BptreeExecption& e) {
    std::cerr << "sth error, " << e.what() << std::endl;
  }
  return 0;
}