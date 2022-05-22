#include <iostream>
#include <string>

#include "bptree/block_manager.h"

int main(int argc, char* argv[]) {
  if (argc != 3) {
    std::cerr << "usage : ./block_print {db name, type:string} {block_index, type:uint32_t}" << std::endl;
    return -1;
  }
  std::string name(argv[1]);
  try {
    bptree::BlockManagerOption option;
    option.file_name = name;
    option.neflag = bptree::NotExistFlag::ERROR;
    option.eflag = bptree::ExistFlag::SUCC;
    option.mode = bptree::Mode::R;
    bptree::BlockManager manager(option);
    uint32_t block_index = atol(argv[2]);
    if (block_index == 0) {
      manager.PrintSuperBlockInfo();
    } else {
      manager.PrintBlockByIndex(block_index);
    }
  } catch (const bptree::BptreeExecption& e) {
    std::cerr << "sth error, " << e.what() << std::endl;
  }
  return 0;
}