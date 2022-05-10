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
    option.create = false;
    bptree::BlockManager manager(option);
    manager.PrintBlockByIndex(atol(argv[2]));
  } catch (const bptree::BptreeExecption& e) {
    std::cerr << "sth error, " << e.what() << std::endl;
  }
  return 0;
}