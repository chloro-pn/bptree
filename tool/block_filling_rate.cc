#include <iostream>
#include <cstdint>

#include "bptree/block_manager.h"

#define none "\033[0m"
#define blue "\033[1;34m"
#define green "\033[1;32m"
#define cyan "\033[1;36m"
#define red "\033[1;31m"
#define purple "\033[1;35m"
#define white "\033[1;37m"
#define yellow "\033[1;33m"

void PrintYellow(uint32_t index, double rate) {
  printf(" %5d - %f" yellow " ■ " none, index, rate);
}

void PrintNone(uint32_t index, double rate) {
  printf(" %5d - %f" none " ■ " none, index, rate);
}

void PrintWhite(uint32_t index, double rate) {
  printf(" %5d - %f" white " ■ " none, index, rate);
}

void PrintRed(uint32_t index, double rate) {
  printf(" %5d - %f" red " ■ " none, index, rate);
}

void PrintGreen(uint32_t index, double rate) {
  printf(" %5d - %f" green " ■ " none, index, rate);
}

void PrintBlue(uint32_t index, double rate) {
  printf(" %5d - %f" blue " ■ " none, index, rate);
}

void PrintBlock(uint32_t index, const bptree::Block* block) {
  double max = block->GetMaxEntrySize();
  double used = block->GetKVView().size();
  double filling_rate = used / max;
  if (filling_rate >= 0 && filling_rate < 0.2) {
    PrintWhite(index, filling_rate);
  } else if (filling_rate >= 0.2 && filling_rate < 0.4) {
    PrintGreen(index, filling_rate);
  } else if (filling_rate >= 0.4 && filling_rate < 0.6) {
    PrintBlue(index, filling_rate);
  } else if (filling_rate >= 0.6 && filling_rate < 0.8) {
    PrintYellow(index, filling_rate);
  } else if (filling_rate >= 0.8 && filling_rate <= 1.0) {
    PrintRed(index, filling_rate);
  } else {
    printf("invalid filling rate : %f", filling_rate);
    exit(-1);
  }
}

int main(int argc, char* argv[]) {
  if (argc != 2) {
    std::cerr << "usage : ./block_filling_rate {db name, type:string}" << std::endl;
    return -1;
  }
  std::string name(argv[1]);
  try {
    bptree::BlockManagerOption option;
    option.db_name = name;
    option.neflag = bptree::NotExistFlag::ERROR;
    option.eflag = bptree::ExistFlag::SUCC;
    option.mode = bptree::Mode::R;
    bptree::BlockManager manager(option);
    uint32_t max = manager.GetMaxBlockIndex();
    size_t max_row = max / 10 + 1;
    for(size_t j = 0; j < max_row; ++j) {
      for(size_t i = 0; i < 10; ++i) {
        size_t index = j * 10 + i;
        if (index > max) {
          break;
        }
        if (index == 0) {
          // super block 
          continue;
        }
        auto block = manager.GetBlock(index);
        PrintBlock(index, &block.Get());
      }
      printf("\n");
    }
  } catch (const bptree::BptreeExecption& e) {
    std::cerr << "sth error, " << e.what() << std::endl;
  }
  return 0;
}