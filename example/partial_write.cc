#include "bptree/block_manager.h"

int main() {
  bptree::BlockManagerOption option;
  option.neflag = bptree::NotExistFlag::CREATE;
  option.eflag = bptree::ExistFlag::ERROR;
  option.mode = bptree::Mode::WR;
  option.file_name = "test.db";
  option.key_size = 1;
  option.value_size = 5;
  bptree::BlockManager manager(option);
  manager.Insert("a", "value");
  manager.GetFaultInjection().RegisterPartialWriteCondition([](uint32_t index) -> bool { return index == 0; });
}