#include "bptree/block_manager.h"
#include <string>

/*
todo
已经完成了b+树的增删查改逻辑（包括节点的合并和分裂）
目前基本完成了和文件系统的交互（used_bytes重新梳理一下，下面检查下有无bug）
对节点的内部格式进行进一步的优化，现在这种每个叶子节点拿出来并重新写入的时候需要重新flush整个节点的数据
kv大小调整为固定的之后，superblock不适合这种格式了，需要为superblock重新写一个
完善build系统，补充单元测试
完善错误处理机制（目前仅用assert断言）
*/

int main() {
  bptree::BlockManager manager("test.db", 1, 5);
  
  for(int i = 0; i < 40; ++i) {
    std::string key;
    key.push_back(char('a' + rand() % 25));
    manager.Insert(key, "value");
    std::cout << " after insert key " << key << std::endl << std::endl;
  }
  std::cout << " ***** " << std::endl << std::endl;
  manager.PrintBpTree();
  /*
  manager.Delete("d");
  manager.Delete("e");
  manager.Delete("h");
  manager.Delete("a");
  manager.PrintBpTree();
  std::cout << " ***** " << std::endl << std::endl;
  for(int i = 0; i < 12; ++i) {
    std::string key;
    key.push_back(char(i + 'a'));
    manager.Delete(key);
    std::cout << " after delete key " << key << std::endl << std::endl;
    manager.PrintBpTree();
  }*/
  manager.FlushToFile();
  return 0;
}