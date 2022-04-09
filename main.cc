#include "bptree.h"
#include <string>

int main() {
  bptree::BPlusTree tree(5);
  for(int i = 0; i < 10; ++i) {
    tree.Insert(std::to_string(i), "value" + std::to_string(i));
  }
  tree.Print();
  for(int i = 0; i < 10; ++i) {
    tree.Delete(std::to_string(i));
  }
  std::cout << "after delete" << std::endl;
  tree.Print();
  return 0;
}