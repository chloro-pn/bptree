#pragma once

#include <functional>
#include <string>
#include <vector>

#include "bptree/exception.h"
#include "bptree/util.h"

namespace bptree {

class BlockManager;

/*
 * 这个类提供非线程安全的事务机制，不通过queue与blockmanager通信
 */

class Transaction {
 public:
  Transaction(BlockManager& manager);

  BPTREE_INTERFACE std::string Get(const std::string& key);

  BPTREE_INTERFACE bool Insert(const std::string& key, const std::string& value);

  BPTREE_INTERFACE std::string Delete(const std::string& key);

  BPTREE_INTERFACE std::string Update(const std::string& key, const std::string& value);

  BPTREE_INTERFACE void Commit();

  BPTREE_INTERFACE void RollBack();

  ~Transaction() {
    if (seq_ != no_wal_sequence) {
      RollBack();
    }
  }

 private:
  BlockManager& manager_;
  std::vector<Operation> operations_;
  uint64_t seq_;

  void seq_check() {
    if (seq_ == no_wal_sequence) {
      throw BptreeExecption("invalid transaction seq");
    }
  }
};

}  // namespace bptree