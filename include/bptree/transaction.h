#pragma once

#include <vector>
#include <string>
#include <functional>

#include "bptree/util.h"
#include "bptree/exception.h"

namespace bptree {

enum class OperationType {
  Get,
  GetRange,
  Insert,
  Update,
  Delete,
};

struct Operation {
  OperationType type;
  std::string key;
  std::string value;
};

class BlockManager;

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

}