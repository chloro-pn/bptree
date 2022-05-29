#pragma once

#include <vector>
#include <string>
#include <functional>

#include "bptree/util.h"

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

  BPTREE_INTERFACE void Commit() {
    if (seq_ == no_wal_sequence) {
      return;
    }
    manager_.GetWal().End(seq_);
    seq_ = no_wal_sequence;
  }

  BPTREE_INTERFACE void RollBack() {
    if (seq_ == no_wal_sequence) {
      return;
    }
    // todo : 需要将operations_中的操作按照逆序撤回，最后向wal日志提交abort or end
    seq_ = no_wal_sequence;
  }
  
 private:
  BlockManager& manager_;
  std::vector<Operation> operations_;
  uint64_t seq_;
};

}