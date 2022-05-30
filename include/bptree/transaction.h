#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "bptree/exception.h"
#include "bptree/util.h"

namespace bptree {

class BlockManager;

void RollBack(const std::vector<std::unique_ptr<Operation>>& ops, BlockManager& manager);

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
  std::vector<std::unique_ptr<Operation>> operations_;
  uint64_t seq_;

  void seq_check() {
    if (seq_ == no_wal_sequence) {
      throw BptreeExecption("invalid transaction seq");
    }
  }
};

class TransactionMt {
 public:
  TransactionMt(BlockManager& manager);

  BPTREE_INTERFACE std::string Get(const std::string& key);

  BPTREE_INTERFACE bool Insert(const std::string& key, const std::string& value);

  BPTREE_INTERFACE std::string Delete(const std::string& key);

  BPTREE_INTERFACE std::string Update(const std::string& key, const std::string& value);

  BPTREE_INTERFACE void Commit();

  BPTREE_INTERFACE void RollBack();

 private:
  BlockManager& manager_;
  uint64_t seq_;
  std::shared_ptr<Queue<Operation>> reply_;

  std::string WaitForGetReply(const std::string& key);

  bool WaitForInsertReply(const std::string& key, const std::string& value);

  std::string WaitForDeleteReply(const std::string& key);

  std::string WaitForUpdateReply(const std::string& key, const std::string& value);

  template <OperationType type>
  void WaitForReply() {
    while (true) {
      auto result = reply_->TryPop();
      if (result.empty()) {
        sleep(std::chrono::milliseconds(5));
        continue;
      }
      assert(result.size() == 1);
      assert(result[0]->type == type);
      assert(result[0]->sequence == seq_);
      break;
    }
  }
};

}  // namespace bptree