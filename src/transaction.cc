#include "bptree/transaction.h"

#include "bptree/block_manager.h"
#include "bptree/exception.h"

namespace bptree {

Transaction::Transaction(BlockManager& manager) : manager_(manager), seq_(no_wal_sequence) {
  seq_ = manager_.GetWal().Begin();
}

// 目前的版本暂且不考虑隔离性问题，后续可能会利用事务号实现mvcc
std::string Transaction::Get(const std::string& key) {
  seq_check();
  Operation op;
  op.type = OperationType::Get;
  op.key = key;
  op.value = manager_.Get(key);
  operations_.push_back(op);
  return op.value;
}

bool Transaction::Insert(const std::string& key, const std::string& value) {
  seq_check();
  Operation op;
  op.type = OperationType::Insert;
  op.key = key;
  // value为空意味着插入失败
  op.value = manager_.Insert(key, value, seq_) ? value : "";
  operations_.push_back(op);
  return op.value == value;
}

std::string Transaction::Delete(const std::string& key) {
  seq_check();
  Operation op;
  op.type = OperationType::Delete;
  op.key = key;
  // value为空意味着删除失败
  op.value = manager_.Delete(key, seq_);
  operations_.push_back(op);
  return op.value;
}

std::string Transaction::Update(const std::string& key, const std::string& value) {
  seq_check();
  Operation op;
  op.type = OperationType::Update;
  op.key = key;
  // value为空意味着更新失败
  op.value = manager_.Update(key, value, seq_);
  operations_.push_back(op);
  return op.value;
}

BPTREE_INTERFACE void Transaction::Commit() {
  if (seq_ == no_wal_sequence) {
    return;
  }
  manager_.GetWal().End(seq_);
  seq_ = no_wal_sequence;
}

BPTREE_INTERFACE void Transaction::RollBack() {
  if (seq_ == no_wal_sequence) {
    return;
  }
  // todo : 需要将operations_中的操作按照逆序撤回，最后向wal日志提交abort or end
  for(auto it = operations_.rbegin(); it != operations_.rend(); ++it) {
    if (it->type == OperationType::Get) {
      // do nothing
    } else if (it->type == OperationType::Insert) {
      if (it->value != "") {
        auto old_v = manager_.Delete(it->key, seq_);
        // 这里的删除操作应该是成功的
        if (old_v != it->value) {
          throw BptreeExecption("transaction rollback fail, invalid delete ", it->key);
        }
      }
    } else if (it->type == OperationType::Delete) {
      if (it->value != "") {
        auto succ = manager_.Insert(it->key, it->value, seq_);
        if (succ == false) {
          throw BptreeExecption("transaction rollback fail, invalid insert ", it->key);
        }
      }
    } else if (it->type == OperationType::Update) {
      if (it->value != "") {
        auto old_v = manager_.Update(it->key, it->value, seq_);
        if (old_v == "") {
          throw BptreeExecption("transaction rollback fail, invalid update ", it->key);
        }
      }
    } else {
      throw BptreeExecption("invalid transaction operation");
    }
  }
  manager_.GetWal().End(seq_);
  seq_ = no_wal_sequence;
}

}  // namespace bptree