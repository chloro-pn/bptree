#include "bptree/transaction.h"

#include "bptree/block_manager.h"

namespace bptree {

Transaction::Transaction(BlockManager& manager) : manager_(manager), seq_(no_wal_sequence) {
  seq_ = manager_.GetWal().Begin();
}

// 目前的版本暂且不考虑隔离性问题，后续可能会利用事务号实现mvcc
std::string Transaction::Get(const std::string& key) {
  Operation op;
  op.type = OperationType::Get;
  op.key = key;
  op.value = manager_.Get(key);
  operations_.push_back(op);
  return op.value;
}

bool Transaction::Insert(const std::string& key, const std::string& value) {
  Operation op;
  op.type = OperationType::Insert;
  op.key = key;
  // value为空意味着插入失败
  op.value = manager_.Insert(key, value, seq_) ? value : "";
  operations_.push_back(op);
  return op.value == value;
}

std::string Transaction::Delete(const std::string& key) {
  Operation op;
  op.type = OperationType::Delete;
  op.key = key;
  // value为空意味着删除失败
  op.value = manager_.Delete(key, seq_);
  operations_.push_back(op);
  return op.value;
}

std::string Transaction::Update(const std::string& key, const std::string& value) {
  Operation op;
  op.type = OperationType::Update;
  op.key = key;
  // value为空意味着更新失败
  op.value = manager_.Update(key, value, seq_);
  operations_.push_back(op);
  return op.value;
}

}  // namespace bptree