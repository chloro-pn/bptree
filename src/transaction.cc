#include "bptree/transaction.h"

#include <cassert>

#include "bptree/block_manager.h"
#include "bptree/exception.h"

namespace bptree {

void RollBack(const std::vector<std::unique_ptr<Operation>>& operations_, BlockManager& manager_) {
  uint64_t seq_ = no_wal_sequence;
  assert(operations_.empty() == false);
  for (auto it = operations_.rbegin(); it != operations_.rend(); ++it) {
    const Operation& op = *(*it).get();
    if (op.type == OperationType::Begin) {
      seq_ = op.sequence;
    } else if (op.type == OperationType::Get) {
      // do nothing
    } else if (op.type == OperationType::Insert) {
      if (op.value != "") {
        auto old_v = manager_.Delete(op.key, seq_);
        // 这里的删除操作应该是成功的
        if (old_v != op.value) {
          throw BptreeExecption("transaction rollback fail, invalid delete ", op.key, " ", old_v, " ", op.value);
        }
      }
    } else if (op.type == OperationType::Delete) {
      if (op.value != "") {
        auto succ = manager_.Insert(op.key, op.value, seq_);
        if (succ == false) {
          throw BptreeExecption("transaction rollback fail, invalid insert ", op.key);
        }
      }
    } else if (op.type == OperationType::Update) {
      if (op.value != "") {
        BPTREE_LOG_INFO("rollvack update operation : {}, {}", op.key, op.value);
        auto old_v = manager_.Update(op.key, op.value, seq_);
        if (old_v == "") {
          throw BptreeExecption("transaction rollback fail, invalid update ", op.key);
        }
      }
    } else {
      throw BptreeExecption("invalid transaction operation");
    }
  }
  manager_.GetWal().End(seq_);
}

Transaction::Transaction(BlockManager& manager) : manager_(manager), seq_(no_wal_sequence) {
  seq_ = manager_.GetWal().RequestSeq();
  manager_.GetWal().Begin(seq_);
}

// 目前的版本暂且不考虑隔离性问题，后续可能会利用事务号实现mvcc
std::string Transaction::Get(const std::string& key) {
  seq_check();
  std::string result;
  std::unique_ptr<Operation> op(new Operation());
  op->type = OperationType::Get;
  op->key = key;
  op->value = manager_.Get(key);
  result = op->value;
  operations_.push_back(std::move(op));
  return result;
}

bool Transaction::Insert(const std::string& key, const std::string& value) {
  seq_check();
  bool result;
  std::unique_ptr<Operation> op(new Operation());
  op->type = OperationType::Insert;
  op->key = key;
  // value为空意味着插入失败
  op->value = manager_.Insert(key, value, seq_) ? value : "";
  result = op->value == value;
  operations_.push_back(std::move(op));
  return result;
}

std::string Transaction::Delete(const std::string& key) {
  seq_check();
  std::string result;
  std::unique_ptr<Operation> op(new Operation());
  op->type = OperationType::Delete;
  op->key = key;
  // value为空意味着删除失败
  op->value = manager_.Delete(key, seq_);
  result = op->value;
  operations_.push_back(std::move(op));
  return result;
}

std::string Transaction::Update(const std::string& key, const std::string& value) {
  seq_check();
  std::string result;
  std::unique_ptr<Operation> op(new Operation());
  op->type = OperationType::Update;
  op->key = key;
  // value为空意味着更新失败
  op->value = manager_.Update(key, value, seq_);
  result = op->value;
  operations_.push_back(std::move(op));
  return result;
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
  ::bptree::RollBack(operations_, manager_);
  seq_ = no_wal_sequence;
}

TransactionMt::TransactionMt(BlockManager& manager)
    : manager_(manager), seq_(no_wal_sequence), reply_(new Queue<Operation>(16)) {
  seq_ = manager_.GetWal().RequestSeq();
  std::unique_ptr<Operation> op(new Operation());
  op->type = OperationType::Begin;
  op->sequence = seq_;
  op->notify_queue_ = reply_;
  manager_.GetQueue().Push(std::move(op));
  WaitForReply<OperationType::Begin>();
}

BPTREE_INTERFACE std::string TransactionMt::Get(const std::string& key) {
  std::unique_ptr<Operation> op(new Operation());
  op->type = OperationType::Get;
  op->sequence = seq_;
  op->key = key;
  op->notify_queue_ = reply_;
  manager_.GetQueue().Push(std::move(op));
  return WaitForGetReply(key);
}

BPTREE_INTERFACE bool TransactionMt::Insert(const std::string& key, const std::string& value) {
  std::unique_ptr<Operation> op(new Operation());
  op->type = OperationType::Insert;
  op->sequence = seq_;
  op->key = key;
  op->value = value;
  op->notify_queue_ = reply_;
  manager_.GetQueue().Push(std::move(op));
  return WaitForInsertReply(key, value);
}

BPTREE_INTERFACE std::string TransactionMt::Delete(const std::string& key) {
  std::unique_ptr<Operation> op(new Operation());
  op->type = OperationType::Delete;
  op->sequence = seq_;
  op->key = key;
  op->notify_queue_ = reply_;
  manager_.GetQueue().Push(std::move(op));
  return WaitForDeleteReply(key);
}

BPTREE_INTERFACE std::string TransactionMt::Update(const std::string& key, const std::string& value) {
  std::unique_ptr<Operation> op(new Operation());
  op->type = OperationType::Update;
  op->sequence = seq_;
  op->key = key;
  op->value = value;
  op->notify_queue_ = reply_;
  manager_.GetQueue().Push(std::move(op));
  return WaitForUpdateReply(key, value);
}

bool TransactionMt::WaitForInsertReply(const std::string& key, const std::string& value) {
  while (true) {
    auto result = reply_->TryPop();
    if (result.empty()) {
      sleep(std::chrono::milliseconds(5));
      continue;
    }
    assert(result.size() == 1);
    assert(result[0]->type == OperationType::Insert);
    assert(result[0]->sequence == seq_);
    assert(result[0]->key == key);
    return result[0]->value == value ? true : false;
  }
}

std::string TransactionMt::WaitForGetReply(const std::string& key) {
  while (true) {
    auto result = reply_->TryPop();
    if (result.empty()) {
      sleep(std::chrono::milliseconds(5));
      continue;
    }
    assert(result.size() == 1);
    assert(result[0]->type == OperationType::Get);
    assert(result[0]->sequence == seq_);
    assert(result[0]->key == key);
    return result[0]->value;
  }
}

std::string TransactionMt::WaitForDeleteReply(const std::string& key) {
  while (true) {
    auto result = reply_->TryPop();
    if (result.empty()) {
      sleep(std::chrono::milliseconds(5));
      continue;
    }
    assert(result.size() == 1);
    assert(result[0]->type == OperationType::Delete);
    assert(result[0]->sequence == seq_);
    assert(result[0]->key == key);
    return result[0]->value;
  }
}

std::string TransactionMt::WaitForUpdateReply(const std::string& key, const std::string& value) {
  while (true) {
    auto result = reply_->TryPop();
    if (result.empty()) {
      sleep(std::chrono::milliseconds(5));
      continue;
    }
    assert(result.size() == 1);
    assert(result[0]->type == OperationType::Update);
    assert(result[0]->sequence == seq_);
    assert(result[0]->key == key);
    return result[0]->value;
  }
}

BPTREE_INTERFACE void TransactionMt::Commit() {
  std::unique_ptr<Operation> op(new Operation());
  op->type = OperationType::End;
  op->sequence = seq_;
  op->notify_queue_ = reply_;
  manager_.GetQueue().Push(std::move(op));
  WaitForReply<OperationType::End>();
}

BPTREE_INTERFACE void TransactionMt::RollBack() {
  std::unique_ptr<Operation> op(new Operation());
  op->type = OperationType::RollBack;
  op->sequence = seq_;
  op->notify_queue_ = reply_;
  manager_.GetQueue().Push(std::move(op));
  WaitForReply<OperationType::RollBack>();
}

}  // namespace bptree