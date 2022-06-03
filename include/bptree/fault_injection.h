#pragma once

#include <cstdint>
#include <functional>

namespace bptree {
class FaultInjection {
 public:
  FaultInjection() {}

  void RegisterPartialWriteCondition(const std::function<bool(uint32_t)>& condition) {
    partial_write_cond_ = condition;
  }

  void RegisterTheLastWalWriteFailCondition(const std::function<bool()>& condition) {
    the_last_check_point_fail_ = condition;
  }

  void RegisterTxRollbackCrashCondition(const std::function<int()>& condition) { tx_rollback_crash_ = condition; }

  std::function<bool(uint32_t)>& GetPartialWriteCondition() { return partial_write_cond_; }

  std::function<bool()>& GetTheLastCheckPointFailCondition() { return the_last_check_point_fail_; }

  std::function<int()>& GetTxRollbackCrashCondition() { return tx_rollback_crash_; }

 private:
  std::function<bool(uint32_t)> partial_write_cond_;
  std::function<bool()> the_last_check_point_fail_;
  std::function<int()> tx_rollback_crash_;
};
}  // namespace bptree