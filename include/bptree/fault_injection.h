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

  std::function<bool(uint32_t)>& GetPartialWriteCondition() { return partial_write_cond_; }

 private:
  std::function<bool(uint32_t)> partial_write_cond_;
};
}  // namespace bptree