#pragma once

#include <exception>
#include <string>

namespace bptree {
class BptreeExecption : public std::exception {
 public:
  explicit BptreeExecption(const std::string& what) : what_(what) {}

  const char* what() const noexcept override { return what_.c_str(); }

 private:
  std::string what_;
};
}  // namespace bptree