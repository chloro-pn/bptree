#pragma once

#include <exception>
#include <string>
#include <vector>

namespace bptree {

class BptreeExecption : public std::exception {
 public:
  template <typename... Args>
  explicit BptreeExecption(Args&&... what) : what_() {
    std::vector<std::string> tmp{what...};
    SetWhatByStrs(tmp);
  }

  const char* what() const noexcept override { return what_.c_str(); }

 private:
  std::string what_;

  void SetWhatByStrs(const std::vector<std::string>& strs) {
    what_.clear();
    for (auto& each : strs) {
      what_.append(each);
    }
  }
};
}  // namespace bptree