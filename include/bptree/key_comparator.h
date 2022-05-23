#pragma once

#include <string_view>

namespace bptree {

/*
 * 支持自定义比较函数，便于用户实现变长key的比较逻辑
 */
class Comparator {
 public:
  virtual int Compare(const std::string_view& v1, const std::string_view& v2) const {
    if (v1 > v2) {
      return 1;
    } else if (v1 == v2) {
      return 0;
    } else {
      return -1;
    }
  }
};

}  // namespace bptree