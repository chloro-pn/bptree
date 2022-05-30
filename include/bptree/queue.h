#pragma once

#include <memory>
#include <vector>

#include "rigtorp/MPMCQueue.h"

namespace bptree {

template <typename T>
class Queue {
 public:
  explicit Queue(size_t capacity) : queue_(capacity) {}

  void Push(std::unique_ptr<T>&& t) { queue_.push(std::move(t)); }

  std::vector<std::unique_ptr<T>> TryPop() {
    std::vector<std::unique_ptr<T>> result;
    while (true) {
      std::unique_ptr<T> item;
      bool succ = queue_.try_pop(item);
      if (succ == true) {
        result.push_back(std::move(item));
      } else {
        break;
      }
      if (result.size() >= 128) {
        break;
      }
    }
    return result;
  }

 private:
  rigtorp::MPMCQueue<std::unique_ptr<T>> queue_;
};

}  // namespace bptree