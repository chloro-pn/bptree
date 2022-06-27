#pragma once

#include <chrono>
#include <string>
#include <vector>

class Timer {
 public:
  Timer() {}
  void Start() { start_ = std::chrono::system_clock::now(); }

  double End() {
    auto end = std::chrono::system_clock::now();
    auto use_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start_);
    return use_ms.count();
  }

 private:
  std::chrono::time_point<std::chrono::system_clock> start_;
};

inline std::string ConstructRandomStr(size_t size) {
  std::string result;
  for (size_t i = 0; i < size; ++i) {
    result.push_back('a' + rand() % 26);
  }
  return result;
}

struct Entry {
  std::string key;
  std::string value;
  bool delete_;
};

inline std::vector<Entry> ConstructRandomKv(size_t size, size_t key_size, size_t value_size) {
  std::vector<Entry> result;
  for (size_t i = 0; i < size; ++i) {
    Entry entry;
    entry.key = ConstructRandomStr(key_size);
    entry.value = ConstructRandomStr(value_size);
    entry.delete_ = false;
    result.push_back(entry);
  }
  return result;
}

template <typename T>
void FisherYatesAlg(std::vector<T>& entries) {
  for (size_t i = 0; i < entries.size(); ++i) {
    size_t j = i + rand() % (entries.size() - i);
    std::swap(entries[i], entries[j]);
  }
}
