#pragma once

#include <functional>
#include <list>
#include <memory>
#include <unordered_map>

#include "bptree/log.h"

namespace bptree {

template <typename Key, typename Value>
class LRUCache {
 public:
  using load_functor_type =
      std::function<std::unique_ptr<Value>(const Key& key)>;
  using free_functor = std::function<void(const Key& key, Value& value)>;
  using iter_type = typename std::list<Key>::iterator;

  explicit LRUCache(uint32_t capacity, const load_functor_type& if_not_exist)
      : capacity_(capacity), if_not_exist_(if_not_exist) {}

  void SetFreeNotify(const free_functor& f) { free_notify_ = f; }

  Value& Get(const Key& key, std::unique_ptr<Value>&& v = nullptr) {
    if (cache_.count(key) == 0) {
      lru_list_.insert(lru_list_.begin(), key);
      std::unique_ptr<Value> value = std::move(v);
      if (value == nullptr) {
        value = if_not_exist_(key);
      }
      Entry entry;
      entry.value = std::move(value);
      entry.iter = lru_list_.begin();
      cache_[key] = std::move(entry);
    } else {
      // 从链表中删除，并重新插入头部
      Entry& entry = cache_[key];
      lru_list_.erase(entry.iter);
      lru_list_.insert(lru_list_.begin(), key);
      entry.iter = lru_list_.begin();
    }
    while (lru_list_.size() > capacity_) {
      Key free_key = lru_list_.back();
      if (free_notify_) {
        free_notify_(free_key, *cache_[free_key].value.get());
      }
      lru_list_.pop_back();
      cache_.erase(free_key);
    }
    return *cache_[key].value.get();
  }

  void Delete(const Key& key, bool notify) {
    if (cache_.count(key) != 0) {
      Entry& entry = cache_[key];
      lru_list_.erase(entry.iter);
      if (free_notify_ && notify) {
        free_notify_(key, *entry.value.get());
      }
      cache_.erase(key);
    }
  }

  void Clear() {
    for (auto& each : lru_list_) {
      if (free_notify_) {
        free_notify_(each, *cache_[each].value.get());
      }
      cache_.erase(each);
    }
    lru_list_.clear();
  }

 private:
  struct Entry {
    std::unique_ptr<Value> value;
    iter_type iter;

    Entry() : value(nullptr), iter() {}
  };
  std::unordered_map<Key, Entry> cache_;
  std::list<Key> lru_list_;
  uint32_t capacity_;

  load_functor_type if_not_exist_;
  // 有些资源不便于在析构函数中释放，可以通过注册本callback进行处理
  free_functor free_notify_;
};

}  // namespace bptree