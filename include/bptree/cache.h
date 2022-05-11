#pragma once

#include <functional>
#include <iostream>
#include <list>
#include <memory>
#include <unordered_map>

#include "bptree/log.h"
#include "exception.h"

namespace bptree {

template <typename Key, typename Value>
class LRUCache {
 public:
  using iter_type = typename std::list<Key>::iterator;
  struct Entry {
    std::unique_ptr<Value> value;
    iter_type iter;
    size_t use_ref_;

    Entry() : value(nullptr), iter(), use_ref_(0) {}
  };

  class Wrapper {
   public:
    Wrapper(LRUCache<Key, Value>& cache, const Key& key, Entry& v)
        : holder_(cache), key_(key), value_(v), unbinded_(false) {
      assert(value_.use_ref_ > 0);
    }

    Wrapper(const Wrapper&) = delete;
    Wrapper& operator=(const Wrapper&) = delete;

    Wrapper(Wrapper&& other) = delete;
    Wrapper& operator=(Wrapper&& other) = delete;

    Value& Get() { return *value_.value.get(); }

    const Value& Get() const { return *value_.value.get(); }

    ~Wrapper() { UnBind(); }

    void UnBind() {
      if (unbinded_ == true) {
        return;
      }
      assert(value_.use_ref_ > 0);
      value_.use_ref_ -= 1;
      if (value_.use_ref_ == 0) {
        //从in_use_链表中移除，插入lru链表首部
        holder_.MoveInUseToLruList(key_, value_);
      }
      unbinded_ = true;
    }

   private:
    LRUCache<Key, Value>& holder_;
    Key key_;
    // 持有无序map元素的引用是安全的，只有对应的元素被删除才会导致失效， from
    // https://en.cppreference.com/w/cpp/container/unordered_map
    Entry& value_;
    bool unbinded_;
  };

  using load_functor_type = std::function<std::unique_ptr<Value>(const Key& key)>;
  using free_functor = std::function<void(const Key& key, Value& value)>;

  explicit LRUCache(uint32_t capacity, const load_functor_type& if_not_exist)
      : capacity_(capacity), if_not_exist_(if_not_exist) {}

  void SetFreeNotify(const free_functor& f) { free_notify_ = f; }

  // v != nullptr的用法特殊，需要确保cache中原先不存在key的元素
  Wrapper Get(const Key& key, std::unique_ptr<Value>&& v = nullptr) {
    if (cache_.count(key) == 0) {
      in_use_.insert(in_use_.begin(), key);
      std::unique_ptr<Value> value = std::move(v);
      if (value == nullptr) {
        value = if_not_exist_(key);
      }
      Entry entry;
      entry.value = std::move(value);
      entry.iter = in_use_.begin();
      entry.use_ref_ = 1;
      cache_[key] = std::move(entry);
    } else {
      if (v != nullptr) {
        throw BptreeExecption("cache's Get method has no-nullptr parameter, key == ", std::to_string(key));
      }
      Entry& entry = cache_[key];
      if (entry.use_ref_ == 0) {
        lru_list_.erase(entry.iter);
        in_use_.insert(in_use_.begin(), key);
        entry.iter = in_use_.begin();
        entry.use_ref_ = 1;
      } else {
        entry.use_ref_ += 1;
      }
    }
    return Wrapper(*this, key, cache_[key]);
  }

  // 重构
  bool Delete(const Key& key, bool notify) {
    if (cache_.count(key) != 0) {
      Entry& entry = cache_[key];
      if (entry.use_ref_ != 0) {
        BPTREE_LOG_WARN("cache delete error, key == {} already in use, use_ref == {}", key, entry.use_ref_);
        return false;
      }
      lru_list_.erase(entry.iter);
      if (free_notify_ && notify) {
        free_notify_(key, *entry.value.get());
      }
      cache_.erase(key);
    }
    return true;
  }

  void MoveInUseToLruList(Key key, Entry& entry) {
    assert(entry.use_ref_ == 0);
    in_use_.erase(entry.iter);
    lru_list_.insert(lru_list_.begin(), key);
    entry.iter = lru_list_.begin();
    assert(capacity_ > 0);
    // todo 执行lru逻辑
    while (lru_list_.size() > capacity_) {
      uint32_t remove_key = lru_list_.back();
      assert(cache_.count(remove_key) != 0);
      Entry& entry = cache_[remove_key];
      assert(entry.use_ref_ == 0);
      if (free_notify_) {
        free_notify_(remove_key, *entry.value.get());
      }
      cache_.erase(remove_key);
      lru_list_.pop_back();
    }
  }

  void PrintInfo() const {
    std::cout << "this is lru cache's info : " << std::endl;
    std::cout << "list in_use_'s size is " << in_use_.size() << std::endl;
    std::cout << "list lru_list's size is " << lru_list_.size() << std::endl;
    std::cout << "cache's size is " << cache_.size() << std::endl;
  }

  // 清空没有被使用的元素，如果还有在被使用的元素，返回false，否则返回true。
  bool Clear() {
    for (auto& each : lru_list_) {
      assert(cache_.count(each) != 0);
      assert(cache_[each].use_ref_ == 0);
      if (free_notify_) {
        free_notify_(each, *cache_[each].value.get());
      }
      cache_.erase(each);
    }
    lru_list_.clear();
    return in_use_.empty();
  }

 private:
  std::unordered_map<Key, Entry> cache_;
  std::list<Key> lru_list_;
  std::list<Key> in_use_;
  uint32_t capacity_;

  load_functor_type if_not_exist_;
  // 有些资源不便于在析构函数中释放，可以通过注册本callback进行处理
  free_functor free_notify_;
};

}  // namespace bptree