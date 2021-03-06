#pragma once

#include <functional>
#include <iostream>
#include <list>
#include <memory>
#include <source_location>
#include <unordered_map>

#include "bptree/exception.h"
#include "bptree/log.h"
#include "bptree/metric/metric.h"

namespace bptree {

/* class LRUCache 注释
 * 该类采用基于LRU算法进行数据缓存
 * 数据成员为两个链表：(in_use_和lru_list_)和一个map(cache_)，两个链表中仅存储key信息；两个链表的长度之和应该等于map.size()
 * 采用引用计数的方法管理被用户持有的Value，当没有用户持有时首先将Value对应的Key插入lru_list_链表中，当lru_list_链表长度到达上限时从末尾开始执行clean操作
 * 可以通过SetFreeNotify接口注册清理前的回调函数，便于执行某些不适合放在析构函数中的操作
 * Get接口返回的是一个Wrapper类，该类基于RALL机制管理Value对象的引用计数，并且该类不支持copy和move
 */
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
    Wrapper(LRUCache<Key, Value>* cache, const Key& key, Entry* v)
        : holder_(cache), key_(key), value_(v), unbinded_(false) {
      if (value_ != nullptr) {
        assert(value_->use_ref_ > 0);
      }
    }

    Wrapper(const Wrapper&) = delete;
    Wrapper& operator=(const Wrapper&) = delete;

    Wrapper(Wrapper&& other)
        : holder_(other.holder_), key_(other.key_), value_(other.value_), unbinded_(other.unbinded_) {
      other.value_ = nullptr;
    }

    Wrapper& operator=(Wrapper&& other) {
      UnBind();
      holder_ = other.holder_;
      key_ = other.key_;
      value_ = other.value_;
      unbinded_ = other.unbinded_;
      other.value_ = nullptr;
      return *this;
    }

    bool Exist() const { return value_ != nullptr; }

    Value& Get() { return *(value_->value.get()); }

    const Value& Get() const { return *(value_->value.get()); }

    ~Wrapper() { UnBind(); }

    void UnBind() {
      if (Exist() == false || unbinded_ == true) {
        return;
      }
      assert(value_->use_ref_ > 0);
      value_->use_ref_ -= 1;
      if (value_->use_ref_ == 0) {
        //从in_use_链表中移除，插入lru链表首部
        holder_->MoveInUseToLruList(key_, *value_);
      }
      unbinded_ = true;
    }

   private:
    LRUCache<Key, Value>* holder_;
    Key key_;
    // 持有无序map元素的引用和指针是安全的，只有对应的元素被删除才会导致失效， from
    // https://en.cppreference.com/w/cpp/container/unordered_map
    Entry* value_;
    bool unbinded_;
  };

  using free_functor = std::function<void(const Key& key, Value& value)>;

  explicit LRUCache(uint32_t capacity) : capacity_(capacity) {}

  void SetFreeNotify(const free_functor& f) { free_notify_ = f; }

  Wrapper Get(const Key& key) {
    if (cache_.count(key) == 0) {
      return Wrapper(this, key, nullptr);
    } else {
      Entry& entry = cache_[key];
      if (entry.use_ref_ == 0) {
        lru_list_.erase(entry.iter);
        in_use_.insert(in_use_.begin(), key);
        entry.iter = in_use_.begin();
        entry.use_ref_ = 1;
      } else {
        entry.use_ref_ += 1;
      }
      return Wrapper(this, key, &entry);
    }
  }

  void Insert(const Key& key, std::unique_ptr<Value>&& v) {
    if (cache_.count(key) != 0) {
      throw BptreeExecption("an existing key was inserted in cache ");
    }
    Entry entry;
    entry.use_ref_ = 0;
    lru_list_.insert(lru_list_.begin(), key);
    entry.iter = lru_list_.begin();
    entry.value = std::move(v);
    cache_[key] = std::move(entry);

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

  std::unique_ptr<Value> Move(const Key& key) {
    assert(cache_.count(key) > 0);
    assert(cache_[key].use_ref_ == 0);
    Entry& entry = cache_[key];
    lru_list_.erase(entry.iter);
    auto result = std::move(entry.value);
    cache_.erase(key);
    return result;
  }

  void ForeachValueInCache(const std::function<void(const Key& key, Value&)>& handler) {
    if (in_use_.empty() == false) {
      throw BptreeExecption("lrucache's ForeachValueInCache is called when in_use_.empty() == false");
    }
    for (auto& each : cache_) {
      handler(each.first, *each.second.value.get());
    }
  }

  void ForeachValueInTheReverseOrderOfLRUList(const std::function<bool(const Key& key, Value&)>& handler) {
    Counter visit_count("visit_count");
    for (auto it = lru_list_.rbegin(); it != lru_list_.rend(); ++it) {
      visit_count.Add();
      bool cont = handler(*it, *cache_[*it].value.get());
      if (cont == false) {
        break;
      }
    }
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
    BPTREE_LOG_INFO("---begin to print block_cache's info---");
    BPTREE_LOG_INFO("the length of the list in_use is {}", in_use_.size());
    BPTREE_LOG_INFO("the length of the list lru is {}", lru_list_.size());
    BPTREE_LOG_INFO("the size of the map cache is {}", cache_.size());
    BPTREE_LOG_INFO("----end to print block_cache's info----");
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

  size_t GetEntrySize() const { return cache_.size(); }

  size_t GetCapacity() const { return capacity_; }

 private:
  std::unordered_map<Key, Entry> cache_;
  std::list<Key> lru_list_;
  std::list<Key> in_use_;
  uint32_t capacity_;

  // 有些资源不便于在析构函数中释放，可以通过注册本callback进行处理
  free_functor free_notify_;
};

}  // namespace bptree