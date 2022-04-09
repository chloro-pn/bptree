#include <cstddef>
#include <memory>
#include <vector>
#include <variant>
#include <algorithm>
#include <cassert>
#include <iostream>

namespace bptree {

size_t CeilD2(size_t i) {
  if (i % 2 == 0) {
    return i / 2;
  } else {
    return (i + 1) / 2;
  }
}

class TreeNode;

struct Merge {
  bool happen;

  Merge() : happen(false) {}

  static Merge Nothing() {
    Merge merge;
    return merge;
  }

  static Merge DoMerge() {
    Merge merge;
    merge.happen = true;
    return merge;
  }
};

struct Split {
  bool happen;
  std::unique_ptr<TreeNode> new_node;

  Split() : happen(false), new_node(nullptr) {

  }

  static Split Nothing() {
    Split tmp;
    tmp.happen = false;
    return tmp;
  }

  static Split DoSplit(std::unique_ptr<TreeNode>&& node) {
    Split tmp;
    tmp.happen = true;
    tmp.new_node = std::move(node);
    return tmp;
  }
};

inline std::ostream& print_level(int level) {
  for(int i = 0; i < level; ++i) {
    std::cout << " ";
  }
  std::cout << "[" << level << "]";
  return std::cout;
}

class TreeNode {
 public:
  friend class BPlusTree;

  using ele_type = std::pair<std::string, std::variant<std::string, std::unique_ptr<TreeNode>>>;

  explicit TreeNode(size_t height, size_t dimen) : height_(height), dimen_(dimen) {

  }

  std::unique_ptr<TreeNode>& GetNode(size_t index) {
    return std::get<1>(kvs_[index].second);
  }

  std::string GetValue(size_t index) {
    return std::get<0>(kvs_[index]);
  }

  std::string Get(const std::string& key) {
    if (height_ == 0) {
      return getLeaf(key);
    } else {
      return getInner(key);
    }
  }

  size_t ElementSize() const {
    return kvs_.size();
  }

  ele_type GetLastElement() {
    auto result = std::move(kvs_.back());
    kvs_.pop_back();
    return result;
  }

  ele_type GetFirstElement() {
    auto result = std::move(kvs_.front());
    kvs_.erase(kvs_.begin());
    return result;
  }
  
  std::string GetMaxKey() {
    assert(kvs_.empty() == false);
    return kvs_.back().first;
  }

  Split Insert(const std::string& key, const std::string& value) {
    if (height_ == 0) {
      return insertLeaf(key, value);
    } else {
      return insertInner(key, value);
    }
  }

  Merge Delete(const std::string& key) {
    if (height_ == 0) {
      return deleteLeaf(key);
    } else {
      return deleteInner(key);
    }
  }

  void Print(int level) const {
    print_level(level) << " height == " << height_ << std::endl;
    if (height_ == 0) {
      for(auto& each : kvs_) {
        print_level(level) << " key : " << each.first << " value : " << std::get<0>(each.second) << std::endl;
      }
    } else {
      for(auto& each : kvs_) {
        print_level(level) << " key : " << each.first << std::endl;
        std::get<1>(each.second)->Print(level + 1);
      }
    }
  }
  
 private:
  // 叶节点 == 0， 索引节点 > 0.
  size_t height_;
  const size_t dimen_;
  std::vector<ele_type> kvs_;

  std::string updateMaxKey(size_t i) {
    assert(GetNode(i)->kvs_.size() != 0);
    std::string key = GetNode(i)->kvs_.back().first;
    std::string old_key = kvs_[i].first;
    kvs_[i].first = key;
    return old_key;
  }

  void insertElement(ele_type&& obj) {
    kvs_.push_back(std::move(obj));
    std::sort(kvs_.begin(), kvs_.end(), [](const ele_type& ele1, const ele_type& ele2) -> bool {
      return ele1.first < ele2.first;
    });
  }

  // 将子节点c2的数据合并在c1中，删除c2子节点，并返回c1下标。
  size_t MergeChild(size_t c1, size_t c2) {
    if (c1 == c2) {
      return c1;
    }
    if (c1 > c2) {
      return MergeChild(c2, c1);
    }
    auto& eles = GetNode(c2)->kvs_;
    for(auto&& each : eles) {
      GetNode(c1)->kvs_.push_back(std::move(each));
    }
    updateMaxKey(c1);
    auto it = kvs_.begin();
    std::advance(it, c2);
    kvs_.erase(it);
    return c1;
  }

  Split insertInner(const std::string& key, const std::string& value) {
    bool insert = false;
    size_t insert_index = 0;
    Split split;
    for(int i = 0; i < kvs_.size(); ++i) {
      if (key <= kvs_[i].first) {
        split = GetNode(i)->Insert(key, value);
        insert_index = i;
        insert = true;
        break;
      }
    }
    // 插入到最后一个子节点中
    if (insert == false) {
      if (kvs_.empty() == true) {
        kvs_.push_back({key, std::unique_ptr<TreeNode>(new TreeNode(height_ - 1, dimen_))});
        split = std::get<1>(kvs_.back().second)->Insert(key, value);
      } else {
        kvs_.back().first = key;
        split = std::get<1>(kvs_.back().second)->Insert(key, value);
      }
      insert_index = kvs_.size() - 1;
    }
    return HandleSplit(insert_index, std::move(split));
  }

  Split insertLeaf(const std::string& key, const std::string& value) {
    for(int i = 0; i < kvs_.size(); ++i) {
      if (kvs_[i].first == key) {
        std::get<0>(kvs_[i].second) = value;
        return Split::Nothing();
      }
    }
    kvs_.push_back({key, value});
    std::sort(kvs_.begin(), kvs_.end(), [](const ele_type& v1, const ele_type& v2) -> bool {
      return v1.first < v2.first;
    });
    if (kvs_.size() > dimen_) {
      int split_num = kvs_.size() / 2;
      std::unique_ptr<TreeNode> new_node(new TreeNode(0, dimen_));
      int move_num = 0;
      for(int i = split_num; i < kvs_.size(); ++i) {
        new_node->kvs_.push_back(std::move(kvs_[i]));
        move_num += 1;
      }
      for(int i = 0; i < move_num; ++i) {
        kvs_.pop_back();
      }
      return Split::DoSplit(std::move(new_node));
    } else {
      return Split::Nothing();
    }
  }

  std::string getInner(const std::string& key) {
    for(int i = 0; i < kvs_.size(); ++i) {
      if (key <= kvs_[i].first) {
        return GetNode(i)->Get(key);
      }
    }
    return "";
  }

  std::string getLeaf(const std::string& key) {
    for(int i = 0; i < kvs_.size(); ++i) {
      if (key == kvs_[i].first) {
        return GetValue(i);
      }
    }
    return "";
  }

  Merge deleteLeaf(const std::string& key) {
    for(int i = 0; i < kvs_.size(); ++i) {
      if (key == kvs_[i].first) {
        auto it = kvs_.begin();
        std::advance(it, i);
        kvs_.erase(it);
        break;
      }
    }
    if (kvs_.size() < CeilD2(dimen_)) {
      return Merge::DoMerge();
    } else {
      return Merge::Nothing();
    }
  }

  Merge deleteInner(const std::string& key) {
    for(int i = 0; i < kvs_.size(); ++i) {
      if (key <= kvs_[i].first) {
        Merge merge = GetNode(i)->Delete(key);
        if (key == kvs_[i].first && GetNode(i)->ElementSize() != 0) {
          updateMaxKey(i);
        }
        return HandleMerge(i, merge);
      }
    }
    return Merge::Nothing();
  }

  // 无论被split的是叶子节点还是内部节点，都用这一套逻辑处理
  Split HandleSplit(size_t insert_index, Split&& split) {
    if (split.happen == false) {
      return Split::Nothing();
    }
    assert(height_ > 0);
    updateMaxKey(insert_index);
    auto it = kvs_.begin();
    std::advance(it, insert_index + 1);
    std::string key = split.new_node->GetMaxKey();
    kvs_.insert(it, {key, std::move(split.new_node)});
    if (kvs_.size() <= dimen_) {
      return Split::Nothing();
    } else {
      int split_num = kvs_.size() / 2;
      std::unique_ptr<TreeNode> new_node(new TreeNode(height_, dimen_));
      int move_num = 0;
      for(int i = split_num; i < kvs_.size(); ++i) {
        new_node->kvs_.push_back(std::move(kvs_[i]));
        move_num += 1;
      }
      for(int i = 0; i < move_num; ++i) {
        kvs_.pop_back();
      }
      return Split::DoSplit(std::move(new_node));
    }
  }

  // 需要处理子节点size为1的情况
  Merge HandleMerge(size_t delete_index, Merge merge) {
    if (merge.happen == false) {
      return merge;
    }
    // 如果只有一个子节点，并且该子节点的元素个数为0，则删除这个子节点，
    // 并且返回DoMerge，由父节点继续删除操作。
    if (ElementSize() == 1 && GetNode(0)->ElementSize() == 0) {
      kvs_.clear();
      return Merge::DoMerge();
    }
    // 尝试找前一个子节点借用
    if (delete_index > 0) {
      size_t element_size = GetNode(delete_index - 1)->ElementSize();
      if (element_size > CeilD2(dimen_)) {
        auto move_obj = GetNode(delete_index - 1)->GetLastElement();
        // update max key
        updateMaxKey(delete_index - 1);
        // insert move_obj.
        GetNode(delete_index)->insertElement(std::move(move_obj));
        return Merge::Nothing();
      } else {
        // 合并
        size_t new_index = MergeChild(delete_index, delete_index - 1);
        if (ElementSize() < CeilD2(dimen_)) {
          return Merge::DoMerge();
        }
      }
    }
    if (delete_index + 1 < kvs_.size()) {
      size_t element_size = GetNode(delete_index + 1)->ElementSize();
      if (element_size > CeilD2(dimen_)) {
        auto move_obj = GetNode(delete_index + 1)->GetFirstElement();
        // update max key
        // insert move_obj to myself.
        GetNode(delete_index)->insertElement(std::move(move_obj));
        updateMaxKey(delete_index);
        return Merge::Nothing();
      } else {
        size_t new_index = MergeChild(delete_index, delete_index + 1);
        if (ElementSize() < CeilD2(dimen_)) {
          return Merge::DoMerge();
        }        
      }
    }
    return Merge::Nothing();
  }
};

class BPlusTree {
 public:
  explicit BPlusTree(size_t dimen) : height_(1), dimen_(dimen) {

  }
  
  std::string Get(const std::string& key) {
    if (root_ != nullptr) {
      return root_->Get(key);
    }
    return "";
  }

  void Insert(const std::string& key, const std::string& value) {
    if (root_ == nullptr) {
      root_.reset(new TreeNode(height_, dimen_));
    }
    Split split = root_->Insert(key, value);
    if (split.happen == true) {
      auto old_root = std::move(root_);
      root_ = std::unique_ptr<TreeNode>(new TreeNode(old_root->height_ + 1, dimen_));
      std::string key1 = old_root->GetMaxKey();
      std::string key2 = split.new_node->GetMaxKey();
      root_->kvs_.push_back({key1, std::move(old_root)});
      root_->kvs_.push_back({key2, std::move(split.new_node)});
      height_ = root_->height_;
    }
  }

  void Delete(const std::string& key) {
    if (root_ == nullptr) {
      return;
    }
    // 根节点不处理
    root_->Delete(key);
  }

  void Print() const {
    root_->Print(0);
  }

 private:
  size_t height_;
  const size_t dimen_;
  std::unique_ptr<TreeNode> root_;
};

} // namespace bptree