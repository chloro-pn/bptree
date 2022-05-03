#pragma once

#include <vector>
#include <cassert>

namespace bptree {

class Bitmap {
 public:
  explicit Bitmap() {

  }

  void Init(uint32_t len) {
    bit_map_.resize(len, 0);
  }

  void Init(uint8_t* ptr, size_t len) {
    bit_map_.reserve(len);
    for(int i = 0; i < len; ++i) {
      bit_map_.push_back(ptr[i]);
    }
  }

  bool CheckFree(uint32_t index) const {
    uint32_t real_index = index / (8*sizeof(uint8_t));
    uint32_t nth_bit = index % (8*sizeof(uint8_t));
    return ((bit_map_[real_index] & (uint8_t(1) << nth_bit)) >> nth_bit) == 0;
  }

  uint32_t GetFirstFreeAndSet() {
    for(int i = 0; i < bit_map_.size(); ++i) {
      if (bit_map_[i] == std::numeric_limits<uint8_t>::max()) {
        continue;
      }
      for(int j = 0; j < 8; ++j) {
        if (((bit_map_[i] & (uint8_t(1) << j)) >> j) == 0) {
          uint8_t tmp = bit_map_[i] | (uint8_t(1) << j);
          bit_map_[i] = tmp;
          return i * sizeof(uint8_t) * 8 + j;
        }
      }
    }
    return std::numeric_limits<uint32_t>::max();
  }

  void SetUse(uint32_t index) {
    uint32_t real_index = index / (8*sizeof(uint8_t));
    uint32_t nth_bit = index % (8 * sizeof(uint8_t));
    assert(((bit_map_[real_index] & (uint8_t(1) << nth_bit)) >> nth_bit) == 0);
    uint8_t tmp = bit_map_[real_index] | (uint8_t(1) << nth_bit);
    bit_map_[real_index] = tmp;
  }

  void SetFree(uint32_t index) {
    // super block 不允许free掉
    assert(index > 0);
    uint32_t real_index = index / (8 * sizeof(uint8_t));
    uint32_t nth_bit = index % (8 * sizeof(uint8_t));
    assert(((bit_map_[real_index] & (uint8_t(1) << nth_bit)) >> nth_bit) == 1);
    uint8_t tmp = bit_map_[real_index] & (~(uint8_t(1) << nth_bit));
  }

  const void* ptr() const {
    assert(bit_map_.empty() == false);
    return &bit_map_[0];
  }

  size_t len() const {
    return bit_map_.size() * sizeof(uint8_t);
  }

 private:
  std::vector<uint8_t> bit_map_;
};

}