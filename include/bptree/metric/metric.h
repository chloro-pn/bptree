#pragma once

#include <cstdint>
#include <string>

namespace bptree {

template <typename T>
class Metric {
 public:
  explicit Metric(const std::string& name, const T& value) : metric_name_(name), value_(value) {}

  const std::string& GetMetricName() const { return metric_name_; }

  virtual void UpdateValue(const T& t) = 0;

  virtual void Clear() = 0;

  const T& GetValue() const { return value_; }

 protected:
  std::string metric_name_;
  T value_;
};

class Counter : public Metric<uint64_t> {
 public:
  Counter(const std::string& name) : Metric(name, 0) {}

  void UpdateValue(const uint64_t& v) override { value_ += v; }

  void Clear() override { value_ = 0; }
};

}  // namespace bptree