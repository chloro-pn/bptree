#pragma once

#include <cstdint>
#include <source_location>
#include <string>

#include "bptree/log.h"

namespace bptree {

class Metric {
 public:
  explicit Metric(const std::string& name, const double& value) : metric_name_(name), value_(value) {}

  const std::string& GetMetricName() const { return metric_name_; }

  virtual void Clear() { value_ = 0.0; }

  const double& GetValue() const { return value_; }

  void PrintToLog(const std::source_location location = std::source_location::current()) {
    BPTREE_LOG_INFO("metric name : {}, value : {}, file name : {}, line : {}, function name : {}", metric_name_, value_,
                    location.file_name(), location.line(), location.function_name());
  }

  virtual ~Metric() {}

 protected:
  std::string metric_name_;
  double value_;
};

class Counter : public Metric {
 public:
  Counter(const std::string& name) : Metric(name, 0) {}

  void Add(const double& v = 1.0) noexcept { value_ += v; }
};

class Gauge : public Metric {
 public:
  Gauge(const std::string& name) : Metric(name, 0.0) {}

  void Add(const double& v = 1.0) noexcept { value_ += v; }

  void Sub(const double& v = 1.0) noexcept { value_ -= v; }
};

}  // namespace bptree