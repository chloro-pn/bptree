#pragma once

#include <concepts>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include "bptree/metric/metric.h"

namespace bptree {

class MetricSet {
 public:
  MetricSet() {}

  template <typename MetricType>
  requires std::derived_from<MetricType, Metric> void CreateMetric(const std::string& name) {
    metrics_[name] = std::make_unique<MetricType>(name);
  }

  template <typename MetricType>
  requires std::derived_from<MetricType, Metric> MetricType* GetAs(const std::string& name) {
    auto it = metrics_.find(name);
    if (it != metrics_.end()) {
      return dynamic_cast<MetricType*>(it->second.get());
    }
    return nullptr;
  }

  std::optional<double> GetValue(const std::string& name) {
    auto it = metrics_.find(name);
    if (it != metrics_.end()) {
      return it->second->GetValue();
    }
    return std::nullopt;
  }

  void Print() {
    BPTREE_LOG_INFO("-----print metric set-----");
    for(auto& each : metrics_) {
      each.second->PrintToLog();
    }
    BPTREE_LOG_INFO("---------end print---------")
  }

 private:
  std::unordered_map<std::string, std::unique_ptr<Metric>> metrics_;
};

}  // namespace bptree