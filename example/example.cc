#include <cstdlib>
#include <filesystem>
#include <string>

#include "bptree/block_manager.h"
#include "gflags/gflags.h"
#include "spdlog/spdlog.h"

DEFINE_uint64(key_size, 10, "key size");
DEFINE_uint64(value_size, 100, "value size");
DEFINE_uint64(kv_count, 100000, "kv count");
DEFINE_uint64(cache_size, 1024, "cache size");
DEFINE_bool(sync_per_write, false, "sync per write");

#include <chrono>

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

std::string ConstructRandomStr(size_t size) {
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

std::vector<Entry> ConstructRandomKv(size_t size, size_t key_size, size_t value_size) {
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

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  Timer tm;

  bptree::BlockManagerOption option;
  option.db_name = "example_db";
  option.neflag = bptree::NotExistFlag::CREATE;
  option.eflag = bptree::ExistFlag::ERROR;
  option.mode = bptree::Mode::WR;
  option.key_size = FLAGS_key_size;
  option.value_size = FLAGS_value_size;
  option.create_check_point_per_ops = 10000000;
  option.cache_size = FLAGS_cache_size;
  option.sync_per_write = FLAGS_sync_per_write;

  bptree::BlockManager manager(option);
  manager.PrintOption();

  auto kvs = ConstructRandomKv(FLAGS_kv_count, FLAGS_key_size, FLAGS_value_size);

  BPTREE_LOG_INFO("begin to randomly insert {} kvs", FLAGS_kv_count);
  tm.Start();
  for (auto& each : kvs) {
    manager.Insert(each.key, each.value);
  }
  auto ms = tm.End();
  BPTREE_LOG_INFO("randomly insert {} kvs use {} ms", FLAGS_kv_count, ms);
  tm.Start();
  for (size_t i = 0; i < FLAGS_kv_count; ++i) {
    auto v = manager.Get(kvs[i].key);
    if (v != kvs[i].value) {
      BPTREE_LOG_ERROR("insert-get check fail");
      return -1;
    }
  }
  ms = tm.End();
  BPTREE_LOG_INFO("randomly get {} kvs use {} ms", FLAGS_kv_count, ms);
  BPTREE_LOG_INFO("insert-get check succ");

  std::sort(kvs.begin(), kvs.end(), [](const Entry& n1, const Entry& n2) -> bool { return n1.key < n2.key; });

  BPTREE_LOG_INFO("begin to randomly delete 10000 kvs");
  tm.Start();
  for (int i = 0; i < 10000; ++i) {
    int delete_index = rand() % kvs.size();
    manager.Delete(kvs[delete_index].key);
    kvs[delete_index].delete_ = true;
  }
  ms = tm.End();
  BPTREE_LOG_INFO("randomly delete {} kvs use {} ms", 10000, ms);

  std::vector<Entry> kvs_after_delete;
  for (auto& each : kvs) {
    if (each.delete_ == false) {
      kvs_after_delete.push_back(each);
    }
  }

  size_t count = 0;
  auto get_kvs =
      manager.GetRange(kvs_after_delete[0].key, [&count](const bptree::Entry& entry) -> bptree::GetRangeOption {
        if (count == 1000) {
          return bptree::GetRangeOption::STOP;
        }
        ++count;
        return bptree::GetRangeOption::SELECT;
      });

  BPTREE_LOG_INFO("range-get the first 1000 kvs and check them");
  for (size_t i = 0; i < get_kvs.size(); ++i) {
    if (kvs_after_delete[i].key != get_kvs[i].first || kvs_after_delete[i].value != get_kvs[i].second) {
      BPTREE_LOG_INFO("range-get check error, {} != {} or {} != {}", kvs_after_delete[i].key, get_kvs[i].first,
                      kvs_after_delete[i].value, get_kvs[i].second);
      return -1;
    }
  }

  BPTREE_LOG_INFO("all check succ");
  manager.PrintMetricSet();
  return 0;
}