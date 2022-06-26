#include <cstdlib>
#include <cassert>
#include <filesystem>
#include <string>

#include "gflags/gflags.h"
#include "spdlog/spdlog.h"
#include "leveldb/db.h"

DEFINE_uint64(key_size, 10, "key size");
DEFINE_uint64(value_size, 100, "value size");
DEFINE_uint64(kv_count, 100000, "kv count");
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

  leveldb::DB* db;
  leveldb::Options options;
  options.create_if_missing = true;
  options.error_if_exists = true;
  leveldb::Status status = leveldb::DB::Open(options, "test_db", &db);
  assert(status.ok());

  auto kvs = ConstructRandomKv(FLAGS_kv_count, FLAGS_key_size, FLAGS_value_size);

  spdlog::info("begin to randomly insert {} kvs", FLAGS_kv_count);
  tm.Start();
  for (auto& each : kvs) {
    auto s = db->Put(leveldb::WriteOptions(), each.key, each.value);
    assert(s.ok());
  }
  auto ms = tm.End();
  spdlog::info("randomly insert {} kvs use {} ms", FLAGS_kv_count, ms);
  tm.Start();
  for (size_t i = 0; i < FLAGS_kv_count; ++i) {
    std::string value;
    auto s = db->Get(leveldb::ReadOptions(), kvs[i].key, &value);
    assert(s.ok());
    if (value != kvs[i].value) {
      spdlog::error("insert-get check fail");
      return -1;
    }
  }
  ms = tm.End();
  spdlog::info("randomly get {} kvs use {} ms", FLAGS_kv_count, ms);
  spdlog::info("insert-get check succ");

  std::sort(kvs.begin(), kvs.end(), [](const Entry& n1, const Entry& n2) -> bool { return n1.key < n2.key; });

  spdlog::info("begin to randomly delete 10000 kvs");
  tm.Start();
  for (int i = 0; i < 10000; ++i) {
    int delete_index = rand() % kvs.size();
    auto s = db->Delete(leveldb::WriteOptions(), kvs[delete_index].key);
    assert(s.ok());
    kvs[delete_index].delete_ = true;
  }
  ms = tm.End();
  spdlog::info("randomly delete {} kvs use {} ms", 10000, ms);

  spdlog::info("all check succ");
  return 0;
}