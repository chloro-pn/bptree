#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <set>
#include <string>

#include "gflags/gflags.h"
#include "helper.h"
#include "leveldb/cache.h"
#include "leveldb/db.h"
#include "spdlog/spdlog.h"

DEFINE_string(db_name, "test_db", "db name");
DEFINE_uint64(key_size, 10, "key size");
DEFINE_uint64(value_size, 100, "value size");
DEFINE_uint64(kv_count, 1000000, "kv count");
DEFINE_uint64(write_buffer_size, 4 * 1024 * 1024, "write buffer size");
DEFINE_uint64(cache_size, 16 * 1024 * 1024, "block cache size (4kb each block)");
DEFINE_int32(random_or_sync, 0, "randomly read (0) or seq read (1)");

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  Timer tm;

  leveldb::DB* db;
  leveldb::Options options;
  options.create_if_missing = false;
  options.error_if_exists = false;
  options.write_buffer_size = FLAGS_write_buffer_size;
  options.block_cache = leveldb::NewLRUCache(4 * 1024 * 2);
  leveldb::Status status = leveldb::DB::Open(options, FLAGS_db_name, &db);
  assert(status.ok());

  auto kvs = ConstructRandomKv(FLAGS_kv_count, FLAGS_key_size, FLAGS_value_size);
  FisherYatesAlg(kvs);
  auto seq_kvs = kvs;
  std::sort(seq_kvs.begin(), seq_kvs.end(), [](const Entry& n1, const Entry& n2) -> bool { return n1.key < n2.key; });

  auto half_seq_kvs = kvs;
  std::vector<int> indexs;
  for (int i = 0; i < 1000; ++i) {
    indexs.push_back(i);
  }
  FisherYatesAlg(indexs);

  spdlog::info("begin to get {} kvs", FLAGS_kv_count);
  tm.Start();
  if (FLAGS_random_or_sync == 0) {
    for (auto& each : kvs) {
      std::string value;
      auto s = db->Get(leveldb::ReadOptions(), each.key, &value);
      assert(s.ok());
      if (value != each.value) {
        spdlog::error("get check fail");
        return -1;
      }
    }
  } else if (FLAGS_random_or_sync == 1) {
    for (auto& each : seq_kvs) {
      std::string value;
      auto s = db->Get(leveldb::ReadOptions(), each.key, &value);
      assert(s.ok());
      if (value != each.value) {
        spdlog::error("get check fail");
        return -1;
      }
    }
  } else {
    for (int i = 0; i < 1000; ++i) {
      int base_index = indexs[i];
      auto edge_indexs = indexs;
      FisherYatesAlg(edge_indexs);
      for (int j = 0; j < 1000; ++j) {
        int index = base_index * 1000 + edge_indexs[j];
        std::string value;
        auto s = db->Get(leveldb::ReadOptions(), seq_kvs[index].key, &value);
        assert(s.ok());
        if (value != seq_kvs[index].value) {
          spdlog::error("get check fail");
          return -1;
        }
      }
    }
  }
  auto ms = tm.End();
  spdlog::info("get {} kvs use {} ms", FLAGS_kv_count, ms);
  return 0;
}