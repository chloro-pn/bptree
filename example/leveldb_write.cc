#include <cassert>
#include <cstdlib>
#include <filesystem>
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
DEFINE_int32(random_or_sync, 0, "randomly write (0) or seq write (1)");

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  Timer tm;

  leveldb::DB* db;
  leveldb::Options options;
  options.create_if_missing = true;
  options.error_if_exists = true;
  options.write_buffer_size = FLAGS_write_buffer_size;
  options.block_cache = leveldb::NewLRUCache(FLAGS_cache_size);
  leveldb::Status status = leveldb::DB::Open(options, FLAGS_db_name, &db);
  assert(status.ok());

  auto kvs = ConstructRandomKv(FLAGS_kv_count, FLAGS_key_size, FLAGS_value_size);
  auto seq_kvs = kvs;
  std::sort(seq_kvs.begin(), seq_kvs.end(), [](const Entry& n1, const Entry& n2) -> bool { return n1.key < n2.key; });

  spdlog::info("begin to insert {} kvs", FLAGS_kv_count);
  tm.Start();
  if (FLAGS_random_or_sync == 0) {
    for (auto& each : kvs) {
      auto s = db->Put(leveldb::WriteOptions(), each.key, each.value);
      assert(s.ok());
    }
  } else {
    for (auto& each : seq_kvs) {
      auto s = db->Put(leveldb::WriteOptions(), each.key, each.value);
      assert(s.ok());
    }
  }
  auto ms = tm.End();
  spdlog::info("insert {} kvs use {} ms", FLAGS_kv_count, ms);
  return 0;
}