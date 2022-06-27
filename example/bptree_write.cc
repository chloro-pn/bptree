#include <cstdlib>
#include <filesystem>
#include <string>

#include "bptree/block_manager.h"
#include "gflags/gflags.h"
#include "helper.h"
#include "spdlog/spdlog.h"

DEFINE_string(db_name, "example_db", "db name");
DEFINE_uint64(key_size, 10, "key size");
DEFINE_uint64(value_size, 100, "value size");
DEFINE_uint64(kv_count, 1000000, "kv count");
DEFINE_uint64(cache_size, 1280, "block cache size (16kb each block)");
DEFINE_bool(sync_per_write, false, "sync per write");
DEFINE_bool(turn_off_double_write, false, "turn off double write");
DEFINE_int32(random_or_sync, 0, "randomly write (0) or seq write (1)");

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  Timer tm;
  bptree::BlockManagerOption option;
  option.db_name = FLAGS_db_name;
  option.neflag = bptree::NotExistFlag::CREATE;
  option.eflag = bptree::ExistFlag::ERROR;
  option.mode = bptree::Mode::WR;
  option.key_size = FLAGS_key_size;
  option.value_size = FLAGS_value_size;
  option.create_check_point_per_ops = 10000000;
  option.cache_size = FLAGS_cache_size;
  option.sync_per_write = FLAGS_sync_per_write;
  option.double_write_turn_off = FLAGS_turn_off_double_write;
  bptree::BlockManager manager(option);

  manager.PrintOption();

  auto kvs = ConstructRandomKv(FLAGS_kv_count, FLAGS_key_size, FLAGS_value_size);
  auto seq_kvs = kvs;
  std::sort(seq_kvs.begin(), seq_kvs.end(), [](const Entry& n1, const Entry& n2) -> bool { return n1.key < n2.key; });

  BPTREE_LOG_INFO("begin to insert {} kvs", FLAGS_kv_count);
  tm.Start();
  if (FLAGS_random_or_sync == 0) {
    for (auto& each : kvs) {
      manager.Insert(each.key, each.value);
    }
  } else {
    for (auto& each : seq_kvs) {
      manager.Insert(each.key, each.value);
    }
  }
  auto ms = tm.End();
  BPTREE_LOG_INFO("insert {} kvs use {} ms", FLAGS_kv_count, ms);

  manager.PrintCacheInfo();
  manager.PrintMetricSet();
  return 0;
}