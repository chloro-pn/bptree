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
DEFINE_int32(random_or_sync, 0, "randomly read (0) or seq read (1) or half_seq(2)");

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  Timer tm;
  bptree::BlockManagerOption option;
  option.db_name = FLAGS_db_name;
  option.neflag = bptree::NotExistFlag::ERROR;
  option.eflag = bptree::ExistFlag::SUCC;
  option.mode = bptree::Mode::R;
  option.key_size = FLAGS_key_size;
  option.value_size = FLAGS_value_size;
  option.create_check_point_per_ops = 10000000;
  option.cache_size = FLAGS_cache_size;
  bptree::BlockManager manager(option);

  manager.PrintOption();

  auto kvs = ConstructRandomKv(FLAGS_kv_count, FLAGS_key_size, FLAGS_value_size);
  auto seq_kvs = kvs;
  std::sort(seq_kvs.begin(), seq_kvs.end(), [](const Entry& n1, const Entry& n2) -> bool { return n1.key < n2.key; });

  auto half_seq_kvs = kvs;
  std::vector<int> indexs;
  for (int i = 0; i < 1000; ++i) {
    indexs.push_back(i);
  }
  FisherYatesAlg(indexs);

  BPTREE_LOG_INFO("begin to get {} kvs", FLAGS_kv_count);
  tm.Start();
  if (FLAGS_random_or_sync == 0) {
    for (auto& each : kvs) {
      auto v = manager.Get(each.key);
      if (v != each.value) {
        BPTREE_LOG_ERROR("get check fail");
        return -1;
      }
    }
  } else if (FLAGS_random_or_sync == 1) {
    for (auto& each : seq_kvs) {
      auto v = manager.Get(each.key);
      if (v != each.value) {
        BPTREE_LOG_ERROR("get check fail");
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
        auto v = manager.Get(seq_kvs[index].key);
        if (v != seq_kvs[index].value) {
          BPTREE_LOG_ERROR("get check fail");
          return -1;
        }
      }
    }
  }
  auto ms = tm.End();
  BPTREE_LOG_INFO("get {} kvs use {} ms", FLAGS_kv_count, ms);

  manager.PrintCacheInfo();
  manager.PrintMetricSet();
  return 0;
}