package(default_visibility = ["//visibility:public"])

# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_library
cc_library(
  name = "bptree",
  srcs = glob(["src/*.cc", "src/*.h", "include/**/*.h"]),
  hdrs = glob(["include/bptree/block_manager.h"]),
  includes = ["include"],
  deps = [
    "@crc32//:crc32c",
    "@spdlog//:spdlog",
    "@mpmcqueue//:mpmc_queue",
    "@fmt//:fmt",
  ]
)

# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_binary
cc_binary(
  name = "bptree_write",
  srcs = ["example/bptree_write.cc", "example/helper.h"],
  deps = [
    ":bptree",
    "@com_github_gflags_gflags//:gflags",
    ]
)

# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_binary
cc_binary(
  name = "bptree_read",
  srcs = ["example/bptree_read.cc", "example/helper.h"],
  deps = [
    ":bptree",
    "@com_github_gflags_gflags//:gflags",
    ]
)

cc_binary(
  name = "leveldb_write",
  srcs = ["example/leveldb_write.cc", "example/helper.h"],
  deps = [
    "//third_party:leveldb",
    "@spdlog//:spdlog",
    "@com_github_gflags_gflags//:gflags",
  ]
)

cc_binary(
  name = "leveldb_read",
  srcs = ["example/leveldb_read.cc", "example/helper.h"],
  deps = [
    "//third_party:leveldb",
    "@spdlog//:spdlog",
    "@com_github_gflags_gflags//:gflags",
  ]
)

cc_binary(
  name = "block_print",
  srcs = ["tool/block_print.cc"],
  deps = [":bptree"],
)

cc_binary(
  name = "find_block_by_key",
  srcs = ["tool/find_block_by_key.cc"],
  deps = [":bptree"],
)

cc_binary(
  name = "block_filling_rate",
  srcs = ["tool/block_filling_rate.cc"],
  deps = [":bptree"],
)

cc_test(
  name = "bptree_test",
  srcs = glob(["test/*.cc"]),
  deps = [
    ":bptree",
    "@crc32//:crc32c",
    "@googletest//:gtest",
    "@googletest//:gtest_main",
    "@fmt//:fmt",
  ],
)