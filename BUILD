package(default_visibility = ["//visibility:public"])

# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_library
cc_library(
  name = "bptree",
  srcs = glob(["src/*.cc", "src/*.h", "include/**/*.h"]),
  hdrs = glob(["include/bptree/block_manager.h"]),
  includes = ["include"],
  deps = [
    "@crc32//:crc32c",
  ]
)

# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_binary
cc_binary(
  name = "get_range",
  srcs = ["example/get_range.cc"],
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
  ],
)