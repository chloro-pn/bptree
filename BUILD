package(default_visibility = ["//visibility:public"])

# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_library
cc_library(
  name = "bptree",
  srcs = glob(["src/*.cc", "src/*.h", "include/**/*.h"]),
  hdrs = glob(["include/bptree/block_manager.h"]),
  includes = ["include"],
)

# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_binary
cc_binary(
  name = "example",
  srcs = ["example/main.cc"],
  deps = [":bptree"],
)

cc_test(
  name = "bptree_test",
  srcs = glob(["test/*.cc"]),
  deps = [
    ":bptree",
    "@googletest//:gtest",
    "@googletest//:gtest_main",
  ],
)