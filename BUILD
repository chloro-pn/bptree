package(default_visibility = ["//visibility:public"])

# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_library
cc_library(
  name = "bptree",
  srcs = glob(["src/*.cc", "src/*.h"]),
  hdrs = glob(["include/**/*.h"]),
  includes = ["include"],
  copts = [
    "-Isrc"
  ]
)

# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_binary
cc_binary(
  name = "example",
  srcs = ["example/main.cc"],
  deps = [":bptree"],
)