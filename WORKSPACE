load('@bazel_tools//tools/build_defs/repo:git.bzl', 'git_repository', 'new_git_repository')

git_repository(
    name = "googletest",
    remote = "https://github.com/google/googletest",
    tag = "release-1.11.0",
)

new_git_repository(
    name = "crc32",
    remote = "https://github.com/gityf/crc",
    branch = "master",
    build_file = "//third_party:crc.build",
)

new_git_repository(
    name = "spdlog",
    remote = "https://github.com/gabime/spdlog",
    tag = "v1.10.0",
    build_file = "//third_party:spdlog.build",
)

new_git_repository(
    name = "mpmcqueue",
    remote = "https://ghproxy.com/https://github.com/rigtorp/MPMCQueue",
    branch = "master",
    build_file = "//third_party:mpmc_queue.build",
)

git_repository(
    name = "fmt",
    tag = "8.1.1",
    remote = "https://github.com/fmtlib/fmt",
    patch_cmds = [
        "mv support/bazel/.bazelrc .bazelrc",
        "mv support/bazel/.bazelversion .bazelversion",
        "mv support/bazel/BUILD.bazel BUILD.bazel",
        "mv support/bazel/WORKSPACE.bazel WORKSPACE.bazel",
    ],
)