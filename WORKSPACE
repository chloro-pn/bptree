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
    branch = "v1.x",
    build_file = "//third_party:spdlog.build",
)