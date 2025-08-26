load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "kafka_connections",
    srcs = ["kafka_connections.cc"],
    hdrs = ["kafka_connections.h"],
    visibility = ["//src/v/redpanda:__subpackages__"],
    deps = [
        "//proto/redpanda/core/admin:kafka_connections_redpanda_proto",
        "//src/v/base",
        "//src/v/kafka/server",
        "//src/v/redpanda/admin/proxy:client",
        "//src/v/serde/protobuf:rpc",
        "@fmt",
        "@seastar",
    ],
)
