load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "kafka_connections",
    srcs = ["kafka_connections.cc"],
    hdrs = ["kafka_connections.h"],
    visibility = ["//src/v/redpanda:__subpackages__"],
    deps = [
        "//proto/redpanda/core/admin/v2:kafka_connections_redpanda_proto",
        "//src/v/base",
        "//src/v/container:chunked_vector",
        "//src/v/kafka/server",
        "//src/v/redpanda/admin/proxy:client",
        "//src/v/serde/protobuf:rpc",
        "//src/v/utils:uuid",
        "@fmt",
        "@seastar",
    ],
)
