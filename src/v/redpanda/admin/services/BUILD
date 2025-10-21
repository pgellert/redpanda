load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/redpanda/admin:__subpackages__"])

redpanda_cc_library(
    name = "broker",
    srcs = ["broker.cc"],
    hdrs = ["broker.h"],
    deps = [
        "//proto/redpanda/core/admin/v2:broker_redpanda_proto",
        "//proto/redpanda/core/admin/v2:kafka_connections_redpanda_proto",
        "//src/v/features",
        "//src/v/redpanda/admin:kafka_connections_service",
        "//src/v/redpanda/admin/proxy:client",
        "//src/v/serde/protobuf:rpc",
        "//src/v/version",
        "@seastar",
    ],
)
