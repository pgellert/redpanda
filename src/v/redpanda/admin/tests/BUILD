load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "api_filter_test",
    timeout = "short",
    srcs = [
        "aip_filter_test.cc",
    ],
    deps = [
        "//proto/redpanda/core/admin/v2:kafka_connections_redpanda_proto",
        "//src/v/base",
        "//src/v/redpanda/admin:aip_filter",
        "//src/v/redpanda/admin:field_registry",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
