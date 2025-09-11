load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "api_filter_test",
    timeout = "short",
    srcs = [
        "aip_filter_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/redpanda/admin:aip_filter",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
