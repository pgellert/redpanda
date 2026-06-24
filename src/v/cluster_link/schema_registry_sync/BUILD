load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "source_reader",
    srcs = [
        "unavailable_source_reader.cc",
    ],
    hdrs = [
        "source_reader.h",
        "unavailable_source_reader.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/container:chunked_vector",
        "//src/v/pandaproxy/schema_registry:types",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "scope",
    srcs = [
        "scope.cc",
    ],
    hdrs = [
        "scope.h",
    ],
    implementation_deps = [
        "//src/v/ssx:sformat",
        "@abseil-cpp//absl/container:flat_hash_set",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cluster_link/model",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/pandaproxy/schema_registry:types",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "reconciler",
    srcs = [
        "reconciler.cc",
    ],
    hdrs = [
        "reconciler.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/cluster_link:logger",
        "//src/v/pandaproxy/schema_registry:exceptions",
        "//src/v/ssx:future_util",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":scope",
        ":source_reader",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/pandaproxy/schema_registry:types",
        "//src/v/schema:registry",
        "//src/v/ssx:semaphore",
        "@seastar",
    ],
)
