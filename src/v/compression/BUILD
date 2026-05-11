load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "compression",
    srcs = [
        "async_stream_zstd.cc",
        "compression.cc",
        "gzip_stream_decompression.cc",
        "internal/gzip_compressor.cc",
        "internal/lz4_frame_compressor.cc",
        "internal/snappy_java_compressor.cc",
        "lz4_decompression_buffers.cc",
        "snappy_standard_compressor.cc",
        "stream_zstd.cc",
    ],
    hdrs = [
        "async_stream_zstd.h",
        "compression.h",
        "gzip_stream_decompression.h",
        "internal/gzip_compressor.h",
        "internal/lz4_frame_compressor.h",
        "internal/snappy_java_compressor.h",
        "internal/zstd_compressor.h",
        "lz4_decompression_buffers.h",
        "snappy_standard_compressor.h",
        "stream_zstd.h",
    ],
    # The BCR `zlib` module (1.3.1.bcr.8) declares `zlib.h` twice in its
    # modulemap: once as a public `textual header` and once as `private
    # textual header`. Under layering_check, clang resolves `<zlib.h>` via
    # the source include path first, hitting the private declaration and
    # erroring with `-Wprivate-header` (which `-Werror` upgrades to a hard
    # failure). Opt out of layering_check for this one target until the
    # BCR module is fixed upstream.
    features = ["-layering_check"],
    local_defines = ["ZSTD_STATIC_LINKING_ONLY"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:ioarray",
        "//src/v/bytes:iobuf",
        "//src/v/utils:object_pool",
        "//src/v/utils:static_deleter_fn",
        "@fmt",
        "@lz4",
        "@lz4//:lz4_frame",
        "@seastar",
        "@snappy",
        "@zlib",
        "@zstd",
    ],
)
