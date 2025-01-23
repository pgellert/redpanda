/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "crash_tracker/recorder.h"

#include "config/node_config.h"
#include "crash_tracker/logger.h"
#include "crash_tracker/types.h"
#include "model/timestamp.h"
#include "random/generators.h"

#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/print_safe.hh>

#include <chrono>

using namespace std::chrono_literals;

namespace crash_tracker {

static constexpr std::string_view crash_report_suffix = ".crash";

recorder& get_recorder() {
    static recorder inst;
    return inst;
}

ss::future<> recorder::start() {
    // Ensure that the crash report directory exists
    auto crash_report_dir = config::node().crash_report_dir_path();
    if (!co_await ss::file_exists(crash_report_dir.string())) {
        vlog(
          ctlog.info,
          "Creating crash report directory {}",
          crash_report_dir.string());
        co_await ss::recursive_touch_directory(crash_report_dir.string());
        vlog(
          ctlog.debug,
          "Successfully created crash report directory {}",
          crash_report_dir.string());
    }

    // Loop a few times to avoid (very unlikely) collisions in the filename
    std::optional<std::filesystem::path> crash_file_name{};
    for (int i = 0; i < 10; ++i) {
        auto time_now = model::timestamp::now().value();
        auto random_int = random_generators::get_int(0, 10000);
        auto try_name = crash_report_dir
                        / fmt::format(
                          "{}_{}{}", time_now, random_int, crash_report_suffix);
        if (co_await ss::file_exists(try_name.string())) {
            // Try again in the rare case of a collision
            continue;
        }

        crash_file_name = try_name;
        break;
    }
    if (!crash_file_name) {
        // The anti-collision above should ensure that we never reach this
        throw std::runtime_error(
          "Failed to create a unique crash recorder file");
    }

    // Note: _writer_guard ensures the visibility of changes to the
    // buffers/state of _writer are visible across threads
    vassert(
      !_writer_guard.exchange(true, std::memory_order_acquire),
      "start() runs before any other call, so _writer_guard must have been "
      "false");
    auto defer = ss::defer(
      [&]() { _writer_guard.exchange(false, std::memory_order_release); });
    co_await _writer.initialize(*crash_file_name);
}

namespace {

void record_backtrace(crash_description& cd) {
    size_t pos = 0;
    ss::backtrace([&cd, &pos](ss::frame f) {
        if (pos >= cd.stacktrace.size()) {
            return; // Prevent buffer overflow
        }

        const bool first = pos == 0;
        auto result = fmt::format_to_n(
          cd.stacktrace.begin() + pos,
          cd.stacktrace.size() - pos,
          "{}{:#x}",
          first ? "" : " ",
          f.addr);

        pos += result.size;
    });
}

void print_skipping() {
    constexpr static std::string_view skipping
      = "Skipping recording crash reason to crash file.\n";
    ss::print_safe(skipping.data(), skipping.size());
}

} // namespace

void recorder::record_crash_exception(std::exception_ptr eptr) {
    if (is_crash_loop_limit_reached(eptr)) {
        // We specifically do not want to record crash_loop_limit_reached errors
        // as crashes because they are not informative and would build up
        // garbage on disk and would force to expire earlier useful crash logs.
        return;
    }

    bool before = false;
    if (!_writer_guard.compare_exchange_strong(
          before, true, std::memory_order_acquire)) {
        // If the _writer_guard is true, it must have been because another crash
        // is already being recorded. In this case, give up and skip recording
        // this crash. Do not spin trying to set this guard to avoid deadlock.
        print_skipping();
        return;
    }
    auto defer = ss::defer(
      [&]() { _writer_guard.exchange(false, std::memory_order_release); });
    if (!_writer.initialized()) {
        // The writer must have already been consumed, so do not record a crash
        // in this case.
        print_skipping();
        return;
    }

    auto& cd = _writer.fill();

    record_backtrace(cd);
    cd.type = crash_type::startup_exception;

    auto& format_buf = cd.crash_message;
    fmt::format_to_n(
      format_buf.begin(),
      format_buf.size(),
      "Failure during startup: {}",
      eptr);

    _writer.write();
}

ss::future<> recorder::stop() {
    vassert(
      !_writer_guard.exchange(true, std::memory_order_acquire),
      "stop() runs only after start() or completed record_crash_exception() so "
      "noone could be holding the lock here.");
    co_await _writer.release();
}

} // namespace crash_tracker
