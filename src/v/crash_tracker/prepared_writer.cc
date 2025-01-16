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

#include "crash_tracker/prepared_writer.h"

#include "crash_tracker/logger.h"
#include "crash_tracker/types.h"
#include "hashing/xx.h"
#include "model/timestamp.h"

#include <seastar/core/file-types.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/print_safe.hh>

#include <fmt/chrono.h>

#include <chrono>
#include <fcntl.h>
#include <system_error>
#include <unistd.h>

using namespace std::chrono_literals;

namespace crash_tracker {

std::ostream& operator<<(std::ostream& os, prepared_writer::state s) {
    switch (s) {
    case prepared_writer::state::uninitialized:
        return os << "uninitialized";
    case prepared_writer::state::initialized:
        return os << "initialized";
    case prepared_writer::state::filled:
        return os << "filled";
    case prepared_writer::state::written:
        return os << "written";
    case prepared_writer::state::released:
        return os << "released";
    }
}

ss::future<>
prepared_writer::initialize(std::filesystem::path crash_file_path) {
    _crash_report_file_name = std::move(crash_file_path);

    _serde_output.reserve_memory(crash_description::serde_size_overestimate);

    // Create the crash recorder file
    auto f = co_await ss::open_file_dma(
      _crash_report_file_name.c_str(),
      ss::open_flags::create | ss::open_flags::rw | ss::open_flags::truncate
        | ss::open_flags::exclusive);
    co_await f.close();

    // Sync the parent dir to ensure that the newly create file is observable to
    // the ::open() call below and to later restarts of the process
    co_await ss::sync_directory(_crash_report_file_name.parent_path().string());

    // Open the crash recorder file using ::open().
    // We need to use the low level open() function here instead of the seastar
    // API or higher-level C++ primitives because we need to be able to
    // manipulate the file using async-signal-safe, allocation-free functions
    // inside signal handlers.
    _fd = ::open(_crash_report_file_name.c_str(), O_WRONLY);
    if (_fd == -1) {
        throw std::system_error(
          errno,
          std::system_category(),
          fmt::format(
            "Failed to open {} to record crash reason",
            _crash_report_file_name));
    }

    _state = state::initialized;
}

crash_description& prepared_writer::fill() {
    vassert(_state == state::initialized, "Unexpected state: {}", _state);
    _state = state::filled;
    _prepared_cd.crash_time = model::timestamp::now();
    return _prepared_cd;
}

void prepared_writer::write() {
    vassert(_state == state::filled, "Unexpected state: {}", _state);
    _state = state::written;

    if (try_write_crash()) {
        constexpr static std::string_view success
          = "Recorded crash reason to crash file.\n";
        ss::print_safe(success.data(), success.size());
    } else {
        constexpr static std::string_view failure
          = "Failed to record crash reason to crash file.\n";
        ss::print_safe(failure.data(), failure.size());
    }
}

bool prepared_writer::try_write_crash() {
    bool success = true;
    serde::write(_serde_output, std::move(_prepared_cd));

    for (const auto& frag : _serde_output) {
        size_t written = 0;
        while (written < frag.size()) {
            auto res = ::write(
              _fd, frag.get() + written, frag.size() - written);
            if (res == -1) {
                // Return that writing the crash failed but try to continue to
                // write later fragments as much information as possible
                success = false;
                break;
            }
            written += res;
        }
    }

    ::fsync(_fd);

    return success;
}

ss::future<> prepared_writer::release() {
    vassert(_state != state::released, "Unexpected state: {}", _state);

    if (_state != state::uninitialized) {
        ::close(_fd);

        // Remove the file and sync the parent dir to ensure that later restarts
        // of the process cannot observe this crash report file
        co_await ss::remove_file(_crash_report_file_name.c_str());
        co_await ss::sync_directory(
          _crash_report_file_name.parent_path().string());

        vlog(
          ctlog.debug,
          "Deleted crash report file: {}",
          _crash_report_file_name);
    }

    _state = state::released;

    co_return;
}

} // namespace crash_tracker
