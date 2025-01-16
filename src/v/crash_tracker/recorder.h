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

#pragma once

#include "base/seastarx.h"
#include "crash_tracker/prepared_writer.h"
#include "crash_tracker/types.h"

namespace crash_tracker {

/// Thread-safe global singleton crash recorder
/// The singleton pattern is used to allow access to the recorder from signal
/// handlers which have to be static functions (/non-capturing lambdas).
class recorder {
public:
    struct recorded_crash {
        crash_description crash;

        ss::future<bool> is_uploaded() const;
        ss::future<> mark_uploaded() const;
    };

    ss::future<> start();
    ss::future<> stop();

    /// Async-signal safe
    void record_crash_sighandler(int signo);

    void record_crash_exception(std::exception_ptr eptr);

    ss::future<std::vector<recorded_crash>> get_recorded_crashes() const;

private:
    recorder() = default;
    ~recorder() = default;

    /// The writer has shared state, so accessing it from multiple threads
    /// is guarded by this lock. We are using the async-signal safe
    /// ss::util::spinlock instead of more efficient locking mechanisms like the
    /// seastar primitives or std::mutex to ensure that we can safely use the
    /// lock in a singal handler context.
    ss::util::spinlock _writer_lock;
    prepared_writer _writer;

    friend recorder& get_recorder();
};

/// Singleton access to global static recorder
recorder& get_recorder();

} // namespace crash_tracker
