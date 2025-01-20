/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "crash_tracker/limiter.h"
#include "crash_tracker/recorder.h"
#include "crash_tracker/types.h"
#include "model/timestamp.h"

#include <gtest/gtest.h>

namespace crash_tracker {

struct LimiterTest : public testing::Test {};

TEST_F(LimiterTest, TestDescribeCrashes) {
    auto crashes = std::vector<recorder::recorded_crash>{};
    EXPECT_EQ(
      crash_tracker::impl::describe_crashes(crashes),
      "(No crash files have been recorded.)");

    auto make_crash = []() {
        return crash_description{
          .crash_time = model::timestamp{0},
          .crash_message = "Assertion error",
          .stacktrace = "0xaaaaaaaa 0xbbbbbbbb",
          .addition_info = "false != true at example.cc:123"};
    };

    crashes.emplace_back(make_crash());

    EXPECT_EQ(
      crash_tracker::impl::describe_crashes(crashes),
      // clang-format off
      "The following crashes have been recorded:"
      "\nCrash #1 at 1970-01-01 00:00:00 UTC - Assertion error Backtrace: 0xaaaaaaaa 0xbbbbbbbb. false != true at example.cc:123"
      // clang-format on
    );

    for (int i = 0; i < 10; i++) {
        crashes.emplace_back(make_crash());
    }

    EXPECT_EQ(
      crash_tracker::impl::describe_crashes(crashes),
      // clang-format off
      "The following crashes have been recorded:"
      "\nCrash #1 at 1970-01-01 00:00:00 UTC - Assertion error Backtrace: 0xaaaaaaaa 0xbbbbbbbb. false != true at example.cc:123"
      "\nCrash #2 at 1970-01-01 00:00:00 UTC - Assertion error Backtrace: 0xaaaaaaaa 0xbbbbbbbb. false != true at example.cc:123"
      "\nCrash #3 at 1970-01-01 00:00:00 UTC - Assertion error Backtrace: 0xaaaaaaaa 0xbbbbbbbb. false != true at example.cc:123"
      "\nCrash #4 at 1970-01-01 00:00:00 UTC - Assertion error Backtrace: 0xaaaaaaaa 0xbbbbbbbb. false != true at example.cc:123"
      "\nCrash #5 at 1970-01-01 00:00:00 UTC - Assertion error Backtrace: 0xaaaaaaaa 0xbbbbbbbb. false != true at example.cc:123"
      "\n    ..."
      "\nCrash #7 at 1970-01-01 00:00:00 UTC - Assertion error Backtrace: 0xaaaaaaaa 0xbbbbbbbb. false != true at example.cc:123"
      "\nCrash #8 at 1970-01-01 00:00:00 UTC - Assertion error Backtrace: 0xaaaaaaaa 0xbbbbbbbb. false != true at example.cc:123"
      "\nCrash #9 at 1970-01-01 00:00:00 UTC - Assertion error Backtrace: 0xaaaaaaaa 0xbbbbbbbb. false != true at example.cc:123"
      "\nCrash #10 at 1970-01-01 00:00:00 UTC - Assertion error Backtrace: 0xaaaaaaaa 0xbbbbbbbb. false != true at example.cc:123"
      "\nCrash #11 at 1970-01-01 00:00:00 UTC - Assertion error Backtrace: 0xaaaaaaaa 0xbbbbbbbb. false != true at example.cc:123"
      // clang-format on
    );
}

} // namespace crash_tracker
