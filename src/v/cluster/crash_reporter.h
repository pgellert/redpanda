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

#include "cluster/fwd.h"
#include "cluster/metrics_reporter.h"
#include "crash_tracker/recorder.h"
#include "http/client.h"
#include "model/timestamp.h"
#include "storage/fwd.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>

#include <chrono>
#include <vector>

namespace cluster {

class crash_reporter {
public:
    struct crash_report_payload {
        struct report {
            uint64_t timestamp{0};
            model::node_id node_id;
            ss::sstring stacktrace;
            ss::sstring reason;
            ss::sstring additional_info;
        };

        ss::sstring cluster_uuid;
        std::vector<report> items;
    };

    static constexpr ss::shard_id shard = 0;

    crash_reporter(
      storage::kvstore&,
      ss::sharded<controller_stm>&,
      ss::sharded<ss::abort_source>&,
      ss::sharded<metrics_reporter>&,
      crash_tracker::recorder&);

    ss::future<> start();
    ss::future<> stop();

    class rate_limiter {
    public:
        using clock = model::timestamp_clock;

        static constexpr auto upload_rate = std::chrono::seconds{30};

        explicit rate_limiter(storage::kvstore& kvstore)
          : _kvstore(kvstore) {}

        /// Called after an upload to rate limit subsequent uploads
        ss::future<> record();

        /// Called before an upload to get how long to wait before uploading
        clock::duration wait_time();

    private:
        storage::kvstore& _kvstore;
    };

private:
    ss::future<> report_crashes();
    ss::future<bool> do_report_crashes();
    ss::future<result<crash_report_payload>> build_crash_report_payload();

    ss::future<http::client> make_http_client();
    ss::future<> do_send_reports(http::client&, iobuf body);

    rate_limiter _rate_limiter;
    ss::sharded<controller_stm>& _controller_stm;
    ss::sharded<metrics_reporter>& _metrics_reporter;
    crash_tracker::recorder& _recorder;
    ss::sharded<ss::abort_source>& _as;
    details::address _address;
    ss::gate _gate;
};
} // namespace cluster
namespace json {
void rjson_serialize(
  json::Writer<json::StringBuffer>&,
  const cluster::crash_reporter::crash_report_payload&);

void rjson_serialize(
  json::Writer<json::StringBuffer>&,
  const cluster::crash_reporter::crash_report_payload::report&);
} // namespace json
