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

#include "cluster/crash_reporter.h"

#include "base/units.h"
#include "bytes/iobuf.h"
#include "cluster/controller_stm.h"
#include "cluster/metrics_reporter.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "crash_tracker/recorder.h"
#include "crash_tracker/types.h"
#include "json/stringbuffer.h"
#include "json/writer.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "net/tls.h"
#include "net/tls_certificate_probe.h"
#include "serde/rw/rw.h"
#include "storage/kvstore.h"
#include "utils/prefix_logger.h"
#include "utils/unresolved_address.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

#include <fmt/core.h>

#include <exception>

namespace cluster {

static ss::logger logger("crash-reporter");

/// Key used to rate limiting metadata in the kvstore
static constexpr std::string_view rate_limiter_kvs_key
  = "rate_limiting_metadata";

/// We can upload multiple crash reports per-request in batch, but want to limit
/// the total request size to avoid large allocations or too large requests.
static constexpr auto upload_req_bound = 100_KiB;
static constexpr auto max_reports_per_request = std::max<size_t>(
  1,
  upload_req_bound / crash_tracker::crash_description::serde_size_overestimate);

crash_reporter::crash_reporter(
  storage::kvstore& kvs,
  ss::sharded<controller_stm>& stm,
  ss::sharded<ss::abort_source>& as,
  ss::sharded<metrics_reporter>& mr,
  crash_tracker::recorder& rec)
  : _rate_limiter(kvs)
  , _controller_stm(stm)
  , _metrics_reporter(mr)
  , _recorder(rec)
  , _as(as) {}

ss::future<> crash_reporter::start() {
    vlog(logger.trace, "Starting Crash Reporter");
    _address = details::parse_url(
      config::shard_local_cfg().metrics_reporter_url() + "/crash_reports");

    ssx::background = ssx::spawn_with_gate_then(_gate, [this] {
                          return report_crashes();
                      }).handle_exception([](std::exception_ptr e) {
        vlog(logger.warn, "Exception reporting crashes: {}", e);
    });

    co_return;
}

ss::future<> crash_reporter::stop() {
    vlog(logger.info, "Stopping Crash Reporter...");
    co_await _gate.close();
}

/// Called after an upload
ss::future<> crash_reporter::rate_limiter::record() {
    auto md = crash_reporter_rate_limiting_metadata{
      .last_upload_time = model::to_timestamp(clock::now())};

    iobuf buf;
    serde::write(buf, md);

    vlog(
      logger.trace,
      "Recording rate limiter last upload time as: {}",
      md.last_upload_time);
    co_await _kvstore.put(
      storage::kvstore::key_space::crash_tracker,
      bytes::from_string(rate_limiter_kvs_key),
      std::move(buf));
}

/// Called before an upload to get how long to wait before uploading
crash_reporter::rate_limiter::clock::duration
crash_reporter::rate_limiter::wait_time() {
    auto buf = _kvstore.get(
      storage::kvstore::key_space::crash_tracker,
      bytes::from_string(rate_limiter_kvs_key));
    if (!buf) {
        return 0s;
    }
    auto md = serde::from_iobuf<crash_reporter_rate_limiting_metadata>(
      std::move(*buf));

    auto rate_limit_time = model::to_time_point(md.last_upload_time)
                           + upload_rate;
    auto remaining = rate_limit_time - clock::now();
    return std::clamp<clock::duration>(remaining, 0s, upload_rate);
}

ss::future<result<crash_reporter::crash_report_payload>>
crash_reporter::build_crash_report_payload() {
    crash_report_payload result;

    result.cluster_uuid
      = _controller_stm.local().get_metrics_reporter_cluster_info().uuid;

    const auto& reports = co_await _recorder.get_recorded_crashes();

    for (const auto& report : reports) {
        if (co_await report.is_uploaded()) {
            continue;
        }

        crash_report_payload::report r;
        r.node_id = config::node().node_id().value_or(
          model::unassigned_node_id);
        r.timestamp = report.timestamp().time_since_epoch() / 1ms;
        if (report.crash) {
            r.stacktrace = ss::sstring{report.crash->stacktrace.c_str()};
            r.reason = fmt::format("{}", report.crash->type);

            if (
              report.crash->type
              != crash_tracker::crash_type::startup_exception) {
                r.additional_info = fmt::format(
                  "{}", report.crash->crash_message.c_str());
            }
        }
        result.items.emplace_back(std::move(r));

        co_await report.mark_uploaded();

        if (result.items.size() >= max_reports_per_request) {
            // Upload batch size limit reached
            break;
        }
    }

    co_return result;
}

iobuf serialize_payload(const crash_reporter::crash_report_payload& payload) {
    json::StringBuffer sb;
    json::Writer<json::StringBuffer> writer(sb);

    json::rjson_serialize(writer, payload);
    iobuf out;
    out.append(sb.GetString(), sb.GetSize());

    return out;
}

ss::future<http::client> crash_reporter::make_http_client() {
    net::base_transport::configuration client_configuration;
    client_configuration.server_addr = net::unresolved_address(
      ss::sstring(_address.host), _address.port);

    client_configuration.disable_metrics = net::metrics_disabled::yes;

    if (_address.protocol == "https") {
        ss::tls::credentials_builder builder;
        builder.set_client_auth(ss::tls::client_auth::NONE);
        builder.set_minimum_tls_version(
          config::from_config(config::shard_local_cfg().tls_min_version()));
        auto ca_file = co_await net::find_ca_file();
        if (ca_file) {
            vlog(logger.trace, "Using {} as crash reporter CA store", ca_file);
            co_await builder.set_x509_trust_file(
              ca_file.value(), ss::tls::x509_crt_format::PEM);
        } else {
            vlog(
              logger.trace,
              "CA file not found, defaulting to system trust store");
            co_await builder.set_system_trust();
        }

        client_configuration.credentials
          = co_await net::build_reloadable_credentials_with_probe<
            ss::tls::certificate_credentials>(
            std::move(builder), "crash_reporter", "httpclient");
        client_configuration.tls_sni_hostname = _address.host;
    }
    co_return http::client(client_configuration, _as.local());
}

ss::future<> crash_reporter::do_send_reports(http::client& client, iobuf body) {
    constexpr auto conn_timeout = 30s;
    constexpr auto req_timeout = 30s;

    auto res = co_await client.get_connected(
      conn_timeout, prefix_logger{logger, "client"});
    if (res != http::reconnect_result_t::connected) {
        vlog(logger.trace, "Unable to send crash report, connection timeout");
        co_return;
    }
    auto resp_stream = co_await client.post(
      _address.path, std::move(body), http::content_type::json, req_timeout);
    co_await resp_stream->prefetch_headers();
}

ss::future<> crash_reporter::report_crashes() {
    constexpr auto retry_interval = 5s;

    // If reporting is disabled, drop out here
    if (!config::shard_local_cfg().enable_metrics_reporter()) {
        co_return;
    }

    vlog(logger.debug, "Waiting for cluster UUID to be initialized");
    co_await _metrics_reporter.local().wait_cluster_info_initialized();

    vlog(logger.debug, "Uploading crash reports");

    bool done = false;
    while (!done) {
        done = co_await do_report_crashes();
        if (!done) {
            co_await ss::sleep_abortable(retry_interval, _as.local());
        }
    }
}

ss::future<bool> crash_reporter::do_report_crashes() {
    const auto& cluster_info
      = _controller_stm.local().get_metrics_reporter_cluster_info();
    if (!cluster_info.is_initialized()) {
        vlog(
          logger.trace,
          "Error collecting crash reports - cluster uuid not yet initialized");
        co_return false;
    }

    auto rl_wait = _rate_limiter.wait_time();
    if (rl_wait > 0s) {
        vlog(
          logger.trace,
          "Sleeping for {}ms for rate limit to pass",
          rl_wait / 1ms);
        co_await ss::sleep_abortable(rl_wait, _as.local());
    }

    // collect crash reports
    auto payload = co_await build_crash_report_payload();
    if (!payload) {
        vlog(
          logger.trace,
          "Error collecting crash reports - {}",
          payload.error().message());
        co_return true;
    }
    const auto n_reports = payload.value().items.size();
    if (n_reports == 0) {
        vlog(logger.trace, "No crash reports to upload");
        co_return true;
    }

    auto out = serialize_payload(payload.value());

    // Record the upload to the rate limiter before sending the report to ensure
    // that if there is a crash while sending telemetry data, the rate limiter
    // will still subsequent uploads
    co_await _rate_limiter.record();
    try {
        co_await http::with_client(
          co_await make_http_client(), [this, &out](http::client& client) {
              return do_send_reports(client, std::move(out));
          });
        vlog(logger.trace, "Successfully uploaded {} crash reports", n_reports);
    } catch (...) {
        vlog(
          logger.trace,
          "Exception thrown while reporting crashes - {}",
          std::current_exception());
    }
    co_return false;
}

} // namespace cluster

namespace json {
void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const cluster::crash_reporter::crash_report_payload& payload) {
    w.StartObject();

    w.Key("cluster_uuid");
    w.String(payload.cluster_uuid);

    w.Key("items");
    w.StartArray();
    for (const auto& r : payload.items) {
        rjson_serialize(w, r);
    }
    w.EndArray();

    w.EndObject();
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const cluster::crash_reporter::crash_report_payload::report& report) {
    w.StartObject();
    w.Key("timestamp");
    w.Uint64(report.timestamp);

    w.Key("node_id");
    w.Int(report.node_id);

    w.Key("stacktrace");
    w.String(report.stacktrace);
    w.Key("reason");
    w.String(report.reason);
    w.Key("additional_info");
    w.String(report.additional_info);
    w.EndObject();
}

} // namespace json
