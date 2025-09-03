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

#include "redpanda/admin/services/kafka_connections/kafka_connections.h"

#include "base/vlog.h"
#include "kafka/server/server.h"
#include "serde/protobuf/rpc.h"

#include <fmt/core.h>

namespace admin {

namespace {
// NOLINTNEXTLINE(*-non-const-global-variables,cert-err58-*)
ss::logger log{"admin_api_server/kafka_connections_service"};

// auto to_ip(const ss::net::inet_address& addr) {
//     auto res = proto::admin::ip_address{};
//     if (addr.is_ipv4()) {
//         res.set_ipv4(addr.as_ipv4_address().ip);
//     } else {
//         iobuf buf{};
//         buf.append(addr.as_ipv6_address().ip);
//         res.set_ipv6(std::move(buf));
//     }
//     return res;
// }
} // namespace

kafka_connections_service_impl::kafka_connections_service_impl(
  admin::proxy::client client, ss::sharded<kafka::server>& kafka_server)
  : _proxy_client(std::move(client))
  , _kafka_server(kafka_server) {}

ss::future<proto::admin::list_kafka_connections_response>
kafka_connections_service_impl::list_kafka_connections(
  serde::pb::rpc::context, proto::admin::list_kafka_connections_request) {
    auto resp = proto::admin::list_kafka_connections_response{};
    auto& conns = resp.get_connections();

    co_await _kafka_server.invoke_on_all([&](kafka::server& server) {
        for (const auto& conn : server._connections) {
            auto& res = conns.emplace_back();
            auto src = proto::admin::source{};
            src.set_ip(fmt::format("{}", conn.client_host()));
            src.set_port(conn.client_port());
            res.set_source(std::move(src));
            res.set_listener_name(ss::sstring{conn.listener()});
        }
    });
    vlog(log.info, "Connection count: {}", resp.get_connections().size());
    vlog(
      log.info,
      "Recent connection count: {}",
      _kafka_server.local()._recent_connections.size());

    co_return resp;

    // TODO: figure out proxying
    // if (ctx.is_proxied()) {
    //     co_return resp;
    // }
    // auto clients
    //   = _proxy_client
    //       .make_clients_for_other_nodes<proto::admin::kafka_connection_service_client>();
    // for (auto& [node_id, client] : clients) {
    //     auto proxy_resp = co_await client.list_kafka_connections(ctx,
    //     proto::admin::list_kafka_connections_request{}); std::ranges::move(
    //       proxy_resp.get_connections(),
    //       std::back_inserter(resp.get_connections()));
    // }
    // co_return resp;
}

ss::future<proto::admin::aggregate_connections_response>
kafka_connections_service_impl::aggregate_connections(
  serde::pb::rpc::context, proto::admin::aggregate_connections_request) {
    throw serde::pb::rpc::unimplemented_exception();
}

} // namespace admin
