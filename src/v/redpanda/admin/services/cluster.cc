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

#include "redpanda/admin/services/cluster.h"

#include "features/feature_table.h"
#include "proto/redpanda/core/admin/v2/cluster.proto.h"
#include "redpanda/admin/kafka_connections_service.h"
#include "redpanda/admin/services/utils.h"
#include "serde/protobuf/rpc.h"

#include <seastar/core/coroutine.hh>

namespace proto {
using namespace proto::admin;
} // namespace proto

namespace admin {

namespace {
// NOLINTNEXTLINE(*-non-const-global-variables,cert-err58-*)
ss::logger brlog{"admin_api_server/cluster_service"};

} // namespace

cluster_service_impl::cluster_service_impl(
  admin::proxy::client client,
  ss::sharded<kafka_connections_service>& kafka_connections_service,
  ss::sharded<features::feature_table>& feature_table)
  : _proxy_client(std::move(client))
  , _kafka_connections_service(kafka_connections_service)
  , _feature_table(feature_table) {}

ss::future<proto::admin::list_kafka_connections_response>
cluster_service_impl::list_kafka_connections(
  serde::pb::rpc::context ctx,
  proto::admin::list_kafka_connections_request req) {
    vlog(brlog.trace, "list_kafka_connections: {}", req);

    utils::check_license(_feature_table.local());

    // Proxy to the target node id if specified in the request
    auto target = req.has_node_id()
                    ? std::make_optional<model::node_id>(req.get_node_id())
                    : std::nullopt;
    if (target && *target != -1 && *target != _proxy_client.self_node_id()) {
        vlog(brlog.debug, "Redirecting to target node id {}", *target);
        co_return co_await _proxy_client
          .make_client_for_node<proto::admin::cluster_service_client>(*target)
          .list_kafka_connections(ctx, std::move(req));
    }

    auto resp = co_await _kafka_connections_service.local()
                  .list_kafka_connections_local(std::move(req));

    vlog(
      brlog.trace,
      "list_kafka_connections: response connections: {} ({}b), total matching: "
      "{}",
      resp.get_connections().size(),
      resp.get_connections().memory_size(),
      resp.get_total_size());

    co_return resp;
}

} // namespace admin
