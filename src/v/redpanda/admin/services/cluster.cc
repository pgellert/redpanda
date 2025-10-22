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
#include "proto/redpanda/core/admin/v2/kafka_connections.proto.h"
#include "redpanda/admin/aip_ordering.h"
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

ss::future<> cluster_service_impl::gather_all_brokers(
  const serde::pb::rpc::context& ctx,
  const response_consumer_t& add_to_response,
  const proto::list_kafka_connections_request& req) {
    auto make_broker_req = [&req](auto node_id) {
        auto client_req = proto::admin::list_kafka_connections_request{};
        client_req.set_node_id(node_id);
        client_req.set_filter(ss::sstring{req.get_filter()});
        client_req.set_order_by(ss::sstring{req.get_order_by()});
        client_req.set_page_size(req.get_page_size());
        return client_req;
    };

    // Iterate one by one for now to limit memory usage to be approximately in
    // the order of 2 x page_size. We could optimize here to issue requests in
    // parallel when page_size is small.
    auto other_node_clients
      = _proxy_client
          .make_clients_for_other_nodes<proto::admin::cluster_service_client>();
    for (auto& [node_id, client] : other_node_clients) {
        auto client_resp = co_await client.list_kafka_connections(
          ctx, make_broker_req(node_id));

        co_await add_to_response(std::move(client_resp));
    }

    auto local_resp = co_await _kafka_connections_service.local()
                        .list_kafka_connections_local(make_broker_req(-1));
    co_await add_to_response(std::move(local_resp));
}

ss::future<proto::admin::list_kafka_connections_response>
cluster_service_impl::list_kafka_connections_cluster_wide(
  const serde::pb::rpc::context& ctx,
  proto::admin::list_kafka_connections_request req) {
    auto limit = _kafka_connections_service.local().get_effective_limit(
      req.get_page_size());

    auto collector = [&req, limit]() -> ss::shared_ptr<connection_collector> {
        if (req.get_order_by().empty()) {
            return ss::make_shared<unordered_collector>(limit);
        } else {
            auto ordering_conf
              = make_ordering_config<proto::admin::kafka_connection>(
                req.get_order_by());
            auto comp = sort_order::parse(ordering_conf);

            return ss::make_shared<ordered_collector<sort_order>>(limit, comp);
        }
    }();

    auto total_count = size_t{0};

    auto add_to_response = response_consumer_t{
      [&collector, &total_count](
        proto::admin::list_kafka_connections_response client_resp) {
          total_count += client_resp.get_total_size();
          return collector->add_all(std::move(client_resp.get_connections()));
      }};

    co_await gather_all_brokers(ctx, add_to_response, req);

    auto resp = proto::admin::list_kafka_connections_response{};
    resp.set_connections(co_await std::move(*collector).extract());
    resp.set_total_size(total_count);
    co_return resp;
}

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
    auto single_broker_request = target.has_value();
    if (target && *target != -1 && *target != _proxy_client.self_node_id()) {
        vlog(brlog.debug, "Redirecting to target node id {}", *target);
        co_return co_await _proxy_client
          .make_client_for_node<proto::admin::cluster_service_client>(*target)
          .list_kafka_connections(ctx, std::move(req));
    }

    auto resp = single_broker_request
                  ? co_await _kafka_connections_service.local()
                      .list_kafka_connections_local(std::move(req))
                  : co_await list_kafka_connections_cluster_wide(
                      ctx, std::move(req));

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
