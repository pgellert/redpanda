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

#include "redpanda/admin/kafka_connections_service.h"

#include "kafka/server/server.h"
#include "proto/redpanda/core/admin/v2/broker.proto.h"
#include "proto/redpanda/core/admin/v2/kafka_connections.proto.h"
#include "redpanda/admin/aip_filter.h"
#include "redpanda/admin/aip_ordering.h"
#include "ssx/async_algorithm.h"

#include <seastar/core/coroutine.hh>

namespace proto {
using namespace proto::admin;
} // namespace proto

namespace admin {

namespace {

struct connection_gather_result {
    chunked_vector<proto::admin::kafka_connection> connections;
    size_t total_matching_count;
};

ss::future<connection_gather_result> gather_connections(
  const kafka::server& server,
  const filter_predicate& filter,
  ss::shared_ptr<connection_collector> collector) {
    auto result = connection_gather_result{};

    auto conn_ptrs = server.list_connections();
    co_await ss::coroutine::maybe_yield();

    auto process_conn = [&result, &collector, &filter](
                          proto::admin::kafka_connection&& conn_proto) {
        bool matches_filter = filter(conn_proto);

        if (matches_filter) {
            result.total_matching_count++;
            collector->add(std::move(conn_proto));
        }
    };

    co_await ssx::async_for_each(
      conn_ptrs, [&process_conn](const auto& conn_ptr) {
          process_conn(conn_ptr->to_proto());
      });

    auto closed_conns = server.list_closed_connections();
    for (auto& elem : closed_conns) {
        auto elem_copy = co_await proto::admin::kafka_connection::from_proto(
          co_await elem->to_proto());
        process_conn(std::move(elem_copy));
    }

    result.connections = std::move(*collector).extract();
    co_return result;
}

ss::future<size_t> gather_all_shards(
  ss::sharded<kafka::server>& kafka_server,
  const filter_predicate& filter,
  const auto& make_local_collector,
  connection_collector& global_collector) {
    size_t total_matching_connections = 0;

    for (ss::shard_id shard = 0; shard < ss::smp::count; ++shard) {
        auto accumulated_count = global_collector.size();
        auto shard_result = co_await kafka_server.invoke_on(
          shard,
          [accumulated_count, &filter, &make_local_collector](
            kafka::server& server) {
              return gather_connections(
                server, filter, make_local_collector(accumulated_count));
          });

        total_matching_connections += shard_result.total_matching_count;

        for (auto& conn : shard_result.connections) {
            global_collector.add(std::move(conn));
        }

        co_await ss::coroutine::maybe_yield();
    }

    co_return total_matching_connections;
}

} // namespace

ss::future<proto::admin::list_kafka_connections_response>
kafka_connections_service::list_kafka_connections_local(
  proto::admin::list_kafka_connections_request req) {
    auto resp = proto::admin::list_kafka_connections_response{};

    auto limit = get_effective_limit(req.get_page_size());

    auto filter_cfg = make_aip_filter_config<proto::kafka_connection>(
      req.get_filter());
    auto filter = aip_filter_parser::create_aip_filter(std::move(filter_cfg));

    if (req.get_order_by().empty()) {
        auto global_collector = unordered_collector{limit};

        auto make_local_collector = [limit](size_t accumulated_count) {
            return ss::make_shared<unordered_collector>(
              limit - accumulated_count);
        };

        auto total_matching_connections = co_await gather_all_shards(
          _kafka_server, filter, make_local_collector, global_collector);

        resp.set_connections(std::move(global_collector).extract());
        resp.set_total_size(total_matching_connections);
    } else {
        auto ordering_conf
          = make_ordering_config<proto::admin::kafka_connection>(
            req.get_order_by());
        auto comp = sort_order::parse(ordering_conf);

        auto global_collector = ordered_collector{limit, comp};

        auto make_local_collector = [limit, &comp](size_t) {
            return ss::make_shared<ordered_collector<decltype(comp)>>(
              limit, comp);
        };

        auto total_matching_connections = co_await gather_all_shards(
          _kafka_server, filter, make_local_collector, global_collector);

        resp.set_connections(
          co_await std::move(global_collector).extract_sorted());
        resp.set_total_size(total_matching_connections);
    }
    co_return resp;
}

size_t kafka_connections_service::get_effective_limit(size_t page_size) {
    constexpr size_t default_limit = 1000;
    return page_size == 0 ? default_limit : page_size;
}

} // namespace admin
