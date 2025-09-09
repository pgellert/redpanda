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
#include "container/chunked_vector.h"
#include "kafka/server/connection_context.h"
#include "kafka/server/server.h"
#include "serde/protobuf/rpc.h"
#include "utils/uuid.h"

#include <fmt/core.h>

#include <algorithm>
#include <chrono>
#include <queue>
#include <ranges>

using namespace std::chrono_literals;

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

namespace {
constexpr auto max_conns_per_shard = 2000;
constexpr auto max_conns_per_broker = 12500; // 64000;
constexpr auto limit = 100;

} // namespace

ss::future<> kafka_connections_service_impl::gather_connections(
  chunked_vector<proto::admin::kafka_connection>& conns,
  kafka::server& server) const {
    if (server._connections.size() > 0) {
        // TODO: consider yield'ing + use uuid as checkpoint or using
        // counted_intrusive_list

        conns.reserve(conns.size() + max_conns_per_shard);
        for (int i = 0; i < max_conns_per_shard; i++) {
            auto add_conn = [&](const kafka::connection_context& conn) {
                auto& res = conns.emplace_back();
                auto src = proto::admin::source{};
                src.set_ip_address(fmt::format("{}", conn.client_host()));
                src.set_port(conn.client_port());
                res.set_source(std::move(src));
                res.set_listener_name(ss::sstring{conn.listener()});
                res.set_uid(fmt::format("{}", uuid_t::create()));
            };
            add_conn(server._connections.front());

            // for (const auto& conn : server._connections) {
            //     add_conn(conn);
            // }
        }
    }

    co_return;
}

namespace {

template<typename T, typename Compare = std::less<T>>
class chunked_heap_sorter {
public:
    static constexpr size_t YIELD_THRESHOLD = 1000;

    static seastar::future<>
    sort_async(chunked_vector<T>& vec, Compare comp = Compare{}) {
        if (vec.size() <= 1) {
            co_return;
        }

        // Build heap
        co_await make_heap_async(vec, comp);

        // Extract elements
        co_await sort_heap_async(vec, comp);
    }

private:
    static seastar::future<>
    make_heap_async(chunked_vector<T>& vec, Compare comp) {
        size_t n = vec.size();
        size_t operations = 0;

        // Start from the last non-leaf node
        for (int i = n / 2 - 1; i >= 0; --i) {
            co_await heapify_async(vec, n, i, comp, operations);
        }
    }

    static seastar::future<>
    sort_heap_async(chunked_vector<T>& vec, Compare comp) {
        size_t n = vec.size();
        size_t operations = 0;

        for (size_t i = n - 1; i > 0; --i) {
            // Move current root to end
            std::swap(vec[0], vec[i]);

            // Call heapify on the reduced heap
            co_await heapify_async(vec, i, 0, comp, operations);

            if (++operations % YIELD_THRESHOLD == 0) {
                co_await seastar::yield();
                operations = 0;
            }
        }
    }

    static seastar::future<> heapify_async(
      chunked_vector<T>& vec,
      size_t n,
      size_t i,
      Compare comp,
      size_t& operations) {
        size_t largest = i;
        size_t left = 2 * i + 1;
        size_t right = 2 * i + 2;

        if (left < n && comp(vec[largest], vec[left])) {
            largest = left;
        }

        if (right < n && comp(vec[largest], vec[right])) {
            largest = right;
        }

        if (largest != i) {
            std::swap(vec[i], vec[largest]);

            if (++operations % YIELD_THRESHOLD == 0) {
                co_await seastar::yield();
                operations = 0;
            }

            co_await heapify_async(vec, n, largest, comp, operations);
        }
    }
};
} // namespace

ss::future<proto::admin::list_kafka_connections_response>
kafka_connections_service_impl::list_kafka_connections(
  serde::pb::rpc::context, proto::admin::list_kafka_connections_request) {
    using proto::admin::kafka_connection;
    auto resp = proto::admin::list_kafka_connections_response{};
    auto& conns = resp.get_connections();

    constexpr auto comparator = [](
                                  const proto::admin::kafka_connection& a,
                                  const proto::admin::kafka_connection& b) {
        return a.get_uid() < b.get_uid();
    };
    chunked_heap_sorter<kafka_connection, decltype(comparator)> sorter{};

    for (int i = 0; i < max_conns_per_broker / max_conns_per_shard; i++) {
        using clock = std::chrono::system_clock;
        auto begin = clock::now();

        // TODO: make this sequential + sort on all cores
        co_await _kafka_server.invoke_on_all([&](kafka::server& server) {
            return gather_connections(conns, server);
        });
        if (i == 0) {
            vlog(
              log.info, "Time per shard: {}ms", (clock::now() - begin) / 1ms);
        }
        co_await ss::maybe_yield();
    }

    vlog(
      log.info,
      "Connection count: {} ({}b)",
      resp.get_connections().size(),
      resp.get_connections().memory_size());
    vlog(
      log.info,
      "Recent connection count: {}",
      _kafka_server.local()._recent_connections.size());

    co_await sorter.sort_async(resp.get_connections(), comparator);

    vlog(log.info, "Sorting done");

    // Limit to configurable number
    if (resp.get_connections().size() > limit) {
        resp.get_connections().pop_back_n(
          resp.get_connections().size() - limit);
    }

    vlog(log.info, "Limiting done");

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
