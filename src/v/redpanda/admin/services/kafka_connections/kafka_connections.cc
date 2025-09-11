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

} // namespace

kafka_connections_service_impl::kafka_connections_service_impl(
  admin::proxy::client client, ss::sharded<kafka::server>& kafka_server)
  : _proxy_client(std::move(client))
  , _kafka_server(kafka_server) {}

namespace {
constexpr auto max_conns_per_shard = 4000;
constexpr auto max_conns_per_broker = 72000;
constexpr auto limit = 1000000000;

} // namespace

ss::future<> kafka_connections_service_impl::gather_connections(
  chunked_vector<proto::admin::kafka_connection>& conns,
  kafka::server& server,
  bool is_first_loop) const {
    if (server._connections.size() > 0) {
        using clock = std::chrono::system_clock;
        auto begin = clock::now();
        auto conn_ptrs
          = chunked_vector<ss::lw_shared_ptr<kafka::connection_context>>{};
        conn_ptrs.reserve(max_conns_per_shard);

        for (int i = 0; i < max_conns_per_shard; i++) {
            conn_ptrs.emplace_back(
              server._connections.front().shared_from_this());
        }

        if (is_first_loop) {
            vlog(
              log.info, "Time per shard: {}us", (clock::now() - begin) / 1us);
        }

        co_await ss::maybe_yield();

        conns.reserve(conns.size() + conn_ptrs.size());
        size_t i = 0;
        for (auto& conn_ptr : conn_ptrs) {
            auto add_conn = [&](const kafka::connection_context& conn) {
                auto& res = conns.emplace_back();
                auto src = proto::admin::source{};
                src.set_ip_address(fmt::format("{}", conn.client_host()));
                src.set_port(conn.client_port());
                res.set_source(std::move(src));
                res.set_listener_name(ss::sstring{conn.listener()});
                // res.set_uid(fmt::format("{}", uuid_t::create()));
            };
            // TODO: any safety checks needed?
            add_conn(*conn_ptr);
            if (++i % 1000 == 0) {
                co_await ss::maybe_yield();
            }
        }
    }

    co_return;
}

namespace {
template<typename T, typename Compare = std::less<T>>
class chunked_heap_sorter {
public:
    static constexpr size_t YIELD_THRESHOLD = 1000;

    explicit chunked_heap_sorter(Compare comp)
      : _operations(0)
      , _comp(std::move(comp)) {};

    ss::future<> sort_and_limit_async(chunked_vector<T>& vec, size_t k) {
        if (vec.size() <= 1 || k == 0) {
            if (k == 0) {
                vec.clear();
            }
            co_return;
        }

        // Clamp k to actual size
        k = std::min(k, vec.size());

        // Build heap
        co_await make_heap_async(vec);

        // Extract only the top k elements
        co_await sort_heap_async(vec, k);

        // Resize to k
        if (vec.size() > k) {
            vec.pop_back_n(vec.size() - k);
        }
    }

private:
    ss::future<> check_yield() {
        if (++_operations % YIELD_THRESHOLD == 0) {
            co_await ss::maybe_yield();
            _operations = 0;
        }
    }

    ss::future<> make_heap_async(chunked_vector<T>& vec) {
        size_t n = vec.size();

        for (int i = n / 2 - 1; i >= 0; --i) {
            co_await heapify_async(vec, n, i);
        }
    }

    ss::future<> sort_heap_async(chunked_vector<T>& vec, size_t k) {
        size_t n = vec.size();

        // Only extract k elements instead of all n
        for (size_t i = n - 1; i > n - k; --i) {
            std::swap(vec[0], vec[i]);
            co_await heapify_async(vec, i, 0);
            co_await check_yield();
        }
    }

    ss::future<> heapify_async(chunked_vector<T>& vec, size_t n, size_t i) {
        size_t largest = i;
        size_t left = 2 * i + 1;
        size_t right = 2 * i + 2;

        if (left < n && _comp(vec[largest], vec[left])) {
            largest = left;
        }

        if (right < n && _comp(vec[largest], vec[right])) {
            largest = right;
        }

        if (largest != i) {
            std::swap(vec[i], vec[largest]);

            co_await check_yield();
            co_await heapify_async(vec, n, largest);
        }
    }

    size_t _operations;
    Compare _comp;
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
    chunked_heap_sorter<kafka_connection, decltype(comparator)> sorter{
      comparator};

    for (int i = 0; i < max_conns_per_broker / max_conns_per_shard; i++) {
        // TODO: make this sequential + sort on all cores
        co_await _kafka_server.invoke_on_all([&](kafka::server& server) {
            return gather_connections(conns, server, i == 0);
        });

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

    co_await sorter.sort_and_limit_async(resp.get_connections(), limit);

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
