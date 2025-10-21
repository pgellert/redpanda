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
#include "container/priority_queue.h"
#include "kafka/server/fwd.h"
#include "proto/redpanda/core/admin/v2/broker.proto.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include <cstddef>

namespace admin {

struct connection_collector {
    virtual ~connection_collector() = default;
    virtual void add(proto::admin::kafka_connection conn) = 0;
    virtual chunked_vector<proto::admin::kafka_connection> extract() && = 0;
    virtual size_t size() const = 0;
};

class unordered_collector : public connection_collector {
    chunked_vector<proto::admin::kafka_connection> _connections;
    size_t _limit;

public:
    explicit unordered_collector(size_t limit)
      : _limit(limit) {}

    void add(proto::admin::kafka_connection conn) final {
        if (_connections.size() < _limit) {
            _connections.emplace_back(std::move(conn));
        }
    }

    chunked_vector<proto::admin::kafka_connection> extract() && final {
        return std::move(_connections);
    }

    size_t size() const final { return _connections.size(); }
};

template<typename Comparator>
class ordered_collector : public connection_collector {
    // Invert the order here to get the min-k instead of the max-k
    chunked_bounded_priority_queue<
      proto::admin::kafka_connection,
      detail::invert_comparator<Comparator>>
      _pq;

public:
    ordered_collector(size_t limit, Comparator comp)
      : _pq(limit, detail::invert_comparator<Comparator>(std::move(comp))) {}

    void add(proto::admin::kafka_connection conn) final {
        _pq.push(std::move(conn));
    }

    chunked_vector<proto::admin::kafka_connection> extract() && final {
        return std::move(_pq).extract_heap();
    }

    size_t size() const final { return _pq.size(); }

    ss::future<chunked_vector<proto::admin::kafka_connection>>
    extract_sorted() && {
        return std::move(_pq).async_extract_sorted();
    }
};

class kafka_connections_service {
public:
    explicit kafka_connections_service(ss::sharded<kafka::server>& kafka_server)
      : _kafka_server(kafka_server) {}

    // List connections from all shards on this node
    ss::future<proto::admin::list_kafka_connections_response>
    list_kafka_connections_local(
      proto::admin::list_kafka_connections_request req);

    static size_t get_effective_limit(size_t page_size);

private:
    ss::sharded<kafka::server>& _kafka_server;
};

} // namespace admin
