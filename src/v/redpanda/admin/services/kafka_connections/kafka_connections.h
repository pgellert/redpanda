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

#include "kafka/server/fwd.h"
#include "proto/redpanda/core/admin/v2/kafka_connections.proto.h"
#include "redpanda/admin/proxy/client.h"

#include <seastar/core/distributed.hh>

namespace admin {
class kafka_connections_service_impl
  : public proto::admin::kafka_connection_service {
public:
    kafka_connections_service_impl(
      admin::proxy::client, ss::sharded<kafka::server>&);

    ss::future<proto::admin::list_kafka_connections_response>
      list_kafka_connections(
        serde::pb::rpc::context,
        proto::admin::list_kafka_connections_request) override;

    ss::future<proto::admin::aggregate_connections_response>
      aggregate_connections(
        serde::pb::rpc::context,
        proto::admin::aggregate_connections_request) override;

private:
    ss::future<> gather_connections(
      chunked_vector<proto::admin::kafka_connection>& conns,
      kafka::server& server,
      bool is_first_loop) const;
    admin::proxy::client _proxy_client;
    ss::sharded<kafka::server>& _kafka_server;
};
} // namespace admin
