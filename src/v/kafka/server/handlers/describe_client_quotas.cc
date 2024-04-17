/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/server/handlers/describe_client_quotas.h"

#include "cluster/config_frontend.h"
#include "kafka/protocol/schemata/describe_client_quotas_response.h"
#include "kafka/types.h"

#include <vector>

namespace kafka {

using describe_result_t = std::pair<
  std::vector<describe_client_quotas_response_entity_data>,
  std::vector<kafka::value_data>>;

describe_result_t describe_default_client() {
    auto add_if_defined =
      [](
        std::vector<value_data>& res, const ss::sstring& key, uint32_t config) {
          if (config == 0) {
              return;
          }
          res.push_back(kafka::value_data{
            .key = key,
            .value = static_cast<double>(config),
          });
      };

    const auto key = std::vector<describe_client_quotas_response_entity_data>{
      {.entity_type = entity_type::client_id, .entity_name = ""}};
    auto value = std::vector<kafka::value_data>{};
    add_if_defined(
      value,
      quota_type::producer_byte_rate,
      config::shard_local_cfg().target_quota_byte_rate.bind()());
    add_if_defined(
      value,
      quota_type::consumer_byte_rate,
      config::shard_local_cfg().target_fetch_quota_byte_rate.bind()().value_or(
        0));
    add_if_defined(
      value,
      quota_type::controller_mutation_rate,
      config::shard_local_cfg().kafka_admin_topic_api_rate.bind()().value_or(
        0));

    return {key, std::move(value)};
}

std::vector<describe_result_t>
describe_specific_client(std::optional<ss::sstring> match) {
    auto result = std::vector<describe_result_t>{};

    auto make_key = [](const ss::sstring& client) {
        return std::vector<describe_client_quotas_response_entity_data>{
          {.entity_type = entity_type::client_id, .entity_name = client}};
    };

    auto produce_bind
      = config::shard_local_cfg().kafka_client_group_byte_rate_quota.bind();
    for (const auto& produce_conf : produce_bind()) {
        if (!match || produce_conf.second.clients_prefix == *match) {
            auto key = make_key(produce_conf.second.clients_prefix);
            auto value = std::vector<kafka::value_data>{
              {.key = quota_type::producer_byte_rate,
               .value = static_cast<double>(produce_conf.second.quota)}};

            result.emplace_back(std::move(key), std::move(value));
        }
    }

    auto consume_bind = config::shard_local_cfg()
                          .kafka_client_group_fetch_byte_rate_quota.bind();

    for (const auto& consume_conf [[maybe_unused]] : consume_bind()) {
        if (!match || consume_conf.second.clients_prefix == *match) {
            auto key = make_key(consume_conf.second.clients_prefix);
            auto value = std::vector<kafka::value_data>{
              {.key = quota_type::consumer_byte_rate,
               .value = static_cast<double>(consume_conf.second.quota)}};

            result.emplace_back(std::move(key), std::move(value));
        }
    }

    // TODO: squash values in result for the same key?
    return result;
}

void make_unsupported_error_response(
  describe_client_quotas_response& resp, ss::sstring&& error_msg) {
    resp.data.error_code = error_code::unsupported_version,
    resp.data.error_message = error_msg;
}

describe_client_quotas_response
make_response(describe_client_quotas_response_data&& resp_data) {
    return describe_client_quotas_response{.data = resp_data};
}

template<>
ss::future<response_ptr> describe_client_quotas_handler::handle(
  request_context ctx, ss::smp_service_group) {
    describe_client_quotas_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    if (!ctx.authorized(
          security::acl_operation::describe_configs,
          security::default_cluster_name)) {
        co_return co_await ctx.respond(make_response({
          .error_code = error_code::cluster_authorization_failed,
          .error_message = ss::sstring{error_code_to_str(
            error_code::cluster_authorization_failed)},
        }));
    }

    if (!ctx.audit()) {
        co_return co_await ctx.respond(make_response({
          .error_code = error_code::broker_not_available,
          .error_message = "Broker not available - audit system failure",
        }));
    }

    if (request.data.components.size() != 1) {
        co_return co_await ctx.respond(make_response({
          .error_code = error_code::unsupported_version,
          .error_message
          = "Unsupported version - only the Client entity type is supported",
        }));
    }

    auto component = request.data.components[0];

    if (component.entity_type != entity_type::client_id) {
        co_return co_await ctx.respond(make_response({
          .error_code = error_code::unsupported_version,
          .error_message
          = "Unsupported version - only the Client entity type is supported",
        }));
    }

    auto results = std::vector<std::pair<
      std::vector<describe_client_quotas_response_entity_data>,
      std::vector<value_data>>>{};

    switch (component.match_type) {
    case describe_client_quotas_match_type::exact_name:
        if (!component.match) {
            co_return co_await ctx.respond(make_response({
              .error_code = error_code::invalid_request,
              .error_message = "Invalid Request - unspecified match for "
                               "exact_name match type",
            }));
        } else {
            auto matches = describe_specific_client(component.match);
            results.insert(results.end(), matches.begin(), matches.end());
        }
        break;
    case describe_client_quotas_match_type::default_name:
        results.push_back(describe_default_client());
        break;
    case describe_client_quotas_match_type::any_specified_name:
        auto matches = describe_specific_client({});
        results.insert(results.end(), matches.begin(), matches.end());
        results.push_back(describe_default_client());
        break;
    }

    describe_client_quotas_response response;

    response.data.entries
      = std::vector<describe_client_quotas_response_entry_data>{};
    response.data.entries->reserve(results.size());
    for (auto& [k, v] : results) {
        response.data.entries->emplace_back(k, v);
    }

    co_return co_await ctx.respond(std::move(response));
}

} // namespace kafka
