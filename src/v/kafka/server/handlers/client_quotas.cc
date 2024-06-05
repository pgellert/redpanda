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

#include "client_quota_serde.h"
#include "client_quota_store.h"
#include "kafka/protocol/schemata/alter_client_quotas_request.h"
#include "kafka/protocol/schemata/alter_client_quotas_response.h"
#include "kafka/protocol/schemata/describe_client_quotas_request.h"
#include "kafka/protocol/schemata/describe_client_quotas_response.h"
#include "kafka/server/client_quota_translator.h"
#include "kafka/server/handlers/alter_client_quotas.h"
#include "kafka/server/handlers/describe_client_quotas.h"

namespace kafka {

namespace {

constexpr std::string_view to_string_view(client_quota_type t) {
    switch (t) {
    case client_quota_type::produce_quota:
        return "producer_byte_rate";
    case client_quota_type::fetch_quota:
        return "consumer_byte_rate";
    case client_quota_type::partition_mutation_quota:
        return "controller_mutation_rate";
    }
}

template<typename T>
requires std::
  is_same_v<std::remove_cvref_t<T>, cluster::client_quota::entity_value>
  auto& get_entity_value_field(client_quota_type cqt, T&& val) {
    switch (cqt) {
    case client_quota_type::produce_quota:
        return std::forward<T>(val).producer_byte_rate;
    case client_quota_type::fetch_quota:
        return std::forward<T>(val).consumer_byte_rate;
    case client_quota_type::partition_mutation_quota:
        return std::forward<T>(val).controller_mutation_rate;
    }
}

void make_error_response(
  alter_client_quotas_request& req, alter_client_quotas_response& resp) {
    for (const auto& entry [[maybe_unused]] : req.data.entries) {
        resp.data.entries.push_back(
          kafka::alter_client_quotas_response_entry_data{
            .error_code = error_code::unsupported_version,
            .error_message = "Unsupported version - not yet implemented",
          });
    }
}

describe_client_quotas_response_entity_data
get_entity_data(const cluster::client_quota::entity_key::part& p) {
    using part = cluster::client_quota::entity_key::part;
    using entity_data = describe_client_quotas_response_entity_data;
    return ss::visit(
      p.part,
      [](const part::client_id_default_match&) {
          return entity_data{
            .entity_type = "client-id", .entity_name = std::nullopt};
      },
      [](const part::client_id_match& m) {
          return entity_data{
            .entity_type = "client-id", .entity_name = m.value};
      },
      [](const part::client_id_prefix_match& m) {
          return entity_data{
            .entity_type = "client-id-prefix", .entity_name = m.value};
      });
}

using entities_data
  = decltype(describe_client_quotas_response_entry_data::entity);

entities_data get_entity_data(const cluster::client_quota::entity_key& k) {
    entities_data ret;
    for (const auto& p : k.parts) {
        ret.emplace_back(get_entity_data(p));
    }
    return ret;
}

using values_data
  = decltype(describe_client_quotas_response_entry_data::values);

values_data get_value_data(const cluster::client_quota::entity_value& val) {
    const auto keys = {
      client_quota_type::produce_quota,
      client_quota_type::fetch_quota,
      client_quota_type::partition_mutation_quota};

    values_data ret;
    ret.reserve(absl::c_count_if(keys, [&val](const auto& v) {
        return get_entity_value_field(v, val).has_value();
    }));

    auto inserter = std::back_inserter(ret);
    const auto maybe_push_back = [&inserter](
                                   client_quota_type key, const auto& val) {
        if (val.has_value()) {
            *inserter = (value_data{
              .key = ss::sstring{to_string_view(key)},
              .value = static_cast<float64_t>(val.value())});
        }
    };
    for (const auto& v : keys) {
        maybe_push_back(v, get_entity_value_field(v, val));
    }
    return ret;
}

using quota_cfg
  = std::unordered_map<ss::sstring, config::client_group_quota>::value_type;

using quota_type = cluster::client_quota::store::container_type::value_type;

describe_client_quotas_response_entry_data make_entry(const quota_cfg& p) {
    return {
      .entity
      = {{.entity_type = "client-id-prefix", .entity_name = p.second.group_name}},
      .values = {
        {.key = p.second.key(),
         .value = static_cast<float64_t>(p.second.quota)}}};
}

describe_client_quotas_response_entry_data make_entry(const quota_type& q) {
    return {
      .entity = get_entity_data(q.first), .values = get_value_data(q.second)};
}

} // namespace

template<>
ss::future<response_ptr> describe_client_quotas_handler::handle(
  request_context ctx, ss::smp_service_group) {
    describe_client_quotas_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    auto quotas = ctx.quota_store().range(
      [](const cluster::client_quota::store::container_type::value_type&) {
          return true;
      });

    // TODO: Remove these in favour of hiding them behind the translator
    auto const& config = config::shard_local_cfg();
    auto const& produce = config.kafka_client_group_byte_rate_quota();
    auto const& fetch = config.kafka_client_group_fetch_byte_rate_quota();

    describe_client_quotas_response res{
      .data = {
        .error_code = kafka::error_code::none,
        .entries
        = chunked_vector<describe_client_quotas_response_entry_data>{}}};
    res.data.entries->reserve(produce.size() + fetch.size() + quotas.size());

    auto out_it = std::back_inserter(*res.data.entries);
    absl::c_transform(
      produce, out_it, [](quota_cfg const& v) { return make_entry(v); });
    absl::c_transform(
      fetch, out_it, [](quota_cfg const& v) { return make_entry(v); });
    absl::c_transform(
      quotas, out_it, [](quota_type const& v) { return make_entry(v); });

    return ctx.respond(std::move(res));
}

template<>
ss::future<response_ptr> alter_client_quotas_handler::handle(
  request_context ctx, ss::smp_service_group) {
    alter_client_quotas_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    // TODO: implement the AlterClientQuotas API
    // ctx.quota_store().get_quota(...);
    // ctx.quota_frontend().alter_quotas(...);

    alter_client_quotas_response response;
    make_error_response(request, response);

    co_return co_await ctx.respond(std::move(response));
}

} // namespace kafka
