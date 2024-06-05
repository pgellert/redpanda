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

#include "client_quota_frontend.h"
#include "client_quota_serde.h"
#include "client_quota_store.h"
#include "kafka/protocol/exceptions.h"
#include "kafka/protocol/schemata/alter_client_quotas_request.h"
#include "kafka/protocol/schemata/alter_client_quotas_response.h"
#include "kafka/protocol/schemata/describe_client_quotas_request.h"
#include "kafka/protocol/schemata/describe_client_quotas_response.h"
#include "kafka/server/client_quota_translator.h"
#include "kafka/server/errors.h"
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

template<typename E>
std::enable_if_t<std::is_enum_v<E>, std::optional<E>>
  from_string_view(std::string_view);

template<>
constexpr std::optional<client_quota_type>
from_string_view<client_quota_type>(std::string_view v) {
    return string_switch<std::optional<client_quota_type>>(v)
      .match(
        to_string_view(client_quota_type::produce_quota),
        client_quota_type::produce_quota)
      .match(
        to_string_view(client_quota_type::fetch_quota),
        client_quota_type::fetch_quota)
      .match(
        to_string_view(client_quota_type::partition_mutation_quota),
        client_quota_type::partition_mutation_quota)
      .default_match(std::nullopt);
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
    using entity_key = cluster::client_quota::entity_key;
    using entity_value = cluster::client_quota::entity_value;

    alter_client_quotas_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    cluster::client_quota::alter_delta_cmd_data cmd;
    constexpr auto make_part = [](const auto& entity) {
        entity_key::part part;
        if (entity.entity_type == "client-id") {
            if (entity.entity_name.value_or("") == "") {
                part.part.emplace<entity_key::part::client_id_default_match>();
            } else {
                part.part.emplace<entity_key::part::client_id_match>(
                  entity_key::part::client_id_match{
                    .value = entity.entity_name.value_or("")});
            }
        } else if (entity.entity_type == "client-id-prefix") {
            part.part.emplace<entity_key::part::client_id_prefix_match>(
              entity_key::part::client_id_prefix_match{
                .value = entity.entity_name.value_or("")});
        } else {
            throw kafka::exception(
              kafka::error_code::invalid_config,
              fmt::format("Unknown entity_type: {}", entity.entity_type));
        }
        return part;
    };

    for (const auto& entry : request.data.entries) {
        entity_key key;
        entity_value val;
        key.parts.reserve(entry.entity.size());
        for (const auto& entity : entry.entity) {
            key.parts.emplace(make_part(entity));
        }
        for (const auto& op : entry.ops) {
            auto cqt = from_string_view<client_quota_type>(op.key);
            if (!cqt) {
                continue;
            }
            get_entity_value_field(cqt.value(), val) = op.value;
        }
        cmd.upsert.push_back({.key = std::move(key), .value = val});
    }

    auto err = co_await ctx.quota_frontend().alter_quotas(
      cmd, model::timeout_clock::now() + 5s);
    alter_client_quotas_response response;

    kafka::error_code ec = kafka::error_code::none;
    ss::sstring msg;

    if (err) {
        ec = [](std::error_code err) {
            if (err.category() == cluster::error_category()) {
                return map_topic_error_code(
                  static_cast<cluster::errc>(err.value()));
            } else {
                return kafka::error_code::unknown_server_error;
            }
        }(err);
        msg = err.message();
    }
    for (const auto& entry : request.data.entries) {
        response.data.entries.push_back(alter_client_quotas_response_entry_data{
          .error_code = ec, .error_message = msg});

        response.data.entries.back().entity.reserve(entry.entity.size());
        for (const auto& e : entry.entity) {
            response.data.entries.back().entity.emplace_back(
              e.entity_type, e.entity_name, e.unknown_tags);
        }
    }

    co_return co_await ctx.respond(std::move(response));
}

} // namespace kafka
