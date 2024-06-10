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
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/alter_client_quotas_request.h"
#include "kafka/protocol/schemata/alter_client_quotas_response.h"
#include "kafka/protocol/schemata/describe_client_quotas_request.h"
#include "kafka/protocol/schemata/describe_client_quotas_response.h"
#include "kafka/server/errors.h"
#include "kafka/server/handlers/alter_client_quotas.h"
#include "kafka/server/handlers/describe_client_quotas.h"

#include <seastar/core/sstring.hh>

#include <absl/algorithm/container.h>
#include <boost/outcome/success_failure.hpp>

namespace kafka {

namespace {

using cluster::client_quota::entity_key;
using cluster::client_quota::entity_value;
using cluster::client_quota::entity_value_diff;

template<typename E>
std::enable_if_t<std::is_enum_v<E>, std::optional<E>>
  from_string_view(std::string_view);

template<>
constexpr std::optional<entity_value_diff::key>
from_string_view<entity_value_diff::key>(std::string_view v) {
    return string_switch<std::optional<entity_value_diff::key>>(v)
      .match(
        to_string_view(entity_value_diff::key::producer_byte_rate),
        entity_value_diff::key::producer_byte_rate)
      .match(
        to_string_view(entity_value_diff::key::consumer_byte_rate),
        entity_value_diff::key::consumer_byte_rate)
      .match(
        to_string_view(entity_value_diff::key::controller_mutation_rate),
        entity_value_diff::key::controller_mutation_rate)
      .default_match(std::nullopt);
}

describe_client_quotas_response_entity_data
get_entity_data(const entity_key::part& p) {
    using entity_data = describe_client_quotas_response_entity_data;
    return ss::visit(
      p.part,
      [](const entity_key::part::client_id_default_match&) -> entity_data {
          return {.entity_type = "client-id", .entity_name = std::nullopt};
      },
      [](const entity_key::part::client_id_match& m) -> entity_data {
          return {.entity_type = "client-id", .entity_name = m.value};
      },
      [](const entity_key::part::client_id_prefix_match& m) -> entity_data {
          return {.entity_type = "client-id-prefix", .entity_name = m.value};
      });
}

using entities_data
  = decltype(describe_client_quotas_response_entry_data::entity);

entities_data get_entity_data(const entity_key& k) {
    entities_data ret;
    ret.reserve(k.parts.size());
    for (const auto& p : k.parts) {
        ret.emplace_back(get_entity_data(p));
    }
    return ret;
}

using values_data
  = decltype(describe_client_quotas_response_entry_data::values);

values_data get_value_data(const entity_value& val) {
    values_data ret;

    if (val.producer_byte_rate) {
        ret.emplace_back(
          ss::sstring(
            to_string_view(entity_value_diff::key::producer_byte_rate)),
          *val.producer_byte_rate);
    }

    if (val.consumer_byte_rate) {
        ret.emplace_back(
          ss::sstring(
            to_string_view(entity_value_diff::key::consumer_byte_rate)),
          *val.consumer_byte_rate);
    }

    if (val.controller_mutation_rate) {
        ret.emplace_back(
          ss::sstring(
            to_string_view(entity_value_diff::key::controller_mutation_rate)),
          *val.controller_mutation_rate);
    }
    return ret;
}

using quota_type = cluster::client_quota::store::container_type::value_type;

result<entity_key::part, kafka::error_code> make_part(const auto& entity) {
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
        return kafka::error_code::invalid_config;
    }
    return part;
};

} // namespace

template<>
ss::future<response_ptr> describe_client_quotas_handler::handle(
  request_context ctx, ss::smp_service_group) {
    describe_client_quotas_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    describe_client_quotas_response res{
      .data = {
        .error_code = kafka::error_code::none,
        .entries = decltype(res.data.entries)::value_type{}}};

    auto quotas = ctx.quota_store().range(
      [](const std::pair<entity_key, entity_value>&) {
          // TODO: Matching strict && components
          return true;
      });

    res.data.entries->reserve(quotas.size());
    for (const auto& q : quotas) {
        res.data.entries->emplace_back(
          get_entity_data(q.first), get_value_data(q.second));
    }

    return ctx.respond(std::move(res));
}

template<>
ss::future<response_ptr> alter_client_quotas_handler::handle(
  request_context ctx, ss::smp_service_group) {
    alter_client_quotas_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    cluster::client_quota::alter_delta_cmd_data cmd;
    alter_client_quotas_response response;
    response.data.entries.reserve(request.data.entries.size());

    for (const auto& entry : request.data.entries) {
        auto& entry_res = response.data.entries.emplace_back();
        entity_key key;
        entity_value_diff diff;
        key.parts.reserve(entry.entity.size());
        for (const auto& entity : entry.entity) {
            auto part = make_part(entity);
            if (part.has_error()) {
                entry_res.error_code = part.assume_error();
                entry_res.error_message = fmt::format(
                  "Unknown entity-type: {}", entity.entity_type);
                break;
            }
            key.parts.emplace(std::move(part).assume_value());
        }
        if (entry_res.error_code != error_code::none) {
            continue;
        }
        for (const auto& op : entry.ops) {
            auto cqt = from_string_view<entity_value_diff::key>(op.key);
            if (!cqt) {
                entry_res.error_code = kafka::error_code::invalid_config;
                entry_res.error_message = fmt::format(
                  "Unknown key: {}", op.key);
                break;
            }
            diff.entries.emplace(
              op.remove ? entity_value_diff::operation::remove
                        : entity_value_diff::operation::upsert,
              *cqt,
              op.value);
        }
        if (entry_res.error_code == error_code::none) {
            cmd.ops.push_back({.key = std::move(key), .diff = std::move(diff)});
        }
    }

    auto err = co_await ctx.quota_frontend().alter_quotas(
      cmd, model::timeout_clock::now() + 5s);

    if (err) {
        // Translate error message
        auto ec = [](std::error_code err) {
            if (err.category() == cluster::error_category()) {
                return map_topic_error_code(
                  static_cast<cluster::errc>(err.value()));
            } else {
                return kafka::error_code::unknown_server_error;
            }
        }(err);
        // Error any response that is not already errored
        for (auto& entry : response.data.entries) {
            if (entry.error_code == error_code::none) {
                entry.error_code = ec;
                entry.error_message = err.message();
            }
        }
    }

    co_return co_await ctx.respond(std::move(response));
}

} // namespace kafka
