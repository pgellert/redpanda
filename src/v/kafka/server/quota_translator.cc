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

#include "kafka/server/quota_translator.h"

#include "config/configuration.h"

#include <optional>

namespace kafka {

quota_translator::quota_translator()
  : _default_target_produce_tp_rate(
    config::shard_local_cfg().target_quota_byte_rate.bind())
  , _default_target_fetch_tp_rate(
      config::shard_local_cfg().target_fetch_quota_byte_rate.bind())
  , _target_partition_mutation_quota(
      config::shard_local_cfg().kafka_admin_topic_api_rate.bind())
  , _target_produce_tp_rate_per_client_group(
      config::shard_local_cfg().kafka_client_group_byte_rate_quota.bind())
  , _target_fetch_tp_rate_per_client_group(
      config::shard_local_cfg()
        .kafka_client_group_fetch_byte_rate_quota.bind()) {
    auto update_quotas = [this]() {
        if (_on_change) {
            (*_on_change)();
        }
    };
    _target_produce_tp_rate_per_client_group.watch(update_quotas);
    _target_fetch_tp_rate_per_client_group.watch(update_quotas);
    _target_partition_mutation_quota.watch(update_quotas);
    _default_target_produce_tp_rate.watch(update_quotas);
    _default_target_fetch_tp_rate.watch(update_quotas);
}

int64_t quota_translator::get_client_target_produce_tp_rate(
  const tracker_key& quota_id) {
    return std::visit(
      [this](const auto& qid) -> int64_t {
          using T = std::decay_t<decltype(qid)>;
          if constexpr (std::is_same_v<k_client_id, T>) {
              return _default_target_produce_tp_rate();
          } else if constexpr (std::is_same_v<k_group_name, T>) {
              auto group = _target_produce_tp_rate_per_client_group().find(qid);
              if (group != _target_produce_tp_rate_per_client_group().end()) {
                  return group->second.quota;
              }
              return _default_target_produce_tp_rate();
          } else {
              static_assert(always_false_v<T>, "Unknown tracker_key type");
          }
      },
      quota_id);
}

std::optional<int64_t>
quota_translator::get_client_target_fetch_tp_rate(const tracker_key& quota_id) {
    return std::visit(
      [this](const auto& qid) -> std::optional<int64_t> {
          using T = std::decay_t<decltype(qid)>;
          if constexpr (std::is_same_v<k_client_id, T>) {
              return _default_target_fetch_tp_rate();
          } else if constexpr (std::is_same_v<k_group_name, T>) {
              auto group = _target_fetch_tp_rate_per_client_group().find(qid);
              if (group != _target_fetch_tp_rate_per_client_group().end()) {
                  return group->second.quota;
              }
              return _default_target_fetch_tp_rate();
          } else {
              static_assert(always_false_v<T>, "Unknown tracker_key type");
          }
      },
      quota_id);
}

namespace {
// If client is part of some group then client quota ID is a group
// else client quota ID is client_id
tracker_key get_client_quota_id(
  const std::optional<std::string_view>& client_id,
  const std::unordered_map<ss::sstring, config::client_group_quota>&
    group_quota) {
    if (!client_id) {
        // requests without a client id are grouped into an anonymous group that
        // shares a default quota. the anonymous group is keyed on empty string.
        return k_client_id{""};
    }
    for (const auto& group_and_limit : group_quota) {
        if (client_id->starts_with(
              std::string_view(group_and_limit.second.clients_prefix))) {
            return k_group_name{group_and_limit.first};
        }
    }
    return k_client_id{*client_id};
}

} // namespace

tracker_key quota_translator::get_produce_key(
  const std::optional<std::string_view>& client_id) {
    return get_client_quota_id(
      client_id, _target_produce_tp_rate_per_client_group());
}

tracker_key quota_translator::get_fetch_key(
  const std::optional<std::string_view>& client_id) {
    return get_client_quota_id(
      client_id, _target_fetch_tp_rate_per_client_group());
}

tracker_key quota_translator::get_partition_mutation_key(
  const std::optional<std::string_view>& client_id) {
    return get_client_quota_id(client_id, {});
}

quota_limits quota_translator::find_quota_value(const tracker_key& key) {
    return quota_limits{
      .produce_limit = get_client_target_produce_tp_rate(key),
      .fetch_limit = get_client_target_fetch_tp_rate(key),
      .partition_mutation_limit = _target_partition_mutation_quota(),
    };
}

} // namespace kafka
