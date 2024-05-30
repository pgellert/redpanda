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

#pragma once

#include "base/seastarx.h"
#include "config/client_group_byte_rate_quota.h"
#include "config/property.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

#include <utility>

namespace kafka {

using k_client_id = named_type<ss::sstring, struct k_client_id_tag>;
using k_group_name = named_type<ss::sstring, struct k_group_name_tag>;

/// tracker_key is the we use to key into the client quotas map
///
/// Note: while the limits applicable to a single tracker_key are always going
/// to be the same, it is not guaranteed that a client id will have the same
/// tracker_key for different types of kafka requests (produce/fetch/partition
/// mutations). For examples, refer to the unit tests.
///
/// Note: the tracker_key is different from the entity_key used to configure
/// client quotas. The tracker_key defines the granularity at which we track
/// quotas, whereas the quota limits might be defined more broadly.
/// For example, if the default client quota applies to the request, the
/// tracker_key may be client id specific even though the matching entity_key is
/// the default client quota. In this case, we may have multiple independent
/// rate trackers for each unique client with all of these rate tracking having
/// the same shared quota limit
using tracker_key = std::variant<k_client_id, k_group_name>;

/// quota_limits describes the limits applicable to a tracker_key
struct quota_limits {
    std::optional<uint64_t> produce_limit;
    std::optional<uint64_t> fetch_limit;
    std::optional<uint64_t> partition_mutation_limit;
};

/// quota_translator is responsible for providing quota_manager with a
/// simplified interface to the quota configurations
///  * It is responsible for translating the quota-specific request context (for
///  now the client.id header field) into the applicable tracker_key.
///  * It is also responsible for finding the quota limits of a tracker_key
class quota_translator {
public:
    using on_change_fn = std::function<void()>;

    quota_translator();

    /// Returns the quota tracker key used for produce requests
    /// Note: because the client quotas configured for produce/fetch/pm might be
    /// different, the tracker_key for produce/fetch/pm might be different
    tracker_key
    get_produce_key(const std::optional<std::string_view>& client_id);

    /// Returns the quota tracker key used for fetch requests
    /// Note: because the client quotas configured for produce/fetch/pm might be
    /// different, the tracker_key for produce/fetch/pm might be different
    tracker_key get_fetch_key(const std::optional<std::string_view>& client_id);

    /// Returns the quota tracker key used for partition mutation requests
    /// Note: because the client quotas configured for produce/fetch/pm might be
    /// different, the tracker_key for produce/fetch/pm might be different
    tracker_key get_partition_mutation_key(
      const std::optional<std::string_view>& client_id);

    /// Finds the limits applicable to the given quota tracker key
    quota_limits find_quota_value(const tracker_key&);

    /// `watch` can be used to register for quota changes
    void watch(on_change_fn&& fn) { _on_change = std::move(fn); };

private:
    using quota_config
      = std::unordered_map<ss::sstring, config::client_group_quota>;

    int64_t get_client_target_produce_tp_rate(const tracker_key& quota_id);
    std::optional<int64_t>
    get_client_target_fetch_tp_rate(const tracker_key& quota_id);

    config::binding<uint32_t> _default_target_produce_tp_rate;
    config::binding<std::optional<uint32_t>> _default_target_fetch_tp_rate;
    config::binding<std::optional<uint32_t>> _target_partition_mutation_quota;
    config::binding<quota_config> _target_produce_tp_rate_per_client_group;
    config::binding<quota_config> _target_fetch_tp_rate_per_client_group;

    std::optional<on_change_fn> _on_change;
};

} // namespace kafka
