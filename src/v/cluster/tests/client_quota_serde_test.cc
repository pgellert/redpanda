// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/client_quota_serde.h"
#include "serde/rw/rw.h"
#include "serde/rw/set.h"     // IWYU pragma: keep
#include "serde/rw/sstring.h" // IWYU pragma: keep

#include <gtest/gtest.h>

namespace cluster::client_quota {

namespace {

// serialize and deserialize to specified type
template<typename To, typename From>
To serde_to(const From& from) {
    auto b = serde::to_iobuf(from);
    return serde::from_iobuf<To>(std::move(b));
}
} // namespace

using part_variant = entity_key::part::variant;

TEST(client_quota_serde, round_trip) {
    const std::vector<part_variant> all_values{
      entity_key::part::client_id_match{.value = "my-consumer"},
      entity_key::part::client_id_prefix_match{.value = "my-cons"},
      entity_key::part::client_id_default_match{},
      entity_key::part::user_match{.value = "alice"},
      entity_key::part::user_default_match{}};

    // Ensure that serde read/write works as expected for inner type
    for (const auto& value : all_values) {
        entity_key::part part{.part = value};
        const auto rt_part = serde_to<entity_key::part>(part);
        EXPECT_EQ(part, rt_part);
    }

    // Ensure that serde read/write works as expected for outer type
    for (const auto& value : all_values) {
        entity_key key{value};
        const auto rt_key = serde_to<entity_key>(key);
        EXPECT_EQ(key, rt_key);
    }
}

} // namespace cluster::client_quota
