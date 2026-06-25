/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster_link/schema_registry_sync/inventory.h"

#include <seastar/core/coroutine.hh>

namespace cluster_link::schema_registry_sync {

ss::future<inventory> scan_destination_inventory(
  schema::registry& destination,
  std::function<bool(const ppsr::context_subject&)> in_scope,
  ss::abort_source& as) {
    as.check();
    auto versions = co_await destination.list_subject_versions(
      std::move(in_scope), ppsr::include_deleted::yes);
    inventory inv;
    inv.all.reserve(versions.size());
    for (const auto& sv : versions) {
        auto node = ppsr::subject_version{sv.sub, sv.version};
        if (sv.deleted == ppsr::is_deleted::no) {
            inv.active.insert(node);
        }
        inv.all.insert(std::move(node));
    }
    co_return inv;
}

} // namespace cluster_link::schema_registry_sync
