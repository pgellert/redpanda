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

#pragma once

#include "container/chunked_hash_map.h"
#include "pandaproxy/schema_registry/types.h"
#include "schema/registry.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <functional>

namespace cluster_link::schema_registry_sync {

namespace ppsr = pandaproxy::schema_registry;

/// A snapshot of the destination Schema Registry's in-scope (subject, version)
/// nodes, retained for diffing against the source during reconciliation.
struct inventory {
    /// Nodes visible without soft-deleted versions (include_deleted::no).
    chunked_hash_set<ppsr::subject_version> active;
    /// All nodes including soft-deleted versions (include_deleted::yes); a
    /// superset of `active`.
    chunked_hash_set<ppsr::subject_version> all;
};

/// Scans the destination registry for every in-scope (subject, version) node.
///
/// `in_scope` decides which context-qualified subjects belong to the link's
/// scope; it must be pure and copyable because it runs on each registry shard.
/// A single include_deleted scan reports every version's soft-delete state, so
/// `active` is derived as the non-deleted subset of `all` from one snapshot.
ss::future<inventory> scan_destination_inventory(
  schema::registry& destination,
  std::function<bool(const pandaproxy::schema_registry::context_subject&)>
    in_scope,
  ss::abort_source& as);

} // namespace cluster_link::schema_registry_sync
