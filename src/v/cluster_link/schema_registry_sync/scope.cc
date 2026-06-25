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

#include "cluster_link/schema_registry_sync/scope.h"

#include "ssx/sformat.h"

#include <seastar/core/sstring.hh>

#include <absl/container/flat_hash_set.h>

#include <iterator>

namespace cluster_link::schema_registry_sync {

std::function<bool(const ppsr::context_subject&)> make_in_scope(
  chunked_hash_set<ppsr::context> contexts,
  chunked_hash_set<ppsr::context_subject> subjects) {
    // Capture copyable sets by value so each shard gets its own copy; the
    // predicate is copied onto foreign shards by the registry's map_reduce.
    absl::flat_hash_set<ppsr::context> ctx_set(
      std::make_move_iterator(contexts.begin()),
      std::make_move_iterator(contexts.end()));
    absl::flat_hash_set<ppsr::context_subject> sub_set(
      std::make_move_iterator(subjects.begin()),
      std::make_move_iterator(subjects.end()));
    // With neither selector configured the filter is absent and everything is
    // in scope. Otherwise the two selectors union: a node is in scope if its
    // whole context is selected, or it is an individually listed subject.
    const bool unfiltered = ctx_set.empty() && sub_set.empty();
    return [unfiltered,
            ctx_set = std::move(ctx_set),
            sub_set = std::move(sub_set)](const ppsr::context_subject& sub) {
        return unfiltered || ctx_set.contains(sub.ctx) || sub_set.contains(sub);
    };
}

std::optional<ss::sstring> check_preconditions(
  const model::schema_registry_sync_config& config,
  const chunked_hash_set<ppsr::context>& in_scope_contexts,
  bool qualified_subjects_enabled) {
    const auto* api = config.api_mode();
    if (api != nullptr && api->destination.has_value()) {
        if (
          std::holds_alternative<
            model::schema_registry_sync_config::exact_context_mapping>(
            *api->destination)) {
            return ss::sstring{
              "context remapping is not supported: the shadowing engine "
              "requires identity context mapping"};
        }
    }

    if (!qualified_subjects_enabled) {
        for (const auto& ctx : in_scope_contexts) {
            if (ctx != ppsr::default_context) {
                return ssx::sformat(
                  "replicating non-default context '{}' requires the "
                  "schema_registry_enable_qualified_subjects cluster config "
                  "to be enabled",
                  ctx);
            }
        }
    }

    return std::nullopt;
}

} // namespace cluster_link::schema_registry_sync
