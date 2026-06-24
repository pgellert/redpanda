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

#include "cluster_link/model/types.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "pandaproxy/schema_registry/types.h"

#include <functional>
#include <optional>

namespace cluster_link::schema_registry_sync {

namespace ppsr = pandaproxy::schema_registry;

/// Resolves a source schema's references to the (subject, version) nodes they
/// point at, so the engine can import referenced schemas before the schemas
/// that depend on them. Unqualified references resolve against the referring
/// schema's own context.
chunked_vector<ppsr::subject_version>
resolve_refs(const ppsr::stored_schema& stored);

/// Builds a pure, copyable predicate reporting whether a context-qualified
/// subject belongs to the link's scope, following the configured source
/// filter's union semantics:
///   * neither `contexts` nor `subjects` configured: no filter, every node is
///     in scope;
///   * otherwise a node is in scope if its context is one of `contexts`, OR it
///     is one of the individually listed `subjects`.
/// A subject filter therefore widens scope -- it adds individual subjects from
/// contexts the context filter does not select -- rather than narrowing it.
/// Copyable so each registry shard can run its own copy.
std::function<bool(const ppsr::context_subject&)> make_in_scope(
  chunked_hash_set<ppsr::context> contexts,
  chunked_hash_set<ppsr::context_subject> subjects);

/// Returns a human-readable fault reason if the config combined with the
/// discovered in-scope contexts describes a configuration this engine cannot
/// replicate, else std::nullopt. `qualified_subjects_enabled` reflects the
/// cluster config and is injected so this check stays pure and testable.
std::optional<ss::sstring> check_preconditions(
  const model::schema_registry_sync_config& config,
  const chunked_hash_set<ppsr::context>& in_scope_contexts,
  bool qualified_subjects_enabled);

} // namespace cluster_link::schema_registry_sync
