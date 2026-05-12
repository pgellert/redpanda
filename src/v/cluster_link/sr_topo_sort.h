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

#include "container/chunked_vector.h"
#include "pandaproxy/schema_registry/types.h"

namespace cluster_link {

/// Result of topologically sorting a set of schemas by their references.
///
/// `sorted` carries the schemas in an order where, for every schema S,
/// every (subject, version) reference of S that is also present in the
/// input appears *before* S. Schemas with references to things *not* in
/// the input are treated as roots (since the dest SR is assumed to
/// already have them, or they'll be picked up on a subsequent pass).
///
/// If the input contains a reference cycle the algorithm gives up and
/// returns the remaining schemas in input order with `cycle_detected =
/// true`. Cycles are rare in real-world SR data (references walk a DAG
/// in practice) but legal at the wire level, so we warn rather than
/// throw.
struct topo_sort_result {
    chunked_vector<pandaproxy::schema_registry::stored_schema> sorted;
    bool cycle_detected{false};
};

/// Kahn's algorithm. Stable: among nodes with no remaining dependencies,
/// preserves the relative order of the input.
topo_sort_result topo_sort_by_refs(
  chunked_vector<pandaproxy::schema_registry::stored_schema> input);

} // namespace cluster_link
