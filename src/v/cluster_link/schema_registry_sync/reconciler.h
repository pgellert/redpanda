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

#include "cluster_link/schema_registry_sync/source_reader.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "pandaproxy/schema_registry/types.h"
#include "schema/registry.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>

#include <cstdint>
#include <functional>

namespace cluster_link::schema_registry_sync {

namespace ppsr = pandaproxy::schema_registry;

/// Outcome counters folded into the task's per-sync summary.
struct reconcile_stats {
    /// Source versions imported into the destination this run.
    uint64_t versions_changed{0};
    /// Per-item failures (unresolvable references, import conflicts). These do
    /// not abort the run; the reconciler counts them and continues.
    uint64_t errors{0};
};

/// The set of changes a reconcile applies. Create-only: every upsert is a
/// (context, subject, version) node present on the source but absent from the
/// destination's active set.
struct work_set {
    chunked_vector<ppsr::subject_version> upserts;
};

/// \brief Imports missing source schema versions into the destination in
/// reference (topological) order.
///
/// The destination Schema Registry validates that a schema's references already
/// exist before accepting an import, so a referrer can only be imported after
/// every schema it references. The reconciler discovers each node's references,
/// builds the dependency graph incrementally, and drains it referent-first.
///
/// A node whose references are all already satisfied is imported on its first
/// fetch (1-fetch path). A node discovered before its dependencies releases its
/// body and is re-fetched once they complete (2-fetch path).
///
/// Not re-entrant: a single reconcile runs at a time. Per-run state is reset at
/// the start of each `reconcile` call.
class reconciler {
public:
    /// Resource bounds for a reconcile run.
    struct limits {
        /// Number of worker fibers draining the discover/import queues.
        size_t parallelism;
    };

    reconciler(
      source_reader* source,
      schema::registry* destination,
      std::function<bool(const ppsr::context_subject&)> in_scope,
      limits lim);

    /// Imports `work_set.upserts` referent-first.
    ///
    /// `seed_replicated` is the destination inventory's `all` set: nodes
    /// already present on the destination (including soft-deleted ones) satisfy
    /// a referrer's references without being re-imported.
    ///
    /// `stats` is incremented live as each node completes: `versions_changed`
    /// on every successful import, `errors` on every per-item failure, at the
    /// moment it happens. The caller can observe partial progress mid-run (it
    /// is single-shard, so the increments are synchronous and race-free). The
    /// caller must clear `stats` before the call if it wants per-run counts.
    ///
    /// If the source becomes unreachable the future resolves with a
    /// `source_error` of kind `source_unavailable`; the caller surfaces this as
    /// link-unavailable. Per-item failures are counted in
    /// `reconcile_stats::errors`, not propagated.
    ss::future<source_result<void>> reconcile(
      work_set work,
      chunked_hash_set<ppsr::subject_version> seed_replicated,
      reconcile_stats& stats,
      ss::abort_source& as);

private:
    /// Lifecycle of a node in the reconcile graph. Every enqueue is gated on
    /// the node's state so a node is never fetched, imported, or counted twice.
    enum class node_state : uint8_t {
        unseen,
        discovering,
        pending,
        importing,
        done,
        errored,
    };

    struct node_data {
        node_state state{node_state::unseen};
        uint32_t in_deg{0};
        chunked_vector<ppsr::subject_version> dependents;
    };

    /// Reads a discover-queue node, resolves its references, and either imports
    /// it now (deps satisfied) or registers it as pending behind its missing
    /// referents. Returns a source_error only on `source_unavailable`.
    ss::future<source_result<void>>
    discover(const ppsr::subject_version& n, ss::abort_source& as);

    /// Reads and imports an import-queue node whose dependencies have all
    /// completed.
    ss::future<source_result<void>>
    do_import(const ppsr::subject_version& n, ss::abort_source& as);

    /// Imports a fetched body, classifying conflicts as per-item failures.
    /// Returns true on a successful import.
    ss::future<bool>
    import_body(const ppsr::subject_version& n, ppsr::stored_schema schema);

    /// Marks a node replicated and releases dependents whose in-degree hits 0.
    void wake(const ppsr::subject_version& n);

    /// Marks a node (and transitively its dependents) failed.
    void fail(const ppsr::subject_version& n);

    node_data& data(const ppsr::subject_version& n);

    /// True while either queue has a node ready to be picked up by a worker.
    bool has_work() const { return !_discover_q.empty() || !_import_q.empty(); }

    /// A single worker fiber: waits for work, runs one node's discover/import,
    /// and drives global-termination detection. Loops until done or aborted.
    /// A source fault is recorded in `_fault` and stops the pool.
    ss::future<> worker(ss::abort_source& as);

    source_reader* _source;
    schema::registry* _destination;
    std::function<bool(const ppsr::context_subject&)> _in_scope;
    limits _limits;

    chunked_hash_set<ppsr::subject_version> _replicated;
    chunked_hash_map<ppsr::subject_version, node_data> _nodes;
    chunked_vector<ppsr::subject_version> _discover_q;
    chunked_vector<ppsr::subject_version> _import_q;
    /// Caller-provided counters, incremented live as nodes complete. Valid only
    /// for the duration of a `reconcile` call.
    reconcile_stats* _stats{nullptr};

    /// Queued-or-in-flight node count. A node counts from the moment it is
    /// enqueued until the worker that popped it finishes its discover/import
    /// (including the synchronous `wake`/`fail` that may re-enqueue
    /// dependents). The run is complete when this reaches zero with both queues
    /// empty.
    size_t _outstanding{0};
    /// Set once the run is complete or must stop (fault/abort); all workers
    /// observe it through the condition variable and exit.
    bool _done{false};
    /// First `source_unavailable` error seen by any worker; stops the pool.
    std::optional<source_error> _fault;
    /// First non-shutdown exception (e.g. an unexpected import error) seen by
    /// any worker; rethrown after the pool drains to fault the whole sync.
    std::exception_ptr _exn;

    ss::condition_variable _cv;
};

} // namespace cluster_link::schema_registry_sync
