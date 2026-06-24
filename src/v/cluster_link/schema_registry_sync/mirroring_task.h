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

#include "cluster_link/schema_registry_sync/inventory.h"
#include "cluster_link/schema_registry_sync/reconciler.h"
#include "cluster_link/schema_registry_sync/source_reader.h"
#include "cluster_link/task.h"
#include "schema/registry.h"

namespace cluster_link::schema_registry_sync {

/// Shadows a source Schema Registry into the local (destination) Schema
/// Registry. Runs on the shard leading `_schemas/0`, a cluster-wide singleton.
/// Each run reconciles the source onto the destination, importing the source
/// schema versions missing from the destination in reference (topological)
/// order.
///
/// Source failures travel as `source_error` values: an unavailable source
/// parks the link, a per-item failure is counted and skipped. Destination and
/// internal faults throw and become `faulted` via the base task runner.
class mirroring_task : public task {
public:
    static constexpr auto task_name = "Schema Registry Shadowing";

    mirroring_task(
      link* link,
      const model::metadata& link_metadata,
      schema::registry* destination,
      source_reader_factory* source_factory);
    mirroring_task(const mirroring_task&) = delete;
    mirroring_task(mirroring_task&&) = delete;
    mirroring_task& operator=(const mirroring_task&) = delete;
    mirroring_task& operator=(mirroring_task&&) = delete;
    ~mirroring_task() override = default;

    void update_config(const model::metadata& link_metadata) override;

    model::enabled_t is_enabled() const final;

    model::task_status_report get_status_report() const override;

protected:
    ss::future<state_transition> run_impl(ss::abort_source&) override;

    bool should_start_impl(ss::shard_id, ::model::node_id) const final;

    bool should_stop_impl(ss::shard_id, ::model::node_id) const final;

private:
    bool leads_schema_registry_partition() const;

    /// Whether a periodic full scan is due (first run, or the full-sync
    /// interval has elapsed). A config change additionally forces one via
    /// `_config_changed`, consumed in `run_impl`.
    bool should_long_sync() const;

    /// Rescans the destination inventory across all in-scope contexts, retains
    /// it on the task, and refreshes the destination counters. Throws on
    /// internal/destination faults.
    ss::future<> refresh_destination_inventory(
      const std::function<bool(const ppsr::context_subject&)>& in_scope,
      ss::abort_source&);

    /// Full source scan and create-only reconcile: discovers the active source
    /// nodes (across `contexts`), imports those missing from the destination's
    /// active set in reference order, and folds the result into `summary` and
    /// the task status. Returns the resulting task state (active, or
    /// link_unavailable if the source becomes unreachable).
    ss::future<state_transition> full_source_sync(
      ss::abort_source&,
      model::schema_registry_sync_summary&,
      const chunked_hash_set<ppsr::context>& contexts,
      const std::function<bool(const ppsr::context_subject&)>& in_scope);

    /// Lists one subject's active versions from the source and folds the result
    /// into `source_active`. A reachable-but-failed listing is counted as a
    /// per-item error (via `summary`) and skipped; a source_unavailable is
    /// captured in `unavailable` so the caller can back the whole sync off.
    /// Run with bounded concurrency from full_source_sync: this is a member
    /// (not a lambda) so the coroutine frame holds `this` and the shared state
    /// directly -- a coroutine lambda passed to max_concurrent_for_each could
    /// be freed while suspended, dangling its captures.
    ss::future<> list_one_subject(
      const ppsr::context_subject& subject,
      ss::abort_source& as,
      chunked_hash_set<ppsr::subject_version>& source_active,
      std::optional<source_error>& unavailable,
      model::schema_registry_sync_summary& summary);

    [[nodiscard]] state_transition make_unavailable(const ss::sstring& reason);
    [[nodiscard]] state_transition make_active();
    [[nodiscard]] state_transition make_faulted(const ss::sstring& reason);

    model::schema_registry_sync_config _config;
    schema::registry* _destination;
    source_reader_factory* _source_factory;
    std::unique_ptr<source_reader> _reader;
    inventory _destination_inventory;
    model::schema_registry_sync_status _status;
    // Live counters for the in-flight reconcile, incremented by the reconciler
    // as each node completes. get_status_report reflects them onto the reported
    // status so a long full sync shows incremental progress; the end-of-sync
    // fold then moves them into the persistent summary/totals and resets this.
    reconcile_stats _reconcile_stats;
    std::optional<ss::lowres_clock::time_point> _last_full_sync;
    // Set by update_config, consumed by run_impl to force a full scan. A flag
    // (rather than mutating _status/_last_full_sync in update_config) avoids
    // racing an in-flight run_impl across its co_await suspension points.
    bool _config_changed{false};
};

class mirroring_task_factory : public task_factory {
public:
    mirroring_task_factory(
      schema::registry* destination, source_reader_factory* source_factory)
      : _destination(destination)
      , _source_factory(source_factory) {}

    std::string_view created_task_name() const noexcept override;

    std::unique_ptr<task> create_task(link* link) override;

private:
    schema::registry* _destination;
    source_reader_factory* _source_factory;
};

} // namespace cluster_link::schema_registry_sync
