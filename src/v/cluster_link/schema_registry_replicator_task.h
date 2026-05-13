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

#include "cluster_link/sr_http_client.h"
#include "cluster_link/task.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <re2/re2.h>

#include <memory>
#include <optional>

namespace cluster_link {

/// Task that replicates a Confluent-compatible Schema Registry into the
/// local Redpanda SR over HTTP, preserving subject names and schema IDs.
///
/// The task is configured via the link's
/// `schema_registry_sync_config::shadow_via_http_api` variant; it is a no-op
/// when the variant is unset, set to a different mode, or carries an empty
/// source URL.
///
/// Lifecycle:
///   - First tick: catch-up. List subjects from source, fetch versions and
///     schemas, toposort by references, write to destination in IMPORT
///     mode preserving IDs. Snapshot what we've seen.
///   - Subsequent ticks: tail. Diff source subjects against snapshot,
///     replicate new ones with high priority; every Nth tick also revisit
///     known subjects to pick up new versions and mirror mode/config drift.
///
/// Status reporting and error surfacing piggyback on the standard
/// cluster_link::task framework: the task transitions to
/// model::task_state::link_unavailable on terminal source connectivity
/// failures, faulted on truly unrecoverable conditions, and stays active
/// otherwise. Per-schema validation failures are logged and counted but
/// do not stop the overall task.
class schema_registry_replicator_task : public controller_locked_task {
public:
    static constexpr auto task_name = "Schema Registry HTTP Replicator";
    static constexpr auto default_run_interval = std::chrono::milliseconds{250};

    schema_registry_replicator_task(
      link* link, const model::metadata& link_metadata);
    schema_registry_replicator_task(const schema_registry_replicator_task&)
      = delete;
    schema_registry_replicator_task(schema_registry_replicator_task&&) = delete;
    schema_registry_replicator_task&
    operator=(const schema_registry_replicator_task&) = delete;
    schema_registry_replicator_task&
    operator=(schema_registry_replicator_task&&) = delete;
    ~schema_registry_replicator_task() override;

    void update_config(const model::metadata& link_metadata) override;
    model::enabled_t is_enabled() const final;

    /// Snapshot of mutable counters for status reporting / tests. All
    /// counters are monotonic for the lifetime of the task.
    struct counters {
        size_t schemas_replicated{0};
        size_t schemas_failed_validation{0};
        size_t schemas_failed_other{0};
        size_t subjects_synchronized{0};
        size_t cycles_observed{0};
        size_t compatibility_levels_replicated{0};
        size_t compatibility_replication_failures{0};
        size_t modes_replicated{0};
        size_t mode_replication_failures{0};
    };
    const counters& get_counters() const { return _counters; }

protected:
    ss::future<state_transition> run_impl(ss::abort_source&) override;

private:
    /// Build / refresh the HTTP clients to reflect the current config.
    /// Returns false if the config indicates the task should be inactive.
    bool maybe_rebuild_clients();

    /// Catch-up: pull every subject, write every (sub, ver) we haven't
    /// already seen. Returns the desired terminal state for this tick.
    ss::future<state_transition> run_catch_up(ss::abort_source&);

    /// Tail: poll for new subjects + version drift on a tighter cadence.
    ss::future<state_transition> run_tail(ss::abort_source&);

    /// Pull schemas for the given list of subjects, filter via the include
    /// regex, dedupe against `_seen`, and replicate the new ones to dest.
    /// Returns the number of schemas successfully replicated.
    ss::future<size_t>
    replicate_subjects(chunked_vector<ss::sstring> subjects, ss::abort_source&);

    /// Ensure the destination SR is in IMPORT mode (idempotent).
    ss::future<bool> ensure_dest_import_mode();

    /// Mirror the global compatibility level and the per-subject overrides
    /// from source to destination. Logs + counts errors without aborting.
    ss::future<> replicate_compatibility(
      const chunked_vector<ss::sstring>& subjects, ss::abort_source&);

    /// Mirror per-subject mode from source to destination. Global mode is
    /// deliberately not mirrored — the dest stays in IMPORT while the link
    /// is active. Logs + counts errors without aborting.
    ss::future<> replicate_modes(
      const chunked_vector<ss::sstring>& subjects, ss::abort_source&);

    /// Tracks the (subject, version) tuples we've successfully written to
    /// the destination. Used to make catch-up + tail idempotent.
    struct seen_subject {
        chunked_hash_map<int32_t, pandaproxy::schema_registry::schema_id>
          version_to_id;
    };

    model::schema_registry_sync_config _config_envelope;
    std::optional<
      cluster_link::model::schema_registry_sync_config::shadow_via_http_api>
      _active_http_cfg;
    std::unique_ptr<RE2> _include_regex;

    std::unique_ptr<sr_http_client> _source_client;
    std::unique_ptr<sr_http_client> _dest_client;

    chunked_hash_map<ss::sstring, seen_subject> _seen;
    bool _dest_import_mode_set{false};
    bool _catch_up_done{false};
    size_t _tick_count{0};

    counters _counters{};
};

/// Factory for schema_registry_replicator_task.
class schema_registry_replicator_task_factory : public task_factory {
public:
    std::string_view created_task_name() const noexcept final;
    std::unique_ptr<task> create_task(link* link) final;
};

} // namespace cluster_link
