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

#include "cluster_link/schema_registry_sync/mirroring_task.h"

#include "cluster_link/link.h"
#include "cluster_link/schema_registry_sync/reconciler.h"
#include "cluster_link/schema_registry_sync/scope.h"
#include "config/configuration.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "model/namespace.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/util/defer.hh>

#include <utility>

namespace cluster_link::schema_registry_sync {

namespace {

ss::lowres_clock::duration
tail_interval(const model::schema_registry_sync_config& cfg) {
    if (const auto* api = cfg.api_mode(); api != nullptr) {
        return api->get_tail_interval();
    }
    return model::schema_registry_sync_config::shadow_schema_registry_api::
      default_tail_interval;
}

ss::lowres_clock::duration
full_sync_interval(const model::schema_registry_sync_config& cfg) {
    if (const auto* api = cfg.api_mode(); api != nullptr) {
        return api->get_full_sync_interval();
    }
    return model::schema_registry_sync_config::shadow_schema_registry_api::
      default_full_sync_interval;
}

} // namespace

mirroring_task::mirroring_task(
  link* link,
  const model::metadata& link_metadata,
  schema::registry* destination,
  source_reader_factory* source_factory)
  : task(
      link,
      tail_interval(link_metadata.configuration.schema_registry_sync_cfg),
      mirroring_task::task_name)
  , _config(link_metadata.configuration.schema_registry_sync_cfg.copy())
  , _destination(destination)
  , _source_factory(source_factory)
  , _reader(_source_factory->create()) {}

void mirroring_task::update_config(const model::metadata& link_metadata) {
    _config = link_metadata.configuration.schema_registry_sync_cfg.copy();
    set_run_interval(tail_interval(_config));
    // The scope (filters/contexts) may have changed; flag a forced full scan so
    // the next run re-derives the inventory. Only a flag is set here: mutating
    // _status/_last_full_sync would race an in-flight run_impl that resumes and
    // overwrites it.
    _config_changed = true;
}

model::enabled_t mirroring_task::is_enabled() const {
    return model::enabled_t(_config.api_mode() != nullptr);
}

bool mirroring_task::leads_schema_registry_partition() const {
    return get_link()->partition_manager().is_current_shard_leader(
      ::model::schema_registry_internal_ntp);
}

bool mirroring_task::should_start_impl(ss::shard_id, ::model::node_id) const {
    return leads_schema_registry_partition();
}

bool mirroring_task::should_stop_impl(ss::shard_id, ::model::node_id) const {
    return !leads_schema_registry_partition();
}

bool mirroring_task::should_long_sync() const {
    if (!_last_full_sync.has_value()) {
        return true;
    }
    return ss::lowres_clock::now() - *_last_full_sync
           >= full_sync_interval(_config);
}

model::task_status_report mirroring_task::get_status_report() const {
    auto report = task::get_status_report();
    // The sync runs only on the shard leading _schemas/0; other shards keep the
    // task stopped with default (empty) status. Emitting that empty status
    // would let a non-leader's report win the cross-shard/node admin
    // aggregation over the leader's real status, so only a running task
    // surfaces it.
    if (get_state() != model::task_state::stopped) {
        auto status = _status;
        // Reflect the in-flight reconcile's live counters onto the reported
        // status so a long full sync shows incremental progress. They are only
        // non-zero while a reconcile is running (current_sync is set); the
        // end-of-sync fold then moves them into the persistent summary/totals,
        // so adding here would double-count outside that window -- guard on it.
        if (status.current_sync.has_value()) {
            status.current_sync->summary.subject_versions_changed
              += _reconcile_stats.versions_changed;
            status.current_sync->summary.errors += _reconcile_stats.errors;
            status.totals_since_task_start.subject_versions_changed
              += _reconcile_stats.versions_changed;
            status.totals_since_task_start.errors += _reconcile_stats.errors;
        }
        report.detail = model::task_detail{
          .schema_registry_sync_status = std::move(status)};
    }
    return report;
}

ss::future<> mirroring_task::refresh_destination_inventory(
  const std::function<bool(const ppsr::context_subject&)>& in_scope,
  ss::abort_source& as) {
    // Scan every in-scope (subject, version) node across all contexts.
    // Destination/internal faults bubble out and become `faulted`.
    _destination_inventory = co_await scan_destination_inventory(
      *_destination, in_scope, as);

    chunked_hash_set<ppsr::context_subject> subjects;
    for (const auto& key : _destination_inventory.active) {
        subjects.insert(key.sub);
    }
    _status.inventory.destination_subjects = static_cast<uint64_t>(
      subjects.size());
    _status.inventory.destination_subject_versions = static_cast<uint64_t>(
      _destination_inventory.active.size());
}

ss::future<> mirroring_task::list_one_subject(
  const ppsr::context_subject& subject,
  ss::abort_source& as,
  chunked_hash_set<ppsr::subject_version>& source_active,
  std::optional<source_error>& unavailable,
  model::schema_registry_sync_summary& summary) {
    // A peer fiber already hit source_unavailable; the whole sync will back
    // off, so skip the remaining round-trips.
    if (unavailable.has_value()) {
        co_return;
    }
    as.check();
    auto versions_res = co_await _reader->list_subject_versions(
      subject, ppsr::include_deleted::no, as);
    if (!versions_res.has_value()) {
        if (
          versions_res.error().kind == source_error_kind::source_unavailable) {
            if (!unavailable.has_value()) {
                unavailable = std::move(versions_res.error());
            }
            co_return;
        }
        // A reachable-but-failed listing is a rare source-side delete race:
        // tolerate it as a per-item error and skip this subject. The full sync
        // still completes and advances the timer, retrying on the normal
        // interval. The shared counters are mutated synchronously after the
        // co_await returns (never straddling a suspension), so sharing them
        // across concurrent fibers is safe on a single reactor.
        ++summary.errors;
        ++_status.totals_since_task_start.errors;
        _status.last_error_message = versions_res.error().message;
        _status.current_sync->summary = summary;
        vlog(
          logger().warn,
          "Schema Registry sync error: {}",
          versions_res.error().message);
        co_return;
    }
    for (const auto& version : versions_res.value()) {
        source_active.insert(ppsr::subject_version{subject, version});
    }
}

ss::future<task::state_transition> mirroring_task::full_source_sync(
  ss::abort_source& as,
  model::schema_registry_sync_summary& summary,
  const chunked_hash_set<ppsr::context>& contexts,
  const std::function<bool(const ppsr::context_subject&)>& in_scope) {
    auto record_error = [this, &summary](std::string_view what) {
        ++summary.errors;
        ++_status.totals_since_task_start.errors;
        _status.last_error_message = ss::sstring{what};
        _status.current_sync->summary = summary;
        vlog(logger().warn, "Schema Registry sync error: {}", what);
    };

    // Memory and parallelism are cluster-global cluster properties; reading
    // them here (mid-sync) is safe because they no longer depend on the
    // per-link config that a concurrent update_config can swap out. There is no
    // rate field: source-request rate limiting is the HTTP source reader's
    // (client) responsibility, where max_source_requests_per_second is
    // consumed, not the reconciler's.
    //
    // The single parallelism bound governs BOTH the version-listing fan-out
    // below and the reconcile engine's import concurrency. Listing wants high
    // concurrency to hide per-request RTT, whereas the reconcile is memory-
    // bound; a future change may want to split these into separate limits.
    auto limits = reconciler::limits{
      .memory_bytes
      = config::shard_local_cfg().schema_registry_sync_memory_bytes(),
      .parallelism
      = config::shard_local_cfg().schema_registry_sync_parallelism()};

    // Discovery only: enumerate every active source (context, subject, version)
    // node. The reconcile engine fetches the schema bodies itself, so no bodies
    // are read here.
    chunked_hash_set<ppsr::subject_version> source_active;
    uint64_t subject_count = 0;

    // First enumerate the subjects in every context. Contexts are few, so this
    // stays sequential; the result is the full set of subjects to list versions
    // for. A source_unavailable here still stops the whole sync.
    chunked_vector<ppsr::context_subject> subjects;
    for (const auto& ctx : contexts) {
        auto subjects_res = co_await _reader->list_subjects(ctx, as);
        if (!subjects_res.has_value()) {
            if (
              subjects_res.error().kind
              == source_error_kind::source_unavailable) {
                co_return make_unavailable(subjects_res.error().message);
            }
            // Could not enumerate this context: the source is reachable (a rare
            // delete race), so tolerate it as a counted per-item error and skip
            // the context. The sync still completes.
            record_error(subjects_res.error().message);
            continue;
        }
        for (auto& subject : subjects_res.value()) {
            // The same in_scope predicate that gates the destination scan and
            // the engine also scopes discovery, so a configured subject filter
            // excludes a subject from both sides consistently.
            if (in_scope(subject)) {
                subjects.push_back(std::move(subject));
            }
        }
    }
    subject_count = subjects.size();

    // The per-subject version listings are independent source round-trips, so
    // run them with bounded concurrency to hide the per-request RTT instead of
    // serialising it. The work is delegated to the list_one_subject member: a
    // coroutine lambda here would risk a use-after-free if its object were
    // freed while suspended, whereas the member coroutine keeps `this` and the
    // shared state in its own frame. The forwarding lambda below is not a
    // coroutine -- it just returns the member's future.
    std::optional<source_error> unavailable;
    co_await ss::max_concurrent_for_each(
      subjects,
      std::max<size_t>(1, limits.parallelism),
      [&](const ppsr::context_subject& subject) {
          return list_one_subject(
            subject, as, source_active, unavailable, summary);
      });
    if (unavailable.has_value()) {
        co_return make_unavailable(unavailable->message);
    }

    _status.inventory.selected_source_subjects = subject_count;
    _status.inventory.selected_source_subject_versions = static_cast<uint64_t>(
      source_active.size());

    // Create-only: import the active source nodes that the destination does not
    // already have active. The reconciler imports them referent-first.
    work_set work;
    for (const auto& node : source_active) {
        // Skip assumes the destination is a managed mirror that only this sync
        // writes to, so a matching (context, subject, version) key implies
        // matching content. Detecting divergent same-key content on the
        // destination is out of scope for create-only and a robustness
        // follow-up.
        if (!_destination_inventory.active.contains(node)) {
            work.upserts.push_back(node);
        }
    }

    auto rec = reconciler{_reader.get(), _destination, in_scope, limits};

    // Soft-deleted destination nodes still satisfy a referrer's references, so
    // seed the engine with the destination's full (active + soft-deleted) set.
    chunked_hash_set<ppsr::subject_version> seed{
      _destination_inventory.all.begin(), _destination_inventory.all.end()};
    // Reset the live counters and let the reconciler increment them as nodes
    // complete; get_status_report reflects them onto the reported status mid-
    // sync. The fold below moves them into the persistent summary/totals.
    _reconcile_stats = reconcile_stats{};
    auto result = co_await rec.reconcile(
      std::move(work), std::move(seed), _reconcile_stats, as);
    if (!result.has_value()) {
        if (result.error().kind == source_error_kind::source_unavailable) {
            co_return make_unavailable(result.error().message);
        }
        // reconcile only surfaces source_unavailable today; treat any other
        // error defensively as a counted per-item failure.
        record_error(result.error().message);
        co_return make_active();
    }

    const auto stats = _reconcile_stats;
    // Fold the live counters into the persistent summary/totals once, then
    // clear them so the report-time reflection (which only adds while
    // current_sync is set) cannot double-count after the fold.
    _reconcile_stats = reconcile_stats{};
    summary.subject_versions_changed += stats.versions_changed;
    summary.errors += stats.errors;
    _status.totals_since_task_start.subject_versions_changed
      += stats.versions_changed;
    _status.totals_since_task_start.errors += stats.errors;
    _status.current_sync->summary = summary;

    vlog(
      logger().info,
      "Schema Registry full sync: {} source subjects ({} versions), {} "
      "destination subjects; imported {} versions, {} errors",
      _status.inventory.selected_source_subjects,
      _status.inventory.selected_source_subject_versions,
      _status.inventory.destination_subjects,
      stats.versions_changed,
      stats.errors);

    summary.finish_time = ::model::timestamp::now();
    _status.last_full_sync = summary;
    // The full sync completed (best-effort over what discovery found, with any
    // per-item source-list failures counted as errors), so advance the timer
    // and retry on the normal full-sync interval. A reachable-but-failed
    // listing is a rare delete race and is not special-cased into a fast retry.
    _last_full_sync = ss::lowres_clock::now();
    co_return make_active();
}

ss::future<task::state_transition>
mirroring_task::run_impl(ss::abort_source& as) {
    // Consume the config-changed flag before any co_await so a concurrent
    // update_config during this run is not lost (it re-arms for the next run).
    const bool long_sync = std::exchange(_config_changed, false)
                           || should_long_sync();

    model::schema_registry_sync_summary summary;
    summary.start_time = ::model::timestamp::now();
    _status.current_sync = model::schema_registry_current_sync{
      .sync_type = long_sync ? model::schema_registry_sync_type::full
                             : model::schema_registry_sync_type::tail,
      .summary = summary};
    // current_sync reflects an in-progress sync only; clear it on every exit
    // (success, unavailable, or a fault that throws out of run_impl) so a stale
    // partial summary is never reported between runs.
    auto clear_current_sync = ss::defer(
      [this] { _status.current_sync.reset(); });

    if (!long_sync) {
        // Incremental tail sync is not implemented yet; nothing to do on a
        // tail tick (in particular, do not rescan the destination).
        vlog(logger().debug, "Schema Registry tail sync not yet implemented");
        co_return make_active();
    }

    // The set of contexts to replicate, and thus the in-scope predicate used
    // for both the destination scan and the reconcile, is derived from the
    // source's contexts intersected with the configured filter. Discover them
    // first.
    auto contexts_res = co_await _reader->list_contexts(as);
    if (!contexts_res.has_value()) {
        if (
          contexts_res.error().kind == source_error_kind::source_unavailable) {
            co_return make_unavailable(contexts_res.error().message);
        }
        _status.last_error_message = contexts_res.error().message;
        co_return make_active();
    }
    // The configured source filter scopes discovery and the in_scope predicate
    // with union semantics: filter.contexts selects whole contexts, while
    // filter.subjects selects individual context-qualified subjects that may
    // live in contexts the context filter does not select. An empty filter
    // (neither selector populated) replicates everything. filter.subjects
    // entries are parsed as qualified subjects (":.context:subject"), so a
    // listed subject carries its own context.
    const auto qualified = ppsr::qualified_subjects_enabled{
      config::shard_local_cfg().schema_registry_enable_qualified_subjects()};
    chunked_hash_set<ppsr::context> filter_contexts;
    chunked_hash_set<ppsr::context_subject> filter_subjects;
    if (const auto* api = _config.api_mode(); api != nullptr) {
        for (const auto& ctx : api->filter.contexts) {
            filter_contexts.insert(ppsr::context{ctx});
        }
        for (const auto& sub : api->filter.subjects) {
            filter_subjects.insert(
              ppsr::context_subject::from_string(sub, qualified));
        }
    }
    const bool unfiltered = filter_contexts.empty() && filter_subjects.empty();
    // A filtered subject's context must be scanned even when the context filter
    // does not select it, otherwise that subject would never be discovered.
    chunked_hash_set<ppsr::context> subject_contexts;
    for (const auto& cs : filter_subjects) {
        subject_contexts.insert(cs.ctx);
    }
    chunked_hash_set<ppsr::context> contexts;
    for (auto& ctx : contexts_res.value()) {
        if (
          unfiltered || filter_contexts.contains(ctx)
          || subject_contexts.contains(ctx)) {
            contexts.insert(std::move(ctx));
        }
    }

    if (
      auto reason = check_preconditions(
        _config,
        contexts,
        config::shard_local_cfg().schema_registry_enable_qualified_subjects());
      reason.has_value()) {
        co_return make_faulted(*reason);
    }

    auto in_scope = make_in_scope(
      std::move(filter_contexts), std::move(filter_subjects));

    co_await refresh_destination_inventory(in_scope, as);

    co_return co_await full_source_sync(as, summary, contexts, in_scope);
}

task::state_transition
mirroring_task::make_unavailable(const ss::sstring& reason) {
    vlog(
      logger().warn, "Schema Registry shadowing task unavailable: {}", reason);
    _status.last_error_message = reason;
    return state_transition{
      .desired_state = model::task_state::link_unavailable, .reason = reason};
}

task::state_transition mirroring_task::make_active() {
    return state_transition{
      .desired_state = model::task_state::active,
      .reason = "Schema Registry shadowing task finished a sync"};
}

task::state_transition mirroring_task::make_faulted(const ss::sstring& reason) {
    vlog(logger().warn, "Schema Registry shadowing task faulted: {}", reason);
    _status.last_error_message = reason;
    return state_transition{
      .desired_state = model::task_state::faulted, .reason = reason};
}

std::string_view mirroring_task_factory::created_task_name() const noexcept {
    return mirroring_task::task_name;
}

std::unique_ptr<task> mirroring_task_factory::create_task(link* link) {
    return std::make_unique<mirroring_task>(
      link, *(link->get_config()), _destination, _source_factory);
}

} // namespace cluster_link::schema_registry_sync
