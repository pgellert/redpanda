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

#include "cluster_link/schema_registry_replicator_task.h"

#include "cluster_link/link.h"
#include "cluster_link/logger.h"
#include "cluster_link/sr_http_client.h"
#include "cluster_link/sr_topo_sort.h"
#include "ssx/future-util.h"

#include <seastar/core/coroutine.hh>

#include <absl/strings/string_view.h>

#include <chrono>
#include <variant>

namespace pps = pandaproxy::schema_registry;

namespace cluster_link {

namespace {

constexpr std::string_view kLocalSrUrl = "http://127.0.0.1:8081";
constexpr size_t kVersionRevisitEveryNTicks = 20; // 250ms * 20 = 5s

/// Pull the schema_registry_sync_config out of the link metadata.
model::schema_registry_sync_config
extract_config(const model::metadata& link_metadata) {
    // schema_registry_sync_config doesn't expose a copy() helper, so we
    // serde-roundtrip via the .copy() path on the parent envelope.
    return model::schema_registry_sync_config{
      .sync_schema_registry_topic_mode
      = link_metadata.configuration.schema_registry_sync_cfg
          .sync_schema_registry_topic_mode};
}

std::optional<model::schema_registry_sync_config::shadow_via_http_api>
active_http_variant(const model::schema_registry_sync_config& cfg) {
    if (!cfg.sync_schema_registry_topic_mode.has_value()) {
        return std::nullopt;
    }
    return ss::visit(
      *cfg.sync_schema_registry_topic_mode,
      [](
        const model::schema_registry_sync_config::
          shadow_entire_schema_registry&)
        -> std::optional<
          model::schema_registry_sync_config::shadow_via_http_api> {
          return std::nullopt;
      },
      [](const model::schema_registry_sync_config::shadow_via_http_api& v)
        -> std::optional<
          model::schema_registry_sync_config::shadow_via_http_api> {
          return v;
      });
}

} // namespace

schema_registry_replicator_task::schema_registry_replicator_task(
  link* link, const model::metadata& link_metadata)
  : controller_locked_task(
      link, default_run_interval, schema_registry_replicator_task::task_name) {
    update_config(link_metadata);
}

schema_registry_replicator_task::~schema_registry_replicator_task() = default;

void schema_registry_replicator_task::update_config(
  const model::metadata& link_metadata) {
    _config_envelope = extract_config(link_metadata);
    _active_http_cfg = active_http_variant(_config_envelope);
    // Drop clients so the next tick picks up the new endpoint config.
    _source_client.reset();
    _dest_client.reset();
    _dest_import_mode_set = false;
    if (_active_http_cfg.has_value()) {
        set_run_interval(_active_http_cfg->get_tail_interval());
        // Compile the include regex. RE2 returns ok()==false on bad input;
        // we accept that and the filter will reject everything.
        _include_regex = std::make_unique<RE2>(absl::string_view{
          _active_http_cfg->include_regex.data(),
          _active_http_cfg->include_regex.size()});
        if (!_include_regex->ok()) {
            vlog(
              cllog.warn,
              "[sr-replicator] include_regex {} failed to compile: {} - all "
              "subjects will be skipped",
              _active_http_cfg->include_regex,
              _include_regex->error());
        }
    } else {
        _include_regex.reset();
    }
}

model::enabled_t schema_registry_replicator_task::is_enabled() const {
    return _active_http_cfg.has_value() ? model::enabled_t::yes
                                        : model::enabled_t::no;
}

bool schema_registry_replicator_task::maybe_rebuild_clients() {
    if (!_active_http_cfg.has_value()) {
        return false;
    }
    if (_active_http_cfg->source_url.empty()) {
        vlog(
          cllog.info,
          "[sr-replicator] source_url empty, task is configured but inactive");
        return false;
    }
    if (!_source_client) {
        auto src_ep = sr_endpoint::from_url(_active_http_cfg->source_url);
        if (!src_ep.has_value()) {
            vlog(
              cllog.warn,
              "[sr-replicator] could not parse source_url: {}",
              _active_http_cfg->source_url);
            return false;
        }
        src_ep->basic_auth_user = _active_http_cfg->basic_auth_user;
        src_ep->basic_auth_pass = _active_http_cfg->basic_auth_pass;
        _source_client = std::make_unique<sr_http_client>(std::move(*src_ep));
    }
    if (!_dest_client) {
        const auto& dest_url = _active_http_cfg->destination_url.value_or(
          ss::sstring{kLocalSrUrl});
        auto dst_ep = sr_endpoint::from_url(dest_url);
        if (!dst_ep.has_value()) {
            vlog(
              cllog.warn,
              "[sr-replicator] could not parse destination_url: {}",
              dest_url);
            return false;
        }
        _dest_client = std::make_unique<sr_http_client>(std::move(*dst_ep));
    }
    return true;
}

ss::future<bool> schema_registry_replicator_task::ensure_dest_import_mode() {
    if (_dest_import_mode_set) {
        co_return true;
    }
    auto res = co_await _dest_client->put_mode(std::nullopt, pps::mode::import);
    if (res.has_error()) {
        // If we got `not_in_import_mode` semantics from an unrelated error,
        // or the dest SR has /mode disabled (e.g. running without write
        // perms), surface a clear warning and try again next tick.
        vlog(
          cllog.warn,
          "[sr-replicator] could not put dest into IMPORT mode: {} ({})",
          res.assume_error().message,
          to_string_view(res.assume_error().errc));
        co_return false;
    }
    vlog(cllog.info, "[sr-replicator] destination SR set to IMPORT mode");
    _dest_import_mode_set = true;
    co_return true;
}

ss::future<task::state_transition>
schema_registry_replicator_task::run_impl(ss::abort_source& as) {
    if (!maybe_rebuild_clients()) {
        co_return state_transition{
          .desired_state = model::task_state::active,
          .reason = "sr replication inactive (no source url / unset config)"};
    }
    ++_tick_count;
    if (!_catch_up_done) {
        co_return co_await run_catch_up(as);
    }
    co_return co_await run_tail(as);
}

ss::future<task::state_transition>
schema_registry_replicator_task::run_catch_up(ss::abort_source& as) {
    vlog(cllog.info, "[sr-replicator] starting catch-up");

    if (!co_await ensure_dest_import_mode()) {
        co_return state_transition{
          .desired_state = model::task_state::link_unavailable,
          .reason = "could not set destination SR to IMPORT mode"};
    }
    as.check();

    auto subjects_res = co_await _source_client->list_subjects();
    if (subjects_res.has_error()) {
        co_return state_transition{
          .desired_state = model::task_state::link_unavailable,
          .reason = ssx::sformat(
            "source list_subjects failed: {}",
            subjects_res.assume_error().message)};
    }

    auto all_subjects = std::move(subjects_res).assume_value();
    auto replicated = co_await replicate_subjects(
      chunked_vector<ss::sstring>{all_subjects.copy()}, as);
    co_await replicate_compatibility(all_subjects, as);
    // Mode mirroring runs after schemas land — setting per-subject mode
    // on the dest requires the subject to exist.
    co_await replicate_modes(all_subjects, as);
    vlog(
      cllog.info,
      "[sr-replicator] catch-up complete: replicated {} schemas, "
      "{} validation failures, {} other failures, "
      "{} compat levels mirrored, {} modes mirrored",
      _counters.schemas_replicated,
      _counters.schemas_failed_validation,
      _counters.schemas_failed_other,
      _counters.compatibility_levels_replicated,
      _counters.modes_replicated);
    (void)replicated;
    _catch_up_done = true;
    co_return state_transition{
      .desired_state = model::task_state::active,
      .reason = "catch-up complete"};
}

ss::future<task::state_transition>
schema_registry_replicator_task::run_tail(ss::abort_source& as) {
    auto subjects_res = co_await _source_client->list_subjects();
    if (subjects_res.has_error()) {
        co_return state_transition{
          .desired_state = model::task_state::link_unavailable,
          .reason = ssx::sformat(
            "tail list_subjects failed: {}",
            subjects_res.assume_error().message)};
    }
    auto all_subjects = std::move(subjects_res).assume_value();

    // Fast path: pick out new subjects first for sub-second discovery
    // latency.
    chunked_vector<ss::sstring> new_subjects;
    chunked_vector<ss::sstring> known_subjects;
    for (auto& s : all_subjects) {
        if (_seen.contains(s)) {
            known_subjects.push_back(s);
        } else {
            new_subjects.push_back(s);
        }
    }
    if (!new_subjects.empty()) {
        vlog(
          cllog.info,
          "[sr-replicator] tail discovered {} new subject(s)",
          new_subjects.size());
        co_await replicate_subjects(std::move(new_subjects), as);
    }
    // Slow path: revisit known subjects on a longer interval so we pick
    // up new versions and mode/config drift.
    if (
      _tick_count % kVersionRevisitEveryNTicks == 0
      && !known_subjects.empty()) {
        vlog(
          cllog.debug,
          "[sr-replicator] tail revisiting {} known subject(s) for new "
          "versions",
          known_subjects.size());
        co_await replicate_subjects(std::move(known_subjects), as);
    }
    co_return state_transition{
      .desired_state = model::task_state::active,
      .reason = "tail tick complete"};
}

ss::future<size_t> schema_registry_replicator_task::replicate_subjects(
  chunked_vector<ss::sstring> subjects, ss::abort_source& as) {
    chunked_vector<pps::stored_schema> pending;
    for (auto& subject : subjects) {
        as.check();
        // Apply include regex.
        if (
          !_include_regex || !_include_regex->ok()
          || !RE2::FullMatch(subject, *_include_regex)) {
            continue;
        }
        auto versions_res = co_await _source_client->list_versions(subject);
        if (versions_res.has_error()) {
            vlog(
              cllog.warn,
              "[sr-replicator] list_versions({}) failed: {}",
              subject,
              versions_res.assume_error().message);
            ++_counters.schemas_failed_other;
            continue;
        }
        auto& seen_for_subject = _seen[subject];
        for (auto version : versions_res.assume_value()) {
            if (seen_for_subject.version_to_id.contains(version)) {
                continue;
            }
            auto schema_res = co_await _source_client->get_subject_version(
              subject, version);
            if (schema_res.has_error()) {
                vlog(
                  cllog.warn,
                  "[sr-replicator] get_subject_version({},{}) failed: {}",
                  subject,
                  version,
                  schema_res.assume_error().message);
                ++_counters.schemas_failed_other;
                continue;
            }
            pending.push_back(std::move(schema_res).assume_value());
        }
    }

    if (pending.empty()) {
        co_return 0;
    }

    auto sort_res = topo_sort_by_refs(std::move(pending));
    if (sort_res.cycle_detected) {
        vlog(
          cllog.warn,
          "[sr-replicator] reference cycle detected; emitting in input "
          "order, dest writes may fail and will retry next tick");
        ++_counters.cycles_observed;
    }

    size_t replicated = 0;
    for (auto& schema : sort_res.sorted) {
        as.check();
        auto subject = schema.schema.sub().sub();
        auto version = schema.version();
        auto id = schema.id();

        auto post_res = co_await _dest_client->post_schema_with_id(
          subject, schema);
        if (post_res.has_error()) {
            const auto& err = post_res.assume_error();
            // Distinguish "schema body invalid" (validation) from other
            // failure modes, since E-199 calls out validation rejection
            // specifically.
            if (
              err.errc == sr_http_errc::bad_request
              && err.message.find("Invalid") != ss::sstring::npos) {
                vlog(
                  cllog.warn,
                  "[sr-replicator] schema validation failed for "
                  "{} v{} id={}: {}",
                  subject,
                  version,
                  id,
                  err.message);
                ++_counters.schemas_failed_validation;
            } else if (err.errc == sr_http_errc::not_in_import_mode) {
                vlog(
                  cllog.warn,
                  "[sr-replicator] dest fell out of IMPORT mode while "
                  "writing {} v{}; will re-set on next tick",
                  subject,
                  version);
                _dest_import_mode_set = false;
                ++_counters.schemas_failed_other;
            } else {
                vlog(
                  cllog.warn,
                  "[sr-replicator] dest POST failed for {} v{} id={}: {} "
                  "({})",
                  subject,
                  version,
                  id,
                  err.message,
                  to_string_view(err.errc));
                ++_counters.schemas_failed_other;
            }
            continue;
        }
        _seen[subject].version_to_id.try_emplace(version, pps::schema_id{id});
        ++replicated;
        ++_counters.schemas_replicated;
    }
    _counters.subjects_synchronized = _seen.size();
    co_return replicated;
}

ss::future<> schema_registry_replicator_task::replicate_compatibility(
  const chunked_vector<ss::sstring>& subjects, ss::abort_source& as) {
    // Global compat first.
    {
        auto src = co_await _source_client->get_compatibility(std::nullopt);
        if (src.has_error()) {
            if (src.assume_error().errc != sr_http_errc::not_found) {
                vlog(
                  cllog.debug,
                  "[sr-replicator] get global compat failed: {}",
                  src.assume_error().message);
            }
        } else {
            auto put_res = co_await _dest_client->put_compatibility(
              std::nullopt, src.assume_value());
            if (put_res.has_error()) {
                vlog(
                  cllog.warn,
                  "[sr-replicator] put global compat failed: {}",
                  put_res.assume_error().message);
                ++_counters.compatibility_replication_failures;
            } else {
                ++_counters.compatibility_levels_replicated;
            }
        }
    }
    // Per-subject compat. 404 means "no explicit override on source" and is
    // not an error -- we just don't touch the dest's per-subject compat.
    for (const auto& subject : subjects) {
        as.check();
        if (
          !_include_regex || !_include_regex->ok()
          || !RE2::FullMatch(subject, *_include_regex)) {
            continue;
        }
        auto src = co_await _source_client->get_compatibility(subject);
        if (src.has_error()) {
            if (src.assume_error().errc == sr_http_errc::not_found) {
                continue;
            }
            vlog(
              cllog.debug,
              "[sr-replicator] get compat({}) failed: {}",
              subject,
              src.assume_error().message);
            ++_counters.compatibility_replication_failures;
            continue;
        }
        auto put_res = co_await _dest_client->put_compatibility(
          subject, src.assume_value());
        if (put_res.has_error()) {
            vlog(
              cllog.warn,
              "[sr-replicator] put compat({}) failed: {}",
              subject,
              put_res.assume_error().message);
            ++_counters.compatibility_replication_failures;
        } else {
            ++_counters.compatibility_levels_replicated;
        }
    }
}

ss::future<> schema_registry_replicator_task::replicate_modes(
  const chunked_vector<ss::sstring>& subjects, ss::abort_source& as) {
    // Per-subject mode only. The dest's global mode is deliberately
    // pinned to IMPORT while the link is active — mirroring source's
    // global would re-enable external writes and break shadowing.
    //
    // Per-subject 404 means "no explicit override on source"; the
    // subject inherits source's global, and we leave dest's per-subject
    // mode unset (it'll fall back to dest's global = IMPORT). That's
    // strictly more restrictive than source, so writes still fail on
    // dest as expected for shadowed state.
    for (const auto& subject : subjects) {
        as.check();
        if (
          !_include_regex || !_include_regex->ok()
          || !RE2::FullMatch(subject, *_include_regex)) {
            continue;
        }
        auto src = co_await _source_client->get_mode(subject);
        if (src.has_error()) {
            if (src.assume_error().errc == sr_http_errc::not_found) {
                continue;
            }
            vlog(
              cllog.debug,
              "[sr-replicator] get mode({}) failed: {}",
              subject,
              src.assume_error().message);
            ++_counters.mode_replication_failures;
            continue;
        }
        auto put_res = co_await _dest_client->put_mode(
          subject, src.assume_value());
        if (put_res.has_error()) {
            vlog(
              cllog.warn,
              "[sr-replicator] put mode({}) failed: {}",
              subject,
              put_res.assume_error().message);
            ++_counters.mode_replication_failures;
        } else {
            ++_counters.modes_replicated;
        }
    }
}

std::string_view
schema_registry_replicator_task_factory::created_task_name() const noexcept {
    return schema_registry_replicator_task::task_name;
}

std::unique_ptr<task>
schema_registry_replicator_task_factory::create_task(link* link) {
    return std::make_unique<schema_registry_replicator_task>(
      link, *(link->get_config()));
}

} // namespace cluster_link
