//// Copyright 2021 Redpanda Data, Inc.
////
//// Use of this software is governed by the Business Source License
//// included in the file licenses/BSL.md
////
//// As of the Change Date specified in that file, in accordance with
//// the Business Source License, use of this software will be governed
//// by the Apache License, Version 2.

#pragma once

#include "base/outcome.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/exceptions.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/transport.h"
#include "pandaproxy/schema_registry/types.h"
#include "random/simple_time_jitter.h"
#include "ssx/semaphore.h"
#include "utils/retry.h"

#include <atomic>
#include <memory>

namespace pandaproxy::schema_registry {

/// Cross-shard cancellation for seq_writer's shard-0 hops: an ss::abort_source
/// is shard-local and cannot cross invoke_on, so a caller that must abandon a
/// write promptly (the shadow-link SR sync's stop path) passes this token —
/// set from an abort-source subscription on its own shard — and the shard-0
/// waits poll it. A null token (the REST paths) keeps today's unbounded waits.
using cancel_token = std::shared_ptr<std::atomic<bool>>;

class sequence_state_checker {
public:
    sequence_state_checker() = default;
    virtual ~sequence_state_checker() = default;
    sequence_state_checker(const sequence_state_checker&) = delete;
    sequence_state_checker& operator=(const sequence_state_checker&) = delete;
    sequence_state_checker(sequence_state_checker&&) = delete;
    sequence_state_checker& operator=(sequence_state_checker&&) = delete;

    using writes_disabled_t = ss::bool_class<struct writes_disabled_tag>;
    /// True if a write from the given source to the given context must be
    /// rejected. The context lets shadow linking block only the contexts owned
    /// by an active mirroring, rather than the whole Schema Registry.
    virtual writes_disabled_t
    writes_disabled(write_source, const context&) const = 0;
};

using namespace std::chrono_literals;

static const int max_retries = 4;

class seq_writer final : public ss::peering_sharded_service<seq_writer> {
public:
    // All reads of the topic must occur on shard 0
    static constexpr ss::shard_id reader_shard = 0;

    seq_writer(
      model::node_id node_id,
      ss::smp_service_group smp_group,
      transport& transport,
      sharded_store& store,
      std::unique_ptr<sequence_state_checker> state_checker)
      : _smp_opts(ss::smp_submit_to_options{smp_group})
      , _transport(&transport)
      , _store(store)
      , _node_id(node_id)
      , _state_checker(std::move(state_checker)) {}

    ss::future<> read_sync(cancel_token cancel = nullptr);

    // Throws 42205 if the subject cannot be modified
    ss::future<> check_mutable(
      const context& ctx,
      const std::optional<subject>& sub,
      write_source src = write_source::client);

    // API for readers: notify us when they have read and applied an offset
    ss::future<> advance_offset(model::offset offset);

    ss::future<sharded_store::insert_result>
    write_subject_version(stored_schema schema);

    /// Internal sync path for importing a subject version with caller-supplied
    /// schema ID, version, and deleted state. Bypasses client write guards
    /// such as read-only mode and mode_mutability.
    ss::future<sharded_store::insert_result> write_subject_version_imported(
      stored_schema schema, cancel_token cancel = nullptr);

    ss::future<bool> write_config(
      context_subject ctx_sub,
      compatibility_level compat,
      write_source src = write_source::client,
      cancel_token cancel = nullptr);

    ss::future<bool> delete_config(
      context_subject ctx_sub,
      write_source src = write_source::client,
      cancel_token cancel = nullptr);

    /// \param f bypasses only the import-mode emptiness check, never
    /// mode_mutability.
    ss::future<bool> write_mode(
      context_subject ctx_sub,
      mode m,
      force f,
      write_source src = write_source::client,
      cancel_token cancel = nullptr);

    ss::future<bool> delete_mode(
      context_subject ctx_sub,
      write_source src = write_source::client,
      cancel_token cancel = nullptr);

    ss::future<> delete_context(context ctx);

    ss::future<bool> delete_subject_version(
      context_subject sub,
      schema_version version,
      write_source src = write_source::client,
      cancel_token cancel = nullptr);

    ss::future<chunked_vector<schema_version>> delete_subject_impermanent(
      context_subject sub, write_source src = write_source::client);

    ss::future<chunked_vector<schema_version>> delete_subject_permanent(
      context_subject sub,
      std::optional<schema_version> version,
      write_source src = write_source::client,
      cancel_token cancel = nullptr);

private:
    ss::smp_submit_to_options _smp_opts;

    transport* _transport;
    sharded_store& _store;

    model::node_id _node_id;

    void advance_offset_inner(model::offset offset);

    ss::future<std::optional<sharded_store::insert_result>>
    do_write_subject_version(stored_schema schema, model::offset write_at);

    ss::future<std::optional<sharded_store::insert_result>>
    do_write_subject_version_imported(
      stored_schema schema, model::offset write_at);

    ss::future<std::optional<bool>> do_write_config(
      context_subject ctx_sub,
      compatibility_level compat,
      model::offset write_at,
      write_source src);

    ss::future<std::optional<bool>>
    do_delete_config(context_subject ctx_sub, write_source src);

    ss::future<std::optional<bool>> do_write_mode(
      context_subject ctx_sub,
      mode m,
      force f,
      model::offset write_at,
      write_source src);

    ss::future<std::optional<bool>> do_delete_mode(
      context_subject ctx_sub, model::offset write_at, write_source src);

    ss::future<std::optional<bool>>
    do_delete_context(context ctx, model::offset write_at);

    ss::future<std::optional<bool>> do_delete_subject_version(
      context_subject sub,
      schema_version version,
      model::offset write_at,
      write_source src);

    ss::future<std::optional<chunked_vector<schema_version>>>
    do_delete_subject_impermanent(
      context_subject sub, model::offset write_at, write_source src);

    ss::future<std::optional<chunked_vector<schema_version>>>
    delete_subject_permanent_inner(
      context_subject sub,
      std::optional<schema_version> version,
      write_source src);

    simple_time_jitter<ss::lowres_clock> _jitter{std::chrono::milliseconds{50}};

    /// Helper for write paths that use sequence+retry logic to synchronize
    /// multiple writing nodes.
    template<typename F>
    auto sequenced_write(
      F f,
      context ctx,
      write_source src = write_source::client,
      cancel_token cancel = nullptr) {
        if (_state_checker->writes_disabled(src, ctx)) [[unlikely]] {
            throw as_exception(writes_disabled());
        }
        auto base_backoff = _jitter.next_duration();
        auto remote = [base_backoff, f, cancel](seq_writer& seq) {
            return seq.do_sequenced_write(f, base_backoff, cancel);
        };

        return container()
          .invoke_on(reader_shard, _smp_opts, remote)
          .then([](auto res) { return std::move(res).value(); });
    }

    /// The shard-zero part of sequenced_write: serialize on _write_sem, then
    /// drive the write with retries. Declared as a separate member function
    /// rather than inline in sequenced_write for the same compiler-issue
    /// reason as sequenced_write_inner.
    template<
      typename F,
      typename DurationType,
      typename invoke_result_t = typename std::
        invoke_result_t<F, model::offset, seq_writer&>::value_type::value_type>
    ss::future<
      outcome::outcome<invoke_result_t, std::error_code, std::exception_ptr>>
    do_sequenced_write(F f, DurationType base_backoff, cancel_token cancel) {
        if (auto waiters = _write_sem.waiters(); waiters != 0) {
            vlog(
              srlog.trace, "sequenced_write waiting for {} waiters", waiters);
        }
        auto units = co_await acquire_units_cancellable(_write_sem, cancel);
        if (auto waiters = _wait_for_sem.waiters(); waiters != 0) {
            vlog(
              srlog.debug,
              "sequenced_write acquired write_sem with {} "
              "wait_for_sem waiters",
              waiters);
        }
        co_return co_await retry_with_backoff(
          max_retries,
          [this, f, cancel]() { return sequenced_write_inner(f, cancel); },
          base_backoff);
    }

    /// The part of sequenced_write that runs on shard zero
    ///
    /// This is declared as a separate member function rather than
    /// inline in sequenced_write, to avoid compiler issues (and resulting
    /// crashes) seen when passing in a coroutine lambda.
    ///
    /// The return of f is wrapped in an outcome<>, to transport exceptions
    /// without causing a retry.
    template<
      typename F,
      typename invoke_result_t = typename std::
        invoke_result_t<F, model::offset, seq_writer&>::value_type::value_type>
    ss::future<
      outcome::outcome<invoke_result_t, std::error_code, std::exception_ptr>>
    sequenced_write_inner(F f, const cancel_token& cancel) {
        // A cancelled write is transported as an exception outcome rather
        // than thrown, so it is not retried.
        if (cancel && cancel->load(std::memory_order_relaxed)) {
            co_return std::make_exception_ptr(exception(
              error_code::internal_server_error,
              "schema registry write cancelled: caller is shutting down"));
        }
        // If we run concurrently with them, redundant replays to the store
        // will be safely dropped based on offset.
        co_await read_sync(cancel);

        auto next_offset = _loaded_offset + model::offset{1};
        std::optional<invoke_result_t> r;
        try {
            r = co_await f(next_offset, *this);
        } catch (const exception& e) {
            co_return std::current_exception();
        }
        if (r.has_value()) {
            co_return std::move(r.value());
        } else {
            throw exception(
              error_code::write_collision,
              fmt::format("Write collision at offset {}", next_offset));
        }
    }

    ss::future<bool> produce_and_apply(
      std::optional<model::offset> write_at, model::record_batch batch);

    /// Block until this offset is available, fetching if necessary
    ss::future<> wait_for(model::offset offset, cancel_token cancel);
    /// The shard-zero part of wait_for.
    ss::future<> do_wait_for(model::offset offset, cancel_token cancel);

    /// Acquire one unit, waiting unboundedly when \p cancel is null; with a
    /// token, poll it between short timed waits and fail once it is set.
    static ss::future<ssx::semaphore_units>
    acquire_units_cancellable(ssx::semaphore& sem, cancel_token cancel);

    std::unique_ptr<sequence_state_checker> _state_checker;

    // Global (Shard 0) State
    // ======================

    /// Serialize wait_for operations, to avoid issuing
    /// gratuitous number of reads to the topic on concurrent GETs.
    ssx::semaphore _wait_for_sem{1, "pproxy/schema-wait"};

    /// Shard 0 only: Reads have progressed as far as this offset
    model::offset _loaded_offset{-1};

    /// Shard 0 only: Serialize write operations.
    ssx::semaphore _write_sem{1, "pproxy/schema-write"};

    // ======================
    // End of Shard 0 state
};

} // namespace pandaproxy::schema_registry
