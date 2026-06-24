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

#include "cluster_link/schema_registry_sync/reconciler.h"

#include "base/units.h"
#include "cluster_link/logger.h"
#include "cluster_link/schema_registry_sync/scope.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/exceptions.h"
#include "ssx/future-util.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/defer.hh>

#include <algorithm>
#include <utility>

namespace cluster_link::schema_registry_sync {

namespace {

// Units reserved against the memory budget before a body is fetched, gating
// fetch entry on memory before the body size is known. After the read the
// reservation is topped up to the real body size.
constexpr size_t reconcile_reserve_bytes = 20_KiB;

bool is_import_conflict(const ppsr::exception& e) {
    const auto& code = e.code();
    return code
             == make_error_code(
               ppsr::error_code::subject_version_schema_id_already_exists)
           || code
                == make_error_code(
                  ppsr::error_code::subject_version_operation_not_permitted);
}

// Byte-size proxy for a schema body: the canonical definition's length. This
// is what the byte-semaphore budgets, and what tests control via the fake.
size_t body_size(const ppsr::stored_schema& s) {
    return s.schema.def().raw()().size_bytes();
}

} // namespace

reconciler::reconciler(
  source_reader* source,
  schema::registry* destination,
  std::function<bool(const ppsr::context_subject&)> in_scope,
  limits lim)
  : _source(source)
  , _destination(destination)
  , _in_scope(std::move(in_scope))
  , _limits(lim)
  , _mem(std::max<size_t>(1, lim.memory_bytes), "schema_registry_sync/memory") {
    // Floor the budget so the clamp `min(body_size, memory_bytes)` and the
    // semaphore agree; a degenerate 0 admits one over-budget body at a time.
    _limits.memory_bytes = std::max<size_t>(1, _limits.memory_bytes);
}

reconciler::node_data& reconciler::data(const ppsr::subject_version& n) {
    return _nodes.try_emplace(n).first->second;
}

ss::future<source_result<void>> reconciler::reconcile(
  work_set work,
  chunked_hash_set<ppsr::subject_version> seed_replicated,
  reconcile_stats& stats,
  ss::abort_source& as) {
    _replicated = std::move(seed_replicated);
    _nodes.clear();
    _discover_q.clear();
    _import_q.clear();
    _stats = &stats;
    _outstanding = 0;
    _done = false;
    _fault.reset();
    _exn = nullptr;

    for (auto& key : work.upserts) {
        auto& d = data(key);
        if (d.state == node_state::unseen) {
            d.state = node_state::discovering;
            _discover_q.push_back(key);
            ++_outstanding;
        }
    }

    // Nothing to do: no fibers needed.
    if (_outstanding == 0) {
        _done = true;
    }

    auto n_workers = std::max<size_t>(1, _limits.parallelism);
    ss::gate gate;
    for (size_t i = 0; i < n_workers; ++i) {
        ssx::spawn_with_gate(gate, [this, &as] { return worker(as); });
    }
    co_await gate.close();

    if (_exn) {
        std::rethrow_exception(_exn);
    }
    if (_fault.has_value()) {
        co_return std::unexpected(std::move(*_fault));
    }
    // An external abort with no source fault: surface as a clean cancellation
    // so the caller does not record a spurious failure.
    as.check();

    // A valid source's reference graph is acyclic, so both queues drain only
    // once every node is done or errored. A malformed cyclic source leaves
    // nodes stuck pending (their in-degree never reaches 0); fail them rather
    // than loop forever.
    chunked_vector<ppsr::subject_version> stuck;
    for (const auto& [key, d] : _nodes) {
        if (
          d.state == node_state::pending
          || d.state == node_state::discovering) {
            stuck.push_back(key);
        }
    }
    for (const auto& key : stuck) {
        fail(key);
    }
    for (const auto& [key, d] : _nodes) {
        vassert(
          d.state == node_state::done || d.state == node_state::errored,
          "reconcile left node {} in non-terminal state {}",
          key.sub,
          static_cast<int>(d.state));
    }

    co_return source_result<void>{};
}

ss::future<> reconciler::worker(ss::abort_source& as) {
    while (true) {
        try {
            co_await _cv.wait([this, &as] {
                return has_work() || _done || as.abort_requested();
            });
            if (_done || as.abort_requested()) {
                co_return;
            }
            // The cv may release several waiters for a single enqueue; only the
            // one that finds work proceeds, the rest loop back to wait.
            if (!has_work()) {
                continue;
            }

            // Pop one node. It moves from queued to in-flight but stays counted
            // in `_outstanding` until this iteration's `discover`/`do_import`
            // (incl. wake/fail) returns, so a mid-flight worker can never
            // trigger premature-done.
            bool discovering = !_discover_q.empty();
            ppsr::subject_version n = discovering ? _discover_q.back()
                                                  : _import_q.back();
            if (discovering) {
                _discover_q.pop_back();
            } else {
                _import_q.pop_back();
            }

            auto res = discovering ? co_await discover(n, as)
                                   : co_await do_import(n, as);
            if (!res.has_value()) {
                if (!_fault.has_value()) {
                    _fault = std::move(res.error());
                }
                _done = true;
                _cv.broadcast();
                co_return;
            }

            // This node is no longer in-flight. `discover`/`wake` may have
            // enqueued new nodes (each bumping `_outstanding` and signalling);
            // the run is complete only when nothing is queued or in-flight.
            if (--_outstanding == 0) {
                _done = true;
                _cv.broadcast();
                co_return;
            }
        } catch (...) {
            auto eptr = std::current_exception();
            // Abort / teardown unblocks the waits; exit quietly and let the
            // caller observe the abort. Any other exception faults the sync.
            if (!ssx::is_shutdown_exception(eptr)) {
                if (!_exn) {
                    _exn = eptr;
                }
            }
            _done = true;
            _cv.broadcast();
            co_return;
        }
    }
}

ss::future<source_result<void>>
reconciler::discover(const ppsr::subject_version& n, ss::abort_source& as) {
    // Gate fetch entry on memory: take a small reservation before the read,
    // clamped to the budget so a tiny budget still admits one fetch. A worker
    // blocked here holds nothing (no hold-and-wait), preserving deadlock
    // freedom. After the read the reservation is topped up to the real body
    // size with consume() (which may drive the semaphore negative, naturally
    // throttling subsequent fetches until large bodies drain). All units are
    // released before this invocation returns -- in particular before a
    // missing-refs node is deferred -- so nothing is held across a wait on
    // another node.
    auto units = co_await ss::get_units(
      _mem, std::min(reconcile_reserve_bytes, _limits.memory_bytes), as);
    size_t consumed = 0;
    auto release_consumed = ss::defer([this, &consumed] {
        if (consumed > 0) {
            _mem.signal(consumed);
        }
    });

    auto fetched = co_await _source->read_subject_version(n.sub, n.version, as);
    if (!fetched.has_value()) {
        if (fetched.error().kind == source_error_kind::source_unavailable) {
            co_return std::unexpected(std::move(fetched.error()));
        }
        fail(n);
        co_return source_result<void>{};
    }

    auto reserved = units.count();
    if (auto size = body_size(fetched.value()); size > reserved) {
        consumed = size - reserved;
        _mem.consume(consumed);
    }

    auto refs = resolve_refs(fetched.value());
    chunked_vector<ppsr::subject_version> missing;
    chunked_hash_set<ppsr::subject_version> missing_seen;
    for (auto& ref : refs) {
        if (!_in_scope(ref.sub)) {
            vlog(
              cllog.warn,
              "Schema reference {}/{} of {}/{} is out of scope; cannot "
              "replicate referrer",
              ref.sub,
              ref.version,
              n.sub,
              n.version);
            fail(n);
            co_return source_result<void>{};
        }
        // Collapse a (subject, version) listed more than once to a single
        // dependency edge: one in_deg unit and one dependents entry, avoiding
        // redundant dependent records and wake work on malformed input.
        if (!_replicated.contains(ref) && missing_seen.insert(ref).second) {
            missing.push_back(ref);
        }
    }

    if (missing.empty()) {
        co_await import_body(n, std::move(fetched.value()));
        co_return source_result<void>{};
    }

    // The body is released here (fetched goes out of scope): the node will be
    // re-fetched once its references complete. A reference that does not exist
    // on the source is discovered as a node whose fetch fails, which fails it
    // and transitively fails this referrer.
    auto& d = data(n);
    d.state = node_state::pending;
    d.in_deg = static_cast<uint32_t>(missing.size());
    for (auto& ref : missing) {
        auto& rd = data(ref);
        if (rd.state == node_state::errored) {
            // A referent already failed; the referrer can never import.
            fail(n);
            co_return source_result<void>{};
        }
        rd.dependents.push_back(n);
        if (rd.state == node_state::unseen) {
            rd.state = node_state::discovering;
            _discover_q.push_back(ref);
            ++_outstanding;
            _cv.signal();
        }
    }
    co_return source_result<void>{};
}

ss::future<source_result<void>>
reconciler::do_import(const ppsr::subject_version& n, ss::abort_source& as) {
    // Same reserve-then-consume model as discover. An import node has no unmet
    // deps: it waits on nothing while holding these units, so the byte
    // semaphore cannot hold-and-wait (deadlock-free).
    auto units = co_await ss::get_units(
      _mem, std::min(reconcile_reserve_bytes, _limits.memory_bytes), as);
    size_t consumed = 0;
    auto release_consumed = ss::defer([this, &consumed] {
        if (consumed > 0) {
            _mem.signal(consumed);
        }
    });

    auto fetched = co_await _source->read_subject_version(n.sub, n.version, as);
    if (!fetched.has_value()) {
        if (fetched.error().kind == source_error_kind::source_unavailable) {
            co_return std::unexpected(std::move(fetched.error()));
        }
        fail(n);
        co_return source_result<void>{};
    }

    auto reserved = units.count();
    if (auto size = body_size(fetched.value()); size > reserved) {
        consumed = size - reserved;
        _mem.consume(consumed);
    }

    co_await import_body(n, std::move(fetched.value()));
    co_return source_result<void>{};
}

ss::future<bool> reconciler::import_body(
  const ppsr::subject_version& n, ppsr::stored_schema schema) {
    // Write pre-await state, then drop the reference: import_schema suspends,
    // and a node_data& into _nodes is not stable across an insertion another
    // fiber may make during the suspension.
    data(n).state = node_state::importing;
    auto fut = co_await ss::coroutine::as_future(
      _destination->import_schema(std::move(schema)));
    if (fut.failed()) {
        auto eptr = fut.get_exception();
        try {
            std::rethrow_exception(eptr);
        } catch (const ppsr::exception& e) {
            if (is_import_conflict(e)) {
                vlog(
                  cllog.warn,
                  "Schema import conflict for {}/{}: {}",
                  n.sub,
                  n.version,
                  e.what());
                fail(n);
                co_return false;
            }
        }
        // Not a per-item conflict: let it fault the whole sync.
        std::rethrow_exception(eptr);
    }
    data(n).state = node_state::done;
    ++_stats->versions_changed;
    wake(n);
    co_return true;
}

void reconciler::wake(const ppsr::subject_version& n) {
    _replicated.insert(n);
    // Snapshot dependents: data(w) below may insert w into _nodes, and a
    // node_data reference (here, into n's dependents vector) is not stable
    // across an insertion.
    auto dependents = std::move(data(n).dependents);
    for (const auto& w : dependents) {
        auto& wd = data(w);
        if (wd.state != node_state::pending) {
            continue;
        }
        if (wd.in_deg > 0 && --wd.in_deg == 0) {
            wd.state = node_state::importing;
            _import_q.push_back(w);
            ++_outstanding;
            _cv.signal();
        }
    }
}

void reconciler::fail(const ppsr::subject_version& n) {
    auto& d = data(n);
    if (d.state == node_state::errored) {
        return;
    }
    d.state = node_state::errored;
    ++_stats->errors;
    vlog(cllog.warn, "Failed to replicate schema {}/{}", n.sub, n.version);
    // A failed node's dependents can never satisfy their references; fail them
    // transitively. Copy the dependents because fail recurses through data().
    auto dependents = std::move(d.dependents);
    for (const auto& w : dependents) {
        fail(w);
    }
}

} // namespace cluster_link::schema_registry_sync
