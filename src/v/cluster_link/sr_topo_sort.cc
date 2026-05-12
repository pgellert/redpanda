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

#include "cluster_link/sr_topo_sort.h"

#include "container/chunked_hash_map.h"

#include <utility>

namespace pps = pandaproxy::schema_registry;

namespace cluster_link {

namespace {

/// Identifier of a schema in the dependency graph.
struct node_key {
    ss::sstring subject;
    int32_t version{};

    bool operator==(const node_key&) const = default;

    template<typename H>
    friend H AbslHashValue(H h, const node_key& k) {
        return H::combine(std::move(h), k.subject, k.version);
    }
};

} // namespace

topo_sort_result topo_sort_by_refs(chunked_vector<pps::stored_schema> input) {
    topo_sort_result out;
    if (input.empty()) {
        return out;
    }

    // Build:
    // - `key_to_idx`: node_key -> index into the input vector. Used to
    //   resolve a reference to a concrete input element.
    // - `indegree`: per-node count of references that point at other
    //   present nodes (i.e. internal edges only).
    // - `reverse_edges`: for each present referent, list of indices that
    //   reference it. Used to decrement indegrees after a node is emitted.
    chunked_hash_map<node_key, size_t> key_to_idx;
    chunked_vector<size_t> indegree;
    indegree.reserve(input.size());
    chunked_vector<chunked_vector<size_t>> reverse_edges;
    reverse_edges.reserve(input.size());

    for (size_t i = 0; i < input.size(); ++i) {
        node_key k{
          .subject = input[i].schema.sub().sub(),
          .version = input[i].version(),
        };
        // Note: in the (unexpected) case of duplicate (subject, version)
        // entries we keep the first index; the duplicate is treated as a
        // standalone node with no internal incoming edges.
        key_to_idx.try_emplace(std::move(k), i);
        indegree.push_back(0);
        reverse_edges.emplace_back();
    }

    for (size_t i = 0; i < input.size(); ++i) {
        for (const auto& ref : input[i].schema.def().refs()) {
            node_key rk{
              .subject = ref.sub.sub(),
              .version = ref.version(),
            };
            auto it = key_to_idx.find(rk);
            if (it == key_to_idx.end()) {
                // Reference points outside the current input — treated as
                // an already-satisfied external dependency.
                continue;
            }
            // Self-references would create an immediate cycle; skip the
            // edge so we don't get stuck.
            if (it->second == i) {
                continue;
            }
            reverse_edges[it->second].push_back(i);
            ++indegree[i];
        }
    }

    // Stable BFS: walk input in order, queue nodes whose indegree is 0.
    chunked_vector<size_t> ready;
    ready.reserve(input.size());
    for (size_t i = 0; i < input.size(); ++i) {
        if (indegree[i] == 0) {
            ready.push_back(i);
        }
    }

    chunked_vector<uint8_t> emitted;
    emitted.reserve(input.size());
    for (size_t i = 0; i < input.size(); ++i) {
        emitted.push_back(0);
    }
    out.sorted.reserve(input.size());

    size_t head = 0;
    while (head < ready.size()) {
        auto idx = ready[head++];
        if (emitted[idx] != 0) {
            continue;
        }
        emitted[idx] = 1;
        out.sorted.push_back(std::move(input[idx]));
        for (auto consumer : reverse_edges[idx]) {
            if (--indegree[consumer] == 0) {
                ready.push_back(consumer);
            }
        }
    }

    if (out.sorted.size() != input.size()) {
        // Cycle. Append the un-emitted nodes in input order so we still
        // make forward progress; the destination SR will reject any of
        // these whose dependencies don't yet exist, and the next pass
        // can retry.
        out.cycle_detected = true;
        for (size_t i = 0; i < input.size(); ++i) {
            if (emitted[i] == 0) {
                out.sorted.push_back(std::move(input[i]));
            }
        }
    }

    return out;
}

} // namespace cluster_link
