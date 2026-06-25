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

#include "cluster_link/schema_registry_sync/scope.h"
#include "pandaproxy/schema_registry/types.h"
#include "test_utils/test.h"

namespace cluster_link::tests {

namespace ppsr = pandaproxy::schema_registry;
namespace srs = cluster_link::schema_registry_sync;

TEST(scope, in_scope_predicate_matches_configured_contexts) {
    chunked_hash_set<ppsr::context> contexts;
    contexts.insert(ppsr::default_context);
    contexts.insert(ppsr::context{".b"});
    // Empty subject set: no per-subject restriction within the in-scope
    // contexts.
    auto in_scope = srs::make_in_scope(std::move(contexts), {});

    EXPECT_TRUE(in_scope(ppsr::context_subject::unqualified("orders")));
    EXPECT_TRUE(
      in_scope(ppsr::context_subject{ppsr::context{".b"}, ppsr::subject{"x"}}));
    EXPECT_FALSE(
      in_scope(ppsr::context_subject{ppsr::context{".c"}, ppsr::subject{"x"}}));
}

TEST(scope, in_scope_filter_union_semantics) {
    // The source filter's context and subject selectors union: contexts select
    // whole contexts, subjects add individual context-qualified subjects, and
    // an empty filter replicates everything. These four cases pin that exact
    // behaviour.
    const auto other = ppsr::context{".other"};
    const auto onemore = ppsr::context{".onemore"};
    const auto example = ppsr::context_subject{
      onemore, ppsr::subject{"example"}};
    const auto onemore_other = ppsr::context_subject{
      onemore, ppsr::subject{"other"}};
    const auto other_x = ppsr::context_subject{other, ppsr::subject{"x"}};
    const auto default_a = ppsr::context_subject::unqualified("a");

    auto ctxs = [](std::initializer_list<ppsr::context> cs) {
        chunked_hash_set<ppsr::context> out;
        for (const auto& c : cs) {
            out.insert(c);
        }
        return out;
    };
    auto subs = [](std::initializer_list<ppsr::context_subject> ss) {
        chunked_hash_set<ppsr::context_subject> out;
        for (const auto& s : ss) {
            out.insert(s);
        }
        return out;
    };

    // context=[], subject=[]: no filter, everything is in scope.
    {
        auto in_scope = srs::make_in_scope(ctxs({}), subs({}));
        EXPECT_TRUE(in_scope(default_a));
        EXPECT_TRUE(in_scope(other_x));
        EXPECT_TRUE(in_scope(example));
    }
    // context=[.other], subject=[]: only the .other context.
    {
        auto in_scope = srs::make_in_scope(ctxs({other}), subs({}));
        EXPECT_TRUE(in_scope(other_x));
        EXPECT_FALSE(in_scope(default_a));
        EXPECT_FALSE(in_scope(example));
    }
    // context=[.other], subject=[:.onemore:example]: all of .other UNION the
    // single .onemore:example subject.
    {
        auto in_scope = srs::make_in_scope(ctxs({other}), subs({example}));
        EXPECT_TRUE(in_scope(other_x)); // whole .other context
        EXPECT_TRUE(in_scope(example)); // the individually-listed subject
        EXPECT_FALSE(in_scope(onemore_other)); // not the rest of .onemore
        EXPECT_FALSE(in_scope(default_a));
    }
    // context=[], subject=[:.onemore:example]: only the single subject.
    {
        auto in_scope = srs::make_in_scope(ctxs({}), subs({example}));
        EXPECT_TRUE(in_scope(example));
        EXPECT_FALSE(in_scope(onemore_other));
        EXPECT_FALSE(in_scope(other_x));
        EXPECT_FALSE(in_scope(default_a));
    }
}

} // namespace cluster_link::tests
