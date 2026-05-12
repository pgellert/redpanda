// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster_link/sr_topo_sort.h"
#include "pandaproxy/schema_registry/types.h"

#include <gtest/gtest.h>

namespace pps = pandaproxy::schema_registry;
namespace cl = cluster_link;

namespace {

pps::stored_schema make_schema(
  std::string_view subject,
  int32_t version,
  int32_t id,
  pps::schema_definition::references refs = {}) {
    pps::schema_definition def{
      pps::schema_definition::raw_string{"{\"type\":\"string\"}"},
      pps::schema_type::avro,
      std::move(refs),
      std::nullopt};
    pps::subject_schema ss{
      pps::context_subject::unqualified(subject), std::move(def)};
    return pps::stored_schema{
      .schema = std::move(ss),
      .version = pps::schema_version{version},
      .id = pps::schema_id{id},
      .deleted = pps::is_deleted::no};
}

pps::schema_reference
ref(std::string_view subject, int32_t version, std::string_view name = "r") {
    return pps::schema_reference{
      .name = ss::sstring{name},
      .sub = pps::context_subject_reference::unqualified(subject),
      .version = pps::schema_version{version}};
}

/// Helper to assert the order of an output, by subject.
std::vector<ss::sstring>
subjects_of(const chunked_vector<pps::stored_schema>& sorted) {
    std::vector<ss::sstring> out;
    out.reserve(sorted.size());
    for (const auto& s : sorted) {
        out.push_back(s.schema.sub().sub());
    }
    return out;
}

} // namespace

TEST(SrTopoSort, EmptyInputProducesEmptyOutput) {
    auto r = cl::topo_sort_by_refs({});
    EXPECT_TRUE(r.sorted.empty());
    EXPECT_FALSE(r.cycle_detected);
}

TEST(SrTopoSort, NoReferencesPreservesInputOrder) {
    chunked_vector<pps::stored_schema> in;
    in.push_back(make_schema("a", 1, 100));
    in.push_back(make_schema("b", 1, 101));
    in.push_back(make_schema("c", 1, 102));

    auto r = cl::topo_sort_by_refs(std::move(in));
    EXPECT_EQ(subjects_of(r.sorted), (std::vector<ss::sstring>{"a", "b", "c"}));
    EXPECT_FALSE(r.cycle_detected);
}

TEST(SrTopoSort, ReferentEmittedBeforeReferrer) {
    // b references a -- a must come first.
    pps::schema_definition::references b_refs;
    b_refs.push_back(ref("a", 1));

    chunked_vector<pps::stored_schema> in;
    in.push_back(make_schema("b", 1, 101, std::move(b_refs)));
    in.push_back(make_schema("a", 1, 100));

    auto r = cl::topo_sort_by_refs(std::move(in));
    EXPECT_EQ(subjects_of(r.sorted), (std::vector<ss::sstring>{"a", "b"}));
    EXPECT_FALSE(r.cycle_detected);
}

TEST(SrTopoSort, ChainOfReferences) {
    // c -> b -> a. Expected order: a, b, c.
    pps::schema_definition::references c_refs;
    c_refs.push_back(ref("b", 1));
    pps::schema_definition::references b_refs;
    b_refs.push_back(ref("a", 1));

    chunked_vector<pps::stored_schema> in;
    in.push_back(make_schema("c", 1, 102, std::move(c_refs)));
    in.push_back(make_schema("b", 1, 101, std::move(b_refs)));
    in.push_back(make_schema("a", 1, 100));

    auto r = cl::topo_sort_by_refs(std::move(in));
    EXPECT_EQ(subjects_of(r.sorted), (std::vector<ss::sstring>{"a", "b", "c"}));
    EXPECT_FALSE(r.cycle_detected);
}

TEST(SrTopoSort, MultipleReferencesToSameReferent) {
    // c references a and b; b references a. Expected: a, b, c.
    pps::schema_definition::references c_refs;
    c_refs.push_back(ref("a", 1, "ar"));
    c_refs.push_back(ref("b", 1, "br"));
    pps::schema_definition::references b_refs;
    b_refs.push_back(ref("a", 1));

    chunked_vector<pps::stored_schema> in;
    in.push_back(make_schema("c", 1, 102, std::move(c_refs)));
    in.push_back(make_schema("a", 1, 100));
    in.push_back(make_schema("b", 1, 101, std::move(b_refs)));

    auto r = cl::topo_sort_by_refs(std::move(in));
    EXPECT_EQ(subjects_of(r.sorted), (std::vector<ss::sstring>{"a", "b", "c"}));
    EXPECT_FALSE(r.cycle_detected);
}

TEST(SrTopoSort, ReferenceOutsideInputTreatedAsRoot) {
    // a references "unknown" which is not in the input. We should still
    // emit a (treating "unknown" as an external satisfied dep).
    pps::schema_definition::references a_refs;
    a_refs.push_back(ref("unknown", 5));

    chunked_vector<pps::stored_schema> in;
    in.push_back(make_schema("a", 1, 100, std::move(a_refs)));

    auto r = cl::topo_sort_by_refs(std::move(in));
    EXPECT_EQ(subjects_of(r.sorted), (std::vector<ss::sstring>{"a"}));
    EXPECT_FALSE(r.cycle_detected);
}

TEST(SrTopoSort, ReferenceToDifferentVersionOfSameSubjectIsAnEdge) {
    // a@v2 references a@v1. a@v1 must come before a@v2.
    pps::schema_definition::references v2_refs;
    v2_refs.push_back(ref("a", 1));

    chunked_vector<pps::stored_schema> in;
    in.push_back(make_schema("a", 2, 101, std::move(v2_refs)));
    in.push_back(make_schema("a", 1, 100));

    auto r = cl::topo_sort_by_refs(std::move(in));
    ASSERT_EQ(r.sorted.size(), 2u);
    EXPECT_EQ(r.sorted[0].version(), 1);
    EXPECT_EQ(r.sorted[1].version(), 2);
    EXPECT_FALSE(r.cycle_detected);
}

TEST(SrTopoSort, SelfReferenceIsTolerated) {
    pps::schema_definition::references self_refs;
    self_refs.push_back(ref("a", 1));

    chunked_vector<pps::stored_schema> in;
    in.push_back(make_schema("a", 1, 100, std::move(self_refs)));

    auto r = cl::topo_sort_by_refs(std::move(in));
    EXPECT_EQ(subjects_of(r.sorted), (std::vector<ss::sstring>{"a"}));
    // Self-reference doesn't count as a cycle for our purposes.
    EXPECT_FALSE(r.cycle_detected);
}

TEST(SrTopoSort, CycleFallsBackToInputOrderWithFlag) {
    // a references b; b references a. Cycle.
    pps::schema_definition::references a_refs;
    a_refs.push_back(ref("b", 1));
    pps::schema_definition::references b_refs;
    b_refs.push_back(ref("a", 1));

    chunked_vector<pps::stored_schema> in;
    in.push_back(make_schema("a", 1, 100, std::move(a_refs)));
    in.push_back(make_schema("b", 1, 101, std::move(b_refs)));

    auto r = cl::topo_sort_by_refs(std::move(in));
    EXPECT_TRUE(r.cycle_detected);
    EXPECT_EQ(r.sorted.size(), 2u);
}

TEST(SrTopoSort, PartialCycleEmitsSatisfiableNodesFirst) {
    // a (root), b<->c (cycle). Expected: a first, then b/c in input order.
    pps::schema_definition::references b_refs;
    b_refs.push_back(ref("c", 1));
    pps::schema_definition::references c_refs;
    c_refs.push_back(ref("b", 1));

    chunked_vector<pps::stored_schema> in;
    in.push_back(make_schema("b", 1, 101, std::move(b_refs)));
    in.push_back(make_schema("a", 1, 100));
    in.push_back(make_schema("c", 1, 102, std::move(c_refs)));

    auto r = cl::topo_sort_by_refs(std::move(in));
    EXPECT_TRUE(r.cycle_detected);
    // a comes first because it has no dependencies; the cyclic pair gets
    // appended in input order.
    EXPECT_EQ(subjects_of(r.sorted), (std::vector<ss::sstring>{"a", "b", "c"}));
}
