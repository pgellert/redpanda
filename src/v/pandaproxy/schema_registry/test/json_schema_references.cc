// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/json.h"
#include "pandaproxy/schema_registry/test/store_fixture.h"
#include "pandaproxy/schema_registry/types.h"

#include <gtest/gtest.h>

namespace pandaproxy::schema_registry {

namespace {

// Build a JSON schema_definition that $ref's a single external schema.
schema_definition make_referencing_schema(
  std::string_view schema_json,
  std::string_view ref_name,
  std::string_view ref_subject,
  schema_version ref_version) {
    return schema_definition{
      schema_json,
      schema_type::json,
      {schema_reference{
        .name = ss::sstring{ref_name},
        .sub = context_subject_reference::unqualified(ss::sstring{ref_subject}),
        .version = ref_version}},
      {}};
}

} // namespace

class JsonSchemaReferencesTest
  : public ::testing::Test
  , public test_utils::store_fixture {
public:
    // Parse first (may resolve references from the store), then insert.
    json_schema_definition register_schema(
      const context_subject& sub,
      const schema_definition& schema_def,
      schema_version version) {
        auto json_def = make_json_schema_definition(
                          _store, {sub, schema_def.share()})
                          .get();

        store_fixture::insert(sub, schema_def, version);

        return json_def;
    }
};

TEST_F(JsonSchemaReferencesTest, RegisterWithReferences) {
    // Basic test: register a referenced schema, then a schema that $ref's it.

    const auto ref_sub = context_subject::unqualified("PersonSubject");
    const auto main_sub = context_subject::unqualified("TeamSubject");

    auto person_schema = schema_definition{
      R"({
    "type": "object",
    "properties": {
        "name": { "type": "string" },
        "age": { "type": "integer" }
    }
})",
      schema_type::json};

    auto team_schema = make_referencing_schema(
      R"({
    "type": "object",
    "properties": {
        "teamName": { "type": "string" },
        "lead": { "$ref": "person.json" }
    }
})",
      "person.json",
      "PersonSubject",
      schema_version(1));

    register_schema(ref_sub, person_schema, schema_version{1});

    ASSERT_NO_THROW(register_schema(main_sub, team_schema, schema_version{1}));
}

constexpr std::string_view team_schema_json = R"({
    "type": "object",
    "properties": {
        "teamName": { "type": "string" },
        "lead": { "$ref": "person.json" }
    }
})";

TEST_F(JsonSchemaReferencesTest, CompatibilityWithReferences) {
    // Verify that compatibility checking resolves external references.
    // A backward-compatible change: adding an optional property to the
    // referenced schema.

    const auto ref_sub = context_subject::unqualified("PersonSubject");
    const auto main_sub = context_subject::unqualified("TeamSubject");

    auto person_v1 = schema_definition{
      R"({
    "type": "object",
    "properties": {
        "name": { "type": "string" }
    }
})",
      schema_type::json};

    auto person_v2 = schema_definition{
      R"({
    "type": "object",
    "properties": {
        "name": { "type": "string" },
        "age": { "type": "integer" }
    }
})",
      schema_type::json};

    register_schema(ref_sub, person_v1, schema_version{1});

    auto team_v1_def = make_referencing_schema(
      team_schema_json, "person.json", "PersonSubject", schema_version(1));

    auto team_v1 = register_schema(main_sub, team_v1_def, schema_version{1});

    // Register person v2 and create team v2 referencing it
    register_schema(ref_sub, person_v2, schema_version{2});

    auto team_v2_def = make_referencing_schema(
      team_schema_json, "person.json", "PersonSubject", schema_version(2));

    auto team_v2 = make_json_schema_definition(
                     _store, {main_sub, team_v2_def.share()})
                     .get();

    // Backward compatible: reader (v1) is superset of writer (v2)
    auto result = check_compatible(team_v1, team_v2);
    ASSERT_TRUE(result.is_compat) << result.messages;
}

constexpr std::string_view order_schema_json = R"({
    "type": "object",
    "properties": {
        "id": { "type": "integer" },
        "address": { "$ref": "address.json" }
    }
})";

TEST_F(JsonSchemaReferencesTest, IncompatibleReferenceChange) {
    // Verify that an incompatible change in a referenced schema is detected.
    // Narrowing a type (string -> integer) is not backward compatible.

    const auto ref_sub = context_subject::unqualified("AddressSubject");
    const auto main_sub = context_subject::unqualified("OrderSubject");

    auto addr_v1 = schema_definition{
      R"({
    "type": "object",
    "properties": {
        "city": { "type": "string" }
    }
})",
      schema_type::json};

    auto addr_v2 = schema_definition{
      R"({
    "type": "object",
    "properties": {
        "city": { "type": "integer" }
    }
})",
      schema_type::json};

    register_schema(ref_sub, addr_v1, schema_version{1});

    auto order_v1_def = make_referencing_schema(
      order_schema_json, "address.json", "AddressSubject", schema_version(1));

    auto order_v1 = register_schema(main_sub, order_v1_def, schema_version{1});

    // Register addr v2 and create order v2
    register_schema(ref_sub, addr_v2, schema_version{2});

    auto order_v2_def = make_referencing_schema(
      order_schema_json, "address.json", "AddressSubject", schema_version(2));

    auto order_v2 = make_json_schema_definition(
                      _store, {main_sub, order_v2_def.share()})
                      .get();

    // Should be incompatible: city type changed from string to integer
    auto result = check_compatible(order_v1, order_v2);
    ASSERT_FALSE(result.is_compat);
}

TEST_F(JsonSchemaReferencesTest, TransitiveReferences) {
    // A -> B -> C chain: Order refs Address, Address refs Country.

    register_schema(
      context_subject::unqualified("CountrySubject"),
      schema_definition{
        R"({
    "type": "object",
    "properties": {
        "code": { "type": "string" },
        "name": { "type": "string" }
    }
})",
        schema_type::json},
      schema_version{1});

    register_schema(
      context_subject::unqualified("AddressSubject"),
      make_referencing_schema(
        R"({
    "type": "object",
    "properties": {
        "street": { "type": "string" },
        "country": { "$ref": "country.json" }
    }
})",
        "country.json",
        "CountrySubject",
        schema_version(1)),
      schema_version{1});

    // Order references Address, which transitively references Country
    auto order_def = make_referencing_schema(
      R"({
    "type": "object",
    "properties": {
        "id": { "type": "integer" },
        "shippingAddress": { "$ref": "address.json" }
    }
})",
      "address.json",
      "AddressSubject",
      schema_version(1));

    ASSERT_NO_THROW(
      make_json_schema_definition(
        _store,
        {context_subject::unqualified("OrderSubject"), order_def.share()})
        .get());
}

TEST_F(JsonSchemaReferencesTest, DiamondDependencies) {
    // Diamond: Main -> {Left, Right} -> Shared

    register_schema(
      context_subject::unqualified("SharedSubject"),
      schema_definition{
        R"({
    "type": "object",
    "properties": {
        "id": { "type": "string" }
    }
})",
        schema_type::json},
      schema_version{1});

    register_schema(
      context_subject::unqualified("LeftSubject"),
      make_referencing_schema(
        R"({
    "type": "object",
    "properties": {
        "shared": { "$ref": "shared.json" },
        "leftData": { "type": "string" }
    }
})",
        "shared.json",
        "SharedSubject",
        schema_version(1)),
      schema_version{1});

    register_schema(
      context_subject::unqualified("RightSubject"),
      make_referencing_schema(
        R"({
    "type": "object",
    "properties": {
        "shared": { "$ref": "shared.json" },
        "rightData": { "type": "integer" }
    }
})",
        "shared.json",
        "SharedSubject",
        schema_version(1)),
      schema_version{1});

    // Main references both Left and Right, which both reference Shared
    auto main_def = schema_definition{
      R"({
    "type": "object",
    "properties": {
        "left": { "$ref": "left.json" },
        "right": { "$ref": "right.json" }
    }
})",
      schema_type::json,
      {schema_reference{
         .name = "left.json",
         .sub = context_subject_reference::unqualified("LeftSubject"),
         .version = schema_version(1)},
       schema_reference{
         .name = "right.json",
         .sub = context_subject_reference::unqualified("RightSubject"),
         .version = schema_version(1)}},
      {}};

    ASSERT_NO_THROW(
      make_json_schema_definition(
        _store, {context_subject::unqualified("MainSubject"), main_def.share()})
        .get());
}

TEST_F(JsonSchemaReferencesTest, MissingReferenceThrows) {
    // Referencing a non-existent schema should throw.

    auto schema_def = make_referencing_schema(
      R"({
    "type": "object",
    "properties": {
        "data": { "$ref": "missing.json" }
    }
})",
      "missing.json",
      "NonExistent",
      schema_version(1));

    ASSERT_THROW(
      make_json_schema_definition(
        _store,
        {context_subject::unqualified("TestSubject"), schema_def.share()})
        .get(),
      std::exception);
}

} // namespace pandaproxy::schema_registry
