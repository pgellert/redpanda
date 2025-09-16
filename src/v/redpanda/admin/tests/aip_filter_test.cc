/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "absl/time/clock.h"
#include "redpanda/admin/aip_filter.h"
#include "redpanda/admin/field_registry.h"
#include "src/v/redpanda/admin/tests/aip_filter_test_messages.proto.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

namespace redpanda::admin {

// Helper function to create a test object with various field values
protobuf_test_messages::editions::test_all_types_edition2023 create_test_object(
  int32_t int32_val = 1,
  uint32_t uint32_val = 0,
  const std::string& string_val = "test-string",
  bool bool_val = false,
  const std::string& client_id = "test-client",
  const std::string& user_name = "admin",
  bool is_enabled = true,
  const std::string& ip_address = "192.168.1.100",
  uint32_t port = 9092,
  uint64_t count_total = 100,
  uint64_t count_recent = 50) {
    protobuf_test_messages::editions::test_all_types_edition2023 obj;

    obj.set_optional_int32(int32_val);
    obj.set_optional_uint32(uint32_val);
    obj.set_optional_string(ss::sstring(string_val));
    obj.set_optional_bool(bool_val);
    obj.set_client_id(ss::sstring(client_id));
    obj.set_user_name(ss::sstring(user_name));
    obj.set_is_enabled(is_enabled);
    obj.set_is_active(true);
    obj.set_count_total(count_total);
    obj.set_count_recent(count_recent);

    // Set nested message fields
    obj.get_optional_nested_message().set_a(42);
    obj.get_optional_nested_message().set_nested_string(
      ss::sstring("nested-value"));

    obj.get_source_info().set_ip_address(ss::sstring(ip_address));
    obj.get_source_info().set_port(port);

    // Set enum fields

    obj.set_optional_nested_enum(
      protobuf_test_messages::editions::test_all_types_edition2023_nested_enum::
        nested_enum_foo);
    obj.set_status_enum(
      protobuf_test_messages::editions::test_all_types_edition2023_status_enum::
        status_success);

    // Set time-based fields
    auto now = absl::Now();
    obj.set_creation_time(now - absl::Minutes(30));
    obj.set_update_time(absl::Time{}); // Not set
    obj.set_idle_duration(absl::Seconds(120));
    obj.set_total_duration(absl::Minutes(10));

    return obj;
}

// Single test fixture using auto registry
class AIPFilterTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto registry = make_field_registry<
          protobuf_test_messages::editions::test_all_types_edition2023>();
        parser_ = std::make_unique<AIPFilterParser<
          protobuf_test_messages::editions::test_all_types_edition2023>>(
          std::move(registry));
    }

private:
    std::unique_ptr<AIPFilterParser<
      protobuf_test_messages::editions::test_all_types_edition2023>>
      parser_;

protected:
    // Provide access to the parser for tests
    AIPFilterParser<
      protobuf_test_messages::editions::test_all_types_edition2023>&
    parser() {
        return *parser_;
    }
};

// =============================================================================
// BASIC FUNCTIONALITY TESTS
// =============================================================================

TEST_F(AIPFilterTest, EmptyFilterMatchesAll) {
    auto predicate = parser().parse("");
    auto obj = create_test_object();
    EXPECT_TRUE(predicate(obj));
}

TEST_F(AIPFilterTest, SimpleIntegerEquality) {
    auto predicate = parser().parse("optional_int32 = 1");

    auto obj1 = create_test_object(1);
    auto obj2 = create_test_object(2);

    EXPECT_TRUE(predicate(obj1));
    EXPECT_FALSE(predicate(obj2));
}

TEST_F(AIPFilterTest, SimpleStringEquality) {
    auto predicate = parser().parse("client_id = \"test-client\"");

    auto obj1 = create_test_object(1, 0, "string", false, "test-client");
    auto obj2 = create_test_object(1, 0, "string", false, "other-client");

    EXPECT_TRUE(predicate(obj1));
    EXPECT_FALSE(predicate(obj2));
}

TEST_F(AIPFilterTest, SimpleBooleanEquality) {
    auto predicate = parser().parse("optional_bool = true");

    auto obj1 = create_test_object(1, 0, "string", true);
    auto obj2 = create_test_object(1, 0, "string", false);

    EXPECT_TRUE(predicate(obj1));
    EXPECT_FALSE(predicate(obj2));
}

// =============================================================================
// COMPARISON OPERATORS TESTS
// =============================================================================

TEST_F(AIPFilterTest, AllComparisonOperators) {
    auto obj = create_test_object(
      5,
      0,
      "string",
      false,
      "client",
      "admin",
      true,
      "192.168.1.100",
      9092,
      100);

    // Test all operators with integers
    EXPECT_TRUE(parser().parse("optional_int32 = 5")(obj));
    EXPECT_TRUE(parser().parse("optional_int32 != 6")(obj));
    EXPECT_TRUE(parser().parse("optional_int32 < 6")(obj));
    EXPECT_TRUE(parser().parse("optional_int32 <= 5")(obj));
    EXPECT_TRUE(parser().parse("optional_int32 > 4")(obj));
    EXPECT_TRUE(parser().parse("optional_int32 >= 5")(obj));

    // Test boundary conditions
    EXPECT_FALSE(parser().parse("optional_int32 < 5")(obj));
    EXPECT_FALSE(parser().parse("optional_int32 > 5")(obj));
    EXPECT_TRUE(parser().parse("optional_int32 <= 5")(obj));
    EXPECT_TRUE(parser().parse("optional_int32 >= 5")(obj));
}

TEST_F(AIPFilterTest, StringComparisons) {
    auto obj = create_test_object(1, 0, "string", false, "client-b");

    EXPECT_TRUE(parser().parse("client_id = \"client-b\"")(obj));
    EXPECT_TRUE(parser().parse("client_id != \"client-a\"")(obj));
    EXPECT_TRUE(parser().parse("client_id > \"client-a\"")(obj));
    EXPECT_TRUE(parser().parse("client_id < \"client-c\"")(obj));
    EXPECT_TRUE(parser().parse("client_id >= \"client-b\"")(obj));
    EXPECT_TRUE(parser().parse("client_id <= \"client-b\"")(obj));
}

TEST_F(AIPFilterTest, BooleanComparisons) {
    auto obj_true = create_test_object(1, 0, "string", true);
    auto obj_false = create_test_object(1, 0, "string", false);

    // Only = and != should work for booleans
    EXPECT_TRUE(parser().parse("optional_bool = true")(obj_true));
    EXPECT_TRUE(parser().parse("optional_bool = false")(obj_false));
    EXPECT_TRUE(parser().parse("optional_bool != false")(obj_true));
    EXPECT_TRUE(parser().parse("optional_bool != true")(obj_false));

    // Other operators should throw
    EXPECT_THROW(parser().parse("optional_bool > true"), std::invalid_argument);
    EXPECT_THROW(
      parser().parse("optional_bool < false"), std::invalid_argument);
}

// =============================================================================
// NESTED FIELD TESTS
// =============================================================================

TEST_F(AIPFilterTest, NestedFieldAccess) {
    auto obj = create_test_object(
      1, 0, "string", false, "client", "admin", true, "192.168.1.100", 9092);

    EXPECT_TRUE(parser().parse("user_name = \"admin\"")(obj));
    EXPECT_TRUE(parser().parse("is_enabled = true")(obj));
    EXPECT_TRUE(
      parser().parse("source_info.ip_address = \"192.168.1.100\"")(obj));
    EXPECT_TRUE(parser().parse("source_info.port = 9092")(obj));
}

TEST_F(AIPFilterTest, DeepNestedFieldAccess) {
    auto obj = create_test_object();
    obj.get_optional_nested_message().set_a(123);
    obj.get_optional_nested_message().set_nested_string(
      ss::sstring("deep-value"));

    EXPECT_TRUE(parser().parse("optional_nested_message.a = 123")(obj));
    EXPECT_TRUE(
      parser().parse("optional_nested_message.nested_string = \"deep-value\"")(
        obj));
}

// =============================================================================
// LOGICAL OPERATORS TESTS
// =============================================================================

TEST_F(AIPFilterTest, SimpleAndOperation) {
    auto predicate = parser().parse(
      "optional_int32 = 1 AND optional_bool = false");

    auto obj1 = create_test_object(1, 0, "string", false);
    auto obj2 = create_test_object(1, 0, "string", true);
    auto obj3 = create_test_object(2, 0, "string", false);

    EXPECT_TRUE(predicate(obj1));
    EXPECT_FALSE(predicate(obj2));
    EXPECT_FALSE(predicate(obj3));
}

TEST_F(AIPFilterTest, MultipleAndOperations) {
    auto predicate = parser().parse(
      "optional_int32 = 1 AND optional_bool = false AND is_enabled = true");

    auto obj1 = create_test_object(
      1, 0, "string", false, "client", "admin", true);
    auto obj2 = create_test_object(
      1, 0, "string", false, "client", "admin", false);

    EXPECT_TRUE(predicate(obj1));
    EXPECT_FALSE(predicate(obj2));
}

TEST_F(AIPFilterTest, ComplexAndWithNestedFields) {
    auto predicate = parser().parse(
      "optional_int32 = 1 AND user_name = \"admin\" AND source_info.port = "
      "9092");

    auto obj = create_test_object(
      1, 0, "string", false, "client", "admin", true, "192.168.1.100", 9092);
    EXPECT_TRUE(predicate(obj));

    // Change one field to make it fail
    obj.get_source_info().set_port(9093);
    EXPECT_FALSE(predicate(obj));
}

// =============================================================================
// AIP-160 DURATION COMPLIANCE TESTS
// =============================================================================

TEST_F(AIPFilterTest, AIP160DurationCompliance) {
    auto obj = create_test_object();

    // Test various duration formats that absl::ParseDuration supports
    obj.set_idle_duration(absl::Seconds(20));
    EXPECT_TRUE(parser().parse("idle_duration = 20s")(obj));

    obj.set_idle_duration(absl::Milliseconds(1200)); // 1.2 seconds
    EXPECT_TRUE(parser().parse("idle_duration = 1.2s")(obj));

    // Test that minutes, hours etc. work (beyond AIP-160 spec but supported by
    // absl)
    obj.set_idle_duration(absl::Minutes(5)); // 300 seconds
    EXPECT_TRUE(parser().parse("idle_duration = 5m")(obj));

    obj.set_idle_duration(absl::Hours(1)); // 3600 seconds
    EXPECT_TRUE(parser().parse("idle_duration = 1h")(obj));
}

TEST_F(AIPFilterTest, AIP160DurationEdgeCases) {
    auto obj = create_test_object();

    // Test fractional seconds
    obj.set_idle_duration(absl::Milliseconds(500)); // 0.5 seconds
    EXPECT_TRUE(parser().parse("idle_duration = 0.5s")(obj));

    // Test zero duration
    obj.set_idle_duration(absl::ZeroDuration());
    EXPECT_TRUE(parser().parse("idle_duration = 0s")(obj));

    // Test very small durations
    obj.set_idle_duration(absl::Milliseconds(1)); // 0.001 seconds
    EXPECT_TRUE(parser().parse("idle_duration = \"0.001s\"")(obj));
}

// =============================================================================
// AIP-160 TIMESTAMP COMPLIANCE TESTS
// =============================================================================

TEST_F(AIPFilterTest, AIP160TimestampCompliance) {
    auto obj = create_test_object();

    // Test basic RFC-3339 formats
    absl::Time test_time;
    std::string error;

    // UTC timezone
    ASSERT_TRUE(
      absl::ParseTime(
        absl::RFC3339_full, "2012-04-21T11:30:00Z", &test_time, &error));
    obj.set_creation_time(absl::Time{test_time});
    EXPECT_TRUE(
      parser().parse("creation_time = \"2012-04-21T11:30:00Z\"")(obj));

    // Timezone with offset
    ASSERT_TRUE(
      absl::ParseTime(
        absl::RFC3339_full, "2012-04-21T11:30:00-04:00", &test_time, &error));
    obj.set_creation_time(absl::Time{test_time});
    EXPECT_TRUE(
      parser().parse("creation_time = \"2012-04-21T11:30:00-04:00\"")(obj));

    // Positive timezone offset
    ASSERT_TRUE(
      absl::ParseTime(
        absl::RFC3339_full, "2012-04-21T11:30:00+05:30", &test_time, &error));
    obj.set_creation_time(absl::Time{test_time});
    EXPECT_TRUE(
      parser().parse("creation_time = \"2012-04-21T11:30:00+05:30\"")(obj));
}

TEST_F(AIPFilterTest, AIP160TimestampWithFractions) {
    auto obj = create_test_object();

    // Test fractional seconds
    absl::Time test_time;
    std::string error;
    ASSERT_TRUE(
      absl::ParseTime(
        absl::RFC3339_full, "2012-04-21T11:30:00.123Z", &test_time, &error));

    // Convert to second precision for comparison
    auto unix_seconds = absl::ToUnixSeconds(test_time);
    auto truncated_time = absl::FromUnixSeconds(unix_seconds);
    obj.set_creation_time(absl::Time{truncated_time});

    // Should match the truncated version
    EXPECT_TRUE(
      parser().parse("creation_time = \"2012-04-21T11:30:00Z\"")(obj));
}

TEST_F(AIPFilterTest, TimezoneEquivalence) {
    auto obj = create_test_object();

    // These times should be equivalent
    absl::Time utc_time, offset_time;
    std::string error;

    ASSERT_TRUE(
      absl::ParseTime(
        absl::RFC3339_full, "2012-04-21T15:30:00Z", &utc_time, &error));
    ASSERT_TRUE(
      absl::ParseTime(
        absl::RFC3339_full, "2012-04-21T11:30:00-04:00", &offset_time, &error));

    // They should represent the same instant
    EXPECT_EQ(absl::ToUnixSeconds(utc_time), absl::ToUnixSeconds(offset_time));

    obj.set_creation_time(absl::Time{utc_time});
    EXPECT_TRUE(
      parser().parse("creation_time = \"2012-04-21T15:30:00Z\"")(obj));
    EXPECT_TRUE(
      parser().parse("creation_time = \"2012-04-21T11:30:00-04:00\"")(obj));
}

// =============================================================================
// ERROR HANDLING WITH ABSL
// =============================================================================

TEST_F(AIPFilterTest, AbslDurationErrorHandling) {
    // Test invalid duration formats that absl will reject
    EXPECT_THROW(
      parser().parse("idle_duration = invalid"), std::invalid_argument);
    EXPECT_THROW(
      parser().parse("idle_duration = 120"),
      std::invalid_argument); // Missing unit
    EXPECT_THROW(
      parser().parse("idle_duration = s"),
      std::invalid_argument); // No number
}

TEST_F(AIPFilterTest, AbslTimestampErrorHandling) {
    // Test invalid timestamp formats that absl will reject
    EXPECT_THROW(
      parser().parse("creation_time = \"invalid-timestamp\""),
      std::invalid_argument);
    EXPECT_THROW(
      parser().parse("creation_time = \"2012-04-21 11:30:00\""),
      std::invalid_argument); // Missing T
    EXPECT_THROW(
      parser().parse("creation_time = \"2012-04-21T25:30:00Z\""),
      std::invalid_argument); // Invalid hour
    EXPECT_THROW(
      parser().parse("creation_time = \"2012-13-21T11:30:00Z\""),
      std::invalid_argument); // Invalid month
}

// =============================================================================
// VALIDATION AND ERROR HANDLING TESTS
// =============================================================================

TEST_F(AIPFilterTest, ValidationMethod) {
    EXPECT_TRUE(parser().validate("optional_int32 = 1"));
    EXPECT_TRUE(
      parser().validate("optional_int32 = 1 AND optional_bool = false"));
    EXPECT_TRUE(parser().validate(""));

    EXPECT_FALSE(parser().validate("invalid_field = 1"));
    EXPECT_FALSE(parser().validate("optional_int32 ="));
    EXPECT_FALSE(parser().validate("= 1"));
    EXPECT_FALSE(parser().validate("optional_int32 1"));
}

TEST_F(AIPFilterTest, UnknownFieldError) {
    EXPECT_THROW(parser().parse("unknown_field = 1"), std::invalid_argument);
    EXPECT_THROW(
      parser().parse("optional_int32 = 1 AND unknown_field = 2"),
      std::invalid_argument);
}

TEST_F(AIPFilterTest, MalformedExpressionErrors) {
    EXPECT_THROW(parser().parse("optional_int32 ="), std::invalid_argument);
    EXPECT_THROW(parser().parse("= 1"), std::invalid_argument);
    EXPECT_THROW(parser().parse("optional_int32 1"), std::invalid_argument);
    EXPECT_THROW(
      parser().parse("optional_int32 = 1 AND"), std::invalid_argument);
    EXPECT_THROW(parser().parse("optional_int32 = 1 ="), std::invalid_argument);
}

TEST_F(AIPFilterTest, InvalidLiteralValues) {
    EXPECT_THROW(
      parser().parse("optional_int32 = \"not_a_number\""),
      std::invalid_argument);
    EXPECT_THROW(
      parser().parse("optional_bool = \"not_a_boolean\""),
      std::invalid_argument);
    EXPECT_THROW(
      parser().parse("optional_int32 = 9223372036854775808"),
      std::invalid_argument); // Overflow
}

TEST_F(AIPFilterTest, UnterminatedStringLiteral) {
    EXPECT_THROW(
      parser().parse("client_id = \"unterminated"), std::invalid_argument);
    EXPECT_THROW(parser().parse("client_id = \""), std::invalid_argument);
}

TEST_F(AIPFilterTest, TrailingCharacters) {
    EXPECT_THROW(
      parser().parse("optional_int32 = 1 extra"), std::invalid_argument);
    EXPECT_THROW(
      parser().parse("optional_int32 = 1 AND optional_bool = false extra"),
      std::invalid_argument);
}

// =============================================================================
// STRING LITERAL HANDLING TESTS
// =============================================================================

TEST_F(AIPFilterTest, StringLiteralEscaping) {
    auto obj = create_test_object(
      1, 0, "string", false, "client\"with\"quotes");

    auto predicate = parser().parse("client_id = \"client\\\"with\\\"quotes\"");
    EXPECT_TRUE(predicate(obj));
}

TEST_F(AIPFilterTest, StringLiteralWithSpaces) {
    auto obj = create_test_object(1, 0, "string", false, "client with spaces");

    auto predicate = parser().parse("client_id = \"client with spaces\"");
    EXPECT_TRUE(predicate(obj));
}

TEST_F(AIPFilterTest, StringLiteralWithSpecialChars) {
    auto obj = create_test_object(1, 0, "string", false, "client@domain.com");

    auto predicate = parser().parse("client_id = \"client@domain.com\"");
    EXPECT_TRUE(predicate(obj));
}

// =============================================================================
// WHITESPACE HANDLING TESTS
// =============================================================================

TEST_F(AIPFilterTest, WhitespaceHandling) {
    auto obj = create_test_object(1, 0, "string", false);

    // All these should work the same
    EXPECT_TRUE(parser().parse("optional_int32=1")(obj));
    EXPECT_TRUE(parser().parse("optional_int32 = 1")(obj));
    EXPECT_TRUE(parser().parse("  optional_int32  =  1  ")(obj));
    EXPECT_TRUE(parser().parse("\toptional_int32\t=\t1\t")(obj));
    EXPECT_TRUE(parser().parse("\noptional_int32\n=\n1\n")(obj));

    // Multiple conditions with various whitespace
    EXPECT_TRUE(
      parser().parse("optional_int32=1 AND optional_bool=false")(obj));
    EXPECT_TRUE(
      parser().parse("  optional_int32  =  1  AND  optional_bool  =  false  ")(
        obj));
    EXPECT_THROW(
      parser().parse("optional_int32=1AND optional_bool=false"),
      std::invalid_argument);
}

// =============================================================================
// FIELD TYPE CONVERSION TESTS
// =============================================================================

TEST_F(AIPFilterTest, IntegerFieldTypes) {
    auto obj = create_test_object(
      1,
      2,
      "string",
      false,
      "client",
      "admin",
      true,
      "192.168.1.100",
      9092,
      100,
      50);

    // Test various integer fields
    EXPECT_TRUE(parser().parse("optional_int32 = 1")(obj));
    EXPECT_TRUE(parser().parse("optional_uint32 = 2")(obj));
    EXPECT_TRUE(parser().parse("source_info.port = 9092")(obj));
    EXPECT_TRUE(parser().parse("count_total = 100")(obj));
    EXPECT_TRUE(parser().parse("count_recent = 50")(obj));
}

// =============================================================================
// CASE SENSITIVITY TESTS
// =============================================================================

TEST_F(AIPFilterTest, CaseSensitiveFieldNames) {
    auto obj = create_test_object(1);

    // Field names should be case sensitive
    EXPECT_NO_THROW(parser().parse("optional_int32 = 1"));
    EXPECT_THROW(parser().parse("OPTIONAL_INT32 = 1"), std::invalid_argument);
    EXPECT_THROW(parser().parse("Optional_Int32 = 1"), std::invalid_argument);
}

TEST_F(AIPFilterTest, CaseInsensitiveLogicalOperators) {
    auto obj = create_test_object(1, 0, "string", false);

    // AND should be case insensitive
    EXPECT_TRUE(
      parser().parse("optional_int32 = 1 AND optional_bool = false")(obj));
    EXPECT_TRUE(
      parser().parse("optional_int32 = 1 and optional_bool = false")(obj));
    EXPECT_TRUE(
      parser().parse("optional_int32 = 1 And optional_bool = false")(obj));
    EXPECT_TRUE(
      parser().parse("optional_int32 = 1 aNd optional_bool = false")(obj));
}

TEST_F(AIPFilterTest, CaseInsensitiveBooleanLiterals) {
    auto obj_true = create_test_object(1, 0, "string", true);
    auto obj_false = create_test_object(1, 0, "string", false);

    // Boolean literals should be case insensitive
    EXPECT_TRUE(parser().parse("optional_bool = true")(obj_true));
    EXPECT_TRUE(parser().parse("optional_bool = TRUE")(obj_true));
    EXPECT_TRUE(parser().parse("optional_bool = True")(obj_true));
    EXPECT_TRUE(parser().parse("optional_bool = false")(obj_false));
    EXPECT_TRUE(parser().parse("optional_bool = FALSE")(obj_false));
    EXPECT_TRUE(parser().parse("optional_bool = False")(obj_false));
}

// =============================================================================
// ENUM SUPPORT TESTS
// =============================================================================

TEST_F(AIPFilterTest, EnumFieldBasicSupport) {
    auto obj = create_test_object();
    obj.set_optional_nested_enum(
      protobuf_test_messages::editions::test_all_types_edition2023_nested_enum::
        nested_enum_foo);
    obj.set_status_enum(
      protobuf_test_messages::editions::test_all_types_edition2023_status_enum::
        status_success);

    // Test enum field access with uppercase string values
    EXPECT_TRUE(
      parser().parse("optional_nested_enum = \"NESTED_ENUM_FOO\"")(obj));
    EXPECT_TRUE(parser().parse("status_enum = \"STATUS_SUCCESS\"")(obj));

    // Test negative cases
    EXPECT_FALSE(
      parser().parse("optional_nested_enum = \"NESTED_ENUM_BAR\"")(obj));
    EXPECT_FALSE(parser().parse("status_enum = \"STATUS_FAILURE\"")(obj));
}

TEST_F(AIPFilterTest, EnumFieldAllValues) {
    auto obj = create_test_object();

    // Test all NestedEnum values
    obj.set_optional_nested_enum(
      protobuf_test_messages::editions::test_all_types_edition2023_nested_enum::
        nested_enum_unspecified);
    EXPECT_TRUE(
      parser().parse("optional_nested_enum = \"NESTED_ENUM_UNSPECIFIED\"")(
        obj));

    obj.set_optional_nested_enum(
      protobuf_test_messages::editions::test_all_types_edition2023_nested_enum::
        nested_enum_foo);
    EXPECT_TRUE(
      parser().parse("optional_nested_enum = \"NESTED_ENUM_FOO\"")(obj));

    obj.set_optional_nested_enum(
      protobuf_test_messages::editions::test_all_types_edition2023_nested_enum::
        nested_enum_bar);
    EXPECT_TRUE(
      parser().parse("optional_nested_enum = \"NESTED_ENUM_BAR\"")(obj));

    obj.set_optional_nested_enum(
      protobuf_test_messages::editions::test_all_types_edition2023_nested_enum::
        nested_enum_baz);
    EXPECT_TRUE(
      parser().parse("optional_nested_enum = \"NESTED_ENUM_BAZ\"")(obj));

    // Test all StatusEnum values
    obj.set_status_enum(
      protobuf_test_messages::editions::test_all_types_edition2023_status_enum::
        status_unspecified);
    EXPECT_TRUE(parser().parse("status_enum = \"STATUS_UNSPECIFIED\"")(obj));

    obj.set_status_enum(
      protobuf_test_messages::editions::test_all_types_edition2023_status_enum::
        status_success);
    EXPECT_TRUE(parser().parse("status_enum = \"STATUS_SUCCESS\"")(obj));

    obj.set_status_enum(
      protobuf_test_messages::editions::test_all_types_edition2023_status_enum::
        status_failure);
    EXPECT_TRUE(parser().parse("status_enum = \"STATUS_FAILURE\"")(obj));

    obj.set_status_enum(
      protobuf_test_messages::editions::test_all_types_edition2023_status_enum::
        status_pending);
    EXPECT_TRUE(parser().parse("status_enum = \"STATUS_PENDING\"")(obj));
}

TEST_F(AIPFilterTest, EnumFieldCaseSensitivity) {
    auto obj = create_test_object();
    obj.set_status_enum(
      protobuf_test_messages::editions::test_all_types_edition2023_status_enum::
        status_success);

    // Enum values should be case-sensitive (AIP-160 requirement)
    EXPECT_TRUE(parser().parse("status_enum = \"STATUS_SUCCESS\"")(obj));

    // These should fail due to case sensitivity - they will parse successfully
    // but won't match at runtime
    auto predicate_wrong_case1 = parser().parse(
      "status_enum = \"status_success\""); // lowercase
    auto predicate_wrong_case2 = parser().parse(
      "status_enum = \"Status_Success\""); // mixed case
    auto predicate_wrong_case3 = parser().parse(
      "status_enum = \"StAtUs_SuCcEsS\""); // random case

    EXPECT_FALSE(predicate_wrong_case1(obj));
    EXPECT_FALSE(predicate_wrong_case2(obj));
    EXPECT_FALSE(predicate_wrong_case3(obj));
}

TEST_F(AIPFilterTest, EnumFieldInvalidFormatValues) {
    // Invalid enum formats should throw during parsing
    EXPECT_THROW(parser().parse("status_enum = \"\""), std::invalid_argument);

    // Values with invalid characters should throw during parsing
    EXPECT_THROW(
      parser().parse("status_enum = \"invalid-value\""), std::invalid_argument);
    EXPECT_THROW(
      parser().parse("status_enum = \"invalid value\""), std::invalid_argument);

    // Numeric values should be invalid (we expect string representation)
    EXPECT_THROW(parser().parse("status_enum = 2"), std::invalid_argument);
    EXPECT_THROW(
      parser().parse("optional_nested_enum = 2"), std::invalid_argument);
}

TEST_F(AIPFilterTest, EnumFieldInvalidEnumValues) {
    auto obj = create_test_object();
    obj.set_status_enum(
      protobuf_test_messages::editions::test_all_types_edition2023_status_enum::
        status_success);

    // These have valid format but are not valid enum values
    // They should parse successfully but fail at runtime (no match)
    auto predicate_invalid1 = parser().parse(
      "status_enum = \"INVALID_STATUS\"");
    auto predicate_invalid2 = parser().parse(
      "optional_nested_enum = \"INVALID_ENUM\"");

    // But they should not match at runtime
    EXPECT_FALSE(predicate_invalid1(obj));
    EXPECT_FALSE(predicate_invalid2(obj));
}

TEST_F(AIPFilterTest, EnumFieldComparisonOperators) {
    auto obj = create_test_object();
    obj.set_status_enum(
      protobuf_test_messages::editions::test_all_types_edition2023_status_enum::
        status_success);

    // Equality and inequality should work
    EXPECT_TRUE(parser().parse("status_enum = \"STATUS_SUCCESS\"")(obj));
    EXPECT_TRUE(parser().parse("status_enum != \"STATUS_FAILURE\"")(obj));

    // Other comparison operators should throw for enums
    EXPECT_THROW(
      parser().parse("status_enum > \"STATUS_FAILURE\""),
      std::invalid_argument);
    EXPECT_THROW(
      parser().parse("status_enum < \"STATUS_UNSPECIFIED\""),
      std::invalid_argument);
    EXPECT_THROW(
      parser().parse("status_enum >= \"STATUS_SUCCESS\""),
      std::invalid_argument);
    EXPECT_THROW(
      parser().parse("status_enum <= \"STATUS_SUCCESS\""),
      std::invalid_argument);
}

TEST_F(AIPFilterTest, EnumFieldLogicalOperations) {
    auto obj = create_test_object();
    obj.set_status_enum(
      protobuf_test_messages::editions::test_all_types_edition2023_status_enum::
        status_success);
    obj.set_optional_nested_enum(
      protobuf_test_messages::editions::test_all_types_edition2023_nested_enum::
        nested_enum_foo);

    // Test AND operations with enums
    EXPECT_TRUE(
      parser().parse(
        "status_enum = \"STATUS_SUCCESS\" AND optional_nested_enum = "
        "\"NESTED_ENUM_FOO\"")(obj));

    EXPECT_FALSE(
      parser().parse(
        "status_enum = \"STATUS_SUCCESS\" AND optional_nested_enum = "
        "\"NESTED_ENUM_BAR\"")(obj));

    // Test with mixed field types
    EXPECT_TRUE(
      parser().parse("optional_int32 = 1 AND status_enum = \"STATUS_SUCCESS\"")(
        obj));

    EXPECT_FALSE(
      parser().parse("optional_int32 = 2 AND status_enum = \"STATUS_SUCCESS\"")(
        obj));
}

TEST_F(AIPFilterTest, EnumFieldEdgeCases) {
    auto obj = create_test_object();

    // Test with unspecified values (default enum values)
    obj.set_status_enum(
      protobuf_test_messages::editions::test_all_types_edition2023_status_enum::
        status_unspecified);
    obj.set_optional_nested_enum(
      protobuf_test_messages::editions::test_all_types_edition2023_nested_enum::
        nested_enum_unspecified);

    EXPECT_TRUE(parser().parse("status_enum = \"STATUS_UNSPECIFIED\"")(obj));
    EXPECT_TRUE(
      parser().parse("optional_nested_enum = \"NESTED_ENUM_UNSPECIFIED\"")(
        obj));

    // Test inequality with unspecified
    EXPECT_TRUE(parser().parse("status_enum != \"STATUS_SUCCESS\"")(obj));
    EXPECT_TRUE(
      parser().parse("optional_nested_enum != \"NESTED_ENUM_FOO\"")(obj));
}

// =============================================================================
// PERFORMANCE AND STRESS TESTS
// =============================================================================

TEST_F(AIPFilterTest, EnumPerformanceWithManyConditions) {
    auto obj = create_test_object();
    obj.set_status_enum(
      protobuf_test_messages::editions::test_all_types_edition2023_status_enum::
        status_success);

    // Test many enum conditions
    std::string filter = "status_enum = \"STATUS_SUCCESS\"";
    for (int i = 0; i < 50; ++i) {
        filter += " AND status_enum = \"STATUS_SUCCESS\"";
    }

    auto predicate = parser().parse(filter);
    EXPECT_TRUE(predicate(obj));
}

TEST_F(AIPFilterTest, LargeIntegerValues) {
    auto obj = create_test_object();
    obj.set_count_total(9223372036854775807LL); // max int64_t

    auto predicate = parser().parse("count_total = 9223372036854775807");
    EXPECT_TRUE(predicate(obj));
}

TEST_F(AIPFilterTest, EmptyStringValues) {
    auto obj = create_test_object(1, 0, "", false, "");

    EXPECT_TRUE(parser().parse("optional_string = \"\"")(obj));
    EXPECT_TRUE(parser().parse("client_id = \"\"")(obj));
}

TEST_F(AIPFilterTest, VeryLongStringValues) {
    std::string long_string(1000, 'a');
    auto obj = create_test_object(1, 0, "string", false, long_string);

    auto predicate = parser().parse("client_id = \"" + long_string + "\"");
    EXPECT_TRUE(predicate(obj));
}

TEST_F(AIPFilterTest, ManyAndConditions) {
    auto obj = create_test_object(
      1, 0, "string", false, "client", "admin", true);

    std::string filter = "optional_int32 = 1";
    for (int i = 0; i < 100; ++i) {
        filter += " AND optional_int32 = 1";
    }

    auto predicate = parser().parse(filter);
    EXPECT_TRUE(predicate(obj));
}

TEST_F(AIPFilterTest, FilterReusability) {
    auto predicate = parser().parse(
      "optional_int32 = 1 AND optional_bool = false");

    // Test that the same predicate can be used multiple times
    auto obj1 = create_test_object(1, 0, "uid1", false);
    auto obj2 = create_test_object(1, 0, "uid2", false);
    auto obj3 = create_test_object(2, 0, "uid3", false);

    EXPECT_TRUE(predicate(obj1));
    EXPECT_TRUE(predicate(obj2));
    EXPECT_FALSE(predicate(obj3));
}

// =============================================================================
// RUNTIME ENUM VALIDATION BEHAVIOR TESTS
// =============================================================================

TEST_F(AIPFilterTest, EnumRuntimeValidationBehavior) {
    auto obj = create_test_object();
    obj.set_status_enum(
      protobuf_test_messages::editions::test_all_types_edition2023_status_enum::
        status_success);

    // Test that runtime validation works correctly
    // These should not match even though they parse successfully
    std::vector<std::string> invalid_but_well_formatted_values = {
      "NONEXISTENT_STATUS", "SOME_OTHER_VALUE", "DEFINITELY_NOT_AN_ENUM_VALUE"};

    for (const auto& invalid_value : invalid_but_well_formatted_values) {
        auto predicate = parser().parse(
          "status_enum = \"" + invalid_value + "\"");
        EXPECT_FALSE(predicate(obj))
          << "Should not match invalid enum value: " << invalid_value;
    }

    // But valid values should still work
    EXPECT_TRUE(parser().parse("status_enum = \"STATUS_SUCCESS\"")(obj));
    EXPECT_TRUE(parser().parse("status_enum != \"STATUS_FAILURE\"")(obj));
}

} // namespace redpanda::admin
