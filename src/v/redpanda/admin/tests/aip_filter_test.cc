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

// Simplified helper function with essential parameters
aip_filter_test::test_message create_test_message(
  int32_t int_val = 42,
  const std::string& str_val = "test",
  bool bool_val = true,
  const std::string& nested_name = "nested",
  int32_t nested_val = 100) {
    aip_filter_test::test_message msg;
    msg.set_int_field(int_val);
    msg.set_string_field(ss::sstring(str_val));
    msg.set_bool_field(bool_val);
    msg.set_uint_field(1000);

    msg.get_nested().set_name(ss::sstring(nested_name));
    msg.get_nested().set_value(nested_val);

    // Use the actual enum values from your codegen
    msg.set_status(aip_filter_test::test_message_status::status_active);
    msg.set_timestamp_field(absl::Now() - absl::Minutes(5));
    msg.set_duration_field(absl::Seconds(30));

    return msg;
}

class AIPFilterTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto registry = make_field_registry<aip_filter_test::test_message>();
        parser_
          = std::make_unique<AIPFilterParser<aip_filter_test::test_message>>(
            std::move(registry));
    }

    AIPFilterParser<aip_filter_test::test_message>& parser() {
        return *parser_;
    }

private:
    std::unique_ptr<AIPFilterParser<aip_filter_test::test_message>> parser_;
};

// =============================================================================
// BASIC FIELD TYPE OPERATIONS
// =============================================================================

TEST_F(AIPFilterTest, IntegerFieldOperations) {
    auto msg = create_test_message(5);

    // All comparison operators
    EXPECT_TRUE(parser().parse("int_field = 5")(msg));
    EXPECT_TRUE(parser().parse("int_field != 6")(msg));
    EXPECT_TRUE(parser().parse("int_field < 6")(msg));
    EXPECT_TRUE(parser().parse("int_field <= 5")(msg));
    EXPECT_TRUE(parser().parse("int_field > 4")(msg));
    EXPECT_TRUE(parser().parse("int_field >= 5")(msg));

    // Boundary conditions
    EXPECT_FALSE(parser().parse("int_field < 5")(msg));
    EXPECT_FALSE(parser().parse("int_field > 5")(msg));
    EXPECT_TRUE(parser().parse("int_field <= 5")(msg));
    EXPECT_TRUE(parser().parse("int_field >= 5")(msg));

    // Large values
    msg.set_uint_field(9223372036854775807ULL);
    EXPECT_TRUE(parser().parse("uint_field = 9223372036854775807")(msg));
}

TEST_F(AIPFilterTest, StringFieldOperations) {
    auto msg = create_test_message(1, "client-b");

    // All comparison operators
    EXPECT_TRUE(parser().parse("string_field = \"client-b\"")(msg));
    EXPECT_TRUE(parser().parse("string_field != \"client-a\"")(msg));
    EXPECT_TRUE(parser().parse("string_field > \"client-a\"")(msg));
    EXPECT_TRUE(parser().parse("string_field < \"client-c\"")(msg));
    EXPECT_TRUE(parser().parse("string_field >= \"client-b\"")(msg));
    EXPECT_TRUE(parser().parse("string_field <= \"client-b\"")(msg));

    // Special characters and escaping
    msg.set_string_field(ss::sstring("client\"with\"quotes"));
    EXPECT_TRUE(
      parser().parse("string_field = \"client\\\"with\\\"quotes\"")(msg));

    msg.set_string_field(ss::sstring("client with spaces"));
    EXPECT_TRUE(parser().parse("string_field = \"client with spaces\"")(msg));

    msg.set_string_field(ss::sstring("client@domain.com"));
    EXPECT_TRUE(parser().parse("string_field = \"client@domain.com\"")(msg));

    // Empty strings
    msg.set_string_field(ss::sstring(""));
    EXPECT_TRUE(parser().parse("string_field = \"\"")(msg));

    // Very long strings
    std::string long_string(1000, 'a');
    msg.set_string_field(ss::sstring(long_string));
    EXPECT_TRUE(parser().parse("string_field = \"" + long_string + "\"")(msg));
}

TEST_F(AIPFilterTest, BooleanFieldOperations) {
    auto msg_true = create_test_message(1, "test", true);
    auto msg_false = create_test_message(1, "test", false);

    // Only equality and inequality should work
    EXPECT_TRUE(parser().parse("bool_field = true")(msg_true));
    EXPECT_TRUE(parser().parse("bool_field = false")(msg_false));
    EXPECT_TRUE(parser().parse("bool_field != false")(msg_true));
    EXPECT_TRUE(parser().parse("bool_field != true")(msg_false));

    // Case insensitive boolean literals
    EXPECT_TRUE(parser().parse("bool_field = TRUE")(msg_true));
    EXPECT_TRUE(parser().parse("bool_field = True")(msg_true));
    EXPECT_TRUE(parser().parse("bool_field = FALSE")(msg_false));
    EXPECT_TRUE(parser().parse("bool_field = False")(msg_false));

    // Other operators should throw
    EXPECT_THROW(parser().parse("bool_field > true"), std::invalid_argument);
    EXPECT_THROW(parser().parse("bool_field < false"), std::invalid_argument);
    EXPECT_THROW(parser().parse("bool_field >= true"), std::invalid_argument);
    EXPECT_THROW(parser().parse("bool_field <= false"), std::invalid_argument);
}

// =============================================================================
// NESTED FIELDS AND LOGICAL OPERATIONS
// =============================================================================

TEST_F(AIPFilterTest, NestedFieldAccess) {
    auto msg = create_test_message(1, "test", true, "admin", 200);

    // Basic nested access
    EXPECT_TRUE(parser().parse("nested.name = \"admin\"")(msg));
    EXPECT_TRUE(parser().parse("nested.value = 200")(msg));

    // Mixed field types in logical operations
    EXPECT_TRUE(
      parser().parse("int_field = 1 AND nested.name = \"admin\"")(msg));
    EXPECT_TRUE(
      parser().parse("nested.value = 200 AND bool_field = true")(msg));
    EXPECT_FALSE(
      parser().parse("nested.value = 200 AND bool_field = false")(msg));
}

TEST_F(AIPFilterTest, LogicalOperators) {
    auto msg = create_test_message(1, "test", false);

    // Simple AND operations
    EXPECT_TRUE(parser().parse("int_field = 1 AND bool_field = false")(msg));
    EXPECT_FALSE(parser().parse("int_field = 1 AND bool_field = true")(msg));
    EXPECT_FALSE(parser().parse("int_field = 2 AND bool_field = false")(msg));

    // Multiple AND operations
    EXPECT_TRUE(
      parser().parse(
        "int_field = 1 AND bool_field = false AND string_field = \"test\"")(
        msg));
    EXPECT_FALSE(
      parser().parse(
        "int_field = 1 AND bool_field = false AND string_field = \"other\"")(
        msg));

    // Case insensitive logical operators
    EXPECT_TRUE(parser().parse("int_field = 1 AND bool_field = false")(msg));
    EXPECT_TRUE(parser().parse("int_field = 1 and bool_field = false")(msg));
    EXPECT_TRUE(parser().parse("int_field = 1 And bool_field = false")(msg));
    EXPECT_TRUE(parser().parse("int_field = 1 aNd bool_field = false")(msg));

    // Many conditions (stress test)
    std::string filter = "int_field = 1";
    for (int i = 0; i < 50; ++i) {
        filter += " AND int_field = 1";
    }
    EXPECT_TRUE(parser().parse(filter)(msg));
}

// =============================================================================
// ENUM SUPPORT
// =============================================================================

TEST_F(AIPFilterTest, EnumFieldOperations) {
    auto msg = create_test_message();

    // Basic enum operations
    msg.set_status(aip_filter_test::test_message_status::status_active);
    EXPECT_TRUE(parser().parse("status = \"STATUS_ACTIVE\"")(msg));
    EXPECT_FALSE(parser().parse("status = \"STATUS_INACTIVE\"")(msg));
    EXPECT_TRUE(parser().parse("status != \"STATUS_INACTIVE\"")(msg));

    // All enum values
    msg.set_status(aip_filter_test::test_message_status::status_unspecified);
    EXPECT_TRUE(parser().parse("status = \"STATUS_UNSPECIFIED\"")(msg));

    msg.set_status(aip_filter_test::test_message_status::status_inactive);
    EXPECT_TRUE(parser().parse("status = \"STATUS_INACTIVE\"")(msg));

    // Case sensitivity (should fail at runtime)
    msg.set_status(aip_filter_test::test_message_status::status_active);
    auto wrong_case = parser().parse("status = \"status_active\"");
    EXPECT_FALSE(wrong_case(msg));

    // Only equality operators should work
    EXPECT_THROW(
      parser().parse("status > \"STATUS_ACTIVE\""), std::invalid_argument);
    EXPECT_THROW(
      parser().parse("status < \"STATUS_INACTIVE\""), std::invalid_argument);
    EXPECT_THROW(
      parser().parse("status >= \"STATUS_ACTIVE\""), std::invalid_argument);
    EXPECT_THROW(
      parser().parse("status <= \"STATUS_ACTIVE\""), std::invalid_argument);

    // Enum in logical operations
    EXPECT_TRUE(
      parser().parse("status = \"STATUS_ACTIVE\" AND int_field = 42")(msg));
    EXPECT_FALSE(
      parser().parse("status = \"STATUS_INACTIVE\" AND int_field = 42")(msg));

    // Invalid enum values (should not match)
    auto invalid_enum = parser().parse("status = \"INVALID_STATUS\"");
    EXPECT_FALSE(invalid_enum(msg));
}

// =============================================================================
// AIP-160 TIME FIELD COMPLIANCE
// =============================================================================

TEST_F(AIPFilterTest, DurationFieldCompliance) {
    auto msg = create_test_message();

    // Basic duration formats
    msg.set_duration_field(absl::Seconds(20));
    EXPECT_TRUE(parser().parse("duration_field = 20s")(msg));

    msg.set_duration_field(absl::Milliseconds(1200)); // 1.2 seconds
    EXPECT_TRUE(parser().parse("duration_field = 1.2s")(msg));

    // Extended units (beyond AIP-160 but supported by absl)
    msg.set_duration_field(absl::Minutes(5));
    EXPECT_TRUE(parser().parse("duration_field = 5m")(msg));

    msg.set_duration_field(absl::Hours(1));
    EXPECT_TRUE(parser().parse("duration_field = 1h")(msg));

    // Edge cases
    msg.set_duration_field(absl::Milliseconds(500)); // 0.5 seconds
    EXPECT_TRUE(parser().parse("duration_field = 0.5s")(msg));

    msg.set_duration_field(absl::ZeroDuration());
    EXPECT_TRUE(parser().parse("duration_field = 0s")(msg));

    msg.set_duration_field(absl::Milliseconds(1)); // 0.001 seconds
    EXPECT_TRUE(parser().parse("duration_field = \"0.001s\"")(msg));
}

TEST_F(AIPFilterTest, TimestampFieldCompliance) {
    auto msg = create_test_message();
    absl::Time test_time;
    std::string error;

    // RFC-3339 formats
    ASSERT_TRUE(
      absl::ParseTime(
        absl::RFC3339_full, "2012-04-21T11:30:00Z", &test_time, &error));
    msg.set_timestamp_field(std::move(test_time));
    EXPECT_TRUE(
      parser().parse("timestamp_field = \"2012-04-21T11:30:00Z\"")(msg));

    // Timezone with negative offset
    ASSERT_TRUE(
      absl::ParseTime(
        absl::RFC3339_full, "2012-04-21T11:30:00-04:00", &test_time, &error));
    msg.set_timestamp_field(std::move(test_time));
    EXPECT_TRUE(
      parser().parse("timestamp_field = \"2012-04-21T11:30:00-04:00\"")(msg));

    // Timezone with positive offset
    ASSERT_TRUE(
      absl::ParseTime(
        absl::RFC3339_full, "2012-04-21T11:30:00+05:30", &test_time, &error));
    msg.set_timestamp_field(std::move(test_time));
    EXPECT_TRUE(
      parser().parse("timestamp_field = \"2012-04-21T11:30:00+05:30\"")(msg));

    // Timezone equivalence
    absl::Time utc_time, offset_time;
    ASSERT_TRUE(
      absl::ParseTime(
        absl::RFC3339_full, "2012-04-21T15:30:00Z", &utc_time, &error));
    ASSERT_TRUE(
      absl::ParseTime(
        absl::RFC3339_full, "2012-04-21T11:30:00-04:00", &offset_time, &error));
    EXPECT_EQ(absl::ToUnixSeconds(utc_time), absl::ToUnixSeconds(offset_time));

    msg.set_timestamp_field(std::move(utc_time));
    EXPECT_TRUE(
      parser().parse("timestamp_field = \"2012-04-21T15:30:00Z\"")(msg));
    EXPECT_TRUE(
      parser().parse("timestamp_field = \"2012-04-21T11:30:00-04:00\"")(msg));
}

// =============================================================================
// ERROR HANDLING AND VALIDATION
// =============================================================================

TEST_F(AIPFilterTest, ParsingAndValidationErrors) {
    // Empty filter should always match
    EXPECT_TRUE(parser().parse("")(create_test_message()));

    // Validation method
    EXPECT_TRUE(parser().validate("int_field = 1"));
    EXPECT_TRUE(parser().validate("int_field = 1 AND bool_field = false"));
    EXPECT_TRUE(parser().validate(""));
    EXPECT_FALSE(parser().validate("invalid_field = 1"));
    EXPECT_FALSE(parser().validate("int_field ="));
    EXPECT_FALSE(parser().validate("= 1"));
    EXPECT_FALSE(parser().validate("int_field 1"));

    // Unknown field errors
    EXPECT_THROW(parser().parse("unknown_field = 1"), std::invalid_argument);
    EXPECT_THROW(
      parser().parse("int_field = 1 AND unknown_field = 2"),
      std::invalid_argument);

    // Malformed expressions
    EXPECT_THROW(parser().parse("int_field ="), std::invalid_argument);
    EXPECT_THROW(parser().parse("= 1"), std::invalid_argument);
    EXPECT_THROW(parser().parse("int_field 1"), std::invalid_argument);
    EXPECT_THROW(parser().parse("int_field = 1 AND"), std::invalid_argument);
    EXPECT_THROW(parser().parse("int_field = 1 ="), std::invalid_argument);

    // Type mismatches
    EXPECT_THROW(
      parser().parse("int_field = \"not_a_number\""), std::invalid_argument);
    EXPECT_THROW(
      parser().parse("bool_field = \"not_a_boolean\""), std::invalid_argument);
    EXPECT_THROW(
      parser().parse("int_field = 9223372036854775808"),
      std::invalid_argument); // Overflow

    // String literal errors
    EXPECT_THROW(
      parser().parse("string_field = \"unterminated"), std::invalid_argument);
    EXPECT_THROW(parser().parse("string_field = \""), std::invalid_argument);

    // Trailing characters
    EXPECT_THROW(parser().parse("int_field = 1 extra"), std::invalid_argument);
    EXPECT_THROW(
      parser().parse("int_field = 1 AND bool_field = false extra"),
      std::invalid_argument);

    // Invalid time formats
    EXPECT_THROW(
      parser().parse("duration_field = invalid"), std::invalid_argument);
    EXPECT_THROW(
      parser().parse("duration_field = 120"),
      std::invalid_argument); // Missing unit
    EXPECT_THROW(
      parser().parse("duration_field = s"), std::invalid_argument); // No number

    EXPECT_THROW(
      parser().parse("timestamp_field = \"invalid-timestamp\""),
      std::invalid_argument);
    EXPECT_THROW(
      parser().parse("timestamp_field = \"2012-04-21 11:30:00\""),
      std::invalid_argument); // Missing T
    EXPECT_THROW(
      parser().parse("timestamp_field = \"2012-04-21T25:30:00Z\""),
      std::invalid_argument); // Invalid hour
    EXPECT_THROW(
      parser().parse("timestamp_field = \"2012-13-21T11:30:00Z\""),
      std::invalid_argument); // Invalid month

    // Invalid enum formats
    EXPECT_THROW(parser().parse("status = \"\""), std::invalid_argument);
    EXPECT_THROW(
      parser().parse("status = \"invalid-value\""), std::invalid_argument);
    EXPECT_THROW(
      parser().parse("status = \"invalid value\""), std::invalid_argument);
    EXPECT_THROW(parser().parse("status = 2"), std::invalid_argument);
}

// =============================================================================
// WHITESPACE AND SYNTAX FLEXIBILITY
// =============================================================================

TEST_F(AIPFilterTest, WhitespaceAndSyntaxHandling) {
    auto msg = create_test_message(1, "test", false);

    // Whitespace variations
    EXPECT_TRUE(parser().parse("int_field=1")(msg));
    EXPECT_TRUE(parser().parse("int_field = 1")(msg));
    EXPECT_TRUE(parser().parse("  int_field  =  1  ")(msg));
    EXPECT_TRUE(parser().parse("\tint_field\t=\t1\t")(msg));
    EXPECT_TRUE(parser().parse("\nint_field\n=\n1\n")(msg));

    // Multiple conditions with whitespace
    EXPECT_TRUE(parser().parse("int_field=1 AND bool_field=false")(msg));
    EXPECT_TRUE(
      parser().parse("  int_field  =  1  AND  bool_field  =  false  ")(msg));

    // Missing space should cause error
    EXPECT_THROW(
      parser().parse("int_field=1AND bool_field=false"), std::invalid_argument);

    // Case sensitivity for field names
    EXPECT_NO_THROW(parser().parse("int_field = 1"));
    EXPECT_THROW(parser().parse("INT_FIELD = 1"), std::invalid_argument);
    EXPECT_THROW(parser().parse("Int_Field = 1"), std::invalid_argument);
}

// =============================================================================
// PREDICATE REUSABILITY AND PERFORMANCE
// =============================================================================

TEST_F(AIPFilterTest, PredicateReusabilityAndPerformance) {
    auto predicate = parser().parse("int_field = 1 AND bool_field = false");

    // Same predicate can be used multiple times
    auto msg1 = create_test_message(1, "uid1", false);
    auto msg2 = create_test_message(1, "uid2", false);
    auto msg3 = create_test_message(2, "uid3", false);

    EXPECT_TRUE(predicate(msg1));
    EXPECT_TRUE(predicate(msg2));
    EXPECT_FALSE(predicate(msg3));

    // Complex filter should still work efficiently
    std::string complex_filter
      = "int_field = 1 AND bool_field = false AND string_field = \"test\"";
    for (int i = 0; i < 20; ++i) {
        complex_filter += " AND int_field = 1";
    }
    auto complex_predicate = parser().parse(complex_filter);
    auto test_msg = create_test_message(1, "test", false);
    EXPECT_TRUE(complex_predicate(test_msg));
}

} // namespace redpanda::admin
