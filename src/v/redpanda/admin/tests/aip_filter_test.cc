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
#include "proto/redpanda/core/admin/v2/kafka_connections.proto.h"
#include "redpanda/admin/aip_filter.h"
#include "redpanda/admin/field_registry.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

namespace redpanda::admin {

// Helper function to create a test kafka_connection with various field values
proto::admin::kafka_connection create_test_connection(
  int32_t node_id = 1,
  uint32_t shard_id = 0,
  const std::string& uid = "test-uid",
  bool aborting = false,
  const std::string& client_id = "test-client",
  const std::string& user_principal = "admin",
  bool tls_enabled = true,
  const std::string& ip_address = "192.168.1.100",
  uint32_t port = 9092,
  uint64_t produce_count = 100,
  uint64_t fetch_count = 50) {
    proto::admin::kafka_connection conn;
    conn.set_node_id(node_id);
    conn.set_shard_id(shard_id);
    conn.set_uid(ss::sstring(uid));
    conn.set_aborting(aborting);
    conn.set_client_id(ss::sstring(client_id));

    conn.get_authentication_info().set_user_principal(
      ss::sstring(user_principal));
    conn.get_authentication_info().set_state(
      proto::admin::authentication_state::success);
    conn.get_authentication_info().set_mechanism(
      proto::admin::authentication_mechanism::sasl_scram);

    conn.get_tls_info().set_enabled(tls_enabled);

    conn.get_source().set_ip_address(ss::sstring(ip_address));
    conn.get_source().set_port(port);

    conn.set_produce_batch_record_count_total(produce_count);
    conn.set_fetch_tput_total(fetch_count);

    // Set time-based fields
    auto now = absl::Now();
    conn.set_open_time(now - absl::Minutes(30));
    conn.set_close_time(absl::Time{}); // Not closed
    conn.set_idle_duration(absl::Seconds(120));

    return conn;
}

// kafka_connection-specific registry creation using manual approach
auto create_kafka_connection_field_registry() {
    using proto::admin::kafka_connection;

    auto builder = ProtobufFieldRegistryBuilder<kafka_connection>{};
    builder
      // Basic scalar fields
      .addInt64Field(
        "node_id",
        [](const kafka_connection& c) {
            return static_cast<int64_t>(c.get_node_id());
        })
      .addInt64Field(
        "shard_id",
        [](const kafka_connection& c) {
            return static_cast<int64_t>(c.get_shard_id());
        })
      .addStringField(
        "uid",
        [](const kafka_connection& c) { return std::string(c.get_uid()); })
      .addBoolField(
        "aborting", [](const kafka_connection& c) { return c.get_aborting(); })
      .addStringField(
        "listener_name",
        [](const kafka_connection& c) {
            return std::string(c.get_listener_name());
        })
      .addStringField(
        "client_id",
        [](const kafka_connection& c) {
            return std::string(c.get_client_id());
        })
      .addStringField(
        "client_software_name",
        [](const kafka_connection& c) {
            return std::string(c.get_client_software_name());
        })
      .addStringField(
        "client_software_version",
        [](const kafka_connection& c) {
            return std::string(c.get_client_software_version());
        })
      .addStringField(
        "transactional_id",
        [](const kafka_connection& c) {
            return std::string(c.get_transactional_id());
        })
      .addStringField(
        "group_id",
        [](const kafka_connection& c) { return std::string(c.get_group_id()); })
      .addStringField(
        "group_instance_id",
        [](const kafka_connection& c) {
            return std::string(c.get_group_instance_id());
        })
      .addStringField(
        "group_member_id",
        [](const kafka_connection& c) {
            return std::string(c.get_group_member_id());
        })

      // Throughput and count fields
      .addInt64Field(
        "produce_tput_total",
        [](const kafka_connection& c) {
            return static_cast<int64_t>(c.get_produce_tput_total());
        })
      .addInt64Field(
        "produce_tput_last_1min",
        [](const kafka_connection& c) {
            return static_cast<int64_t>(c.get_produce_tput_last_1min());
        })
      .addInt64Field(
        "fetch_tput_total",
        [](const kafka_connection& c) {
            return static_cast<int64_t>(c.get_fetch_tput_total());
        })
      .addInt64Field(
        "fetch_tput_last_1min",
        [](const kafka_connection& c) {
            return static_cast<int64_t>(c.get_fetch_tput_last_1min());
        })
      .addInt64Field(
        "request_count_total",
        [](const kafka_connection& c) {
            return static_cast<int64_t>(c.get_request_count_total());
        })
      .addInt64Field(
        "request_count_last_1min",
        [](const kafka_connection& c) {
            return static_cast<int64_t>(c.get_request_count_last_1min());
        })
      .addInt64Field(
        "produce_batch_record_bytes_total",
        [](const kafka_connection& c) {
            return static_cast<int64_t>(
              c.get_produce_batch_record_bytes_total());
        })
      .addInt64Field(
        "produce_batch_record_count_total",
        [](const kafka_connection& c) {
            return static_cast<int64_t>(
              c.get_produce_batch_record_count_total());
        })

      // Legacy field alias for backward compatibility
      .addInt64Field(
        "field1",
        [](const kafka_connection& c) {
            return static_cast<int64_t>(
              c.get_produce_batch_record_count_total());
        })

      // Nested authentication_info fields
      .addStringField(
        "authentication_info.user_principal",
        [](const kafka_connection& c) {
            return std::string(
              c.get_authentication_info().get_user_principal());
        })
      .addEnumField(
        "authentication_info.state",
        [](const kafka_connection& c) -> std::string {
            auto state = c.get_authentication_info().get_state();
            return std::string(proto::admin::enum_to_string(state));
        })
      .addEnumField(
        "authentication_info.mechanism",
        [](const kafka_connection& c) -> std::string {
            auto mechanism = c.get_authentication_info().get_mechanism();
            return std::string(proto::admin::enum_to_string(mechanism));
        })

      // Nested tls_info fields
      .addBoolField(
        "tls_info.enabled",
        [](const kafka_connection& c) {
            return c.get_tls_info().get_enabled();
        })

      // Nested source fields
      .addStringField(
        "source.ip_address",
        [](const kafka_connection& c) {
            return std::string(c.get_source().get_ip_address());
        })
      .addInt64Field(
        "source.port",
        [](const kafka_connection& c) {
            return static_cast<int64_t>(c.get_source().get_port());
        })

      .addDurationField(
        "idle_duration",
        [](const kafka_connection& c) { return c.get_idle_duration(); })
      .addTimestampField(
        "open_time",
        [](const kafka_connection& c) { return c.get_open_time(); })
      .addTimestampField("close_time", [](const kafka_connection& c) {
          return c.get_close_time();
      });

    return std::move(builder).build();
}

class KafkaConnectionFilterTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create the manual registry and parser in SetUp()
        auto manual_registry = std::make_unique<
          ProtobufFieldRegistry<proto::admin::kafka_connection>>(
          create_kafka_connection_field_registry());
        parser_
          = std::make_unique<AIPFilterParser<proto::admin::kafka_connection>>(
            std::move(manual_registry));
    }

private:
    std::unique_ptr<AIPFilterParser<proto::admin::kafka_connection>> parser_;

protected:
    // Provide access to the parser for tests
    AIPFilterParser<proto::admin::kafka_connection>& parser() {
        return *parser_;
    }
};

class KafkaConnectionAutoFilterTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create the automatic registry and parser in SetUp()
        auto auto_registry
          = make_auto_field_registry<proto::admin::kafka_connection>();
        auto_parser_
          = std::make_unique<AIPFilterParser<proto::admin::kafka_connection>>(
            std::move(auto_registry));
    }

private:
    std::unique_ptr<AIPFilterParser<proto::admin::kafka_connection>>
      auto_parser_;

protected:
    // Provide access to the automatic parser for tests
    AIPFilterParser<proto::admin::kafka_connection>& auto_parser() {
        return *auto_parser_;
    }
};

class KafkaConnectionUnifiedFilterTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Test both registry types through the same interface
        manual_registry_
          = make_manual_field_registry<proto::admin::kafka_connection>(
            create_kafka_connection_field_registry().get_accessors());
        auto_registry_
          = make_auto_field_registry<proto::admin::kafka_connection>();

        manual_parser_
          = std::make_unique<AIPFilterParser<proto::admin::kafka_connection>>(
            *manual_registry_);
        auto_parser_
          = std::make_unique<AIPFilterParser<proto::admin::kafka_connection>>(
            *auto_registry_);
    }

private:
    std::unique_ptr<IProtobufFieldRegistry<proto::admin::kafka_connection>>
      manual_registry_;
    std::unique_ptr<IProtobufFieldRegistry<proto::admin::kafka_connection>>
      auto_registry_;
    std::unique_ptr<AIPFilterParser<proto::admin::kafka_connection>>
      manual_parser_;
    std::unique_ptr<AIPFilterParser<proto::admin::kafka_connection>>
      auto_parser_;

protected:
    IProtobufFieldRegistry<proto::admin::kafka_connection>& manual_registry() {
        return *manual_registry_;
    }

    IProtobufFieldRegistry<proto::admin::kafka_connection>& auto_registry() {
        return *auto_registry_;
    }

    AIPFilterParser<proto::admin::kafka_connection>& manual_parser() {
        return *manual_parser_;
    }

    AIPFilterParser<proto::admin::kafka_connection>& auto_parser() {
        return *auto_parser_;
    }
};

// =============================================================================
// BASIC FUNCTIONALITY TESTS
// =============================================================================

TEST_F(KafkaConnectionFilterTest, EmptyFilterMatchesAll) {
    auto predicate = parser().parse("");
    auto conn = create_test_connection();
    EXPECT_TRUE(predicate(conn));
}

TEST_F(KafkaConnectionFilterTest, SimpleIntegerEquality) {
    auto predicate = parser().parse("node_id = 1");

    auto conn1 = create_test_connection(1);
    auto conn2 = create_test_connection(2);

    EXPECT_TRUE(predicate(conn1));
    EXPECT_FALSE(predicate(conn2));
}

TEST_F(KafkaConnectionFilterTest, SimpleStringEquality) {
    auto predicate = parser().parse("client_id = \"test-client\"");

    auto conn1 = create_test_connection(1, 0, "uid", false, "test-client");
    auto conn2 = create_test_connection(1, 0, "uid", false, "other-client");

    EXPECT_TRUE(predicate(conn1));
    EXPECT_FALSE(predicate(conn2));
}

TEST_F(KafkaConnectionFilterTest, SimpleBooleanEquality) {
    auto predicate = parser().parse("aborting = true");

    auto conn1 = create_test_connection(1, 0, "uid", true);
    auto conn2 = create_test_connection(1, 0, "uid", false);

    EXPECT_TRUE(predicate(conn1));
    EXPECT_FALSE(predicate(conn2));
}

// =============================================================================
// COMPARISON OPERATORS TESTS
// =============================================================================

TEST_F(KafkaConnectionFilterTest, AllComparisonOperators) {
    using proto::admin::kafka_connection;

    auto conn = create_test_connection(
      5, 0, "uid", false, "client", "admin", true, "192.168.1.100", 9092, 100);

    // Test all operators with integers
    EXPECT_TRUE(parser().parse("node_id = 5")(conn));
    EXPECT_TRUE(parser().parse("node_id != 6")(conn));
    EXPECT_TRUE(parser().parse("node_id < 6")(conn));
    EXPECT_TRUE(parser().parse("node_id <= 5")(conn));
    EXPECT_TRUE(parser().parse("node_id > 4")(conn));
    EXPECT_TRUE(parser().parse("node_id >= 5")(conn));

    // Test boundary conditions
    EXPECT_FALSE(parser().parse("node_id < 5")(conn));
    EXPECT_FALSE(parser().parse("node_id > 5")(conn));
    EXPECT_TRUE(parser().parse("node_id <= 5")(conn));
    EXPECT_TRUE(parser().parse("node_id >= 5")(conn));
}

TEST_F(KafkaConnectionFilterTest, StringComparisons) {
    auto conn = create_test_connection(1, 0, "uid", false, "client-b");

    EXPECT_TRUE(parser().parse("client_id = \"client-b\"")(conn));
    EXPECT_TRUE(parser().parse("client_id != \"client-a\"")(conn));
    EXPECT_TRUE(parser().parse("client_id > \"client-a\"")(conn));
    EXPECT_TRUE(parser().parse("client_id < \"client-c\"")(conn));
    EXPECT_TRUE(parser().parse("client_id >= \"client-b\"")(conn));
    EXPECT_TRUE(parser().parse("client_id <= \"client-b\"")(conn));
}

TEST_F(KafkaConnectionFilterTest, BooleanComparisons) {
    auto conn_true = create_test_connection(1, 0, "uid", true);
    auto conn_false = create_test_connection(1, 0, "uid", false);

    // Only = and != should work for booleans
    EXPECT_TRUE(parser().parse("aborting = true")(conn_true));
    EXPECT_TRUE(parser().parse("aborting = false")(conn_false));
    EXPECT_TRUE(parser().parse("aborting != false")(conn_true));
    EXPECT_TRUE(parser().parse("aborting != true")(conn_false));

    // Other operators should throw
    EXPECT_THROW(parser().parse("aborting > true"), std::invalid_argument);
    EXPECT_THROW(parser().parse("aborting < false"), std::invalid_argument);
}

// =============================================================================
// NESTED FIELD TESTS
// =============================================================================

TEST_F(KafkaConnectionFilterTest, NestedFieldAccess) {
    auto conn = create_test_connection(
      1, 0, "uid", false, "client", "admin", true, "192.168.1.100", 9092);

    EXPECT_TRUE(
      parser().parse("authentication_info.user_principal = \"admin\"")(conn));
    EXPECT_TRUE(parser().parse("tls_info.enabled = true")(conn));
    EXPECT_TRUE(parser().parse("source.ip_address = \"192.168.1.100\"")(conn));
    EXPECT_TRUE(parser().parse("source.port = 9092")(conn));
}

TEST_F(KafkaConnectionFilterTest, DeepNestedFieldAccess) {
    auto conn = create_test_connection();
    conn.get_authentication_info().set_state(
      proto::admin::authentication_state::success);
    conn.get_authentication_info().set_mechanism(
      proto::admin::authentication_mechanism::sasl_scram);

    // FIXED: Use uppercase string representations for enum values
    EXPECT_TRUE(
      parser().parse(
        "authentication_info.state = \"AUTHENTICATION_STATE_SUCCESS\"")(conn));
    EXPECT_TRUE(
      parser().parse(
        "authentication_info.mechanism = "
        "\"AUTHENTICATION_MECHANISM_SASL_SCRAM\"")(conn));
}

// =============================================================================
// LOGICAL OPERATORS TESTS
// =============================================================================

TEST_F(KafkaConnectionFilterTest, SimpleAndOperation) {
    auto predicate = parser().parse("node_id = 1 AND aborting = false");

    auto conn1 = create_test_connection(1, 0, "uid", false);
    auto conn2 = create_test_connection(1, 0, "uid", true);
    auto conn3 = create_test_connection(2, 0, "uid", false);

    EXPECT_TRUE(predicate(conn1));
    EXPECT_FALSE(predicate(conn2));
    EXPECT_FALSE(predicate(conn3));
}

TEST_F(KafkaConnectionFilterTest, MultipleAndOperations) {
    auto predicate = parser().parse(
      "node_id = 1 AND aborting = false AND tls_info.enabled = true");

    auto conn1 = create_test_connection(
      1, 0, "uid", false, "client", "admin", true);
    auto conn2 = create_test_connection(
      1, 0, "uid", false, "client", "admin", false);

    EXPECT_TRUE(predicate(conn1));
    EXPECT_FALSE(predicate(conn2));
}

TEST_F(KafkaConnectionFilterTest, ComplexAndWithNestedFields) {
    auto predicate = parser().parse(
      "node_id = 1 AND authentication_info.user_principal = \"admin\" AND "
      "source.port = 9092");

    auto conn = create_test_connection(
      1, 0, "uid", false, "client", "admin", true, "192.168.1.100", 9092);
    EXPECT_TRUE(predicate(conn));

    // Change one field to make it fail
    conn.get_source().set_port(9093);
    EXPECT_FALSE(predicate(conn));
}

// =============================================================================
// AIP-160 DURATION COMPLIANCE TESTS
// =============================================================================

TEST_F(KafkaConnectionFilterTest, AIP160DurationCompliance) {
    auto conn = create_test_connection();

    // Test various duration formats that absl::ParseDuration supports
    conn.set_idle_duration(absl::Seconds(20));
    EXPECT_TRUE(parser().parse("idle_duration = 20s")(conn));

    conn.set_idle_duration(absl::Milliseconds(1200)); // 1.2 seconds
    EXPECT_TRUE(parser().parse("idle_duration = 1.2s")(conn));

    // Test that minutes, hours etc. work (beyond AIP-160 spec but supported by
    // absl)
    conn.set_idle_duration(absl::Minutes(5)); // 300 seconds
    EXPECT_TRUE(parser().parse("idle_duration = 5m")(conn));

    conn.set_idle_duration(absl::Hours(1)); // 3600 seconds
    EXPECT_TRUE(parser().parse("idle_duration = 1h")(conn));
}

TEST_F(KafkaConnectionFilterTest, AIP160DurationEdgeCases) {
    auto conn = create_test_connection();

    // Test fractional seconds
    conn.set_idle_duration(absl::Milliseconds(500)); // 0.5 seconds
    EXPECT_TRUE(parser().parse("idle_duration = 0.5s")(conn));

    // Test zero duration
    conn.set_idle_duration(absl::ZeroDuration());
    EXPECT_TRUE(parser().parse("idle_duration = 0s")(conn));

    // Test very small durations
    conn.set_idle_duration(absl::Milliseconds(1)); // 0.001 seconds
    EXPECT_TRUE(parser().parse("idle_duration = 0.001s")(conn));
}

// =============================================================================
// AIP-160 TIMESTAMP COMPLIANCE TESTS
// =============================================================================

TEST_F(KafkaConnectionFilterTest, AIP160TimestampCompliance) {
    auto conn = create_test_connection();

    // Test basic RFC-3339 formats
    absl::Time test_time;
    std::string error;

    // UTC timezone
    ASSERT_TRUE(
      absl::ParseTime(
        absl::RFC3339_full, "2012-04-21T11:30:00Z", &test_time, &error));
    conn.set_open_time(absl::Time{test_time});
    EXPECT_TRUE(parser().parse("open_time = \"2012-04-21T11:30:00Z\"")(conn));

    // Timezone with offset
    ASSERT_TRUE(
      absl::ParseTime(
        absl::RFC3339_full, "2012-04-21T11:30:00-04:00", &test_time, &error));
    conn.set_open_time(absl::Time{test_time});
    EXPECT_TRUE(
      parser().parse("open_time = \"2012-04-21T11:30:00-04:00\"")(conn));

    // Positive timezone offset
    ASSERT_TRUE(
      absl::ParseTime(
        absl::RFC3339_full, "2012-04-21T11:30:00+05:30", &test_time, &error));
    conn.set_open_time(absl::Time{test_time});
    EXPECT_TRUE(
      parser().parse("open_time = \"2012-04-21T11:30:00+05:30\"")(conn));
}

TEST_F(KafkaConnectionFilterTest, AIP160TimestampWithFractions) {
    auto conn = create_test_connection();

    // Test fractional seconds (note: precision limited to seconds in our field
    // accessors)
    absl::Time test_time;
    std::string error;
    ASSERT_TRUE(
      absl::ParseTime(
        absl::RFC3339_full, "2012-04-21T11:30:00.123Z", &test_time, &error));

    // Convert to second precision for comparison
    auto unix_seconds = absl::ToUnixSeconds(test_time);
    auto truncated_time = absl::FromUnixSeconds(unix_seconds);
    conn.set_open_time(absl::Time{truncated_time});

    // Should match the truncated version
    EXPECT_TRUE(parser().parse("open_time = \"2012-04-21T11:30:00Z\"")(conn));
}

TEST_F(KafkaConnectionFilterTest, TimezoneEquivalence) {
    auto conn = create_test_connection();

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

    conn.set_open_time(absl::Time{utc_time});
    EXPECT_TRUE(parser().parse("open_time = \"2012-04-21T15:30:00Z\"")(conn));
    EXPECT_TRUE(
      parser().parse("open_time = \"2012-04-21T11:30:00-04:00\"")(conn));
}

// =============================================================================
// ERROR HANDLING WITH ABSL
// =============================================================================

TEST_F(KafkaConnectionFilterTest, AbslDurationErrorHandling) {
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

TEST_F(KafkaConnectionFilterTest, AbslTimestampErrorHandling) {
    // Test invalid timestamp formats that absl will reject
    EXPECT_THROW(
      parser().parse("open_time = \"invalid-timestamp\""),
      std::invalid_argument);
    EXPECT_THROW(
      parser().parse("open_time = \"2012-04-21 11:30:00\""),
      std::invalid_argument); // Missing T
    EXPECT_THROW(
      parser().parse("open_time = \"2012-04-21T25:30:00Z\""),
      std::invalid_argument); // Invalid hour
    EXPECT_THROW(
      parser().parse("open_time = \"2012-13-21T11:30:00Z\""),
      std::invalid_argument); // Invalid month
}

// =============================================================================
// VALIDATION AND ERROR HANDLING TESTS
// =============================================================================

TEST_F(KafkaConnectionFilterTest, ValidationMethod) {
    EXPECT_TRUE(parser().validate("node_id = 1"));
    EXPECT_TRUE(parser().validate("node_id = 1 AND aborting = false"));
    EXPECT_TRUE(parser().validate(""));

    EXPECT_FALSE(parser().validate("invalid_field = 1"));
    EXPECT_FALSE(parser().validate("node_id ="));
    EXPECT_FALSE(parser().validate("= 1"));
    EXPECT_FALSE(parser().validate("node_id 1"));
}

TEST_F(KafkaConnectionFilterTest, UnknownFieldError) {
    EXPECT_THROW(parser().parse("unknown_field = 1"), std::invalid_argument);
    EXPECT_THROW(
      parser().parse("node_id = 1 AND unknown_field = 2"),
      std::invalid_argument);
}

TEST_F(KafkaConnectionFilterTest, MalformedExpressionErrors) {
    EXPECT_THROW(parser().parse("node_id ="), std::invalid_argument);
    EXPECT_THROW(parser().parse("= 1"), std::invalid_argument);
    EXPECT_THROW(parser().parse("node_id 1"), std::invalid_argument);
    EXPECT_THROW(parser().parse("node_id = 1 AND"), std::invalid_argument);
    EXPECT_THROW(parser().parse("node_id = 1 ="), std::invalid_argument);
}

TEST_F(KafkaConnectionFilterTest, InvalidLiteralValues) {
    EXPECT_THROW(
      parser().parse("node_id = \"not_a_number\""), std::invalid_argument);
    EXPECT_THROW(
      parser().parse("aborting = \"not_a_boolean\""), std::invalid_argument);
    EXPECT_THROW(
      parser().parse("node_id = 9223372036854775808"),
      std::invalid_argument); // Overflow
}

TEST_F(KafkaConnectionFilterTest, UnterminatedStringLiteral) {
    EXPECT_THROW(
      parser().parse("client_id = \"unterminated"), std::invalid_argument);
    EXPECT_THROW(parser().parse("client_id = \""), std::invalid_argument);
}

TEST_F(KafkaConnectionFilterTest, TrailingCharacters) {
    EXPECT_THROW(parser().parse("node_id = 1 extra"), std::invalid_argument);
    EXPECT_THROW(
      parser().parse("node_id = 1 AND aborting = false extra"),
      std::invalid_argument);
}

// =============================================================================
// STRING LITERAL HANDLING TESTS
// =============================================================================

TEST_F(KafkaConnectionFilterTest, StringLiteralEscaping) {
    auto conn = create_test_connection(
      1, 0, "uid", false, "client\"with\"quotes");

    auto predicate = parser().parse("client_id = \"client\\\"with\\\"quotes\"");
    EXPECT_TRUE(predicate(conn));
}

TEST_F(KafkaConnectionFilterTest, StringLiteralWithSpaces) {
    auto conn = create_test_connection(
      1, 0, "uid", false, "client with spaces");

    auto predicate = parser().parse("client_id = \"client with spaces\"");
    EXPECT_TRUE(predicate(conn));
}

TEST_F(KafkaConnectionFilterTest, StringLiteralWithSpecialChars) {
    auto conn = create_test_connection(1, 0, "uid", false, "client@domain.com");

    auto predicate = parser().parse("client_id = \"client@domain.com\"");
    EXPECT_TRUE(predicate(conn));
}

// =============================================================================
// WHITESPACE HANDLING TESTS
// =============================================================================

TEST_F(KafkaConnectionFilterTest, WhitespaceHandling) {
    auto conn = create_test_connection(1, 0, "uid", false);

    // All these should work the same
    EXPECT_TRUE(parser().parse("node_id=1")(conn));
    EXPECT_TRUE(parser().parse("node_id = 1")(conn));
    EXPECT_TRUE(parser().parse("  node_id  =  1  ")(conn));
    EXPECT_TRUE(parser().parse("\tnode_id\t=\t1\t")(conn));
    EXPECT_TRUE(parser().parse("\nnode_id\n=\n1\n")(conn));

    // Multiple conditions with various whitespace
    EXPECT_TRUE(parser().parse("node_id=1 AND aborting=false")(conn));
    EXPECT_TRUE(
      parser().parse("  node_id  =  1  AND  aborting  =  false  ")(conn));
    EXPECT_THROW(
      parser().parse("node_id=1AND aborting=false"), std::invalid_argument);
}

// =============================================================================
// FIELD TYPE CONVERSION TESTS
// =============================================================================

TEST_F(KafkaConnectionFilterTest, IntegerFieldTypes) {
    auto conn = create_test_connection(
      1,
      2,
      "uid",
      false,
      "client",
      "admin",
      true,
      "192.168.1.100",
      9092,
      100,
      50);

    // Test various integer fields
    EXPECT_TRUE(parser().parse("node_id = 1")(conn));
    EXPECT_TRUE(parser().parse("shard_id = 2")(conn));
    EXPECT_TRUE(parser().parse("source.port = 9092")(conn));
    EXPECT_TRUE(parser().parse("produce_batch_record_count_total = 100")(conn));
    EXPECT_TRUE(parser().parse("fetch_tput_total = 50")(conn));
}

TEST_F(KafkaConnectionFilterTest, LegacyFieldAlias) {
    auto conn = create_test_connection(
      1, 0, "uid", false, "client", "admin", true, "192.168.1.100", 9092, 100);

    // field1 should be an alias for produce_batch_record_count_total
    EXPECT_TRUE(parser().parse("field1 = 100")(conn));
    EXPECT_TRUE(parser().parse("produce_batch_record_count_total = 100")(conn));

    // Both should give same result
    auto predicate1 = parser().parse("field1 >= 50");
    auto predicate2 = parser().parse("produce_batch_record_count_total >= 50");
    EXPECT_EQ(predicate1(conn), predicate2(conn));
}

// =============================================================================
// CASE SENSITIVITY TESTS
// =============================================================================

TEST_F(KafkaConnectionFilterTest, CaseSensitiveFieldNames) {
    auto conn = create_test_connection(1);

    // Field names should be case sensitive
    EXPECT_NO_THROW(parser().parse("node_id = 1"));
    EXPECT_THROW(parser().parse("NODE_ID = 1"), std::invalid_argument);
    EXPECT_THROW(parser().parse("Node_Id = 1"), std::invalid_argument);
}

TEST_F(KafkaConnectionFilterTest, CaseInsensitiveLogicalOperators) {
    auto conn = create_test_connection(1, 0, "uid", false);

    // AND should be case insensitive
    EXPECT_TRUE(parser().parse("node_id = 1 AND aborting = false")(conn));
    EXPECT_TRUE(parser().parse("node_id = 1 and aborting = false")(conn));
    EXPECT_TRUE(parser().parse("node_id = 1 And aborting = false")(conn));
    EXPECT_TRUE(parser().parse("node_id = 1 aNd aborting = false")(conn));
}

TEST_F(KafkaConnectionFilterTest, CaseInsensitiveBooleanLiterals) {
    auto conn_true = create_test_connection(1, 0, "uid", true);
    auto conn_false = create_test_connection(1, 0, "uid", false);

    // Boolean literals should be case insensitive
    EXPECT_TRUE(parser().parse("aborting = true")(conn_true));
    EXPECT_TRUE(parser().parse("aborting = TRUE")(conn_true));
    EXPECT_TRUE(parser().parse("aborting = True")(conn_true));
    EXPECT_TRUE(parser().parse("aborting = false")(conn_false));
    EXPECT_TRUE(parser().parse("aborting = FALSE")(conn_false));
    EXPECT_TRUE(parser().parse("aborting = False")(conn_false));
}

// =============================================================================
// AUTO REGISTRY TESTS
// =============================================================================

TEST_F(KafkaConnectionAutoFilterTest, AutoRegistryBasicFunctionality) {
    auto conn = create_test_connection(
      1, 0, "uid", false, "client", "admin", true);

    EXPECT_TRUE(auto_parser().parse("node_id = 1")(conn));
    EXPECT_TRUE(auto_parser().parse("aborting = false")(conn));
    EXPECT_TRUE(
      auto_parser().parse("authentication_info.user_principal = \"admin\"")(
        conn));
    EXPECT_TRUE(auto_parser().parse("tls_info.enabled = true")(conn));
}

TEST_F(KafkaConnectionAutoFilterTest, AutoRegistryComplexFilters) {
    auto conn = create_test_connection(
      1, 0, "uid", false, "client", "admin", true, "192.168.1.100", 9092);

    auto predicate = auto_parser().parse(
      "node_id = 1 AND authentication_info.user_principal = \"admin\" AND "
      "source.port = 9092");
    EXPECT_TRUE(predicate(conn));
}

// =============================================================================
// REGISTRY COMPATIBILITY TESTS
// =============================================================================

TEST_F(KafkaConnectionUnifiedFilterTest, BothRegistriesSupportSameFields) {
    // Both registries should support common fields
    EXPECT_TRUE(manual_registry().has_field("node_id"));
    EXPECT_TRUE(auto_registry().has_field("node_id"));

    EXPECT_TRUE(
      manual_registry().has_field("authentication_info.user_principal"));
    EXPECT_TRUE(
      auto_registry().has_field("authentication_info.user_principal"));

    EXPECT_TRUE(manual_registry().has_field("tls_info.enabled"));
    EXPECT_TRUE(auto_registry().has_field("tls_info.enabled"));
}

TEST_F(KafkaConnectionUnifiedFilterTest, BothRegistriesProduceSameResults) {
    auto conn = create_test_connection(
      1, 0, "uid", false, "client", "admin", true);

    // Same filter should work the same way on both registries
    const std::string filter_expr = "node_id = 1 AND tls_info.enabled = true";

    auto manual_predicate = manual_parser().parse(filter_expr);
    auto auto_predicate = auto_parser().parse(filter_expr);

    EXPECT_TRUE(manual_predicate(conn));
    EXPECT_TRUE(auto_predicate(conn));

    // Test with non-matching data
    conn.set_node_id(2);
    EXPECT_FALSE(manual_predicate(conn));
    EXPECT_FALSE(auto_predicate(conn));
}

TEST_F(KafkaConnectionUnifiedFilterTest, ComplexFilterCompatibility) {
    auto conn = create_test_connection(
      1,
      2,
      "test-uid",
      false,
      "test-client",
      "admin",
      true,
      "192.168.1.100",
      9092,
      100);

    const std::string complex_filter
      = "node_id = 1 AND shard_id = 2 AND client_id = \"test-client\" AND "
        "authentication_info.user_principal = \"admin\" AND tls_info.enabled = "
        "true AND "
        "source.ip_address = \"192.168.1.100\" AND "
        "produce_batch_record_count_total = 100";

    auto manual_predicate = manual_parser().parse(complex_filter);
    auto auto_predicate = auto_parser().parse(complex_filter);

    EXPECT_TRUE(manual_predicate(conn));
    EXPECT_TRUE(auto_predicate(conn));
}

// =============================================================================
// EDGE CASES AND STRESS TESTS
// =============================================================================

TEST_F(KafkaConnectionFilterTest, LargeIntegerValues) {
    auto conn = create_test_connection();
    conn.set_produce_batch_record_count_total(
      9223372036854775807LL); // max int64_t

    auto predicate = parser().parse(
      "produce_batch_record_count_total = 9223372036854775807");
    EXPECT_TRUE(predicate(conn));
}

TEST_F(KafkaConnectionFilterTest, EmptyStringValues) {
    auto conn = create_test_connection(1, 0, "", false, "");

    EXPECT_TRUE(parser().parse("uid = \"\"")(conn));
    EXPECT_TRUE(parser().parse("client_id = \"\"")(conn));
}

TEST_F(KafkaConnectionFilterTest, VeryLongStringValues) {
    std::string long_string(1000, 'a');
    auto conn = create_test_connection(1, 0, "uid", false, long_string);

    auto predicate = parser().parse("client_id = \"" + long_string + "\"");
    EXPECT_TRUE(predicate(conn));
}

TEST_F(KafkaConnectionFilterTest, ManyAndConditions) {
    auto conn = create_test_connection(
      1, 0, "uid", false, "client", "admin", true);

    std::string filter = "node_id = 1";
    for (int i = 0; i < 100; ++i) {
        filter += " AND node_id = 1";
    }

    auto predicate = parser().parse(filter);
    EXPECT_TRUE(predicate(conn));
}

TEST_F(KafkaConnectionFilterTest, FieldPathWithManySegments) {
    auto conn = create_test_connection();

    // Test nested field access
    EXPECT_TRUE(
      parser().parse("authentication_info.user_principal = \"admin\"")(conn));
    EXPECT_TRUE(parser().parse("source.ip_address = \"192.168.1.100\"")(conn));
}

// =============================================================================
// PERFORMANCE HINTS TESTS (for future optimization)
// =============================================================================

TEST_F(KafkaConnectionFilterTest, FilterReusability) {
    auto predicate = parser().parse("node_id = 1 AND aborting = false");

    // Test that the same predicate can be used multiple times
    auto conn1 = create_test_connection(1, 0, "uid1", false);
    auto conn2 = create_test_connection(1, 0, "uid2", false);
    auto conn3 = create_test_connection(2, 0, "uid3", false);

    EXPECT_TRUE(predicate(conn1));
    EXPECT_TRUE(predicate(conn2));
    EXPECT_FALSE(predicate(conn3));
}

// =============================================================================
// REGRESSION TESTS
// =============================================================================

TEST_F(KafkaConnectionFilterTest, RegressionFieldNameValidation) {
    // Ensure field names with underscores work correctly
    auto conn = create_test_connection();

    EXPECT_NO_THROW(parser().parse("node_id = 1"));
    EXPECT_NO_THROW(parser().parse("shard_id = 0"));
    EXPECT_NO_THROW(parser().parse("client_id = \"test\""));
    EXPECT_NO_THROW(
      parser().parse("authentication_info.user_principal = \"admin\""));
}

TEST_F(KafkaConnectionFilterTest, RegressionStringEscaping) {
    // Test various escape sequences
    auto conn1 = create_test_connection(1, 0, "uid", false, "client\\test");
    auto conn2 = create_test_connection(1, 0, "uid", false, "client\"test");

    EXPECT_TRUE(parser().parse("client_id = \"client\\\\test\"")(conn1));
    EXPECT_TRUE(parser().parse("client_id = \"client\\\"test\"")(conn2));
}

// =============================================================================
// ENUM SUPPORT TESTS
// =============================================================================
TEST_F(KafkaConnectionFilterTest, EnumFieldBasicSupport) {
    auto conn = create_test_connection();
    conn.get_authentication_info().set_state(
      proto::admin::authentication_state::success);
    conn.get_authentication_info().set_mechanism(
      proto::admin::authentication_mechanism::sasl_scram);

    // Test enum field access with uppercase string values
    EXPECT_TRUE(
      parser().parse(
        "authentication_info.state = \"AUTHENTICATION_STATE_SUCCESS\"")(conn));
    EXPECT_TRUE(
      parser().parse(
        "authentication_info.mechanism = "
        "\"AUTHENTICATION_MECHANISM_SASL_SCRAM\"")(conn));

    // Test negative cases
    EXPECT_FALSE(
      parser().parse("authentication_info.state = \"FAILURE\"")(conn));
    EXPECT_FALSE(
      parser().parse("authentication_info.mechanism = \"MTLS\"")(conn));
}

TEST_F(KafkaConnectionFilterTest, EnumFieldAllValues) {
    auto conn = create_test_connection();

    // Test all authentication_state values
    conn.get_authentication_info().set_state(
      proto::admin::authentication_state::unspecified);
    EXPECT_TRUE(
      parser().parse(
        "authentication_info.state = \"AUTHENTICATION_STATE_UNSPECIFIED\"")(
        conn));

    conn.get_authentication_info().set_state(
      proto::admin::authentication_state::unauthenticated);
    EXPECT_TRUE(
      parser().parse(
        "authentication_info.state = \"AUTHENTICATION_STATE_UNAUTHENTICATED\"")(
        conn));

    conn.get_authentication_info().set_state(
      proto::admin::authentication_state::success);
    EXPECT_TRUE(
      parser().parse(
        "authentication_info.state = \"AUTHENTICATION_STATE_SUCCESS\"")(conn));

    conn.get_authentication_info().set_state(
      proto::admin::authentication_state::failure);
    EXPECT_TRUE(
      parser().parse(
        "authentication_info.state = \"AUTHENTICATION_STATE_FAILURE\"")(conn));

    // Test all authentication_mechanism values
    conn.get_authentication_info().set_mechanism(
      proto::admin::authentication_mechanism::unspecified);
    EXPECT_TRUE(
      parser().parse(
        "authentication_info.mechanism = "
        "\"AUTHENTICATION_MECHANISM_UNSPECIFIED\"")(conn));

    conn.get_authentication_info().set_mechanism(
      proto::admin::authentication_mechanism::mtls);
    EXPECT_TRUE(
      parser().parse(
        "authentication_info.mechanism = \"AUTHENTICATION_MECHANISM_MTLS\"")(
        conn));

    conn.get_authentication_info().set_mechanism(
      proto::admin::authentication_mechanism::sasl_scram);
    EXPECT_TRUE(
      parser().parse(
        "authentication_info.mechanism = "
        "\"AUTHENTICATION_MECHANISM_SASL_SCRAM\"")(conn));

    conn.get_authentication_info().set_mechanism(
      proto::admin::authentication_mechanism::sasl_oauthbearer);
    EXPECT_TRUE(
      parser().parse(
        "authentication_info.mechanism = "
        "\"AUTHENTICATION_MECHANISM_SASL_OAUTHBEARER\"")(conn));

    conn.get_authentication_info().set_mechanism(
      proto::admin::authentication_mechanism::sasl_plain);
    EXPECT_TRUE(
      parser().parse(
        "authentication_info.mechanism = "
        "\"AUTHENTICATION_MECHANISM_SASL_PLAIN\"")(conn));

    conn.get_authentication_info().set_mechanism(
      proto::admin::authentication_mechanism::sasl_gssapi);
    EXPECT_TRUE(
      parser().parse(
        "authentication_info.mechanism = "
        "\"AUTHENTICATION_MECHANISM_SASL_GSSAPI\"")(conn));
}

TEST_F(KafkaConnectionFilterTest, EnumFieldCaseSensitivity) {
    auto conn = create_test_connection();
    conn.get_authentication_info().set_state(
      proto::admin::authentication_state::success);

    // Enum values should be case-sensitive (AIP-160 requirement)
    EXPECT_TRUE(
      parser().parse(
        "authentication_info.state = \"AUTHENTICATION_STATE_SUCCESS\"")(conn));

    // These should fail due to case sensitivity - they will parse successfully
    // but won't match at runtime
    auto predicate_wrong_case1 = parser().parse(
      "authentication_info.state = \"authentication_state_success\""); // lowercase
    auto predicate_wrong_case2 = parser().parse(
      "authentication_info.state = \"Authentication_State_Success\""); // mixed
                                                                       // case
    auto predicate_wrong_case3 = parser().parse(
      "authentication_info.state = \"Authentication_sTate_sUcCeSs\""); // random
                                                                       // case

    EXPECT_FALSE(predicate_wrong_case1(conn));
    EXPECT_FALSE(predicate_wrong_case2(conn));
    EXPECT_FALSE(predicate_wrong_case3(conn));
}

TEST_F(KafkaConnectionFilterTest, EnumFieldComparisonOperators) {
    auto conn = create_test_connection();
    conn.get_authentication_info().set_state(
      proto::admin::authentication_state::success);

    // Equality and inequality should work
    EXPECT_TRUE(
      parser().parse(
        "authentication_info.state = \"AUTHENTICATION_STATE_SUCCESS\"")(conn));
    EXPECT_TRUE(
      parser().parse("authentication_info.state != \"FAILURE\"")(conn));

    // Other comparison operators should throw for enums
    EXPECT_THROW(
      parser().parse("authentication_info.state > \"FAILURE\""),
      std::invalid_argument);
    EXPECT_THROW(
      parser().parse("authentication_info.state < \"UNSPECIFIED\""),
      std::invalid_argument);
    EXPECT_THROW(
      parser().parse(
        "authentication_info.state >= \"AUTHENTICATION_STATE_SUCCESS\""),
      std::invalid_argument);
    EXPECT_THROW(
      parser().parse(
        "authentication_info.state <= \"AUTHENTICATION_STATE_SUCCESS\""),
      std::invalid_argument);
}

TEST_F(KafkaConnectionFilterTest, EnumFieldLogicalOperations) {
    auto conn = create_test_connection();
    conn.get_authentication_info().set_state(
      proto::admin::authentication_state::success);
    conn.get_authentication_info().set_mechanism(
      proto::admin::authentication_mechanism::sasl_scram);

    // Test AND operations with enums
    EXPECT_TRUE(
      parser().parse(
        "authentication_info.state = \"AUTHENTICATION_STATE_SUCCESS\" AND "
        "authentication_info.mechanism = "
        "\"AUTHENTICATION_MECHANISM_SASL_SCRAM\"")(conn));

    EXPECT_FALSE(
      parser().parse(
        "authentication_info.state = \"AUTHENTICATION_STATE_SUCCESS\" AND "
        "authentication_info.mechanism = \"MTLS\"")(conn));

    // Test with mixed field types
    EXPECT_TRUE(
      parser().parse(
        "node_id = 1 AND authentication_info.state = "
        "\"AUTHENTICATION_STATE_SUCCESS\"")(conn));

    EXPECT_FALSE(
      parser().parse(
        "node_id = 2 AND authentication_info.state = "
        "\"AUTHENTICATION_STATE_SUCCESS\"")(conn));
}

TEST_F(KafkaConnectionFilterTest, EnumFieldEdgeCases) {
    auto conn = create_test_connection();

    // Test with unspecified values (default enum values)
    conn.get_authentication_info().set_state(
      proto::admin::authentication_state::unspecified);
    conn.get_authentication_info().set_mechanism(
      proto::admin::authentication_mechanism::unspecified);

    EXPECT_TRUE(
      parser().parse(
        "authentication_info.state = \"AUTHENTICATION_STATE_UNSPECIFIED\"")(
        conn));
    EXPECT_TRUE(
      parser().parse(
        "authentication_info.mechanism = "
        "\"AUTHENTICATION_MECHANISM_UNSPECIFIED\"")(conn));

    // Test inequality with unspecified
    EXPECT_TRUE(
      parser().parse(
        "authentication_info.state != \"AUTHENTICATION_STATE_SUCCESS\"")(conn));
    EXPECT_TRUE(
      parser().parse(
        "authentication_info.mechanism != "
        "\"AUTHENTICATION_MECHANISM_SASL_SCRAM\"")(conn));
}

// =============================================================================
// AUTO REGISTRY ENUM TESTS - UPDATED TO UPPERCASE
// =============================================================================

TEST_F(KafkaConnectionAutoFilterTest, AutoRegistryEnumSupport) {
    auto conn = create_test_connection();
    conn.get_authentication_info().set_state(
      proto::admin::authentication_state::success);
    conn.get_authentication_info().set_mechanism(
      proto::admin::authentication_mechanism::sasl_scram);

    // Auto registry should also support enum fields
    EXPECT_TRUE(
      auto_parser().parse(
        "authentication_info.state = \"AUTHENTICATION_STATE_SUCCESS\"")(conn));
    EXPECT_TRUE(
      auto_parser().parse(
        "authentication_info.mechanism = "
        "\"AUTHENTICATION_MECHANISM_SASL_SCRAM\"")(conn));

    // Invalid format values should still throw during parsing
    EXPECT_THROW(
      auto_parser().parse("authentication_info.state = \"\""),
      std::invalid_argument);

    // Valid format but invalid enum values should parse but not match
    auto predicate_invalid = auto_parser().parse(
      "authentication_info.state = \"INVALID_BUT_FORMATTED_CORRECTLY\"");
    EXPECT_FALSE(predicate_invalid(conn));
}

// =============================================================================
// REGISTRY COMPATIBILITY ENUM TESTS - UPDATED TO UPPERCASE
// =============================================================================

TEST_F(KafkaConnectionUnifiedFilterTest, EnumCompatibilityBetweenRegistries) {
    auto conn = create_test_connection();
    conn.get_authentication_info().set_state(
      proto::admin::authentication_state::success);
    conn.get_authentication_info().set_mechanism(
      proto::admin::authentication_mechanism::sasl_scram);

    const std::string enum_filter
      = "authentication_info.state = \"AUTHENTICATION_STATE_SUCCESS\" AND "
        "authentication_info.mechanism = "
        "\"AUTHENTICATION_MECHANISM_SASL_SCRAM\"";

    auto manual_predicate = manual_parser().parse(enum_filter);
    auto auto_predicate = auto_parser().parse(enum_filter);

    EXPECT_TRUE(manual_predicate(conn));
    EXPECT_TRUE(auto_predicate(conn));

    // Test with different values
    conn.get_authentication_info().set_state(
      proto::admin::authentication_state::failure);

    EXPECT_FALSE(manual_predicate(conn));
    EXPECT_FALSE(auto_predicate(conn));
}

// =============================================================================
// PERFORMANCE AND STRESS TESTS FOR ENUMS - UPDATED TO UPPERCASE
// =============================================================================

TEST_F(KafkaConnectionFilterTest, EnumPerformanceWithManyConditions) {
    auto conn = create_test_connection();
    conn.get_authentication_info().set_state(
      proto::admin::authentication_state::success);

    // Test many enum conditions
    std::string filter
      = "authentication_info.state = \"AUTHENTICATION_STATE_SUCCESS\"";
    for (int i = 0; i < 50; ++i) {
        filter += " AND authentication_info.state = "
                  "\"AUTHENTICATION_STATE_SUCCESS\"";
    }

    auto predicate = parser().parse(filter);
    EXPECT_TRUE(predicate(conn));
}

// =============================================================================
// RUNTIME ENUM VALIDATION BEHAVIOR TESTS - UPDATED TO UPPERCASE
// =============================================================================

TEST_F(KafkaConnectionFilterTest, EnumRuntimeValidationBehavior) {
    auto conn = create_test_connection();
    conn.get_authentication_info().set_state(
      proto::admin::authentication_state::success);

    // Test that runtime validation works correctly
    // These should not match even though they parse successfully
    std::vector<std::string> invalid_but_well_formatted_values = {
      "NONEXISTENT_STATE", "SOME_OTHER_VALUE", "DEFINITELY_NOT_AN_ENUM_VALUE"};

    for (const auto& invalid_value : invalid_but_well_formatted_values) {
        auto predicate = parser().parse(
          "authentication_info.state = \"" + invalid_value + "\"");
        EXPECT_FALSE(predicate(conn))
          << "Should not match invalid enum value: " << invalid_value;
    }

    // But valid values should still work
    EXPECT_TRUE(
      parser().parse(
        "authentication_info.state = \"AUTHENTICATION_STATE_SUCCESS\"")(conn));
    EXPECT_TRUE(
      parser().parse("authentication_info.state != \"FAILURE\"")(conn));
}

} // namespace redpanda::admin
