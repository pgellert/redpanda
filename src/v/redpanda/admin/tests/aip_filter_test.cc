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

#include "proto/redpanda/core/admin/v2/kafka_connections.proto.h"
#include "redpanda/admin/aip_filter.h"
#include "redpanda/admin/field_registry.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

namespace redpanda::admin {

// kafka_connection-specific registry creation
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
      .addInt64Field(
        "authentication_info.state",
        [](const kafka_connection& c) {
            return static_cast<int64_t>(
              c.get_authentication_info().get_state());
        })
      .addInt64Field(
        "authentication_info.mechanism",
        [](const kafka_connection& c) {
            return static_cast<int64_t>(
              c.get_authentication_info().get_mechanism());
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

      // Time-based fields (converted to Unix timestamps for comparison)
      .addInt64Field(
        "open_time",
        [](const kafka_connection& c) {
            return absl::ToUnixSeconds(c.get_open_time());
        })
      .addInt64Field(
        "close_time",
        [](const kafka_connection& c) {
            return absl::ToUnixSeconds(c.get_close_time());
        })
      .addInt64Field("idle_duration_seconds", [](const kafka_connection& c) {
          return absl::ToInt64Seconds(c.get_idle_duration());
      });
    return std::move(builder).build();
}

class KafkaConnectionFilterTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create the registry and parser in SetUp()
        registry_ = std::make_unique<
          ProtobufFieldRegistry<proto::admin::kafka_connection>>(
          create_kafka_connection_field_registry());
        parser_
          = std::make_unique<AIPFilterParser<proto::admin::kafka_connection>>(
            *registry_);
    }

private:
    // Use std::unique_ptr for both for consistency
    std::unique_ptr<ProtobufFieldRegistry<proto::admin::kafka_connection>>
      registry_;
    std::unique_ptr<AIPFilterParser<proto::admin::kafka_connection>> parser_;

protected:
    // Provide access to the parser for tests
    AIPFilterParser<proto::admin::kafka_connection>& parser() {
        return *parser_;
    }
};

TEST_F(KafkaConnectionFilterTest, SimpleNumericFilter) {
    using proto::admin::kafka_connection;

    auto predicate = parser().parse("field1 >= 5 AND field1 <= 5");

    kafka_connection conn1;
    conn1.set_produce_batch_record_count_total(5);

    kafka_connection conn2;
    conn2.set_produce_batch_record_count_total(6);

    EXPECT_TRUE(predicate(conn1));
    EXPECT_FALSE(predicate(conn2));
}

TEST_F(KafkaConnectionFilterTest, StringFieldFilter) {
    using proto::admin::authentication_info;
    using proto::admin::kafka_connection;

    auto predicate = parser().parse(
      "authentication_info.user_principal = \"admin\"");

    kafka_connection conn1;
    conn1.get_authentication_info().set_user_principal("admin");

    kafka_connection conn2;
    conn2.get_authentication_info().set_user_principal("user");

    EXPECT_TRUE(predicate(conn1));
    EXPECT_FALSE(predicate(conn2));
}

TEST_F(KafkaConnectionFilterTest, BooleanFieldFilter) {
    using proto::admin::kafka_connection;

    auto predicate = parser().parse("tls_info.enabled = true");

    kafka_connection conn1;
    conn1.get_tls_info().set_enabled(true);

    kafka_connection conn2;
    conn2.get_tls_info().set_enabled(false);

    EXPECT_TRUE(predicate(conn1));
    EXPECT_FALSE(predicate(conn2));
}

TEST_F(KafkaConnectionFilterTest, ComplexFilter) {
    using proto::admin::kafka_connection;

    auto predicate = parser().parse(
      "produce_batch_record_count_total > 100 AND "
      "tls_info.enabled = true AND "
      "authentication_info.user_principal = \"admin\"");

    kafka_connection matching_conn;
    matching_conn.set_produce_batch_record_count_total(150);
    matching_conn.get_tls_info().set_enabled(true);
    matching_conn.get_authentication_info().set_user_principal("admin");

    kafka_connection non_matching_conn;
    non_matching_conn.set_produce_batch_record_count_total(
      50); // Fails first condition
    non_matching_conn.get_tls_info().set_enabled(true);
    non_matching_conn.get_authentication_info().set_user_principal("admin");

    EXPECT_TRUE(predicate(matching_conn));
    EXPECT_FALSE(predicate(non_matching_conn));
}

TEST_F(KafkaConnectionFilterTest, AutomaticRegistryBuilder) {
    using proto::admin::kafka_connection;

    // Test the automatic registry builder with reflection
    std::vector<std::string> field_paths = {
      "node_id",
      "shard_id",
      "uid",
      "authentication_info.user_principal",
      "tls_info.enabled"};

    auto auto_registry
      = AutoProtobufFieldRegistryBuilder<kafka_connection>::create_registry(
        field_paths);
    AIPFilterParser<kafka_connection> auto_parser(auto_registry);

    auto predicate = auto_parser.parse(
      "node_id = 1 AND tls_info.enabled = true");

    kafka_connection conn;
    conn.set_node_id(1);
    conn.get_tls_info().set_enabled(true);

    EXPECT_TRUE(predicate(conn));
}

TEST_F(KafkaConnectionFilterTest, InvalidFieldThrowsException) {
    EXPECT_THROW(
      parser().parse("nonexistent_field = 123"), std::invalid_argument);
}

TEST_F(KafkaConnectionFilterTest, InvalidSyntaxThrowsException) {
    EXPECT_THROW(
      parser().parse("field1 = = 123"), // Invalid syntax
      std::invalid_argument);

    EXPECT_THROW(
      parser().parse("field1 123"), // Missing operator
      std::invalid_argument);
}

TEST_F(KafkaConnectionFilterTest, ValidationMethod) {
    EXPECT_TRUE(parser().validate("field1 = 123"));
    EXPECT_TRUE(parser().validate("tls_info.enabled = true"));
    EXPECT_FALSE(parser().validate("nonexistent_field = 123"));
    EXPECT_FALSE(parser().validate("field1 = = 123"));
}

TEST_F(KafkaConnectionFilterTest, EmptyFilterMatchesAll) {
    using proto::admin::kafka_connection;

    auto predicate = parser().parse("");

    kafka_connection conn;
    // Empty filter should match all objects
    EXPECT_TRUE(predicate(conn));
}

} // namespace redpanda::admin
