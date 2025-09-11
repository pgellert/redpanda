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

#include "base/vassert-register.h"
#include "proto/redpanda/core/admin/kafka_connections.proto.h"
#include "redpanda/admin/aip_filter.h"

#include <gtest/gtest.h>

#include <vector>

namespace {

auto createKafkaConnectionRegistry() {
    using proto::admin::kafka_connection;
    auto builder
      = FieldAccessorRegistryBuilder<kafka_connection>{}
          .addInt64Field(
            "field1",
            [](const kafka_connection& c) {
                return c.get_produce_batch_record_count_total();
            })
          .addStringField(
            "authentication_info.user_principal",
            [](const kafka_connection& c) {
                return c.get_authentication_info().get_user_principal();
            })
          .addBoolField("tls_info.enabled", [](const kafka_connection& c) {
              return c.get_tls_info().get_enabled();
          });
    return std::move(builder).build();
}

} // namespace

struct AIPFilterTest : public testing::Test {};

TEST_F(AIPFilterTest, SimpleTest) {
    using proto::admin::kafka_connection;
    try {
        auto field_reg = createKafkaConnectionRegistry();

        // Build a filter predicate from a filter string
        auto registry = createKafkaConnectionRegistry();
        FilterParser<kafka_connection> parser(registry);

        auto pred = parser.parse("field1 >= 5 AND field1 <= 5");

        // Predicate pred = FilterParser::parse(
        //   "field1 >= 5 AND field1 <= 5 AND unknownField = 10");

        // Example kafka_connection instances to test
        kafka_connection conn1;
        conn1.set_produce_batch_record_count_total(5);

        kafka_connection conn2;
        conn2.set_produce_batch_record_count_total(6);

        // kafka_connection conn3;
        // conn3.field1 = 7;
        // conn3.tls_info.protocol = "aaa";

        // Apply the predicate to each instance
        EXPECT_TRUE(pred(conn1));
        EXPECT_FALSE(pred(conn2));
        // std::cout << "conn1 matches? " << pred(conn1)
        //           << std::endl; // true if field1==5 and protocol < "abc"
        // std::cout << "conn2 matches? " << pred(conn2)
        //           << std::endl; // false (protocol "def" is not < "abc")
        // std::cout << "conn3 matches? " << pred(conn3)
        //           << std::endl; // false (field1 is not 5)
    } catch (const std::invalid_argument& e) {
        FAIL() << "Unexpected filter parsing error: " << e.what();
    }
}
