// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster_link/sr_http_client.h"

#include <gtest/gtest.h>

namespace cl = cluster_link;

TEST(SrHttpClient, BasicAuthHeaderEncodesCredentials) {
    // base64("user:pass") = "dXNlcjpwYXNz"
    EXPECT_EQ(cl::make_basic_auth_header("user", "pass"), "Basic dXNlcjpwYXNz");
}

TEST(SrHttpClient, BasicAuthHeaderHandlesEmptyPassword) {
    // base64("user:") = "dXNlcjo="
    EXPECT_EQ(cl::make_basic_auth_header("user", ""), "Basic dXNlcjo=");
}

TEST(SrHttpClient, BasicAuthHeaderHandlesColonInPassword) {
    // Per RFC 7617 the user can't contain a colon but the password can.
    // base64("admin:p:a:ss") = "YWRtaW46cDphOnNz"
    EXPECT_EQ(
      cl::make_basic_auth_header("admin", "p:a:ss"), "Basic YWRtaW46cDphOnNz");
}

TEST(SrEndpoint, FromUrlHostPort) {
    auto e = cl::sr_endpoint::from_url("http://cflt-sr:8081");
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->addr.host(), "cflt-sr");
    EXPECT_EQ(e->addr.port(), 8081);
    EXPECT_TRUE(e->path_prefix.empty());
}

TEST(SrEndpoint, FromUrlNoPortDefaultsTo8081) {
    auto e = cl::sr_endpoint::from_url("http://cflt-sr");
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->addr.host(), "cflt-sr");
    EXPECT_EQ(e->addr.port(), 8081);
}

TEST(SrEndpoint, FromUrlWithPath) {
    auto e = cl::sr_endpoint::from_url("http://cflt-sr:8081/sr/v1");
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->addr.host(), "cflt-sr");
    EXPECT_EQ(e->addr.port(), 8081);
    EXPECT_EQ(e->path_prefix, "/sr/v1");
}

TEST(SrEndpoint, FromUrlStripsTrailingSlash) {
    auto e = cl::sr_endpoint::from_url("http://cflt-sr:8081/sr/");
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->path_prefix, "/sr");
}

TEST(SrEndpoint, FromUrlRejectsNonHttpScheme) {
    EXPECT_FALSE(cl::sr_endpoint::from_url("https://cflt-sr:8081").has_value());
    EXPECT_FALSE(cl::sr_endpoint::from_url("cflt-sr:8081").has_value());
}

TEST(SrEndpoint, FromUrlRejectsInvalidPort) {
    EXPECT_FALSE(cl::sr_endpoint::from_url("http://cflt-sr:abc").has_value());
    EXPECT_FALSE(cl::sr_endpoint::from_url("http://cflt-sr:0").has_value());
    EXPECT_FALSE(cl::sr_endpoint::from_url("http://cflt-sr:99999").has_value());
}

TEST(SrEndpoint, FromUrlRejectsNegativePort) {
    EXPECT_FALSE(cl::sr_endpoint::from_url("http://cflt-sr:-1").has_value());
}
