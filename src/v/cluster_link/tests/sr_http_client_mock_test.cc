// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf.h"
#include "cluster_link/sr_http_client.h"
#include "http/client.h"

#include <seastar/core/future.hh>

#include <boost/beast/http/status.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace bh = boost::beast::http;
namespace pps = pandaproxy::schema_registry;
namespace cl = cluster_link;

using ::testing::_;
using ::testing::HasSubstr;

namespace {

class mock_client : public http::abstract_client {
public:
    MOCK_METHOD(
      ss::future<http::downloaded_response>,
      request_and_collect_response,
      (bh::request_header<>&&,
       std::optional<iobuf>,
       ss::lowres_clock::duration),
      (override));
    MOCK_METHOD(ss::future<>, shutdown_and_stop, (), (override));
};

/// Records what the mock client received for off-future inspection.
struct captured_request {
    ss::sstring target;
    ss::sstring method;
    std::optional<ss::sstring> auth;
    std::optional<ss::sstring> body;
};

captured_request
capture(bh::request_header<>&& req, const std::optional<iobuf>& payload) {
    captured_request cap;
    auto tv = req.target();
    cap.target = ss::sstring{tv.data(), tv.size()};
    auto mv = bh::to_string(req.method());
    cap.method = ss::sstring{mv.data(), mv.size()};
    auto auth_it = req.find(bh::field::authorization);
    if (auth_it != req.end()) {
        cap.auth = ss::sstring{
          auth_it->value().data(), auth_it->value().size()};
    }
    if (payload.has_value()) {
        ss::sstring body;
        for (const auto& frag : *payload) {
            body.append(frag.get(), frag.size());
        }
        cap.body = std::move(body);
    }
    return cap;
}

http::downloaded_response ok_response(std::string_view body) {
    iobuf buf;
    buf.append(body.data(), body.size());
    return http::downloaded_response{
      .status = bh::status::ok, .body = std::move(buf)};
}

http::downloaded_response
error_response(bh::status status, std::string_view body) {
    iobuf buf;
    buf.append(body.data(), body.size());
    return http::downloaded_response{.status = status, .body = std::move(buf)};
}

cl::sr_endpoint make_endpoint() {
    auto e = cl::sr_endpoint::from_url("http://test-sr:8081");
    return std::move(*e);
}

cl::sr_endpoint make_endpoint_with_basic_auth() {
    auto e = make_endpoint();
    e.basic_auth_user = "u";
    e.basic_auth_pass = "p";
    return e;
}

// Reduce boilerplate: a single helper that scripts the mock to record
// the call and respond with a fixed response.
void expect_one_call(
  mock_client& mc, captured_request& cap, http::downloaded_response response) {
    EXPECT_CALL(mc, request_and_collect_response(_, _, _))
      .WillOnce(
        [&cap, response = std::move(response)](
          bh::request_header<>&& req,
          std::optional<iobuf> payload,
          ss::lowres_clock::duration) mutable
          -> ss::future<http::downloaded_response> {
            cap = capture(std::move(req), payload);
            return ss::make_ready_future<http::downloaded_response>(
              std::move(response));
        });
}

} // namespace

TEST(SrHttpClientLive, ListSubjectsParsesArrayOfStrings) {
    mock_client mc;
    captured_request cap;
    expect_one_call(mc, cap, ok_response(R"(["a","b","c"])"));
    cl::sr_http_client c{make_endpoint(), mc};
    auto r = c.list_subjects().get();
    EXPECT_EQ(cap.target, "/subjects");
    EXPECT_EQ(cap.method, "GET");
    ASSERT_FALSE(r.has_error()) << r.assume_error().message;
    auto& subjects = r.assume_value();
    ASSERT_EQ(subjects.size(), 3u);
    EXPECT_EQ(subjects[0], "a");
    EXPECT_EQ(subjects[1], "b");
    EXPECT_EQ(subjects[2], "c");
}

TEST(SrHttpClientLive, ListSubjectsAttachesBasicAuth) {
    mock_client mc;
    captured_request cap;
    expect_one_call(mc, cap, ok_response("[]"));
    cl::sr_http_client c{make_endpoint_with_basic_auth(), mc};
    auto r = c.list_subjects().get();
    EXPECT_FALSE(r.has_error());
    // base64("u:p") = "dTpw"
    ASSERT_TRUE(cap.auth.has_value());
    EXPECT_EQ(*cap.auth, "Basic dTpw");
}

TEST(SrHttpClientLive, ListSubjectsMapsServerErrors) {
    mock_client mc;
    captured_request cap;
    expect_one_call(
      mc, cap, error_response(bh::status::internal_server_error, "boom"));
    cl::sr_http_client c{make_endpoint(), mc};
    auto r = c.list_subjects().get();
    ASSERT_TRUE(r.has_error());
    EXPECT_EQ(r.assume_error().errc, cl::sr_http_errc::server_error);
}

TEST(SrHttpClientLive, ListSubjectsRejectsNonArray) {
    mock_client mc;
    captured_request cap;
    expect_one_call(mc, cap, ok_response(R"({"hello":"world"})"));
    cl::sr_http_client c{make_endpoint(), mc};
    auto r = c.list_subjects().get();
    ASSERT_TRUE(r.has_error());
    EXPECT_EQ(r.assume_error().errc, cl::sr_http_errc::unexpected_response);
}

TEST(SrHttpClientLive, GetSubjectVersionExtractsIdAndType) {
    mock_client mc;
    captured_request cap;
    expect_one_call(
      mc,
      cap,
      ok_response(
        R"({"id": 42, "version": 2, "subject": "topic-value", "schemaType":"AVRO", "schema":"{\"type\":\"string\"}"})"));
    cl::sr_http_client c{make_endpoint(), mc};
    auto r = c.get_subject_version("topic-value", 2).get();
    EXPECT_EQ(cap.target, "/subjects/topic-value/versions/2");
    ASSERT_FALSE(r.has_error()) << r.assume_error().message;
    auto& schema = r.assume_value();
    EXPECT_EQ(schema.id(), 42);
    EXPECT_EQ(schema.version(), 2);
    EXPECT_EQ(schema.schema.type(), pps::schema_type::avro);
}

TEST(SrHttpClientLive, GetSubjectVersionParsesReferences) {
    mock_client mc;
    captured_request cap;
    expect_one_call(mc, cap, ok_response(R"({
            "id": 7, "version": 1, "schemaType":"JSON",
            "schema":"{}",
            "references": [
                {"name":"common.json", "subject":"common", "version":1}
            ]})"));
    cl::sr_http_client c{make_endpoint(), mc};
    auto r = c.get_subject_version("topic", 1).get();
    ASSERT_FALSE(r.has_error()) << r.assume_error().message;
    auto& schema = r.assume_value();
    ASSERT_EQ(schema.schema.def().refs().size(), 1u);
    EXPECT_EQ(schema.schema.def().refs()[0].name, "common.json");
    EXPECT_EQ(schema.schema.def().refs()[0].version(), 1);
}

TEST(SrHttpClientLive, PutModeSendsCorrectBody) {
    mock_client mc;
    captured_request cap;
    expect_one_call(mc, cap, ok_response(R"({"mode":"IMPORT"})"));
    cl::sr_http_client c{make_endpoint(), mc};
    auto r = c.put_mode(std::nullopt, pps::mode::import).get();
    EXPECT_FALSE(r.has_error());
    EXPECT_EQ(cap.method, "PUT");
    EXPECT_EQ(cap.target, "/mode");
    ASSERT_TRUE(cap.body.has_value());
    EXPECT_THAT(*cap.body, HasSubstr("IMPORT"));
}

TEST(SrHttpClientLive, PostSchemaWithIdSendsIdAndSchemaBody) {
    mock_client mc;
    captured_request cap;
    expect_one_call(mc, cap, ok_response(R"({"id":99})"));
    cl::sr_http_client c{make_endpoint(), mc};
    pps::schema_definition def{
      pps::schema_definition::raw_string{R"({"type":"record"})"},
      pps::schema_type::avro,
      {},
      std::nullopt};
    pps::subject_schema ss{
      pps::context_subject::unqualified("topic-value"), std::move(def)};
    pps::stored_schema stored{
      .schema = std::move(ss),
      .version = pps::schema_version{3},
      .id = pps::schema_id{99},
      .deleted = pps::is_deleted::no};
    auto r = c.post_schema_with_id("topic-value", stored).get();
    ASSERT_FALSE(r.has_error()) << r.assume_error().message;
    EXPECT_EQ(r.assume_value(), 99);
    EXPECT_EQ(cap.target, "/subjects/topic-value/versions");
    EXPECT_EQ(cap.method, "POST");
    ASSERT_TRUE(cap.body.has_value());
    // Must include the explicit id, version, schemaType, and properly
    // JSON-escape the embedded schema body.
    EXPECT_THAT(*cap.body, HasSubstr(R"("id":99)"));
    EXPECT_THAT(*cap.body, HasSubstr(R"("version":3)"));
    EXPECT_THAT(*cap.body, HasSubstr(R"("schemaType":"AVRO")"));
    EXPECT_THAT(*cap.body, HasSubstr(R"(\"type\":\"record\")"));
}

TEST(SrHttpClientLive, PostSchemaWithIdDetectsNotInImportMode) {
    mock_client mc;
    captured_request cap;
    expect_one_call(
      mc,
      cap,
      error_response(
        bh::status::unprocessable_entity,
        R"({"error_code":42204,"message":"Subject is not in import mode"})"));
    cl::sr_http_client c{make_endpoint(), mc};
    pps::schema_definition def{
      pps::schema_definition::raw_string{"{}"},
      pps::schema_type::json,
      {},
      std::nullopt};
    pps::subject_schema ss{
      pps::context_subject::unqualified("topic"), std::move(def)};
    pps::stored_schema stored{
      .schema = std::move(ss),
      .version = pps::schema_version{1},
      .id = pps::schema_id{1},
      .deleted = pps::is_deleted::no};
    auto r = c.post_schema_with_id("topic", stored).get();
    ASSERT_TRUE(r.has_error());
    EXPECT_EQ(r.assume_error().errc, cl::sr_http_errc::not_in_import_mode);
}
