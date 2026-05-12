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

#include "cluster_link/sr_http_client.h"

#include "bytes/bytes.h"
#include "cluster_link/logger.h"
#include "http/utils.h"
#include "json/document.h"
#include "json/stringbuffer.h"
#include "json/writer.h"
#include "net/types.h"
#include "pandaproxy/schema_registry/types.h"
#include "utils/base64.h"

#include <seastar/core/coroutine.hh>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/beast/http/field.hpp>
#include <boost/beast/http/verb.hpp>

#include <charconv>
#include <string>
#include <string_view>
#include <system_error>

namespace pps = pandaproxy::schema_registry;
namespace bh = boost::beast::http;

namespace cluster_link {

namespace {

constexpr std::string_view kJsonContentType
  = "application/vnd.schemaregistry.v1+json";

/// Pull a string field out of a rapidjson object, returning std::nullopt if
/// absent or non-string.
std::optional<ss::sstring>
get_str(const json::Value& obj, std::string_view name) {
    auto it = obj.FindMember(
      rapidjson::GenericStringRef<char>(name.data(), name.size()));
    if (it == obj.MemberEnd() || !it->value.IsString()) {
        return std::nullopt;
    }
    return ss::sstring{it->value.GetString(), it->value.GetStringLength()};
}

std::optional<int32_t> get_int(const json::Value& obj, std::string_view name) {
    auto it = obj.FindMember(
      rapidjson::GenericStringRef<char>(name.data(), name.size()));
    if (it == obj.MemberEnd() || !it->value.IsInt()) {
        return std::nullopt;
    }
    return it->value.GetInt();
}

bool status_is_4xx(bh::status s) {
    return static_cast<unsigned>(s) >= 400 && static_cast<unsigned>(s) < 500;
}

bool status_is_5xx(bh::status s) {
    return static_cast<unsigned>(s) >= 500 && static_cast<unsigned>(s) < 600;
}

sr_http_errc errc_for_status(bh::status s) {
    if (s == bh::status::not_found) {
        return sr_http_errc::not_found;
    }
    if (status_is_5xx(s)) {
        return sr_http_errc::server_error;
    }
    return sr_http_errc::bad_request;
}

/// Convert an iobuf response body to an ss::sstring.
ss::sstring iobuf_to_string(const iobuf& buf) {
    std::string tmp;
    tmp.reserve(buf.size_bytes());
    for (const auto& frag : buf) {
        tmp.append(frag.get(), frag.size());
    }
    return ss::sstring{tmp.data(), tmp.size()};
}

} // namespace

std::string_view to_string_view(sr_http_errc e) {
    switch (e) {
    case sr_http_errc::transport:
        return "transport";
    case sr_http_errc::bad_request:
        return "bad_request";
    case sr_http_errc::not_found:
        return "not_found";
    case sr_http_errc::server_error:
        return "server_error";
    case sr_http_errc::unexpected_response:
        return "unexpected_response";
    case sr_http_errc::not_in_import_mode:
        return "not_in_import_mode";
    }
    return "unknown";
}

sr_http_error sr_http_error::from_status(bh::status status, ss::sstring msg) {
    return {
      .errc = errc_for_status(status),
      .message = std::move(msg),
      .http_status = status};
}

std::optional<sr_endpoint> sr_endpoint::from_url(std::string_view url) {
    constexpr std::string_view http_scheme = "http://";
    if (!boost::starts_with(url, http_scheme)) {
        return std::nullopt;
    }
    url.remove_prefix(http_scheme.size());

    // Split host:port from path.
    auto slash = url.find('/');
    auto authority = url.substr(0, slash);
    auto path = slash == std::string_view::npos ? std::string_view{}
                                                : url.substr(slash);

    auto colon = authority.find(':');
    if (colon == std::string_view::npos) {
        // Default to HTTP port 8081 (Confluent SR default) when not given.
        return sr_endpoint{
          .addr = net::unresolved_address{ss::sstring{authority}, 8081},
          .path_prefix = ss::sstring{path},
        };
    }

    auto host = authority.substr(0, colon);
    auto port_str = authority.substr(colon + 1);
    int port = 0;
    auto parsed = std::from_chars(
      port_str.data(), port_str.data() + port_str.size(), port);
    if (parsed.ec != std::errc{} || port <= 0 || port > 65535) {
        return std::nullopt;
    }

    // Strip a trailing slash on the prefix so we can later append "/segment".
    std::string prefix{path};
    while (!prefix.empty() && prefix.back() == '/') {
        prefix.pop_back();
    }
    return sr_endpoint{
      .addr
      = net::unresolved_address{ss::sstring{host}, static_cast<uint16_t>(port)},
      .path_prefix = ss::sstring{prefix.data(), prefix.size()},
    };
}

ss::sstring
make_basic_auth_header(std::string_view user, std::string_view password) {
    std::string joined;
    joined.reserve(user.size() + 1 + password.size());
    joined.append(user.data(), user.size());
    joined.push_back(':');
    joined.append(password.data(), password.size());
    auto encoded = bytes_to_base64(
      bytes_view{
        reinterpret_cast<const uint8_t*>(joined.data()), joined.size()});
    return ssx::sformat("Basic {}", encoded);
}

sr_http_client::sr_http_client(sr_endpoint endpoint)
  : _endpoint(std::move(endpoint))
  , _owned_client(
      std::make_unique<http::client>(
        net::base_transport::configuration{.server_addr = _endpoint.addr})) {}

sr_http_client::sr_http_client(
  sr_endpoint endpoint, http::abstract_client& borrowed)
  : _endpoint(std::move(endpoint))
  , _borrowed_client(&borrowed) {}

sr_http_client::~sr_http_client() = default;

http::abstract_client& sr_http_client::client() {
    if (_owned_client) {
        return *_owned_client;
    }
    return *_borrowed_client;
}

ss::future<> sr_http_client::stop() {
    if (_owned_client) {
        co_await _owned_client->shutdown_and_stop();
    }
}

namespace {
boost::beast::string_view as_beast_sv(const ss::sstring& s) {
    return {s.data(), s.size()};
}
boost::beast::string_view as_beast_sv(std::string_view s) {
    return {s.data(), s.size()};
}
} // namespace

bh::request_header<> sr_http_client::build_request(
  bh::verb verb,
  const ss::sstring& relative_path,
  bool include_content_type) const {
    bh::request_header<> req;
    req.method(verb);
    ss::sstring target = _endpoint.path_prefix;
    if (!relative_path.empty() && relative_path.front() != '/') {
        target.append("/", 1);
    }
    target.append(relative_path.data(), relative_path.size());
    req.target(boost::beast::string_view{target.data(), target.size()});
    auto host_hdr = ssx::sformat(
      "{}:{}", _endpoint.addr.host(), _endpoint.addr.port());
    req.set(bh::field::host, as_beast_sv(host_hdr));
    req.set(bh::field::accept, as_beast_sv(kJsonContentType));
    if (include_content_type) {
        req.set(bh::field::content_type, as_beast_sv(kJsonContentType));
    }
    if (_endpoint.basic_auth_user.has_value()) {
        auto auth = make_basic_auth_header(
          *_endpoint.basic_auth_user, _endpoint.basic_auth_pass.value_or(""));
        req.set(bh::field::authorization, as_beast_sv(auth));
    }
    return req;
}

template<typename T, typename Parse>
ss::future<sr_result<T>>
sr_http_client::do_get_json(const ss::sstring& path, Parse parse) {
    auto req = build_request(
      bh::verb::get, path, /*include_content_type=*/false);
    http::downloaded_response resp;
    try {
        resp = co_await client().request_and_collect_response(
          std::move(req), std::nullopt);
    } catch (...) {
        co_return sr_http_error::transport(
          ssx::sformat("GET {} failed: {}", path, std::current_exception()));
    }
    if (resp.status != bh::status::ok) {
        co_return sr_http_error::from_status(
          resp.status,
          ssx::sformat(
            "GET {} returned {}: {}",
            path,
            static_cast<unsigned>(resp.status),
            iobuf_to_string(resp.body)));
    }
    auto body = iobuf_to_string(resp.body);
    co_return parse(body);
}

ss::future<sr_result<ss::sstring>> sr_http_client::do_write_json(
  bh::verb verb, const ss::sstring& path, const ss::sstring& body) {
    auto req = build_request(verb, path, /*include_content_type=*/true);
    // beast/http::client doesn't auto-set Content-Length when the body is
    // streamed via request_and_collect_response, so we must set it
    // explicitly or the server reads zero bytes and complains about a
    // parse error at offset 0.
    auto cl = ssx::sformat("{}", body.size());
    req.set(bh::field::content_length, as_beast_sv(cl));
    iobuf payload;
    payload.append(body.data(), body.size());
    http::downloaded_response resp;
    try {
        resp = co_await client().request_and_collect_response(
          std::move(req), std::move(payload));
    } catch (...) {
        co_return sr_http_error::transport(
          ssx::sformat(
            "{} {} failed: {}",
            bh::to_string(verb),
            path,
            std::current_exception()));
    }
    if (
      resp.status != bh::status::ok && resp.status != bh::status::created
      && resp.status != bh::status::no_content) {
        // Detect the Schema Registry's "subject not in IMPORT mode" error.
        auto err_body = iobuf_to_string(resp.body);
        if (
          status_is_4xx(resp.status)
          && err_body.find("import mode") != ss::sstring::npos) {
            co_return sr_http_error{
              .errc = sr_http_errc::not_in_import_mode,
              .message = ssx::sformat(
                "{} {}: {}", bh::to_string(verb), path, err_body),
              .http_status = resp.status};
        }
        co_return sr_http_error::from_status(
          resp.status,
          ssx::sformat(
            "{} {} returned {}: {}",
            bh::to_string(verb),
            path,
            static_cast<unsigned>(resp.status),
            err_body));
    }
    co_return iobuf_to_string(resp.body);
}

ss::future<sr_result<chunked_vector<ss::sstring>>>
sr_http_client::list_subjects() {
    co_return co_await do_get_json<chunked_vector<ss::sstring>>(
      "/subjects",
      [](const ss::sstring& body) -> sr_result<chunked_vector<ss::sstring>> {
          json::Document doc;
          if (doc.Parse(body).HasParseError() || !doc.IsArray()) {
              return sr_http_error{
                .errc = sr_http_errc::unexpected_response,
                .message = "GET /subjects: expected JSON array"};
          }
          chunked_vector<ss::sstring> out;
          out.reserve(doc.Size());
          for (const auto& v : doc.GetArray()) {
              if (!v.IsString()) {
                  return sr_http_error{
                    .errc = sr_http_errc::unexpected_response,
                    .message = "GET /subjects: array element not a string"};
              }
              out.emplace_back(v.GetString(), v.GetStringLength());
          }
          return out;
      });
}

ss::future<sr_result<chunked_vector<int32_t>>>
sr_http_client::list_versions(const ss::sstring& subject) {
    auto encoded = http::uri_encode(subject, http::uri_encode_slash::yes);
    auto path = ssx::sformat("/subjects/{}/versions", encoded);
    co_return co_await do_get_json<chunked_vector<int32_t>>(
      path, [&](const ss::sstring& body) -> sr_result<chunked_vector<int32_t>> {
          json::Document doc;
          if (doc.Parse(body).HasParseError() || !doc.IsArray()) {
              return sr_http_error{
                .errc = sr_http_errc::unexpected_response,
                .message = ssx::sformat(
                  "GET /subjects/{}/versions: expected JSON array", subject)};
          }
          chunked_vector<int32_t> out;
          out.reserve(doc.Size());
          for (const auto& v : doc.GetArray()) {
              if (!v.IsInt()) {
                  return sr_http_error{
                    .errc = sr_http_errc::unexpected_response,
                    .message = "list_versions: element not int"};
              }
              out.push_back(v.GetInt());
          }
          return out;
      });
}

ss::future<sr_result<pps::stored_schema>> sr_http_client::get_subject_version(
  const ss::sstring& subject, int32_t version) {
    auto encoded = http::uri_encode(subject, http::uri_encode_slash::yes);
    auto path = ssx::sformat("/subjects/{}/versions/{}", encoded, version);
    co_return co_await do_get_json<pps::stored_schema>(
      path, [&](const ss::sstring& body) -> sr_result<pps::stored_schema> {
          json::Document doc;
          if (doc.Parse(body).HasParseError() || !doc.IsObject()) {
              return sr_http_error{
                .errc = sr_http_errc::unexpected_response,
                .message = "get_subject_version: expected JSON object"};
          }
          auto id = get_int(doc, "id");
          auto ver = get_int(doc, "version");
          auto schema_str = get_str(doc, "schema");
          auto schema_type_str = get_str(doc, "schemaType");
          if (!id.has_value() || !ver.has_value() || !schema_str.has_value()) {
              return sr_http_error{
                .errc = sr_http_errc::unexpected_response,
                .message = "get_subject_version: missing id/version/schema"};
          }
          pps::schema_type st = pps::schema_type::avro;
          if (schema_type_str.has_value()) {
              auto parsed = pps::from_string_view<pps::schema_type>(
                *schema_type_str);
              if (!parsed.has_value()) {
                  return sr_http_error{
                    .errc = sr_http_errc::unexpected_response,
                    .message = ssx::sformat(
                      "unknown schemaType: {}", *schema_type_str)};
              }
              st = *parsed;
          }
          // References (optional).
          pps::schema_definition::references refs;
          if (
            auto r_it = doc.FindMember("references");
            r_it != doc.MemberEnd() && r_it->value.IsArray()) {
              for (const auto& r : r_it->value.GetArray()) {
                  if (!r.IsObject()) {
                      continue;
                  }
                  auto name = get_str(r, "name");
                  auto ref_sub = get_str(r, "subject");
                  auto ref_ver = get_int(r, "version");
                  if (
                    !name.has_value() || !ref_sub.has_value()
                    || !ref_ver.has_value()) {
                      continue;
                  }
                  pps::schema_reference sr_ref;
                  sr_ref.name = std::move(*name);
                  sr_ref.sub = pps::context_subject_reference::unqualified(
                    *ref_sub);
                  sr_ref.version = pps::schema_version{*ref_ver};
                  refs.push_back(std::move(sr_ref));
              }
          }
          pps::schema_definition def{
            pps::schema_definition::raw_string{*schema_str},
            st,
            std::move(refs),
            std::nullopt};
          pps::subject_schema ss{
            pps::context_subject::unqualified(subject), std::move(def)};
          pps::stored_schema out{
            .schema = std::move(ss),
            .version = pps::schema_version{*ver},
            .id = pps::schema_id{*id},
            .deleted = pps::is_deleted::no,
          };
          return out;
      });
}

ss::future<sr_result<pps::mode>>
sr_http_client::get_mode(std::optional<ss::sstring> subject) {
    auto path = subject.has_value()
                  ? ssx::sformat(
                      "/mode/{}",
                      http::uri_encode(*subject, http::uri_encode_slash::yes))
                  : ss::sstring{"/mode"};
    co_return co_await do_get_json<pps::mode>(
      path, [&](const ss::sstring& body) -> sr_result<pps::mode> {
          json::Document doc;
          if (doc.Parse(body).HasParseError() || !doc.IsObject()) {
              return sr_http_error{
                .errc = sr_http_errc::unexpected_response,
                .message = "get_mode: expected JSON object"};
          }
          auto mode_str = get_str(doc, "mode");
          if (!mode_str.has_value()) {
              return sr_http_error{
                .errc = sr_http_errc::unexpected_response,
                .message = "get_mode: missing 'mode' field"};
          }
          auto parsed = pps::from_string_view<pps::mode>(*mode_str);
          if (!parsed.has_value()) {
              return sr_http_error{
                .errc = sr_http_errc::unexpected_response,
                .message = ssx::sformat("unknown mode: {}", *mode_str)};
          }
          return *parsed;
      });
}

ss::future<sr_result<pps::compatibility_level>>
sr_http_client::get_compatibility(std::optional<ss::sstring> subject) {
    auto path = subject.has_value()
                  ? ssx::sformat(
                      "/config/{}",
                      http::uri_encode(*subject, http::uri_encode_slash::yes))
                  : ss::sstring{"/config"};
    co_return co_await do_get_json<pps::compatibility_level>(
      path,
      [&](const ss::sstring& body) -> sr_result<pps::compatibility_level> {
          json::Document doc;
          if (doc.Parse(body).HasParseError() || !doc.IsObject()) {
              return sr_http_error{
                .errc = sr_http_errc::unexpected_response,
                .message = "get_compat: expected JSON object"};
          }
          // SR returns "compatibilityLevel"; Confluent CC sometimes returns
          // "compatibility" too. Accept either.
          auto val = get_str(doc, "compatibilityLevel");
          if (!val.has_value()) {
              val = get_str(doc, "compatibility");
          }
          if (!val.has_value()) {
              return sr_http_error{
                .errc = sr_http_errc::unexpected_response,
                .message = "get_compat: missing compatibilityLevel field"};
          }
          auto parsed = pps::from_string_view<pps::compatibility_level>(*val);
          if (!parsed.has_value()) {
              return sr_http_error{
                .errc = sr_http_errc::unexpected_response,
                .message = ssx::sformat("unknown compat: {}", *val)};
          }
          return *parsed;
      });
}

ss::future<sr_result<void>>
sr_http_client::put_mode(std::optional<ss::sstring> subject, pps::mode m) {
    auto path = subject.has_value()
                  ? ssx::sformat(
                      "/mode/{}",
                      http::uri_encode(*subject, http::uri_encode_slash::yes))
                  : ss::sstring{"/mode"};
    auto body = ssx::sformat(R"({{"mode":"{}"}})", pps::to_string_view(m));
    auto resp = co_await do_write_json(bh::verb::put, path, body);
    if (resp.has_error()) {
        co_return resp.assume_error();
    }
    co_return outcome::success();
}

ss::future<sr_result<void>> sr_http_client::put_compatibility(
  std::optional<ss::sstring> subject, pps::compatibility_level lvl) {
    auto path = subject.has_value()
                  ? ssx::sformat(
                      "/config/{}",
                      http::uri_encode(*subject, http::uri_encode_slash::yes))
                  : ss::sstring{"/config"};
    auto body = ssx::sformat(
      R"({{"compatibility":"{}"}})", pps::to_string_view(lvl));
    auto resp = co_await do_write_json(bh::verb::put, path, body);
    if (resp.has_error()) {
        co_return resp.assume_error();
    }
    co_return outcome::success();
}

ss::future<sr_result<int32_t>> sr_http_client::post_schema_with_id(
  const ss::sstring& subject, const pps::stored_schema& s) {
    auto encoded = http::uri_encode(subject, http::uri_encode_slash::yes);
    auto path = ssx::sformat("/subjects/{}/versions", encoded);

    // Build the request body with explicit id + version. This is the
    // IMPORT-mode wire format honored by post_subject_versions.h.
    //
    // Note: we intentionally use rapidjson for the schema string field so
    // that the inlined schema body is correctly JSON-escaped (avro/json
    // schemas contain embedded quotes).
    json::StringBuffer sb;
    json::Writer<json::StringBuffer> w{sb};
    w.StartObject();
    w.Key("id");
    w.Int(s.id());
    w.Key("version");
    w.Int(s.version());
    w.Key("schemaType");
    auto type_sv = pps::to_string_view(s.schema.type());
    w.String(type_sv.data(), type_sv.size());
    {
        // Render the raw schema body as a string. The raw is an iobuf; copy
        // it into a temporary std::string for rapidjson.
        std::string raw;
        const auto& def_iobuf = s.schema.def().raw()();
        raw.reserve(def_iobuf.size_bytes());
        for (const auto& frag : def_iobuf) {
            raw.append(frag.get(), frag.size());
        }
        w.Key("schema");
        w.String(raw.data(), raw.size());
    }
    if (!s.schema.def().refs().empty()) {
        w.Key("references");
        w.StartArray();
        for (const auto& r : s.schema.def().refs()) {
            w.StartObject();
            w.Key("name");
            w.String(r.name.data(), r.name.size());
            w.Key("subject");
            auto sub_s = ssx::sformat("{}", r.sub.sub);
            w.String(sub_s.data(), sub_s.size());
            w.Key("version");
            w.Int(r.version());
            w.EndObject();
        }
        w.EndArray();
    }
    w.EndObject();

    auto resp = co_await do_write_json(
      bh::verb::post, path, ss::sstring{sb.GetString(), sb.GetSize()});
    if (resp.has_error()) {
        co_return resp.assume_error();
    }
    // Response shape: {"id": <id>}
    json::Document doc;
    if (
      doc.Parse(resp.assume_value().data()).HasParseError()
      || !doc.IsObject()) {
        co_return sr_http_error{
          .errc = sr_http_errc::unexpected_response,
          .message = "post_schema_with_id: expected JSON object"};
    }
    auto id = get_int(doc, "id");
    if (!id.has_value()) {
        co_return sr_http_error{
          .errc = sr_http_errc::unexpected_response,
          .message = "post_schema_with_id: missing 'id' in response"};
    }
    co_return *id;
}

} // namespace cluster_link
