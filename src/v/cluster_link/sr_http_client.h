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

#pragma once

#include "base/outcome.h"
#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "container/chunked_vector.h"
#include "http/client.h"
#include "pandaproxy/schema_registry/types.h"
#include "utils/unresolved_address.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <boost/beast/http/status.hpp>

#include <optional>

namespace cluster_link {

/// Endpoint configuration for talking to a remote Schema Registry over HTTP.
///
/// POC scope: HTTP Basic auth only, plaintext HTTP only. mTLS, bearer
/// tokens, and TLS-wrapped transports are deliberately deferred — see
/// docs/plans/2026-05-12-sl-sr-poc-design.md.
struct sr_endpoint {
    /// Scheme is implicit (http://), only host:port + path-prefix today.
    net::unresolved_address addr;
    /// Path prefix to apply to every request, e.g. "/" or "/sr".
    /// Stored without a trailing slash, so a path is built as
    /// `path_prefix + "/" + path_segment`.
    ss::sstring path_prefix;
    std::optional<ss::sstring> basic_auth_user;
    std::optional<ss::sstring> basic_auth_pass;

    /// Construct an sr_endpoint from a URL string of the form
    /// "http://host:port[/path]". Returns std::nullopt on parse failure.
    static std::optional<sr_endpoint> from_url(std::string_view url);
};

/// Encode an HTTP Basic auth header value as "Basic <base64(user:pass)>".
/// Public-ish so it can be unit-tested without standing up an HTTP server.
ss::sstring
make_basic_auth_header(std::string_view user, std::string_view password);

/// Errors returned from sr_http_client methods. All concrete operations
/// map their failure modes onto these.
enum class sr_http_errc {
    /// Connection failed, timed out, or response could not be parsed.
    transport,
    /// Source SR returned 4xx (other than 404).
    bad_request,
    /// Source SR returned 404 for a subject / id that should exist.
    not_found,
    /// Source SR returned 5xx.
    server_error,
    /// Response body was syntactically valid JSON but the shape was wrong.
    unexpected_response,
    /// Source SR rejected a write because the target subject is in
    /// READWRITE / READONLY mode instead of IMPORT.
    not_in_import_mode,
};

std::string_view to_string_view(sr_http_errc);

struct sr_http_error {
    sr_http_errc errc;
    ss::sstring message;
    /// Populated when the underlying HTTP call produced a response.
    std::optional<boost::beast::http::status> http_status;

    static sr_http_error transport(ss::sstring msg) {
        return {.errc = sr_http_errc::transport, .message = std::move(msg)};
    }
    static sr_http_error
    from_status(boost::beast::http::status status, ss::sstring msg);
};

template<typename T>
using sr_result = result<T, sr_http_error>;

/// Thin RPC-style wrapper around http::abstract_client, exposing the
/// Schema Registry endpoints the replicator task needs.
///
/// Owns the http::client lifetime when constructed from a config; can
/// also wrap a borrowed client (for tests with mocks).
///
/// All methods are co_awaitable. Operations are *one-shot* — there is no
/// internal retry; the caller is expected to bound concurrency and
/// retry policy at the task layer.
class sr_http_client {
public:
    /// Construct an owning client that opens a fresh connection to `endpoint`.
    explicit sr_http_client(sr_endpoint endpoint);

    /// Construct a client wrapping a borrowed abstract_client (test seam).
    sr_http_client(sr_endpoint endpoint, http::abstract_client& borrowed);

    sr_http_client(const sr_http_client&) = delete;
    sr_http_client(sr_http_client&&) = delete;
    sr_http_client& operator=(const sr_http_client&) = delete;
    sr_http_client& operator=(sr_http_client&&) = delete;

    ~sr_http_client();

    ss::future<> stop();

    /// GET /subjects
    ss::future<sr_result<chunked_vector<ss::sstring>>> list_subjects();

    /// GET /subjects/{subject}/versions
    ss::future<sr_result<chunked_vector<int32_t>>>
    list_versions(const ss::sstring& subject);

    /// GET /subjects/{subject}/versions/{version}
    /// Returns the full subject_schema body + the schema's global ID.
    ss::future<sr_result<pandaproxy::schema_registry::stored_schema>>
    get_subject_version(const ss::sstring& subject, int32_t version);

    /// GET /mode/{subject} (subject=nullopt -> global mode)
    ss::future<sr_result<pandaproxy::schema_registry::mode>>
    get_mode(std::optional<ss::sstring> subject = std::nullopt);

    /// GET /config/{subject} (subject=nullopt -> global compat)
    ss::future<sr_result<pandaproxy::schema_registry::compatibility_level>>
    get_compatibility(std::optional<ss::sstring> subject = std::nullopt);

    /// PUT /mode/{subject} body {"mode": "IMPORT"} (or READWRITE / READONLY)
    ss::future<sr_result<void>> put_mode(
      std::optional<ss::sstring> subject,
      pandaproxy::schema_registry::mode mode);

    /// PUT /config/{subject} body {"compatibility": "BACKWARD"}
    ss::future<sr_result<void>> put_compatibility(
      std::optional<ss::sstring> subject,
      pandaproxy::schema_registry::compatibility_level lvl);

    /// POST /subjects/{subject}/versions with explicit id + version in body.
    /// This is the IMPORT-mode wire format; the destination SR must be in
    /// IMPORT mode at the subject scope (or globally) or this returns
    /// sr_http_errc::not_in_import_mode.
    ///
    /// Returns the id assigned by the destination. On success it equals the
    /// id passed in `s`.
    ss::future<sr_result<int32_t>> post_schema_with_id(
      const ss::sstring& subject,
      const pandaproxy::schema_registry::stored_schema& s);

private:
    sr_endpoint _endpoint;
    /// Either _owned_client is populated (owning case) or _borrowed_client is
    /// populated (test seam).
    std::unique_ptr<http::client> _owned_client;
    http::abstract_client* _borrowed_client{nullptr};

    http::abstract_client& client();

    /// Build a beast request_header with the given verb, path (relative to
    /// path_prefix), and content-type, applying Basic auth if configured.
    boost::beast::http::request_header<> build_request(
      boost::beast::http::verb verb,
      const ss::sstring& relative_path,
      bool include_content_type) const;

    /// Issue a GET request and parse the JSON body, mapping HTTP errors
    /// to sr_http_error. Caller provides a `parse` function that returns
    /// sr_result<T> given the body as a string.
    template<typename T, typename Parse>
    ss::future<sr_result<T>> do_get_json(const ss::sstring& path, Parse parse);

    /// Issue a write request (PUT/POST) with a JSON body. Returns the raw
    /// response body on success so the caller can parse the response if
    /// needed.
    ss::future<sr_result<ss::sstring>> do_write_json(
      boost::beast::http::verb verb,
      const ss::sstring& path,
      const ss::sstring& body);
};

} // namespace cluster_link
