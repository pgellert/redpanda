# OIDC HTTP proxy Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Route OIDC discovery + JWKS fetches through an operator-configured HTTP forward proxy, so Redpanda brokers in corporate-proxy environments can use OIDC (e.g. Azure AD). Fixes CORE-16095.

**Architecture:** Extend `net::base_transport::configuration` with an optional `proxy_config` sub-struct. `base_transport::do_connect` learns to TCP-connect to the proxy, optionally TLS-wrap that connection (for `https://` proxy URLs), send an HTTP CONNECT to the origin, verify a `200` response, then TLS-wrap to the origin as today. A new cluster config `oidc_http_proxy` is the only caller in v1; OIDC's `make_request` sets the proxy field on the transport configuration when the cluster config is non-empty. Any caller that doesn't set the field is behaviourally unchanged.

**Tech Stack:** C++23 / Seastar / Boost.Beast (HTTP parsing) / gtest for unit tests (optional) / ducktape + mitmproxy + Keycloak for e2e.

**Branch:** `feat/http-proxy` (already checked out, based on `dev`, design doc committed as `f622b00295`).

**Design doc:** `docs/plans/2026-04-21-oidc-http-proxy-design.md` — read first for context.

---

## Task 1: Seastar stream-lifecycle probe (de-risk the CONNECT handshake)

**Why:** The CONNECT helper needs to write bytes to a `ss::connected_socket`, read the response, then hand the socket off to `ss::tls::wrap_client`. The open question is whether `ss::output_stream<char>` returned from `connected_socket::output()` can be used-and-dropped without closing the underlying socket, or whether we need to use lower-level `sink()`/`source()` primitives. Answering this early prevents rewriting the helper halfway through.

**Files:**
- Read: `external/+non_module_dependencies+seastar/include/seastar/net/api.hh` or the Seastar installed-path equivalent for `connected_socket` — to see the public API surface for stream lifecycle.
- Read: any existing Redpanda code that uses `connected_socket::output()` and then moves or TLS-wraps the socket. Likely candidates: grep for `tls::wrap_client` across the tree.

**Step 1: Locate Seastar's connected_socket header**

Run: `find external -name 'api.hh' -path '*seastar*' 2>/dev/null | head -3` to find the Seastar network API header.

Expected: one or more paths under `external/+non_module_dependencies+seastar/` or similar Bazel external path.

**Step 2: Inspect `connected_socket` for sink/source surface**

Read the `connected_socket` class. Look for:
- `data_sink sink()` / `data_source source()` — low-level
- `input_stream<char> input()` / `output_stream<char> output(...)` — buffered high-level
- Whether `output_stream::close()` or destruction closes the underlying sink

**Step 3: Grep the tree for prior art**

Run via the Grep tool: pattern `tls::wrap_client`, type `cpp`. Examine each callsite: does any of them create temporary streams before the TLS wrap? If yes, they've already solved this lifecycle question; mirror the pattern.

**Step 4: Write a short design note in this plan under Task 4**

Record the chosen approach (high-level `input_stream`/`output_stream` with flush-but-no-close, OR low-level `sink()`/`source()`) as a comment at the top of Task 4. This becomes the pattern the helper implements.

**Step 5: No commit** — this task produces no code, just a design decision recorded in Task 4's header comment.

---

## Task 2: Add `proxy_connect_error` exception type

**Files:**
- Modify: `src/v/net/transport.h`

**Step 1: Add the exception type**

Open `src/v/net/transport.h`. Just before `class base_transport` (around line 46), add:

```cpp
/// Thrown when a forward-proxy CONNECT handshake fails, so error messages
/// can name the proxy and the origin instead of producing a generic timeout.
class proxy_connect_error : public std::runtime_error {
public:
    proxy_connect_error(
      const unresolved_address& proxy,
      const unresolved_address& origin,
      std::string_view detail)
      : std::runtime_error(fmt::format(
          "proxy {} failed to CONNECT to origin {}: {}",
          proxy,
          origin,
          detail)) {}
};
```

**Step 2: Ensure `#include <fmt/format.h>` is present**

Check the top of `transport.h`. If `fmt/format.h` is not already included (directly or transitively), add it.

**Step 3: Build sanity-check**

Run: `bazel build //src/v/net:net`

Expected: clean build, no warnings about unused class.

**Step 4: No commit yet** — combined with Task 3 into a single commit.

---

## Task 3: Add `proxy_config` struct to `base_transport::configuration`

**Files:**
- Modify: `src/v/net/transport.h` (the `configuration` struct around lines 49–59, and the private member area around lines 132–137)
- Modify: `src/v/net/transport.cc` (the constructor at lines 58–63)

**Step 1: Extend the `configuration` struct**

In `src/v/net/transport.h`, replace the `configuration` struct (lines 49–59) with:

```cpp
struct configuration {
    unresolved_address server_addr;
    ss::shared_ptr<ss::tls::certificate_credentials> credentials;
    net::metrics_disabled disable_metrics = net::metrics_disabled::no;
    net::public_metrics_disabled disable_public_metrics
      = net::public_metrics_disabled::no;
    /// Optional server name indication (SNI) for TLS connection
    std::optional<ss::sstring> tls_sni_hostname;
    /// Potentially skip wait for EOF after BYE message on TLS session end
    bool wait_for_tls_server_eof = true;

    /// When set, the transport will route the connection through an HTTP
    /// forward proxy. The proxy must accept CONNECT requests addressed to
    /// server_addr. If credentials is non-null, the connection to the
    /// proxy itself is TLS-wrapped before the CONNECT request is sent
    /// (i.e. an https:// proxy URL).
    struct proxy_config {
        unresolved_address address;
        ss::shared_ptr<ss::tls::certificate_credentials> credentials;
        std::optional<ss::sstring> tls_sni_hostname;
    };
    std::optional<proxy_config> proxy;
};
```

**Step 2: Add the `_proxy` private member**

In `src/v/net/transport.h`, in the `private:` section of `base_transport` (around line 126), add after the existing private members:

```cpp
std::optional<configuration::proxy_config> _proxy;
```

**Step 3: Initialize `_proxy` in the constructor**

In `src/v/net/transport.cc`, replace the constructor (lines 58–63) with:

```cpp
base_transport::base_transport(configuration c, seastar::logger* log)
  : _server_addr(c.server_addr)
  , _creds(c.credentials)
  , _tls_sni_hostname(c.tls_sni_hostname)
  , _wait_for_tls_server_eof(c.wait_for_tls_server_eof)
  , _log(log)
  , _proxy(std::move(c.proxy)) {}
```

**Step 4: Build sanity-check**

Run: `bazel build //src/v/net:net //src/v/http:http //src/v/security:security //src/v/cloud_storage_clients:cloud_storage_clients`

Expected: clean build. The new optional field should be default-empty for every existing caller, so no call sites need to change.

**Step 5: Commit Tasks 2 + 3 together**

```bash
git add src/v/net/transport.h src/v/net/transport.cc
git commit -m "net: add optional CONNECT-proxy configuration to base_transport

Extends net::base_transport::configuration with an optional proxy_config
sub-struct and a matching proxy_connect_error exception type. No
behaviour change yet: do_connect still ignores the proxy field. Wiring
the field into do_connect comes in the next commit.

Callers that do not set the proxy field are bit-identical to today."
```

---

## Task 4: Implement the CONNECT helper

**Chosen stream-lifecycle approach (from Task 1 probe):**

Use `connected_socket::output()` + `write()` + `flush()` and let the stream go out of scope **without** calling `close()`. Same for `input()`.

Verified against Seastar source:
- `output_stream` destructor asserts only that `_end == 0 && _zc_len == 0` (iostream.hh:502). After a successful `flush()`, both are zero — destruction is safe.
- `output_stream::close()` explicitly calls `_fd.close()` on the underlying `data_sink`, which closes the socket's output side (iostream-impl.hh:514). We deliberately avoid this.
- `connected_socket` does not expose `sink()`/`source()` publicly — only `input()`/`output()` (net/api.hh:232-237). Stream API is the only path.
- `input_stream` has no `read_until` method; use `read_exactly()` with a byte-by-byte scan, or `consume()` with a stateful consumer. We use a small read-a-byte-at-a-time loop for line reading since CONNECT responses are tiny.

Nested TLS (`ss::tls::wrap_client` on an already-TLS-wrapped `connected_socket`) is supported by the API signature but unverified in the tree. The `https://` proxy path exercises it; if it misbehaves at runtime, the follow-up gtest will catch it.

**Files:**
- Modify: `src/v/net/transport.cc` (add helper in the anonymous namespace around lines 14–54)

**Step 1: Add the CONNECT helper in the anonymous namespace**

In `src/v/net/transport.cc`, inside the anonymous `namespace { ... }` block starting at line 14, add (after `connect_with_timeout`):

```cpp
/// Sends an HTTP CONNECT request over fd and reads the response.
/// Throws proxy_connect_error on non-200 status, malformed response,
/// or transport error. Does not close fd; the caller is expected to
/// continue using it (typically by TLS-wrapping it).
ss::future<> send_connect_and_read_response(
  ss::connected_socket& fd,
  const net::unresolved_address& origin,
  const net::unresolved_address& proxy,
  seastar::logger* log) {
    auto request = fmt::format(
      "CONNECT {}:{} HTTP/1.1\r\n"
      "Host: {}:{}\r\n"
      "\r\n",
      origin.host(),
      origin.port(),
      origin.host(),
      origin.port());

    vlog(log->trace, "Sending CONNECT to proxy {} for origin {}", proxy, origin);

    // Use high-level streams for readability, flush before letting them go
    // out of scope. Do NOT call close() on these streams — that would close
    // the underlying socket's read/write sides, which we want to keep open
    // for the subsequent TLS-to-origin handshake.
    auto out = fd.output();
    co_await out.write(request);
    co_await out.flush();

    auto in = fd.input();

    // Read status line: "HTTP/1.x NNN <reason>\r\n"
    // Using read_until('\n') and checking for the terminating CRLF.
    auto status_line_buf = co_await in.read_until('\n');
    if (status_line_buf.empty()) {
        throw net::proxy_connect_error(
          proxy, origin, "proxy closed connection before sending status line");
    }
    std::string_view status_line(status_line_buf.get(), status_line_buf.size());
    // Trim trailing CRLF
    while (!status_line.empty()
           && (status_line.back() == '\n' || status_line.back() == '\r')) {
        status_line.remove_suffix(1);
    }

    // Parse "HTTP/1.x NNN <reason>"
    // Accept only status 200 (matches Go's strict behaviour).
    if (!status_line.starts_with("HTTP/1.")) {
        throw net::proxy_connect_error(
          proxy,
          origin,
          fmt::format("unexpected status line: {}", status_line));
    }
    // Find first space after version
    auto sp1 = status_line.find(' ');
    if (sp1 == std::string_view::npos) {
        throw net::proxy_connect_error(
          proxy,
          origin,
          fmt::format("malformed status line: {}", status_line));
    }
    auto code_view = status_line.substr(sp1 + 1);
    int status_code = 0;
    auto [_, ec] = std::from_chars(
      code_view.data(), code_view.data() + code_view.size(), status_code);
    if (ec != std::errc{}) {
        throw net::proxy_connect_error(
          proxy,
          origin,
          fmt::format("non-numeric status code in: {}", status_line));
    }
    if (status_code != 200) {
        throw net::proxy_connect_error(
          proxy, origin, fmt::format("status {}", status_line));
    }

    // Discard remaining response headers until the terminating blank line.
    while (true) {
        auto line_buf = co_await in.read_until('\n');
        if (line_buf.empty()) {
            throw net::proxy_connect_error(
              proxy, origin, "proxy closed connection mid-headers");
        }
        std::string_view line(line_buf.get(), line_buf.size());
        while (!line.empty()
               && (line.back() == '\n' || line.back() == '\r')) {
            line.remove_suffix(1);
        }
        if (line.empty()) {
            break; // end of headers
        }
    }

    vlog(log->trace, "CONNECT to {} via proxy {} succeeded", origin, proxy);
}
```

**Step 2: Ensure required headers are included**

In `src/v/net/transport.cc`, after the existing includes, verify the presence of:

```cpp
#include <charconv>      // for std::from_chars
#include <string_view>   // for std::string_view
```

Add them if missing.

**Step 3: Build**

Run: `bazel build //src/v/net:net`

Expected: clean build. If it fails with "input_stream<char>::read_until not found", consult Seastar docs — the actual method may be named differently or need an overload with a size limit.

**Step 4: No commit yet** — combined with Task 5.

---

## Task 5: Wire the proxy into `do_connect`

**Files:**
- Modify: `src/v/net/transport.cc` (`do_connect` at lines 65–115)

**Step 1: Refactor do_connect to handle proxy**

Replace the body of `do_connect` (lines 65–115) with:

```cpp
ss::future<> base_transport::do_connect(clock_type::time_point timeout) {
    if (is_valid() || _dispatch_gate.is_closed()) {
        throw std::runtime_error(fmt::format(
          "cannot do_connect with a valid connection. remote:{}",
          server_address()));
    }
    try {
        base_transport::reset_state();
        reset_state();

        // Resolve the TCP peer. When a proxy is configured, the TCP
        // connection opens to the proxy; the CONNECT handshake reveals
        // the true origin inside that connection. Without a proxy,
        // TCP opens directly to the origin as today.
        const auto& tcp_target = _proxy.has_value()
                                   ? _proxy->address
                                   : server_address();
        auto resolved_address = co_await net::resolve_dns(tcp_target);
        vlog(_log->trace, "Resolved address {}", resolved_address);
        ss::connected_socket fd = co_await connect_with_timeout(
          resolved_address, timeout, _log);

        // If the proxy URL scheme was https://, wrap the TCP socket in
        // TLS with SNI = proxy hostname before any HTTP bytes flow.
        if (_proxy.has_value() && _proxy->credentials) {
            // CORE-14958
            REDPANDA_BEGIN_IGNORE_DEPRECATIONS
            fd = co_await ss::tls::wrap_client(
              _proxy->credentials,
              std::move(fd),
              ss::tls::tls_options{
                .server_name = _proxy->tls_sni_hostname.value_or("")});
            REDPANDA_END_IGNORE_DEPRECATIONS
        }

        // Issue the CONNECT handshake. On success, fd is a tunnel to the
        // origin; on failure, throws proxy_connect_error.
        if (_proxy.has_value()) {
            co_await send_connect_and_read_response(
              fd, server_address(), _proxy->address, _log);
        }

        // TLS to the origin (unchanged from pre-proxy behaviour). This
        // handshake runs inside the CONNECT tunnel when a proxy is in use.
        if (_creds) {
            // CORE-14958
            REDPANDA_BEGIN_IGNORE_DEPRECATIONS
            fd = co_await ss::tls::wrap_client(
              _creds,
              std::move(fd),
              ss::tls::tls_options{
                .wait_for_eof_on_shutdown = _wait_for_tls_server_eof,
                .server_name = _tls_sni_hostname.value_or("")});
            REDPANDA_END_IGNORE_DEPRECATIONS
        }

        _fd = std::make_unique<ss::connected_socket>(std::move(fd));
        if (auto* p = _probe.value_or(nullptr); p != nullptr) {
            p->connection_established();
        }
        _in = _fd->input();

        vassert(
          !_out.has_value() || !_out->is_valid(),
          "destroyed output_stream without stopping");
        _out = net::batched_output_stream(_fd->output());
    } catch (...) {
        auto e = std::current_exception();
        if (auto* p = _probe.value_or(nullptr); p != nullptr) {
            p->connection_error();
        }
        vlog(_log->trace, "Connection error: {}", e);
        std::rethrow_exception(e);
    }

    co_return;
}
```

**Step 2: Build the full transport + dependents**

Run: `bazel build //src/v/net:net //src/v/http:http`

Expected: clean build.

**Step 3: Sanity-check by building a dependent target that uses base_transport**

Run: `bazel build //src/v/security:security //src/v/cloud_storage_clients:cloud_storage_clients`

Expected: clean build. Behaviour for existing callers must be unchanged because `_proxy` defaults to `std::nullopt`.

**Step 4: Commit Tasks 4 + 5**

```bash
git add src/v/net/transport.cc
git commit -m "net: route base_transport through a forward proxy when configured

When base_transport::configuration::proxy is set, do_connect now:

  1. TCP-connects to the proxy (not the origin).
  2. Optionally TLS-wraps to the proxy (for https:// proxy URLs),
     with SNI set to the proxy hostname.
  3. Sends an HTTP CONNECT request for the origin; requires a 200
     response. Throws proxy_connect_error on failure, naming the
     proxy and the origin for actionable diagnostics.
  4. TLS-wraps the tunnelled socket to the origin as today, with
     SNI set to the origin hostname.

Callers that do not set the proxy field are unchanged."
```

---

## Task 6: Add `oidc_http_proxy` cluster config

**Files:**
- Modify: `src/v/config/configuration.h`
- Modify: `src/v/config/configuration.cc`

**Step 1: Locate the existing OIDC config block**

Run via the Grep tool: pattern `oidc_discovery_url`, path `src/v/config`, output mode `content`, `-n` true. Note the line numbers in both `configuration.h` and `configuration.cc`.

**Step 2: Declare the new property in the header**

In `src/v/config/configuration.h`, in the block where other `oidc_*` properties are declared (near `oidc_discovery_url`), add:

```cpp
property<ss::sstring> oidc_http_proxy;
```

**Step 3: Initialize the property in the cc**

In `src/v/config/configuration.cc`, in the initializer list next to the other `oidc_*` entries, add:

```cpp
, oidc_http_proxy(
    *this,
    "oidc_http_proxy",
    "URL of the HTTP forward proxy used for OIDC discovery and JWKS "
    "fetches. Accepts http://host:port or https://host:port. Leave "
    "empty to connect directly.",
    {.needs_restart = needs_restart::no, .visibility = visibility::user},
    "")
```

**Step 4: Build**

Run: `bazel build //src/v/config:config`

Expected: clean build.

**Step 5: Commit**

```bash
git add src/v/config/configuration.h src/v/config/configuration.cc
git commit -m "config: add oidc_http_proxy cluster config

New cluster config that will be consumed in the next commit by the
OIDC service to route discovery and JWKS fetches through an HTTP
forward proxy. The property is live-reloadable (needs_restart::no)
and user-visible."
```

---

## Task 7: Parse and validate the proxy URL helper

**Files:**
- Modify: `src/v/security/oidc_service.cc` (anonymous namespace around line 44)

**Step 1: Add a proxy-URL parse helper in the anonymous namespace**

In `src/v/security/oidc_service.cc`, inside the anonymous `namespace { ... }` block (around line 44, before `return_exception`), add:

```cpp
/// Parses a proxy URL into a net::base_transport::configuration::proxy_config.
/// Accepts http:// and https:// schemes; any other scheme throws.
/// For https:// schemes, the caller must supply TLS credentials for the
/// proxy (typically system trust).
result<net::base_transport::configuration::proxy_config> parse_proxy_url(
  std::string_view url_str,
  ss::shared_ptr<ss::tls::certificate_credentials> system_creds) {
    auto parsed = parse_url(url_str);
    if (parsed.has_error()) {
        return errc::metadata_invalid;
    }
    auto url = std::move(parsed).assume_value();

    net::base_transport::configuration::proxy_config cfg;
    cfg.address = net::unresolved_address{url.host, url.port};

    if (url.scheme == "http") {
        cfg.credentials = nullptr;
    } else if (url.scheme == "https") {
        cfg.credentials = system_creds;
        cfg.tls_sni_hostname.emplace(url.host);
    } else {
        return errc::metadata_invalid;
    }

    return cfg;
}
```

**Step 2: Verify `parse_url` and `net::base_transport::configuration::proxy_config` are visible**

Ensure these headers are included at the top of `oidc_service.cc`:

```cpp
#include "net/transport.h"   // for proxy_config
```

`parse_url` is already available via `security/oidc_url_parser.h` which is already included.

**Step 3: Build**

Run: `bazel build //src/v/security:security`

Expected: clean build. Helper is unused at this point — that's fine; wiring comes in Task 8.

**Step 4: No commit yet** — combined with Task 8.

---

## Task 8: Thread `oidc_http_proxy` binding into the OIDC service

**Files:**
- Modify: `src/v/security/oidc_service.h`
- Modify: `src/v/security/oidc_service.cc`
- Likely modify: any caller that constructs `security::oidc::service` — grep for it.

**Step 1: Locate construction sites of `security::oidc::service`**

Run via the Grep tool: pattern `security::oidc::service\(|make_shared<security::oidc::service>|make_unique<security::oidc::service>`, type `cpp`, output mode `content`.

Expected: the production construction site (likely `src/v/redpanda/application.cc`) plus any test fixtures.

**Step 2: Add the binding to the service header**

In `src/v/security/oidc_service.h`, locate the `service` class constructor declaration. Add a new parameter:

```cpp
config::binding<ss::sstring> http_proxy,
```

in the same position it will occupy in the impl.

**Step 3: Add the binding to `impl` and store it**

In `src/v/security/oidc_service.cc`:

- Add `config::binding<ss::sstring> _http_proxy;` to the `impl` struct's member block (around line 436–457).
- Add `config::binding<ss::sstring> http_proxy` to the `impl` constructor's parameter list (around lines 145–156).
- Add `, _http_proxy(std::move(http_proxy))` to the initializer list.
- Add a `.watch()` wiring like other bindings (around line 181), but instead of re-running `update()` immediately, note this is sufficient because `_http_proxy` is read on every `make_request` call:

```cpp
_http_proxy.watch([this]() {
    ssx::spawn_with_gate(_gate, [this] { return update(); });
});
```

**Step 4: Pass the binding through `service::service`**

In the same file, at `service::service` (around line 459–481), add a `config::binding<ss::sstring> http_proxy` parameter and forward it to the impl.

**Step 5: Update all construction sites**

For each callsite found in Step 1, add a new argument sourcing `config::shard_local_cfg().oidc_http_proxy.bind()` (or the equivalent `config::mock_property` in tests).

**Step 6: Build**

Run: `bazel build //src/v/security:security //src/v/redpanda:redpanda`

Expected: clean build. If any test fixture still passes the old argument count, fix it.

**Step 7: No commit yet** — combined with Task 9.

---

## Task 9: Attach proxy config in `make_request`

**Files:**
- Modify: `src/v/security/oidc_service.cc` (`make_request` at line 373)

**Step 1: Insert the proxy-config resolution into `make_request`**

In `src/v/security/oidc_service.cc`, inside `make_request` (lines 373–434), after the existing TLS-credentials setup block and before the `http::client client{...}` construction (around line 403), add:

```cpp
std::optional<net::base_transport::configuration::proxy_config> proxy_cfg;
if (const auto& proxy_url_str = _http_proxy(); !proxy_url_str.empty()) {
    // TLS-capable system-trust credentials for https:// proxies. We
    // reuse _creds if already built (origin is https, which is always
    // true for OIDC discovery); otherwise build a minimal system-trust
    // credential set.
    if (!_creds) {
        ss::tls::credentials_builder builder;
        builder.set_client_auth(ss::tls::client_auth::NONE);
        const auto& cfg = config::shard_local_cfg();
        builder.set_minimum_tls_version(
          config::from_config(cfg.tls_min_version()));
        builder.set_cipher_string(cfg.tls_v1_2_cipher_suites);
        builder.set_ciphersuites(cfg.tls_v1_3_cipher_suites);
        co_await builder.set_system_trust();
        _creds = co_await net::build_reloadable_credentials_with_probe<
          ss::tls::certificate_credentials>(
          std::move(builder), "oidc_provider", "httpclient");
    }

    auto parsed = parse_proxy_url(proxy_url_str, _creds);
    if (parsed.has_error()) {
        co_await return_exception(
          parsed.assume_error(),
          "invalid oidc_http_proxy: {}",
          proxy_url_str);
    }
    proxy_cfg.emplace(std::move(parsed).assume_value());
}
```

Then change the `http::client` construction to attach the proxy:

```cpp
http::client client{net::base_transport::configuration{
  .server_addr = {url.host, url.port},
  .credentials = is_https ? _creds : nullptr,
  .tls_sni_hostname = tls_host,
  .wait_for_tls_server_eof = false,
  .proxy = std::move(proxy_cfg),
}};
```

**Step 2: Add the startup / first-use log line**

Near the top of `make_request`, add:

```cpp
if (!_http_proxy().empty()) {
    vlog(
      seclog.info,
      "OIDC: routing request to {} via HTTP proxy {}",
      url,
      _http_proxy());
}
```

(Logging at INFO on every request is verbose but matches the cadence of existing WARN messages; if too noisy in practice, downgrade to DEBUG or log only on binding change.)

**Step 3: Build**

Run: `bazel build //src/v/security:security //src/v/redpanda:redpanda`

Expected: clean build.

**Step 4: Quick functional sanity check (no proxy set)**

Run any existing OIDC test to confirm the default path still works:

Run: `bazel test //src/v/security/tests:oidc_authenticator_bench` (or another OIDC test binary that exists — check `ls src/v/security/tests/` first).

Expected: passes (no behaviour change when `_http_proxy()` is empty).

**Step 5: Commit Tasks 7 + 8 + 9**

```bash
git add src/v/security/oidc_service.h src/v/security/oidc_service.cc \
        # plus any application-layer file that updates construction sites
git commit -m "security/oidc: route discovery and JWKS through oidc_http_proxy

When the oidc_http_proxy cluster config is set, the OIDC service
parses the URL (accepting http:// or https:// schemes) and attaches
it to the base_transport configuration for each discovery/JWKS
fetch. An https:// proxy URL triggers a TLS handshake with the
proxy using system trust before the CONNECT request is sent; the
origin TLS handshake runs inside the CONNECT tunnel as before.

Live-reloadable: changing oidc_http_proxy triggers a metadata
refresh without a broker restart."
```

---

## Task 10: Add a `MitmproxyService` ducktape service

**Files:**
- Create: `tests/rptest/services/mitmproxy.py`

**Step 1: Verify mitmproxy install path on ducktape nodes**

Check the ducktape node Docker image or installation scripts to confirm mitmproxy is preinstalled. If not, add an install step to the service's constructor (pip install or apt install).

Run via the Grep tool: pattern `mitmproxy`, path `tests/docker/`. If no matches, mitmproxy needs installation in the ducktape image.

**Step 2: Write the service skeleton**

Create `tests/rptest/services/mitmproxy.py` with:

```python
# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import signal

from ducktape.services.service import Service
from ducktape.utils.util import wait_until

MITMPROXY_PORT = 8888
LOG_PATH = "/var/log/mitmproxy.log"
FLOWS_PATH = "/tmp/mitmproxy.flows"


class MitmproxyService(Service):
    """Runs mitmproxy as a CONNECT-only forward proxy (no TLS
    interception). Exposes proxy_url() that the test configures
    Redpanda to use via the oidc_http_proxy cluster config.

    The --ignore-hosts '.*' flag disables mitmproxy's TLS MITM so
    every CONNECT request is tunnelled through transparently to the
    origin. This is what we want for this test because we only care
    about whether Redpanda speaks CONNECT correctly, not about
    inspecting the TLS contents.
    """

    logs = {
        "mitmproxy_log": {"path": LOG_PATH, "collect_default": True},
        "mitmproxy_flows": {"path": FLOWS_PATH, "collect_default": True},
    }

    def __init__(self, context):
        super().__init__(context, num_nodes=1)

    @property
    def node(self):
        return self.nodes[0]

    def proxy_url(self):
        return f"http://{self.node.account.hostname}:{MITMPROXY_PORT}"

    def start_node(self, node, **kwargs):
        cmd = (
            f"nohup mitmdump "
            f"--mode regular "
            f"--listen-port {MITMPROXY_PORT} "
            f"--ignore-hosts '.*' "
            f"--save-stream-file {FLOWS_PATH} "
            f">{LOG_PATH} 2>&1 &"
        )
        node.account.ssh(cmd)
        wait_until(
            lambda: self._is_listening(node),
            timeout_sec=30,
            err_msg="mitmproxy did not start listening",
        )

    def stop_node(self, node, **kwargs):
        node.account.kill_process("mitmdump", allow_fail=True)

    def clean_node(self, node, **kwargs):
        node.account.ssh(
            f"rm -f {LOG_PATH} {FLOWS_PATH}", allow_fail=True
        )

    def _is_listening(self, node):
        out = node.account.ssh_output(
            f"ss -lnt | grep ':{MITMPROXY_PORT}' || true", allow_fail=True
        )
        return bool(out and out.strip())

    def assert_proxied_host(self, expected_host):
        """Assert that mitmproxy's log shows a CONNECT to the given
        host. Call this after the test workload has run. Parses the
        mitmproxy access log for a line matching 'CONNECT <host>:<port>'.
        """
        log_contents = self.node.account.ssh_output(
            f"cat {LOG_PATH}", allow_fail=True
        )
        if not log_contents:
            raise AssertionError("mitmproxy log is empty")
        text = log_contents.decode() if isinstance(log_contents, bytes) else log_contents
        if f"CONNECT {expected_host}" not in text:
            raise AssertionError(
                f"no CONNECT to {expected_host} found in mitmproxy log:\n{text}"
            )
```

**Step 3: Lint check**

Run: `yapf -i tests/rptest/services/mitmproxy.py` (or whichever formatter rptest uses).

Expected: clean output or in-place formatting.

**Step 4: No commit yet** — combined with Task 11.

---

## Task 11: Write the end-to-end ducktape test

**Files:**
- Create: `tests/rptest/tests/redpanda_oauth_proxy_test.py`
- Reference: `tests/rptest/tests/redpanda_oauth_test.py` (existing OAuth test — use as a template)

**Step 1: Read the existing OAuth test as a template**

Read `tests/rptest/tests/redpanda_oauth_test.py`. Identify the test class, the Keycloak service setup, the OAUTHBEARER Kafka client configuration. Capture the setup idioms needed.

**Step 2: Write the proxy-specific test class**

Create `tests/rptest/tests/redpanda_oauth_proxy_test.py`:

```python
# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark import matrix
from rptest.services.cluster import cluster
from rptest.services.keycloak import KeycloakService, DEFAULT_REALM
from rptest.services.mitmproxy import MitmproxyService
from rptest.tests.redpanda_test import RedpandaTest
# ... additional imports copied from redpanda_oauth_test.py as needed


class OIDCViaProxyTest(RedpandaTest):
    """End-to-end test proving Redpanda's OIDC discovery + JWKS fetch
    succeed when the only path to the IdP is via a forward proxy.

    Setup:
      1. Keycloak is the IdP (pre-existing service).
      2. mitmproxy runs as a CONNECT-only forward proxy.
      3. iptables on the Redpanda node DROPs direct egress to the
         Keycloak port — forcing mitmproxy to be the only working path.
      4. Redpanda is configured with cluster.oidc_http_proxy pointing
         at mitmproxy.

    Asserts:
      - A Kafka client using OAUTHBEARER with a Keycloak-issued token
        can produce to a topic (OIDC validation succeeded, which means
        discovery + JWKS fetch traversed mitmproxy).
      - The mitmproxy access log contains a CONNECT entry for the
        Keycloak hostname.
    """

    def __init__(self, test_context):
        super().__init__(
            test_context=test_context,
            num_brokers=1,
            # Do not set oidc_http_proxy here; set it in the test body
            # so we can compute the mitmproxy URL first.
        )
        self.keycloak = KeycloakService(test_context, realm=DEFAULT_REALM)
        self.mitmproxy = MitmproxyService(test_context)

    def setUp(self):
        self.keycloak.start()
        self.mitmproxy.start()
        super().setUp()

    @cluster(num_nodes=4)
    def test_oidc_discovery_via_http_proxy(self):
        # 1. Configure Keycloak: create a realm, user, client. Copy
        #    patterns from redpanda_oauth_test.py.
        kc_node = self.keycloak.nodes[0]

        # 2. Block direct egress from each Redpanda node to the
        #    Keycloak port. Forces oidc_http_proxy to be the only path.
        for broker_node in self.redpanda.nodes:
            broker_node.account.ssh(
                f"sudo iptables -A OUTPUT -d {kc_node.account.hostname} "
                f"-p tcp --dport {self.keycloak.https_port} "
                f"-j DROP",
                allow_fail=False,
            )

        try:
            # 3. Apply cluster config.
            discovery_url = self.keycloak.get_discovery_url()
            self.redpanda.set_cluster_config({
                "oidc_discovery_url": discovery_url,
                "oidc_http_proxy": self.mitmproxy.proxy_url(),
                "sasl_mechanisms": ["SCRAM", "OAUTHBEARER"],
                "http_authentication": ["OIDC", "BASIC"],
                # plus any audience/principal_mapping required by the test
            })

            # 4. Wait for OIDC metadata to be fetched successfully.
            #    The service logs "Error updating metadata" on failure;
            #    absence of that after the refresh interval means success.
            self.redpanda.wait_until(
                lambda: self._oidc_metadata_fetched(),
                timeout_sec=30,
                err_msg="OIDC metadata was never fetched",
            )

            # 5. Obtain a token from Keycloak and authenticate via OAUTHBEARER.
            #    Reuse whatever helper redpanda_oauth_test.py uses.
            #    Assert the Kafka produce succeeds.
            token = self.keycloak.get_access_token()
            producer = self._make_oauthbearer_producer(token)
            producer.produce("test-topic", b"payload")
            producer.flush(timeout_sec=10)

            # 6. Confirm mitmproxy saw the CONNECT.
            self.mitmproxy.assert_proxied_host(kc_node.account.hostname)

        finally:
            # Restore network
            for broker_node in self.redpanda.nodes:
                broker_node.account.ssh(
                    f"sudo iptables -D OUTPUT -d {kc_node.account.hostname} "
                    f"-p tcp --dport {self.keycloak.https_port} "
                    f"-j DROP",
                    allow_fail=True,
                )

    def _oidc_metadata_fetched(self):
        """Returns True if no recent 'Error updating metadata' log
        lines appear since the proxy was configured."""
        # Implementation: use redpanda.search_log_all() for the error
        # string and the success string; the service logs
        # "Error updating metadata" on failure.
        # See redpanda_oauth_test.py for the existing pattern.
        ...

    def _make_oauthbearer_producer(self, token):
        # Copy from redpanda_oauth_test.py.
        ...
```

**Note:** several helpers (`get_discovery_url`, `get_access_token`, `_make_oauthbearer_producer`) are likely already in `KeycloakService` or `redpanda_oauth_test.py`. Cross-reference and reuse rather than reimplementing.

**Step 3: Lint/format check**

Run: `yapf -i tests/rptest/tests/redpanda_oauth_proxy_test.py`

Expected: clean output.

**Step 4: Commit Tasks 10 + 11**

```bash
git add tests/rptest/services/mitmproxy.py \
        tests/rptest/tests/redpanda_oauth_proxy_test.py
git commit -m "tests/rptest: add mitmproxy service and OIDC-via-proxy e2e test

New MitmproxyService runs mitmproxy as a CONNECT-only forward
proxy (TLS interception disabled via --ignore-hosts '.*'),
exposing proxy_url() for test configuration.

OIDCViaProxyTest:
  - Keycloak as IdP.
  - iptables DROP on Redpanda nodes for direct egress to Keycloak,
    forcing mitmproxy as the only path.
  - Redpanda configured with oidc_http_proxy = mitmproxy URL.
  - Asserts Kafka OAUTHBEARER authentication succeeds AND
    mitmproxy logs show a CONNECT to the Keycloak hostname.

The iptables DROP is load-bearing: without it, a regression where
Redpanda silently ignores oidc_http_proxy and connects directly
would pass the test."
```

---

## Task 12: Run the ducktape test locally

**Files:** none; this is a run-and-verify task.

**Step 1: Identify the ducktape command**

Run via the Grep tool: pattern `ducktape --globals`, path `tests/`, output mode `content`. Reference existing runbook / README patterns.

Alternatively, check `tests/docker/run_tests.sh` or `tests/run.py` if present.

**Step 2: Build the Redpanda binary in a debug/ubsan mode appropriate for ducktape**

Run: `bazel build --config=debug //:redpanda`

Expected: build completes. Note the output path.

**Step 3: Run the new test**

Run: the appropriate ducktape command for this one test. Rough form:

```bash
cd tests/ && ./run.py --test-path rptest.tests.redpanda_oauth_proxy_test.OIDCViaProxyTest
```

(Exact form depends on the runbook — adjust per what Step 1 turned up.)

Expected: test passes.

**Step 4: If the test fails:**

- Check Redpanda logs on the broker node for `proxy_connect_error` or `OIDC: routing request to` lines. If neither appears, the config binding isn't being read.
- Check mitmproxy log. If empty, Redpanda isn't talking to mitmproxy — verify iptables rules, verify `oidc_http_proxy` is actually set, verify the proxy URL.
- Check Keycloak log for the discovery request. If present, it's flowing; OIDC-side failures are downstream.

Iterate on the code until the test passes.

**Step 5: No commit** — this is a verification task.

---

## Task 13 (optional, per user direction): gtest unit tests

Skip in v1 per user direction ("sprinkle in some gtest unit tests as appropriate later"). If needed later, the units worth covering are:

- `parse_proxy_url` — valid http, valid https, bogus scheme, malformed URL.
- `send_connect_and_read_response` — via a tiny in-process TCP listener acting as a mock proxy. Exercise: success (200), authentication-required (407), malformed status line, early EOF.

Document location suggestion: `src/v/net/tests/proxy_connect_test.cc` + `src/v/security/tests/oidc_proxy_url_parse_test.cc`.

---

## Rollout notes for the PR

Include in the PR description:

- Link to CORE-16095.
- Short note that Console already shipped its equivalent (UX-995); this is the broker-side counterpart.
- Scope explicit: OIDC-only in v1. Transport-layer field is ready for other callers as opt-in follow-ups.
- Upgrade guidance: existing customers using env vars need to migrate to the new `oidc_http_proxy` cluster config; no silent env-var fallback in v1.
- Linked customer-facing notes:
  - Customers should set `NO_PROXY` (for rpk) to include in-cluster admin-API hostnames, independent of this fix (explains the separate "Access violation" issue surfaced in the Slack thread).
  - Known observability follow-up: OAUTHBEARER SASL error currently says "Invalid credentials" even when root cause is JWKS unreachable. Separate ticket.
