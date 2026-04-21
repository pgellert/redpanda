# OIDC HTTP proxy support — design (v1)

## Problem

Redpanda's internal HTTP client used by the OIDC service (`src/v/security/oidc_service.cc`) has no proxy awareness. In enterprise environments where outbound HTTPS requires a forward proxy, the broker times out when fetching the OIDC discovery document and JWKS, producing:

```
WARN  http - make_request timed-out connection attempt .../.well-known/openid-configuration
ERROR security - Error updating metadata: Failed to retrieve metadata (timed out)
ERROR security - Error updating jwks: jwks_uri is not set
```

Customer reported in CORE-16095 (self-hosted, Azure AD + corporate proxy). Blocks OIDC in any deployment requiring outbound proxy. Unrelated Redpanda Console was fixed separately in UX-995; this ticket is the broker counterpart.

## Scope (v1)

### In

- Single new cluster config: **`oidc_http_proxy`** (string, default empty).
- Value accepts URL form: `http://host:port` or `https://host:port`.
- Implementation as optional `proxy` field on `net::base_transport::configuration`; OIDC's `make_request` is the only caller that sets it in v1.
- CONNECT tunneling for HTTPS origins (the only origin scheme OIDC uses, per OIDC Core §15.2).
- Both proxy URL schemes supported:
    - `http://proxy` — plaintext TCP to proxy → `CONNECT` → TLS to origin.
    - `https://proxy` — TLS to proxy (system trust) → `CONNECT` → TLS to origin (nested TLS).
- Dedicated `proxy_connect_error` exception naming the proxy and the failing origin, replacing the opaque `timed_out_error` seen today.
- One ducktape test exercising plaintext proxy via mitmproxy in CONNECT-only mode, against Keycloak.

### Out (deferred as follow-ups)

- Environment-variable fallback (`HTTP_PROXY` / `HTTPS_PROXY` / `NO_PROXY`). Cluster config is the only input in v1.
- `NO_PROXY` / bypass list. Single config key is either-or.
- Proxy authentication (`Proxy-Authorization`). Customer does not use it.
- Per-proxy trust anchors. System trust only in v1.
- Generalizing the feature to other `http::client` callers (S3, Azure Blob, metrics reporter, cloud-roles, Iceberg REST). The transport-layer field is ready for them; each is a separate opt-in ticket.
- ducktape coverage of the `https://` proxy scheme. Covered by a gtest; ducktape stays plaintext-proxy for simplicity.

## Design

### Config surface

Add `property<ss::sstring> oidc_http_proxy` to `src/v/config/configuration.{h,cc}`, in the existing OIDC block. `needs_restart::no`, `visibility::user`, default `""`. Validation deferred to use-site so that URL-parse errors surface as actionable log messages rather than generic config-validation failures.

### Transport layer

Extend `net::base_transport::configuration` with:

```cpp
struct proxy_config {
    unresolved_address address;
    // null = plaintext proxy; non-null = TLS proxy (https://)
    ss::shared_ptr<ss::tls::certificate_credentials> credentials;
    std::optional<ss::sstring> tls_sni_hostname;
};
std::optional<proxy_config> proxy;
```

Modify `net::base_transport::do_connect` (`src/v/net/transport.cc:65`) to consult `_proxy`:

1. Resolve and TCP-connect to the proxy if set, else to the origin (existing behavior).
2. If `proxy->credentials` set, TLS-wrap the socket with SNI = proxy hostname. Reuses the existing `ss::tls::wrap_client` pattern at lines 84–91.
3. If proxy set, send a CONNECT request for the origin, read response, verify `200`. Throw `proxy_connect_error` on non-200 or transport error, with the proxy URL and origin address in the message.
4. Existing origin TLS-wrap (lines 82–92) runs unchanged on the post-CONNECT socket. SNI is the origin, as today.

The ordering is non-commutative and matches RFC 9110 §9.3.6:

- Plaintext proxy, HTTPS origin: TCP → CONNECT → TLS(origin) → HTTP
- TLS proxy, HTTPS origin: TCP → TLS(proxy) → CONNECT → TLS(origin) → HTTP

### OIDC wiring

Thread a new `config::binding<ss::sstring>` for `oidc_http_proxy` into `security::oidc::service::impl`. In `make_request` (`oidc_service.cc:373`):

- If empty, unchanged behavior.
- If non-empty, parse as URL, validate scheme is `http` or `https`, construct `base_transport::configuration::proxy_config` accordingly, and set it on the transport config before constructing `http::client`. The origin credentials (`_creds`) and origin SNI (`tls_host`) are unchanged.

For `https://` proxies, reuse the existing `_creds` (which already builds with system trust and no client auth) — it works for any TLS-capable peer the system trusts. If `_creds` is not yet initialized (first request, no HTTPS origin seen yet), build one with the same semantics as today's lines 378–401.

Log once at INFO on first use: `OIDC: HTTP proxy configured for discovery/JWKS: {url}`. Log the `proxy_connect_error` at WARN with full detail.

### CONNECT helper

New free function in `src/v/net/`, roughly:

```cpp
ss::future<void> send_connect_and_read_response(
    ss::connected_socket& fd,
    const unresolved_address& origin,
    const unresolved_address& proxy,  // for error messages only
    seastar::logger* log);
```

Writes the CONNECT request-line + `Host:` header + blank line. Reads the response status line and discards headers until double-CRLF. Throws `proxy_connect_error` on malformed response, non-200 status, or IO error. Accepts exactly status 200 (matching Go's strict behavior — real proxies universally return 200, and matching Go keeps us consistent with rpk/Console).

## Test strategy

### ducktape end-to-end (required for merge)

New service `tests/rptest/services/mitmproxy.py`: installs mitmproxy on a ducktape node, launches with `--mode regular` and `--ignore-hosts '.*'` so all CONNECTs are passed through as blind tunnels without TLS interception. Exposes `proxy_url()` returning `http://<node>:<port>`.

New test `tests/rptest/tests/redpanda_oauth_proxy_test.py`, one test method `test_oidc_discovery_via_http_proxy`:

1. Start `KeycloakService` with a pre-seeded realm.
2. Start `MitmproxyService`.
3. iptables DROP rule on the Redpanda node for direct egress to Keycloak's IP/port — forces the proxy to be the only path.
4. Start Redpanda with `cluster.oidc_http_proxy = <mitmproxy URL>`.
5. Connect a Kafka client using OAUTHBEARER with a Keycloak-issued token; assert produce succeeds.
6. Assert the mitmproxy flow log contains a CONNECT entry for the Keycloak host.

The iptables DROP is the load-bearing assertion. Without it, a bug where Redpanda silently ignores the proxy config and connects directly would still pass the test.

### Unit tests (optional follow-up)

gtest coverage for:

- Proxy URL parsing — valid/invalid schemes, malformed URLs.
- CONNECT framing — request format, status line parsing, header discard loop.
- Nested TLS sequencing (covers `https://` proxy path without needing mitmproxy TLS setup).

Not required for initial PR per agreement; can be added in a follow-up commit or separate PR.

## Risks and mitigations

- **Seastar stream lifecycle around CONNECT.** The CONNECT helper must read/write on a `ss::connected_socket` that will subsequently be moved into `ss::tls::wrap_client`. `output_stream::close()` closes the underlying socket, which is the wrong behavior mid-handshake. Options: (a) flush but do not close the temporary stream and let it go out of scope; (b) use lower-level `sink()` / `source()`. Pick during implementation based on which composes cleanly with surrounding code. Fallback plan: do the proxy handshake outside `base_transport` in OIDC and add a new constructor on `http::client` taking a pre-connected socket.
- **Nested TLS (`https://` proxy + HTTPS origin).** Verified mechanically possible via `ss::tls::wrap_client` over an already-wrapped `connected_socket`, but not exercised in the tree today. Covered by gtest (not ducktape) to keep ducktape simple; any issue surfaces early in unit tests.
- **Config hot-reload.** `oidc_http_proxy` is a `config::binding` with `needs_restart::no`; OIDC's existing watch pattern for `_discovery_url` is mirrored so a proxy URL change triggers a fresh `update()`. No broker restart required.
- **Metrics stability.** `security_idp_latency_seconds` and `security_idp_errors_total` labels continue to carry the IdP hostname, not the proxy. Proxy on/off is transparent to dashboards.

## Commit structure

Four atomic commits on `feat/http-proxy`:

1. `net: add optional CONNECT-proxy support to base_transport` — transport.h/cc, proxy_connect_error type.
2. `config: add oidc_http_proxy cluster config` — configuration.{h,cc}.
3. `security/oidc: route discovery and JWKS through oidc_http_proxy` — oidc_service.cc.
4. `tests/rptest: add mitmproxy service and OIDC-via-proxy e2e test` — mitmproxy.py, redpanda_oauth_proxy_test.py.

Each is reviewable in isolation. The first is the most scrutinized (transport-layer change); the rest are additive.

## Follow-ups

- **Cross-field commit-time validator for `oidc_http_proxy` vs `oidc_discovery_url` scheme compatibility.** v1 rejects the `oidc_http_proxy` set + `oidc_discovery_url` http:// combination at request time (in `make_request`), which means the config commits cluster-wide and first surfaces as repeated error logs on the next OIDC refresh. A commit-time cross-field validator would reject the bad combination at `rpk cluster config set` time before replication. Adding this requires plumbing into `configuration` that lets a single-field validator read sibling property values, or a post-commit invariants pass. Flagged by adversarial review rounds 6–7.
- Env-var fallback (`HTTP_PROXY` / `HTTPS_PROXY` / `NO_PROXY`).
- `Proxy-Authorization` Basic-auth support.
- Absolute-form (RFC 9112 §3.2.2) HTTP request rewriting so plaintext OIDC origins can also be proxied (v1 requires https:// origin + proxy).
- Opt-in of other callers: metrics reporter, Iceberg REST catalog, AWS STS refresh, Azure AKS federated credentials, cloud storage (S3/ABS). Each is a one-line wiring change on the caller side; `net::base_transport::configuration::proxy` is already the opt-in surface.
- gtest unit coverage for CONNECT framing, nested TLS, oversized-header rejection, classified retry behaviour, and preservation-of-last-proxy-error-on-deadline.
- Docs: "OIDC behind a corporate proxy" operator guide, including guidance on `NO_PROXY` for rpk and in-cluster admin-API hostnames (customer-reported issue outside this ticket's scope).
- Observability fix (separate ticket): distinguish "JWKS unavailable" from "signature invalid" from "audience mismatch" in the OAUTHBEARER SASL error path.
