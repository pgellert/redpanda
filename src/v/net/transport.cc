#include "net/transport.h"

#include "base/compiler_utils.h"
#include "base/vassert.h"
#include "base/vlog.h"
#include "net/dns.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/util/later.hh>

#include <charconv>
#include <string_view>
#include <system_error>

namespace {

class timed_out_error : public ss::timed_out_error {
public:
    explicit timed_out_error(ss::sstring msg)
      : _msg{std::move(msg)} {}
    const char* what() const noexcept override { return _msg.c_str(); }

private:
    ss::sstring _msg;
};

ss::future<ss::connected_socket> connect_with_timeout(
  const seastar::socket_address& address,
  net::clock_type::time_point timeout,
  seastar::logger* log) {
    auto socket = ss::make_lw_shared<ss::socket>(ss::engine().net().socket());
    auto f = socket->connect(address).finally([socket] {});
    return ss::with_timeout(timeout, std::move(f))
      .handle_exception([socket, address, log](const std::exception_ptr& e) {
          try {
              std::rethrow_exception(e);
          } catch (const ss::timed_out_error& ex) {
              socket->shutdown();
              return ss::make_exception_future<ss::connected_socket>(
                timed_out_error(
                  ssx::sformat("connection to {} - {}", address, e)));
          } catch (const std::system_error& ex) {
              socket->shutdown();
              return ss::make_exception_future<ss::connected_socket>(
                std::system_error(
                  ex.code(), fmt::format("connection to {}", address)));
          } catch (...) {
              vlog(log->trace, "error connecting to {} - {}", address, e);
              socket->shutdown();
              return ss::make_exception_future<ss::connected_socket>(e);
          }
      });
}

/// input_stream consumer that parses an HTTP CONNECT response in a single
/// pass. Stops as soon as the terminating blank line is seen and reports
/// whether any bytes arrived past it.
///
/// The "no post-terminator bytes" invariant is load-bearing: because the
/// CONNECT response parser uses a temporary input_stream and the socket is
/// then moved into ss::tls::wrap_client for the origin TLS handshake, any
/// bytes that land in the input_stream's internal buffer past \r\n\r\n are
/// lost when the stream is destroyed. For our OIDC use case the origin
/// protocol (TLS ClientHello) is client-speaks-first, so the proxy cannot
/// have forwarded any origin bytes by the time we read the CONNECT reply
/// and this never fires in practice. If a future caller reuses this helper
/// for a server-speaks-first protocol, had_post_terminator_bytes will flag
/// the corruption risk rather than silently break the inner handshake.
struct connect_response_parser {
    enum class phase { in_status, in_headers, done };
    phase state = phase::in_status;
    ss::sstring current_line;
    ss::sstring status_line;
    /// Concatenated header lines, bounded, captured for non-2xx diagnostics.
    ss::sstring headers_context;
    static constexpr size_t max_headers_bytes = 512;
    /// true once we've seen the blank line that terminates the header block.
    bool saw_terminator = false;
    /// true if `consume()` handed us bytes beyond the blank line — see the
    /// class comment.
    bool had_post_terminator_bytes = false;

    using result_t = ss::consumption_result<char>;

    ss::future<result_t> operator()(ss::temporary_buffer<char> buf) {
        if (buf.empty()) {
            // EOF. Stop; caller inspects saw_terminator.
            return ss::make_ready_future<result_t>(
              ss::stop_consuming<char>({}));
        }
        size_t i = 0;
        while (i < buf.size() && state != phase::done) {
            char c = buf.get()[i++];
            current_line.append(&c, 1);
            if (
              current_line.size() >= 2
              && current_line[current_line.size() - 2] == '\r'
              && current_line[current_line.size() - 1] == '\n') {
                current_line.resize(current_line.size() - 2);
                if (state == phase::in_status) {
                    status_line = std::move(current_line);
                    current_line.resize(0);
                    state = phase::in_headers;
                } else {
                    if (current_line.empty()) {
                        state = phase::done;
                        saw_terminator = true;
                    } else if (headers_context.size() < max_headers_bytes) {
                        if (!headers_context.empty()) {
                            headers_context.append("; ", 2);
                        }
                        auto remaining = max_headers_bytes
                                         - headers_context.size();
                        auto to_copy = std::min(current_line.size(), remaining);
                        headers_context.append(current_line.data(), to_copy);
                    }
                    current_line.resize(0);
                }
            }
        }
        if (state == phase::done) {
            had_post_terminator_bytes = (i < buf.size());
            return ss::make_ready_future<result_t>(
              ss::stop_consuming<char>(buf.share(i, buf.size() - i)));
        }
        return ss::make_ready_future<result_t>(ss::continue_consuming{});
    }
};

/// Sends an HTTP CONNECT request over fd and reads the response.
/// Throws proxy_connect_error on non-200 status, malformed response,
/// transport error, or if the proxy sent data past the CONNECT response
/// terminator (which would corrupt the subsequent tunneled protocol).
/// Does not close fd; the caller is expected to continue using it
/// (typically by TLS-wrapping it).
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

    vlog(
      log->trace, "Sending CONNECT to proxy {} for origin {}", proxy, origin);

    // Note on stream lifecycle: connected_socket::output() enables
    // batch_flushes, so flush() returns a ready future immediately and the
    // actual send is deferred to the reactor's flush poller, leaving
    // _in_batch non-empty. The output_stream destructor asserts !_in_batch,
    // and calling close() would shut down the socket's write side
    // (SHUT_WR), which we cannot do — the subsequent TLS-to-origin
    // handshake still needs to write. We therefore flush, then
    // co_await ss::yield() so the flush poller gets a tick to drain the
    // batch before the stream falls out of scope.
    auto out = fd.output();
    co_await out.write(request);
    co_await out.flush();
    co_await ss::yield();

    // Parse the CONNECT response via consume(). A single pass over the
    // bytes that arrive on the socket, stopping exactly at the blank-line
    // terminator, with explicit detection of any bytes past it. See the
    // connect_response_parser class comment for why post-terminator bytes
    // are treated as a fatal error for this helper.
    auto in = fd.input();
    connect_response_parser parser;
    co_await in.consume(parser);

    if (!parser.saw_terminator) {
        throw net::proxy_connect_error(
          proxy,
          origin,
          parser.status_line.empty()
            ? "proxy closed connection before sending status line"
            : "proxy closed connection mid-headers");
    }

    if (parser.had_post_terminator_bytes) {
        // See class-level comment: this would silently corrupt the inner
        // handshake because the bytes are buffered in `in` and lost when
        // `in` is destroyed. Fail explicitly so operators see the real
        // cause instead of an opaque TLS error.
        throw net::proxy_connect_error(
          proxy,
          origin,
          "proxy sent data past CONNECT response terminator; tunneled "
          "protocol would be corrupted (helper assumes client-speaks-first "
          "origin)");
    }

    // Parse "HTTP/1.x NNN <reason>". Accept only status 200 (matches Go's
    // strict behaviour; real proxies universally return 200).
    std::string_view sl(parser.status_line);
    if (!sl.starts_with("HTTP/1.")) {
        throw net::proxy_connect_error(
          proxy,
          origin,
          fmt::format("unexpected status line: {}", parser.status_line));
    }
    auto sp1 = sl.find(' ');
    if (sp1 == std::string_view::npos) {
        throw net::proxy_connect_error(
          proxy,
          origin,
          fmt::format("malformed status line: {}", parser.status_line));
    }
    auto code_view = sl.substr(sp1 + 1);
    int status_code = 0;
    auto [_, ec] = std::from_chars(
      code_view.data(), code_view.data() + code_view.size(), status_code);
    if (ec != std::errc{}) {
        throw net::proxy_connect_error(
          proxy,
          origin,
          fmt::format("non-numeric status code in: {}", parser.status_line));
    }
    if (status_code != 200) {
        throw net::proxy_connect_error(
          proxy,
          origin,
          parser.headers_context.empty()
            ? fmt::format("status {}", parser.status_line)
            : fmt::format(
                "status {}; headers: {}",
                parser.status_line,
                parser.headers_context));
    }

    vlog(log->trace, "CONNECT to {} via proxy {} succeeded", origin, proxy);
}

} // namespace

namespace net {

base_transport::base_transport(configuration c, seastar::logger* log)
  : _server_addr(c.server_addr)
  , _creds(c.credentials)
  , _tls_sni_hostname(c.tls_sni_hostname)
  , _wait_for_tls_server_eof(c.wait_for_tls_server_eof)
  , _log(log)
  , _proxy(std::move(c.proxy)) {}

ss::future<> base_transport::do_connect(clock_type::time_point timeout) {
    // hold invariant of having an always valid dispatch gate
    // and make sure we don't have a live connection already
    if (is_valid() || _dispatch_gate.is_closed()) {
        throw std::runtime_error(
          fmt::format(
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
        const auto& tcp_target = _proxy.has_value() ? _proxy->address
                                                    : server_address();
        auto resolved_address = co_await net::resolve_dns(tcp_target);
        vlog(_log->trace, "Resolved address {}", resolved_address);
        ss::connected_socket fd = co_await connect_with_timeout(
          resolved_address, timeout, _log);

        // If the proxy URL scheme was https://, wrap the TCP socket in
        // TLS with SNI = proxy hostname before any HTTP bytes flow.
        // Applies the caller's timeout so a stalled proxy TLS handshake
        // fails fast rather than wedging the auth control-plane.
        if (_proxy.has_value() && _proxy->credentials) {
            // CORE-14958
            REDPANDA_BEGIN_IGNORE_DEPRECATIONS
            fd = co_await ss::with_timeout(
              timeout,
              ss::tls::wrap_client(
                _proxy->credentials,
                std::move(fd),
                ss::tls::tls_options{
                  .server_name = _proxy->tls_sni_hostname.value_or("")}));
            REDPANDA_END_IGNORE_DEPRECATIONS
        }

        // Issue the CONNECT handshake. On success, fd is a tunnel to the
        // origin; on failure, throws proxy_connect_error. The timeout
        // guards against proxies that accept TCP and then stall.
        if (_proxy.has_value()) {
            co_await ss::with_timeout(
              timeout,
              send_connect_and_read_response(
                fd, server_address(), _proxy->address, _log));
        }

        // TLS to the origin (unchanged from pre-proxy behaviour). This
        // handshake runs inside the CONNECT tunnel when a proxy is in use.
        // The same deadline bounds the origin TLS handshake so a stalled
        // origin (or proxy that opened the tunnel but then wedges the
        // inner handshake) still surfaces a timeout rather than hanging.
        if (_creds) {
            // CORE-14958
            REDPANDA_BEGIN_IGNORE_DEPRECATIONS
            fd = co_await ss::with_timeout(
              timeout,
              ss::tls::wrap_client(
                _creds,
                std::move(fd),
                ss::tls::tls_options{
                  .wait_for_eof_on_shutdown = _wait_for_tls_server_eof,
                  .server_name = _tls_sni_hostname.value_or("")}));
            REDPANDA_END_IGNORE_DEPRECATIONS
        }
        _fd = std::make_unique<ss::connected_socket>(std::move(fd));
        if (auto* p = _probe.value_or(nullptr); p != nullptr) {
            p->connection_established();
        }
        _in = _fd->input();

        // Never implicitly destroy a live output stream here: output streams
        // are only safe to destroy after/during stop()
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

void base_transport::set_keepalive_parameters(
  const ss::net::keepalive_params& params) {
    if (_fd) {
        _fd->set_keepalive_parameters(params);
    }
}

void base_transport::set_keepalive(bool keepalive) {
    if (_fd) {
        _fd->set_keepalive(keepalive);
    }
}

ss::future<>
base_transport::connect(clock_type::time_point connection_timeout) {
    // in order to hold concurrency correctness invariants we must guarantee 3
    // things before we attempt to send a payload:
    // 1. there are no background futures waiting
    // 2. the _dispatch_gate() is open
    // 3. the connection is valid
    //
    return stop().then([this, connection_timeout] {
        _dispatch_gate = {};
        return do_connect(connection_timeout);
    });
}

ss::future<> base_transport::stop() {
    fail_outstanding_futures();

    co_await _dispatch_gate.close();

    // We must call stop() on our output stream, because
    // seastar::output_stream may not be safely destroyed without a call to
    // close(), and this class may be destroyed after stop() is called.

    try {
        if (_out.has_value()) {
            co_await _out->stop();
        }
    } catch (...) {
        // Closing the output stream can throw bad pipe if
        // it had unflushed bytes, as we already closed FD.
        vlog(
          _log->debug,
          "Exception while stopping transport: {}",
          std::current_exception());
    }

    // Set _out to nullopt here, so that do_connect can assert that
    // it isn't dropping an un-stopped output stream when it
    // assigns to _out. Note that this happens even if _out->stop()
    // above throws: because the most common case is that the flush
    // implied by stop(), but close() still closes the stream in that
    // case using a finally. So though we don't *know* if stop() closed
    // the underlying stream, we *hope* it did.
    _out = std::nullopt;

    if (_in.has_value()) {
        co_await _in->close();
        _in = std::nullopt;
    }
}

void base_transport::shutdown() noexcept {
    try {
        if (_fd && !std::exchange(_shutdown, true)) {
            _fd->shutdown_input();
            _fd->shutdown_output();
        }
    } catch (...) {
        vlog(
          _log->debug,
          "Failed to shutdown transport: {}",
          std::current_exception());
    }
}

ss::future<> base_transport::wait_input_shutdown() {
    if (_fd && _shutdown) {
        co_return co_await _fd->wait_input_shutdown();
    }
}

void base_transport::set_probe(client_probe* probe) {
    vassert(!_probe.has_value(), "Transport already has registered probe");
    _probe = probe;
}

} // namespace net
