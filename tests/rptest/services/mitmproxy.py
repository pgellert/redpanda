# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

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

    def proxy_url(self) -> str:
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
        node.account.ssh(f"rm -f {LOG_PATH} {FLOWS_PATH}", allow_fail=True)

    def _is_listening(self, node) -> bool:
        out = node.account.ssh_output(
            f"ss -lnt | grep ':{MITMPROXY_PORT}' || true", allow_fail=True
        )
        return bool(out and out.strip())

    def assert_proxied_host(self, expected_host: str) -> None:
        """Assert that mitmproxy's log shows a CONNECT to the given
        host. Call this after the test workload has run. Parses the
        mitmproxy access log for a line matching 'CONNECT <host>:<port>'.
        """
        log_contents = self.node.account.ssh_output(f"cat {LOG_PATH}", allow_fail=True)
        if not log_contents:
            raise AssertionError("mitmproxy log is empty")
        text = (
            log_contents.decode() if isinstance(log_contents, bytes) else log_contents
        )
        if f"CONNECT {expected_host}" not in text:
            raise AssertionError(
                f"no CONNECT to {expected_host} found in mitmproxy log:\n{text}"
            )
