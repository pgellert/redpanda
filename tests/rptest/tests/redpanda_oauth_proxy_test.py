# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.utils.util import wait_until

from rptest.clients.python_librdkafka import PythonLibrdkafka
from rptest.services.cluster import cluster
from rptest.services.keycloak import KC_HTTPS_PORT
from rptest.services.mitmproxy import MitmproxyService
from rptest.tests.redpanda_oauth_test import (
    CLIENT_ID,
    EXAMPLE_TOPIC,
    RedpandaOIDCTestBase,
)


class OIDCViaProxyTest(RedpandaOIDCTestBase):
    """End-to-end test that Redpanda's OIDC discovery and JWKS fetches
    flow through the forward proxy configured via oidc_http_proxy.

    Loadbearing detail: an iptables DROP rule blocks direct egress from
    each Redpanda node to the Keycloak hostname+port, so the only
    working path to the IdP is via mitmproxy. Without the DROP rule a
    regression where Redpanda silently ignores oidc_http_proxy would
    still pass the test.
    """

    def __init__(self, test_context, **kwargs):
        # use_ssl=True so Keycloak's discovery URL is https://. The broker
        # refuses to proxy plaintext OIDC origins (plaintext origin via
        # CONNECT tunnel is unsupported, see oidc_service.cc), so the test
        # must exercise the supported HTTPS-origin path that matches the
        # real customer scenario (Azure AD, Okta, etc).
        super().__init__(test_context, use_ssl=True, **kwargs)
        self.mitmproxy = MitmproxyService(test_context)

    def setUp(self):
        # Start mitmproxy before Redpanda so the proxy URL is available
        # to include in the cluster config and so the DROP rule can be
        # applied first.
        self.mitmproxy.start()

        self.redpanda.add_extra_rp_conf({"oidc_http_proxy": self.mitmproxy.proxy_url()})

        self._block_direct_keycloak_egress()
        self._iptables_applied = True

        try:
            super().setUp()
        except Exception:
            self._restore_direct_keycloak_egress()
            self._iptables_applied = False
            raise

    def tearDown(self):
        try:
            if getattr(self, "_iptables_applied", False):
                self._restore_direct_keycloak_egress()
                self._iptables_applied = False
        finally:
            super().tearDown()

    @property
    def _keycloak_host(self) -> str:
        return self.keycloak.host(self.keycloak.nodes[0])

    def _iptables_rule(self, action: str) -> str:
        # iptables resolves the hostname at rule-insert/delete time to
        # one or more A records. Using the hostname (rather than an IP)
        # keeps this consistent with how Redpanda resolves the
        # discovery URL.
        return (
            f"iptables {action} OUTPUT -p tcp "
            f"-d {self._keycloak_host} --dport {KC_HTTPS_PORT} -j DROP"
        )

    def _block_direct_keycloak_egress(self):
        cmd = self._iptables_rule("-A")
        for node in self.redpanda.nodes:
            self.logger.info(
                f"Blocking direct egress to {self._keycloak_host}:{KC_HTTPS_PORT} on "
                f"{node.account.hostname}"
            )
            node.account.ssh(cmd)

    def _restore_direct_keycloak_egress(self):
        cmd = self._iptables_rule("-D")
        for node in self.redpanda.nodes:
            try:
                node.account.ssh(cmd, allow_fail=True)
            except Exception as e:
                self.logger.warn(
                    f"Failed to remove iptables DROP on {node.account.hostname}: {e}"
                )

    @cluster(num_nodes=5)
    def test_oidc_discovery_via_http_proxy(self):
        kc_node = self.keycloak.nodes[0]

        client_id = CLIENT_ID
        service_user_id = self.create_service_user(client_id)

        self.rpk.create_topic(EXAMPLE_TOPIC)
        self.rpk.sasl_allow_principal(
            f"User:{service_user_id}",
            ["all"],
            "topic",
            EXAMPLE_TOPIC,
            self.su_username,
            self.su_password,
            self.su_algorithm,
        )

        cfg = self.keycloak.generate_oauth_config(kc_node, client_id)
        assert cfg.client_secret is not None
        assert cfg.token_endpoint is not None

        k_client = PythonLibrdkafka(
            self.redpanda,
            algorithm="OAUTHBEARER",
            oauth_config=cfg,
            tls_cert=self.client_cert,
        )
        producer = k_client.get_producer()

        # Explicit poll triggers the OIDC token flow. The broker must
        # validate the bearer token, which requires it to have
        # successfully fetched the discovery document and JWKS. Because
        # direct egress to Keycloak is blocked, both fetches must have
        # gone through mitmproxy.
        producer.poll(0.0)

        expected_topics = {EXAMPLE_TOPIC}
        wait_until(
            lambda: set(producer.list_topics(timeout=5).topics.keys())
            == expected_topics,
            timeout_sec=30,
            err_msg="OAUTHBEARER-authenticated client could not list topics",
        )

        producer.produce(topic=EXAMPLE_TOPIC, key="k", value="v")
        producer.flush(10)

        # Confirm mitmproxy actually saw Redpanda's CONNECT to the
        # Keycloak hostname. This rules out the degenerate pass where
        # Redpanda somehow bypassed the DROP rule.
        self.mitmproxy.assert_proxied_host(self._keycloak_host)
