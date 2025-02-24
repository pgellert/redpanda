# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import hashlib
import json
import random

from rptest.services.cluster import cluster
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.utils.rpenv import sample_license
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.http_server import HttpServer
from rptest.tests.metrics_reporter_test import MetricsReporterServer
from rptest.tests.crash_loop_checks_test import HOSTNAME_ERRORS


class CrashReporterServer(MetricsReporterServer):
    def crash_reports(self):
        return [
            json.loads(r['body']) for r in self.requests()
            if r['path'] == '/metrics/crash_reports'
        ]


class CrashReporterTest(RedpandaTest):
    def __init__(self, test_ctx):
        self._ctx = test_ctx
        self.telemetry = CrashReporterServer(self._ctx)
        super(CrashReporterTest,
              self).__init__(test_context=test_ctx,
                             num_brokers=3,
                             extra_rp_conf={
                                 "health_monitor_max_metadata_age": 1000,
                                 "retention_bytes": 20000,
                                 **self.telemetry.rp_conf(),
                             })
        self.redpanda.set_environment({"REDPANDA_ENVIRONMENT": "test"})

    def setUp(self):
        # Start HTTP server before redpanda to avoid connection errors
        self.telemetry.start()
        self.redpanda.start()

    @cluster(num_nodes=4,
             log_allow_list=RESTART_LOG_ALLOW_LIST + HOSTNAME_ERRORS)
    def test_redpanda_crash_reporting(self):
        """
        Test that redpanda nodes send well formed messages to the metrics endpoint
        """

        # self.metrics.clear_requests()

        crashing_broker = self.redpanda.nodes[0]
        self.logger.info(f"Triggering a crash on {crashing_broker.name}")
        self.redpanda.stop_node(crashing_broker)
        invalid_conf = dict(
            kafka_api=dict(address="unreachable_host.com", port=9092))
        self.redpanda.start_node(crashing_broker,
                                 override_cfg_params=invalid_conf,
                                 expect_fail=True)

        self.logger.info(f"Restarting {crashing_broker.name} after the crash")
        self.redpanda.start_node(crashing_broker)

        # Load and put a license at start. This is to check the SHA-256 checksum
        # admin = Admin(self.redpanda)
        # license = sample_license()
        # if license is None:
        #     self.logger.info(
        #         "Skipping test, REDPANDA_SAMPLE_LICENSE env var not found")
        #     return

        # assert admin.put_license(
        #     license).status_code == 200, "PUT License failed"

        # blow away the metrics state so we can test the has_license flag later

        # total_topics = 5
        # total_partitions = 0
        # for _ in range(0, total_topics):
        #     partitions = random.randint(1, 8)
        #     total_partitions += partitions
        #     self.client().create_topic([
        #         TopicSpec(partition_count=partitions,
        #                   replication_factor=len(self.redpanda.nodes))
        #     ])

        # create topics
        # self.redpanda.logger.info(
        #     f"created {total_topics} topics with {total_partitions} partitions"
        # )

        def _request_received():
            if self.telemetry.crash_reports():
                r = self.telemetry.crash_reports()[-1]
                self.logger.info(f"Latest request: {r}")
                return True
            else:
                self.logger.info("No requests yet")
                return False

        wait_until(_request_received, 20, backoff_sec=1)
        self.telemetry.stop()
        crash_reports = self.telemetry.crash_reports()

        assert len(crash_reports) == 1, f"Unexpected: {len(crash_reports)=}"

        report = crash_reports[0]
        assert len(report['cluster_uuid']) > 0, \
                    f"Unexpected: {report['cluster_uuid']=}"

        crashes = report['items']
        assert len(crashes) == 1, f"Unexpected: {len(crashes)=}"

        crash = crashes[0]
        assert int(crash['timestamp']) > 1740422170009, \
            f"Unexpected: {crash['timestamp']=}"
        assert crash['node_id'] == 1, \
            f"Unexpected: {crash['node_id']=}"
        assert len(crash['stacktrace']) > 0, \
            f"Unexpected: {crash['stacktrace']=}"
        assert crash['reason'] == "startup_exception", \
            f"Unexpected: {crash['reason']=}"
        assert crash['additional_info'] == "", \
            f"Unexpected: {crash['additional_info']}"
