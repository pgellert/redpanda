# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

"""
Scale-and-stability characterization for the HTTP-API SR shadow link.

Parameterized over N ∈ {100, 500, 1000} schemas, this test measures:

- End-to-end catch-up time when the source is pre-seeded with N subjects.
- The distribution (min / median / p95 / max) of new-subject discovery
  latency on the tail loop, observed across 20 fresh registrations that
  happen *after* the initial catch-up has completed.

The goal is to find the practical local-docker-harness ceiling for the
POC and document how the tail-loop latency degrades (if it does) as
the source-side subject inventory grows.
"""

import json
import statistics
import time

import google.protobuf.duration_pb2 as duration_pb2
import requests
from ducktape.mark import matrix
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.version import KafkaVersion

from rptest.clients.admin.proto.redpanda.core.admin.v2 import shadow_link_pb2
from rptest.clients.admin.v2 import Admin as AdminV2
from rptest.services.cluster import cluster
from rptest.services.confluent_schema_registry import ConfluentSchemaRegistryService
from rptest.services.kafka import KafkaServiceAdapter
from rptest.services.redpanda import SchemaRegistryConfig
from rptest.tests.redpanda_test import RedpandaTest

SR_CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"


def _avro_record_name_for(subject: str) -> str:
    parts = [p for p in subject.replace("-", "_").split("_") if p]
    if not parts:
        return "Record"
    return "".join(p[:1].upper() + p[1:] for p in parts)


def _avro_record(subject: str) -> str:
    return json.dumps(
        {
            "type": "record",
            "name": _avro_record_name_for(subject),
            "fields": [{"name": "f1", "type": "string"}],
        }
    )


def _register(sr_url: str, subject: str, body: str) -> int:
    resp = requests.post(
        f"{sr_url}/subjects/{subject}/versions",
        headers={"Content-Type": SR_CONTENT_TYPE},
        data=json.dumps({"schema": body, "schemaType": "AVRO"}),
        timeout=30,
    )
    assert resp.status_code == 200, (
        f"register {subject} failed: {resp.status_code} {resp.text}"
    )
    return resp.json()["id"]


def _subjects(sr_url: str) -> set[str]:
    resp = requests.get(f"{sr_url}/subjects", timeout=10)
    assert resp.status_code == 200, f"list subjects: {resp.status_code}"
    return set(resp.json())


class ConfluentSrShadowLinkScaleTest(RedpandaTest):
    """
    Scale + stability characterization for the SR shadow-link replicator.

    Each parameterization seeds `n_schemas` subjects on Confluent SR
    (1 version each), creates the link, waits for catch-up, then runs
    20 fresh-subject registrations and times each round-trip-to-visibility
    on the destination.
    """

    LINK_NAME = "cflt-sr-scale-link"
    FRESH_PROBES = 20

    def __init__(self, test_context: TestContext):
        sr_cfg = SchemaRegistryConfig()
        sr_cfg.mode_mutability = True
        super().__init__(
            test_context=test_context,
            num_brokers=1,
            schema_registry_config=sr_cfg,
            extra_rp_conf={
                "enable_shadow_linking": True,
                "group_initial_rebalance_delay": 1000,
            },
        )

        self._kafka = KafkaServiceAdapter(
            test_context,
            KafkaService(
                test_context,
                num_nodes=1,
                zk=None,
                version=KafkaVersion("3.8.0"),
                quorum_info_provider=lambda kafka: quorum.ServiceQuorumInfo(
                    quorum_type="COMBINED_KRAFT", kafka=kafka
                ),
            ),
        )
        # Use a non-default port so it never collides with Redpanda's own
        # SR (port 8081) if ducktape re-uses a container between matrix
        # variants and the kernel still holds the prior binding in
        # TIME_WAIT.
        self._confluent_sr = ConfluentSchemaRegistryService(
            test_context, bootstrap_provider=self._kafka, port=18081
        )

    def setUp(self):
        self._kafka.start()
        super().setUp()
        self._confluent_sr.start()

    def _rp_sr(self) -> str:
        return self.redpanda.schema_reg().split(",", 1)[0]

    def _create_link(self, source_sr_url: str) -> None:
        client = AdminV2(self.redpanda).shadow_link()
        sr_options = shadow_link_pb2.SchemaRegistrySyncOptions(
            shadow_via_http_api=shadow_link_pb2.SchemaRegistrySyncOptions.ShadowViaHttpApi(
                source_url=source_sr_url,
                include_regex=".*",
                tail_interval=duration_pb2.Duration(seconds=0, nanos=250_000_000),
                version_revisit_interval=duration_pb2.Duration(seconds=2),
                destination_url=self._rp_sr(),
            )
        )
        link_cfg = shadow_link_pb2.ShadowLinkConfigurations(
            client_options=shadow_link_pb2.ShadowLinkClientOptions(
                bootstrap_servers=self._kafka.brokers_list()
            ),
            topic_metadata_sync_options=shadow_link_pb2.TopicMetadataSyncOptions(
                interval=duration_pb2.Duration(seconds=30)
            ),
            consumer_offset_sync_options=shadow_link_pb2.ConsumerOffsetSyncOptions(
                interval=duration_pb2.Duration(seconds=30)
            ),
            security_sync_options=shadow_link_pb2.SecuritySettingsSyncOptions(
                interval=duration_pb2.Duration(seconds=30)
            ),
            schema_registry_sync_options=sr_options,
        )
        req = shadow_link_pb2.CreateShadowLinkRequest()
        req.shadow_link.CopyFrom(
            shadow_link_pb2.ShadowLink(name=self.LINK_NAME, configurations=link_cfg)
        )
        client.create_shadow_link(req=req)

    def _seed_n(self, sr_url: str, n: int) -> dict[str, int]:
        """Register N subjects with one schema each. Returns subject -> id."""
        seeded: dict[str, int] = {}
        seed_start = time.monotonic()
        for i in range(n):
            subject = f"scale-subject-{i:05d}-value"
            seeded[subject] = _register(sr_url, subject, _avro_record(subject))
        elapsed = time.monotonic() - seed_start
        self.logger.info(
            f"seeded {n} subjects on Confluent in {elapsed:.1f}s "
            f"({n / elapsed:.0f} reg/s)"
        )
        return seeded

    def _wait_for_catchup(self, expected: set[str], timeout_sec: float) -> float:
        """Block until all expected subjects are on the dest. Return wall time."""
        rp = self._rp_sr()
        start = time.monotonic()

        def all_present() -> bool:
            try:
                got = _subjects(rp)
            except Exception:
                return False
            return expected.issubset(got)

        wait_until(
            all_present,
            timeout_sec=timeout_sec,
            backoff_sec=0.5,
            err_msg=(
                f"catch-up did not finish within {timeout_sec}s "
                f"({len(expected)} subjects expected)"
            ),
        )
        return time.monotonic() - start

    def _probe_fresh_latencies(self, source_sr: str) -> list[float]:
        """
        After catch-up, register FRESH_PROBES new subjects one at a time and
        time each one's appearance on dest. Each probe waits for its own
        subject to land before registering the next, so we measure
        independent round trips rather than a queued burst.
        """
        rp = self._rp_sr()
        latencies: list[float] = []
        for i in range(self.FRESH_PROBES):
            probe_subject = f"fresh-probe-{i:03d}-value"
            t0 = time.monotonic()
            _register(source_sr, probe_subject, _avro_record(probe_subject))
            # Poll the dest with a tight loop to capture latency at finer
            # resolution than the 250ms tail tick we configured the task
            # with.
            wait_until(
                lambda: probe_subject in _subjects(rp),
                timeout_sec=10,
                backoff_sec=0.05,
                err_msg=f"probe {probe_subject} not replicated in 10s",
            )
            latencies.append(time.monotonic() - t0)
        return latencies

    @cluster(num_nodes=3)
    @matrix(n_schemas=[100, 500, 1000])
    def test_catchup_and_tail_latency_scale(self, n_schemas: int):
        """
        Pre-seed N subjects on Confluent, time catch-up against the
        destination, then drive FRESH_PROBES post-catch-up registrations
        and capture the latency distribution.
        """
        cflt = self._confluent_sr.url()

        seeded = self._seed_n(cflt, n_schemas)
        expected = set(seeded.keys())

        self._create_link(cflt)
        # Generous timeout: scales with N. The task does one GET per
        # subject + one GET per version + one POST per (subject, version),
        # so roughly 3N round trips for the catch-up at this version=1
        # shape, plus IMPORT mode + compat work.
        catchup_timeout = max(60.0, n_schemas * 0.5)
        catchup_s = self._wait_for_catchup(expected, timeout_sec=catchup_timeout)
        catchup_rate = n_schemas / catchup_s if catchup_s > 0 else float("inf")
        self.logger.info(
            f"[scale n={n_schemas}] catch-up: {catchup_s:.2f}s "
            f"({catchup_rate:.0f} schemas/s)"
        )

        latencies = self._probe_fresh_latencies(cflt)
        latencies_ms = [x * 1000 for x in latencies]
        sorted_ms = sorted(latencies_ms)
        p50 = statistics.median(sorted_ms)
        # 95th percentile via index — simple and dependency-free.
        p95_idx = max(0, int(0.95 * len(sorted_ms)) - 1)
        p95 = sorted_ms[p95_idx]
        self.logger.info(
            f"[scale n={n_schemas}] fresh-subject latency (ms): "
            f"min={min(sorted_ms):.0f} p50={p50:.0f} "
            f"p95={p95:.0f} max={max(sorted_ms):.0f} "
            f"mean={statistics.mean(sorted_ms):.0f} "
            f"stdev={statistics.stdev(sorted_ms):.0f} "
            f"samples={len(sorted_ms)}"
        )

        # Sanity assertions — keep them loose. The interesting output is
        # the measurement; we only fail if the system is wildly broken
        # under load.
        rp = self._rp_sr()
        assert expected.issubset(_subjects(rp)), (
            f"catch-up reported success but {len(expected - _subjects(rp))} "
            f"subjects went missing afterwards"
        )
        assert max(sorted_ms) < 10_000, (
            f"some fresh-subject probe took >10s (max={max(sorted_ms):.0f}ms); "
            f"latencies={sorted_ms}"
        )
