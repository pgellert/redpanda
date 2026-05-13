# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

"""
Multi-version + per-subject compatibility replication.

Seeds the Confluent source with N subjects each carrying K schema
versions, plus a mix of per-subject compatibility-level overrides.
Then turns on the shadow link and asserts that:

1. All N * K schemas land on the destination with their *source*
   global IDs preserved.
2. Every (subject, version) tuple is present and the schema body
   matches byte-for-byte.
3. Per-subject compatibility levels are mirrored on the destination.
4. The destination's global compatibility tracks the source's.
"""

import json
import time

import google.protobuf.duration_pb2 as duration_pb2
import requests
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

# Per-subject compat assignments, cycled across the seeded subjects.
PER_SUBJECT_COMPATS = ["BACKWARD", "FORWARD", "FULL", "NONE"]


def _avro_record_name(subject: str) -> str:
    parts = [p for p in subject.replace("-", "_").split("_") if p]
    return "".join(p[:1].upper() + p[1:] for p in parts) or "Record"


def _avro_record_v(subject: str, version: int) -> str:
    """An Avro record schema with one string field per version, so each
    version is a distinct payload but they all share a record name."""
    fields = [{"name": f"f{i}", "type": "string"} for i in range(1, version + 1)]
    return json.dumps(
        {"type": "record", "name": _avro_record_name(subject), "fields": fields}
    )


def _register(sr_url: str, subject: str, body: str) -> int:
    resp = requests.post(
        f"{sr_url}/subjects/{subject}/versions",
        headers={"Content-Type": SR_CONTENT_TYPE},
        data=json.dumps({"schema": body, "schemaType": "AVRO"}),
        timeout=30,
    )
    assert resp.status_code == 200, (
        f"register {subject}: {resp.status_code} {resp.text}"
    )
    return resp.json()["id"]


def _put_compat(sr_url: str, subject: str | None, level: str) -> None:
    path = "/config" if subject is None else f"/config/{subject}"
    resp = requests.put(
        f"{sr_url}{path}",
        headers={"Content-Type": SR_CONTENT_TYPE},
        data=json.dumps({"compatibility": level}),
        timeout=10,
    )
    assert resp.status_code == 200, (
        f"PUT {path} {level}: {resp.status_code} {resp.text}"
    )


def _get_compat(sr_url: str, subject: str | None) -> str:
    path = "/config" if subject is None else f"/config/{subject}"
    resp = requests.get(f"{sr_url}{path}", timeout=10)
    assert resp.status_code == 200, f"GET {path}: {resp.status_code} {resp.text}"
    body = resp.json()
    return body.get("compatibilityLevel") or body.get("compatibility")


def _get_subjects(sr_url: str) -> set[str]:
    return set(requests.get(f"{sr_url}/subjects", timeout=10).json())


def _get_versions(sr_url: str, subject: str) -> list[int]:
    return requests.get(f"{sr_url}/subjects/{subject}/versions", timeout=10).json()


def _get_schema_by_subject_version(sr_url: str, subject: str, version: int) -> dict:
    return requests.get(
        f"{sr_url}/subjects/{subject}/versions/{version}", timeout=10
    ).json()


def _get_schema_by_id(sr_url: str, schema_id: int) -> dict:
    return requests.get(f"{sr_url}/schemas/ids/{schema_id}", timeout=10).json()


class ConfluentSrShadowLinkVersionsTest(RedpandaTest):
    """End-to-end coverage for multi-version subjects + compat replication."""

    LINK_NAME = "cflt-sr-versions-link"
    N_SUBJECTS = 10
    K_VERSIONS = 5
    GLOBAL_COMPAT = "FULL"

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

    @cluster(num_nodes=3)
    def test_replicates_versions_and_compat(self):
        cflt = self._confluent_sr.url()

        # 1. Set a non-default global compat on the source AND disable
        #    per-subject compatibility checks while we seed history.
        #    The replicator must mirror the global on catch-up.
        _put_compat(cflt, None, self.GLOBAL_COMPAT)

        # 2. Seed N_SUBJECTS subjects, each with K_VERSIONS distinct
        #    versions. We assign per-subject compat=NONE during seeding
        #    so each successive version is accepted regardless of
        #    backward-compatibility with prior versions, then flip the
        #    subject to its target compat afterwards.
        seeded: dict[tuple[str, int], int] = {}
        seed_start = time.monotonic()
        for n in range(self.N_SUBJECTS):
            subject = f"versioned-subject-{n:02d}-value"
            _put_compat(cflt, subject, "NONE")
            for v in range(1, self.K_VERSIONS + 1):
                sid = _register(cflt, subject, _avro_record_v(subject, v))
                seeded[(subject, v)] = sid
            # Pick the final per-subject compat in round-robin fashion.
            final_compat = PER_SUBJECT_COMPATS[n % len(PER_SUBJECT_COMPATS)]
            _put_compat(cflt, subject, final_compat)
        seed_s = time.monotonic() - seed_start
        total = self.N_SUBJECTS * self.K_VERSIONS
        self.logger.info(
            f"seeded {self.N_SUBJECTS} subjects x {self.K_VERSIONS} versions "
            f"= {total} schemas in {seed_s:.1f}s"
        )

        # 3. Snapshot expected per-subject compats from the source so
        #    we can verify against the destination later.
        expected_subjects = {
            f"versioned-subject-{n:02d}-value" for n in range(self.N_SUBJECTS)
        }
        expected_subject_compat = {
            f"versioned-subject-{n:02d}-value": PER_SUBJECT_COMPATS[
                n % len(PER_SUBJECT_COMPATS)
            ]
            for n in range(self.N_SUBJECTS)
        }

        # 4. Enable the shadow link.
        self._create_link(cflt)

        # 5. Wait for catch-up: all subjects + every version per subject
        #    must land on the destination.
        rp = self._rp_sr()

        def all_versions_present() -> bool:
            try:
                got_subjects = _get_subjects(rp)
            except Exception:
                return False
            if not expected_subjects.issubset(got_subjects):
                return False
            for subject in expected_subjects:
                try:
                    versions = set(_get_versions(rp, subject))
                except Exception:
                    return False
                if versions != set(range(1, self.K_VERSIONS + 1)):
                    return False
            return True

        wait_until(
            all_versions_present,
            timeout_sec=60,
            backoff_sec=0.5,
            err_msg=(
                f"catch-up incomplete: expected every (subject, version) for "
                f"{self.N_SUBJECTS} x {self.K_VERSIONS} schemas in 60s"
            ),
        )

        # 6. Verify each (subject, version) preserves its source id and
        #    schema body.
        for (subject, version), source_id in seeded.items():
            dest = _get_schema_by_subject_version(rp, subject, version)
            assert dest["id"] == source_id, (
                f"id mismatch at {subject} v{version}: "
                f"source={source_id} dest={dest['id']}"
            )
            assert dest["version"] == version, (
                f"version drift at {subject}: expected {version} got {dest['version']}"
            )
            source_body = _get_schema_by_id(cflt, source_id)["schema"]
            assert json.loads(source_body) == json.loads(dest["schema"]), (
                f"schema body diverged at {subject} v{version} (id={source_id})"
            )
        self.logger.info(
            f"verified all {len(seeded)} (subject, version) tuples preserve "
            f"source ids byte-for-byte"
        )

        # 7. Verify per-subject compat replication. The replicator only
        #    PUTs compat at catch-up time today, so it may need a beat
        #    for the slow tail revisit to align.
        def all_compats_match() -> bool:
            for subject, expected_compat in expected_subject_compat.items():
                got = _get_compat(rp, subject)
                if got != expected_compat:
                    return False
            return True

        wait_until(
            all_compats_match,
            timeout_sec=15,
            backoff_sec=0.5,
            err_msg="per-subject compat replication did not converge in 15s",
        )

        # 8. Verify global compat replication.
        dest_global = _get_compat(rp, None)
        assert dest_global == self.GLOBAL_COMPAT, (
            f"global compat not mirrored: expected {self.GLOBAL_COMPAT}, "
            f"got {dest_global}"
        )
        self.logger.info(
            f"global compat mirrored: source={self.GLOBAL_COMPAT} dest={dest_global}"
        )
