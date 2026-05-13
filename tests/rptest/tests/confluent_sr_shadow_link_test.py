# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

"""
End-to-end test for the HTTP-API Schema Registry shadow link.

Topology:
- Apache Kafka 3.8 (KRaft, single broker) on its own ducktape node.
- Confluent Platform 7.7.x Schema Registry on a second node, backed by
  the Apache Kafka cluster. This is the migration *source*.
- A single-broker Redpanda cluster, which is the migration *destination*.

The test seeds N schemas in the Confluent SR — including a schema with
a reference to another subject — then creates a shadow link on the
Redpanda side configured with SchemaRegistrySyncOptions.shadow_via_http_api
pointing at the Confluent SR's URL. It then asserts that:

1. Every seeded schema appears on Redpanda's local SR with the same
   global ID as on the Confluent source.
2. The schema with a reference is replicated correctly — the referent
   exists in the destination at the right ID before the referrer.
3. A schema added to the source *after* catch-up shows up on the
   destination within a few seconds (tail-loop discovery).
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


def _avro_record_name_for(subject: str) -> str:
    """Avro record names must match [A-Za-z_][A-Za-z0-9_]* — coerce a
    subject like 'orders-value' into 'OrdersValue'."""
    parts = [p for p in subject.replace("-", "_").split("_") if p]
    if not parts:
        return "Record"
    return "".join(p[:1].upper() + p[1:] for p in parts)


def _avro_record(subject: str, field_name: str = "f1") -> str:
    """An Avro record schema with a single string field. The record's
    type name is derived from the subject so it satisfies Avro's
    identifier rules even when the subject contains hyphens."""
    return json.dumps(
        {
            "type": "record",
            "name": _avro_record_name_for(subject),
            "fields": [{"name": field_name, "type": "string"}],
        }
    )


def _register_schema(
    sr_url: str,
    subject: str,
    schema_body: str,
    schema_type: str = "AVRO",
    references: list[dict] | None = None,
) -> int:
    """POST a schema to the given SR, return the id assigned."""
    body: dict = {"schema": schema_body, "schemaType": schema_type}
    if references is not None:
        body["references"] = references
    resp = requests.post(
        f"{sr_url}/subjects/{subject}/versions",
        headers={"Content-Type": SR_CONTENT_TYPE},
        data=json.dumps(body),
        timeout=30,
    )
    assert resp.status_code == 200, (
        f"register {subject} failed: {resp.status_code} {resp.text}"
    )
    return resp.json()["id"]


def _get_subjects(sr_url: str) -> list[str]:
    resp = requests.get(f"{sr_url}/subjects", timeout=10)
    assert resp.status_code == 200, f"list subjects failed: {resp.status_code}"
    return resp.json()


def _get_schema_by_id(sr_url: str, schema_id: int) -> dict:
    resp = requests.get(f"{sr_url}/schemas/ids/{schema_id}", timeout=10)
    assert resp.status_code == 200, (
        f"get schema {schema_id} failed: {resp.status_code} {resp.text}"
    )
    return resp.json()


class ConfluentSrShadowLinkTest(RedpandaTest):
    """End-to-end Confluent SR → Redpanda SR shadow-link replication."""

    LINK_NAME = "cflt-sr-link"
    BASE_SUBJECTS = [
        "orders-value",
        "shipments-value",
        "customers-value",
    ]
    REFERENT_SUBJECT = "common-types-value"
    REFERRER_SUBJECT = "events-with-common-value"

    @staticmethod
    def _build_redpanda_config() -> dict:
        return {
            "enable_shadow_linking": True,
            # group_initial_rebalance_delay defaults to several seconds in
            # production; cut it down so the (unused-by-SR) consumer group
            # mirroring task doesn't slow test startup.
            "group_initial_rebalance_delay": 1000,
        }

    def __init__(self, test_context: TestContext):
        sr_cfg = SchemaRegistryConfig()
        # The task drives the destination into IMPORT mode. Requires the
        # SR to allow mode mutation.
        sr_cfg.mode_mutability = True
        super().__init__(
            test_context=test_context,
            num_brokers=1,
            schema_registry_config=sr_cfg,
            extra_rp_conf=self._build_redpanda_config(),
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
            test_context, bootstrap_provider=self._kafka
        )

    def setUp(self):
        # Ordering: Kafka up, then Redpanda (parent), then Confluent SR
        # (Kafka must be live for the SR to seed its _schemas topic).
        self._kafka.start()
        super().setUp()
        self._confluent_sr.start()

    def _redpanda_sr_url(self) -> str:
        return self.redpanda.schema_reg().split(",", 1)[0]

    def _create_shadow_link(self, source_sr_url: str) -> None:
        """Create a shadow link configured for HTTP-API SR replication."""
        client = AdminV2(self.redpanda).shadow_link()

        sr_options = shadow_link_pb2.SchemaRegistrySyncOptions(
            shadow_via_http_api=shadow_link_pb2.SchemaRegistrySyncOptions.ShadowViaHttpApi(
                source_url=source_sr_url,
                include_regex=".*",
                tail_interval=duration_pb2.Duration(seconds=0, nanos=250_000_000),
                version_revisit_interval=duration_pb2.Duration(seconds=2),
                destination_url=self._redpanda_sr_url(),
            )
        )

        # Topic / consumer-group / security sync are required by the
        # shadow link framework but irrelevant to this test; we leave
        # the filters empty so the corresponding tasks are no-ops.
        client_options = shadow_link_pb2.ShadowLinkClientOptions(
            # Required by the proto but we don't actually mirror topics.
            # Point at the Confluent Kafka cluster so the framework can
            # at least open a TCP connection to it.
            bootstrap_servers=self._kafka.brokers_list()
        )

        link_cfg = shadow_link_pb2.ShadowLinkConfigurations(
            client_options=client_options,
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
        link_resource = shadow_link_pb2.ShadowLink(
            name=self.LINK_NAME, configurations=link_cfg
        )
        req = shadow_link_pb2.CreateShadowLinkRequest()
        req.shadow_link.CopyFrom(link_resource)

        self.logger.info(
            f"Creating shadow link {self.LINK_NAME} with source {source_sr_url}"
        )
        client.create_shadow_link(req=req)

    def _wait_for_replication(
        self, expected_subjects: set[str], timeout_s: int
    ) -> None:
        """Poll the destination SR until every expected subject appears."""
        rp_sr = self._redpanda_sr_url()

        def all_present() -> bool:
            try:
                got = set(_get_subjects(rp_sr))
            except Exception as e:
                self.logger.debug(f"subject poll failed: {e}")
                return False
            missing = expected_subjects - got
            if missing:
                self.logger.debug(
                    f"replication progress: {len(got & expected_subjects)}/"
                    f"{len(expected_subjects)} expected; missing={sorted(missing)}"
                )
                return False
            return True

        wait_until(
            all_present,
            timeout_sec=timeout_s,
            backoff_sec=1,
            err_msg=(
                f"Destination SR did not receive all expected subjects within "
                f"{timeout_s}s"
            ),
        )

    @cluster(num_nodes=3)
    def test_replicates_schemas_with_references_and_preserves_ids(self):
        confluent_url = self._confluent_sr.url()
        rp_sr = self._redpanda_sr_url()

        # 1. Seed Confluent SR.
        seeded: dict[str, int] = {}
        for s in self.BASE_SUBJECTS:
            seeded[s] = _register_schema(confluent_url, s, _avro_record(s))
            self.logger.info(f"seeded {s} -> id={seeded[s]} on Confluent")

        # Subject with a reference: a `common.Address` referent, then a
        # referrer that uses it as a field type.
        seeded[self.REFERENT_SUBJECT] = _register_schema(
            confluent_url,
            self.REFERENT_SUBJECT,
            json.dumps(
                {
                    "type": "record",
                    "name": "Address",
                    "namespace": "common",
                    "fields": [{"name": "street", "type": "string"}],
                }
            ),
        )
        seeded[self.REFERRER_SUBJECT] = _register_schema(
            confluent_url,
            self.REFERRER_SUBJECT,
            json.dumps(
                {
                    "type": "record",
                    "name": "EventWithAddress",
                    "fields": [
                        {"name": "id", "type": "long"},
                        {"name": "addr", "type": "common.Address"},
                    ],
                }
            ),
            references=[
                {
                    "name": "common.Address",
                    "subject": self.REFERENT_SUBJECT,
                    "version": 1,
                }
            ],
        )
        self.logger.info(
            f"seeded {self.REFERRER_SUBJECT} with reference to {self.REFERENT_SUBJECT}"
        )

        all_subjects = set(seeded.keys())
        self.logger.info(
            f"All {len(all_subjects)} seeded subjects on Confluent: {sorted(all_subjects)}"
        )

        # 2. Create the shadow link.
        self._create_shadow_link(confluent_url)

        # 3. Wait for catch-up.
        self._wait_for_replication(all_subjects, timeout_s=60)

        # 4. Assert byte-for-byte schema bodies + ID preservation.
        rp_subjects = set(_get_subjects(rp_sr))
        assert all_subjects.issubset(rp_subjects), (
            f"Redpanda missing subjects: {all_subjects - rp_subjects}"
        )

        for subject, source_id in seeded.items():
            source_body = _get_schema_by_id(confluent_url, source_id)["schema"]
            dest_body = _get_schema_by_id(rp_sr, source_id)["schema"]
            assert json.loads(source_body) == json.loads(dest_body), (
                f"Schema body diverged for {subject} id={source_id}:\n"
                f"  source: {source_body}\n"
                f"  dest:   {dest_body}"
            )
            self.logger.info(
                f"id-preserving replication confirmed: {subject} id={source_id}"
            )

        # 5. Reference DAG correctness: the referent must exist at the
        # right id and the referrer's references[] field must reflect it.
        ref_meta = _get_schema_by_id(rp_sr, seeded[self.REFERRER_SUBJECT])
        refs = ref_meta.get("references", [])
        assert any(
            r.get("subject") == self.REFERENT_SUBJECT and r.get("version") == 1
            for r in refs
        ), f"Referrer schema is missing the expected reference; got references={refs}"

        # 6. Tail-loop discovery: register a fresh subject in source and
        # assert it appears on dest. We give the tail loop a generous
        # bound (10s) rather than 1s to keep the test stable under
        # ducktape's typical scheduling jitter; the task's tail interval
        # itself is 250ms.
        fresh_subject = "fresh-subject-value"
        fresh_id = _register_schema(
            confluent_url, fresh_subject, _avro_record(fresh_subject)
        )
        self.logger.info(
            f"seeded {fresh_subject} -> id={fresh_id} on Confluent (post-catchup)"
        )
        start = time.monotonic()
        wait_until(
            lambda: fresh_subject in _get_subjects(rp_sr),
            timeout_sec=10,
            backoff_sec=0.25,
            err_msg=f"{fresh_subject} not replicated in 10s",
        )
        latency = time.monotonic() - start
        self.logger.info(
            f"new-subject discovery latency for {fresh_subject}: {latency * 1000:.0f}ms"
        )

        # Final ID check on the fresh subject.
        dest_fresh = _get_schema_by_id(rp_sr, fresh_id)
        source_fresh = _get_schema_by_id(confluent_url, fresh_id)
        assert json.loads(dest_fresh["schema"]) == json.loads(source_fresh["schema"])
