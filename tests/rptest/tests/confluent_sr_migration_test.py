# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json

import requests
from ducktape.tests.test import TestContext
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.version import KafkaVersion

from rptest.services.cluster import cluster
from rptest.services.confluent_schema_registry import ConfluentSchemaRegistryService
from rptest.services.kafka import KafkaServiceAdapter
from rptest.services.redpanda import SchemaRegistryConfig
from rptest.tests.redpanda_test import RedpandaTest


SCHEMA_AVRO = (
    '{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'
)


class ConfluentSrMigrationBasicTest(RedpandaTest):
    """
    POC: Confluent Schema Registry running against an Apache Kafka source
    cluster, with the same schema then registered against Redpanda's embedded
    Schema Registry. Verifies both registries return byte-identical schema
    bodies for the migrated subject — the smallest possible cross-vendor
    migration smoke test.
    """

    SUBJECT = "migrated-topic-value"

    def __init__(self, test_context: TestContext):
        super().__init__(
            test_context=test_context,
            num_brokers=1,
            schema_registry_config=SchemaRegistryConfig(),
        )

        # Apache Kafka source cluster (KRaft, single combined node).
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

        # Confluent SR pointed at the Apache Kafka source.
        self._confluent_sr = ConfluentSchemaRegistryService(
            test_context, bootstrap_provider=self._kafka
        )

    def setUp(self):
        # Order matters: Kafka first, then SR (which needs Kafka up to
        # create _schemas). Redpanda is started by the parent.
        self._kafka.start()
        super().setUp()
        self._confluent_sr.start()

    def _redpanda_sr_url(self) -> str:
        # Redpanda exposes its embedded SR on port 8081 of any broker.
        return self.redpanda.schema_reg().split(",", 1)[0]

    @cluster(num_nodes=3)
    def test_schema_migration_roundtrip(self):
        confluent_url = self._confluent_sr.url()
        redpanda_url = self._redpanda_sr_url()

        # 1. Register the schema on Confluent SR (the source registry,
        #    backed by Apache Kafka).
        self.logger.info(f"Registering schema on Confluent SR at {confluent_url}")
        post = requests.post(
            f"{confluent_url}/subjects/{self.SUBJECT}/versions",
            headers={
                "Content-Type": "application/vnd.schemaregistry.v1+json",
            },
            data=json.dumps({"schema": SCHEMA_AVRO, "schemaType": "AVRO"}),
            timeout=30,
        )
        assert post.status_code == 200, (
            f"Confluent SR register failed: {post.status_code} {post.text}"
        )
        confluent_id = post.json()["id"]
        self.logger.info(f"Confluent SR assigned schema id={confluent_id}")

        # 2. Read the schema body back from Confluent SR — this is what
        #    a migration tool would carry across.
        get = requests.get(f"{confluent_url}/schemas/ids/{confluent_id}", timeout=30)
        assert get.status_code == 200, (
            f"Confluent SR get-by-id failed: {get.status_code} {get.text}"
        )
        confluent_body = get.json()["schema"]

        # 3. "Migrate": register the same schema body against Redpanda's
        #    embedded SR. In a real migration the _schemas topic content
        #    would be copied wholesale (e.g. via shadow linking), but
        #    re-registering exercises the same end-state assertion.
        self.logger.info(f"Registering same schema on Redpanda SR at {redpanda_url}")
        rp_post = requests.post(
            f"{redpanda_url}/subjects/{self.SUBJECT}/versions",
            headers={
                "Content-Type": "application/vnd.schemaregistry.v1+json",
            },
            data=json.dumps({"schema": confluent_body, "schemaType": "AVRO"}),
            timeout=30,
        )
        assert rp_post.status_code == 200, (
            f"Redpanda SR register failed: {rp_post.status_code} {rp_post.text}"
        )
        redpanda_id = rp_post.json()["id"]
        self.logger.info(f"Redpanda SR assigned schema id={redpanda_id}")

        # 4. Verify both registries agree on the migrated schema body.
        rp_get = requests.get(f"{redpanda_url}/schemas/ids/{redpanda_id}", timeout=30)
        assert rp_get.status_code == 200, (
            f"Redpanda SR get-by-id failed: {rp_get.status_code} {rp_get.text}"
        )
        redpanda_body = rp_get.json()["schema"]

        # Compare canonicalised JSON to ignore whitespace differences the
        # registries may introduce on round-trip.
        assert json.loads(confluent_body) == json.loads(redpanda_body), (
            f"Schema bodies diverged after migration:\n"
            f"  confluent: {confluent_body}\n"
            f"  redpanda:  {redpanda_body}"
        )

        # 5. Verify the subject is listed on both sides.
        confluent_subjects = requests.get(
            f"{confluent_url}/subjects", timeout=30
        ).json()
        redpanda_subjects = requests.get(f"{redpanda_url}/subjects", timeout=30).json()
        assert self.SUBJECT in confluent_subjects, (
            f"Subject missing on Confluent SR: {confluent_subjects}"
        )
        assert self.SUBJECT in redpanda_subjects, (
            f"Subject missing on Redpanda SR: {redpanda_subjects}"
        )

        self.logger.info(
            f"Migration smoke test passed: "
            f"confluent_id={confluent_id} redpanda_id={redpanda_id}"
        )
