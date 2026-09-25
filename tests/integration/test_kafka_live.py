"""Opt-in Kafka broker integration test for the Phase 4 transport adapter."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from time import monotonic
from uuid import uuid4

import pytest

from custodian.config import KafkaSettings
from custodian.events import EventType, PipelineEvent
from custodian.events.kafka import KafkaEventConsumer, KafkaEventPublisher

pytestmark = pytest.mark.skipif(
    os.getenv("CUSTODIAN_KAFKA_INTEGRATION") != "1",
    reason="set CUSTODIAN_KAFKA_INTEGRATION=1 to use a local Kafka-compatible broker",
)


def test_runtime_event_publishes_consumes_and_acknowledges() -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    bootstrap_servers = tuple(
        value.strip()
        for value in os.getenv(
            "CUSTODIAN_KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"
        ).split(",")
        if value.strip()
    )
    settings = KafkaSettings(
        enabled=True,
        bootstrap_servers=bootstrap_servers,
        consumer_group=f"custodian-integration-{uuid4().hex[:12]}",
    )
    topic = "custodian.v1.runtime_event"

    admin = AdminClient({"bootstrap.servers": ",".join(settings.bootstrap_servers)})
    creation = admin.create_topics(
        [NewTopic(topic, num_partitions=1, replication_factor=1)],
        request_timeout=10,
    )
    try:
        creation[topic].result(timeout=10)
    except Exception as exc:
        # Existing topic is expected when reusing a local pilot broker.
        if "TOPIC_ALREADY_EXISTS" not in str(exc):
            raise

    source = PipelineEvent(
        event_id=uuid4(),
        event_type=EventType.RUNTIME_EVENT,
        schema_version="runtime_event.v1",
        occurred_at=datetime.now(UTC),
        run_id=uuid4(),
        capture_id="kafka-integration-check",
        correlation_id=uuid4(),
        sequence=1,
        payload={
            "name": "integration_check",
            "state": "ok",
            "details": {"metadata_only": True},
        },
    )
    publisher = KafkaEventPublisher(settings)
    consumer = KafkaEventConsumer(settings, event_types=(EventType.RUNTIME_EVENT,))
    try:
        publisher.verify(timeout_seconds=5)
        publisher.publish(source, timeout_seconds=5)
        publisher.flush(timeout_seconds=10)

        received = None
        deadline = monotonic() + 20
        while monotonic() < deadline:
            event = consumer.poll(timeout_seconds=1)
            if event is None:
                continue
            consumer.ack(event)
            if event.event_id == source.event_id:
                received = event
                break

        assert received is not None, "published event was not consumed"
        assert (
            received.run_id,
            received.capture_id,
            received.correlation_id,
            received.sequence,
            received.payload,
        ) == (
            source.run_id,
            source.capture_id,
            source.correlation_id,
            source.sequence,
            source.payload,
        )
        assert publisher.healthy
        assert consumer.healthy
    finally:
        consumer.close()
        publisher.flush(timeout_seconds=5)
