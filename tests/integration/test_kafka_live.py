"""Opt-in Kafka broker integration test for the Phase 4 transport adapter."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from time import monotonic
from uuid import NAMESPACE_URL, uuid4, uuid5

import pytest

from custodian.config import KafkaSettings
from custodian.events import EventType, PipelineEvent
from custodian.events.kafka import KafkaEventConsumer, KafkaEventPublisher


def _ensure_topic(admin, topic: str) -> None:
    from confluent_kafka.admin import NewTopic

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


pytestmark = pytest.mark.skipif(
    os.getenv("CUSTODIAN_KAFKA_INTEGRATION") != "1",
    reason="set CUSTODIAN_KAFKA_INTEGRATION=1 to use a local Kafka-compatible broker",
)


def test_runtime_event_publishes_consumes_and_acknowledges() -> None:
    from confluent_kafka.admin import AdminClient

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
    _ensure_topic(admin, topic)

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


def test_invalid_event_is_sent_to_sanitized_dead_letter_topic() -> None:
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient

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
        consumer_group=f"custodian-poison-{uuid4().hex[:12]}",
    )
    admin = AdminClient({"bootstrap.servers": ",".join(settings.bootstrap_servers)})
    _ensure_topic(admin, "custodian.v1.packet_observation")
    _ensure_topic(admin, "custodian.v1.dead_letter")

    producer = Producer({"bootstrap.servers": ",".join(settings.bootstrap_servers)})
    delivered = []
    producer.produce(
        "custodian.v1.packet_observation",
        value=b'{"invalid":"schema-only test record"}',
        on_delivery=lambda error, message: delivered.append((error, message)),
    )
    assert producer.flush(10) == 0
    assert delivered and delivered[0][0] is None
    source_message = delivered[0][1]
    expected_dead_letter_id = uuid5(
        NAMESPACE_URL,
        f"custodian.v1.packet_observation:{source_message.partition()}:{source_message.offset()}",
    )

    source_consumer = KafkaEventConsumer(
        settings, event_types=(EventType.PACKET_OBSERVATION,)
    )
    try:
        deadline = monotonic() + 20
        while monotonic() < deadline and source_consumer.dead_letter_count == 0:
            source_consumer.poll(timeout_seconds=1)
        assert source_consumer.dead_letter_count == 1
        assert source_consumer.healthy is False
    finally:
        source_consumer.close()

    dead_letter_consumer = KafkaEventConsumer(
        KafkaSettings(
            enabled=True,
            bootstrap_servers=bootstrap_servers,
            consumer_group=f"custodian-dead-letter-check-{uuid4().hex[:12]}",
        ),
        event_types=(EventType.DEAD_LETTER,),
    )
    try:
        deadline = monotonic() + 20
        dead_letter = None
        while monotonic() < deadline:
            candidate = dead_letter_consumer.poll(timeout_seconds=1)
            if candidate is None:
                continue
            dead_letter_consumer.ack(candidate)
            if candidate.event_id == expected_dead_letter_id:
                dead_letter = candidate
                break
        assert dead_letter is not None
        assert dead_letter.payload["source_topic"] == "custodian.v1.packet_observation"
        assert dead_letter.payload["error_summary"] == "schema validation failed"
        assert dead_letter.payload["original_event_id"] is None
        assert "schema-only test record" not in str(dead_letter.model_dump())
    finally:
        dead_letter_consumer.close()
