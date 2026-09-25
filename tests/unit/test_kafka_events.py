"""Kafka adapter behavior using in-memory client doubles."""

from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest

from custodian.api.app import ReplaySession
from custodian.config import KafkaSettings, load_config_bundle
from custodian.core.enums import TransportProtocol
from custodian.events import EventBusError, EventType, PipelineEvent
from custodian.events.kafka import KafkaEventConsumer, KafkaEventPublisher, KafkaEventWorker
from custodian.runtime.engine import CustodianEngine


def event() -> PipelineEvent:
    return PipelineEvent(
        event_id=uuid4(),
        event_type=EventType.PACKET_OBSERVATION,
        schema_version="packet_observation.v1",
        occurred_at=datetime.now(UTC),
        run_id=uuid4(),
        capture_id="capture-test",
        correlation_id=uuid4(),
        sequence=1,
        payload={
            "timestamp": datetime.now(UTC).isoformat(),
            "src_ip": "192.0.2.1",
            "dst_ip": "192.0.2.2",
            "src_port": 12345,
            "dst_port": 53,
            "protocol": TransportProtocol.UDP.value,
            "packet_length": 128,
            "payload_length": 100,
        },
    )


class FakeProducer:
    def __init__(self):
        self.records = []
        self.callbacks = []

    def list_topics(self, timeout):
        assert timeout > 0
        return object()

    def produce(self, topic, *, key, value, on_delivery):
        self.records.append((topic, key, value))
        self.callbacks.append(on_delivery)

    def poll(self, _timeout):
        while self.callbacks:
            self.callbacks.pop(0)(None, object())

    def flush(self, _timeout):
        return 0

    def __len__(self):
        return len(self.records) - len(self.callbacks)


class FakeMessage:
    def __init__(self, topic, value):
        self._topic = topic
        self._value = value

    def error(self):
        return None

    def topic(self):
        return self._topic

    def value(self):
        return self._value


class FakeConsumer:
    def __init__(self, messages):
        self.messages = list(messages)
        self.commits = []
        self.subscribed = None

    def subscribe(self, topics):
        self.subscribed = topics

    def poll(self, _timeout):
        return self.messages.pop(0) if self.messages else None

    def commit(self, *, message, asynchronous):
        self.commits.append((message, asynchronous))
        return []

    def close(self):
        pass


class RecordingPublisher:
    def __init__(self):
        self.events = []

    def publish(self, pipeline_event, *, timeout_seconds=0.0):
        self.events.append(pipeline_event)

    def flush(self, timeout_seconds=5.0):
        pass


def test_kafka_publisher_sends_validated_event_to_prefixed_topic() -> None:
    client = FakeProducer()
    publisher = KafkaEventPublisher(
        KafkaSettings(topic_prefix="custodian.test.v1"), producer=client
    )
    source_event = event()

    publisher.verify()
    publisher.publish(source_event)

    topic, key, value = client.records[0]
    assert topic == "custodian.test.v1.packet_observation"
    assert key == f"capture-test:{source_event.run_id}".encode()
    assert PipelineEvent.validate_serialized(value) == source_event
    assert publisher.healthy
    publisher.flush()


def test_kafka_consumer_validates_topic_and_requires_explicit_ack() -> None:
    source_event = event()
    topic = "custodian.v1.packet_observation"
    message = FakeMessage(topic, source_event.to_bytes())
    client = FakeConsumer([message])
    consumer = KafkaEventConsumer(KafkaSettings(), consumer=client)

    assert topic in client.subscribed
    received = consumer.poll()
    assert received == source_event
    with pytest.raises(EventBusError, match="acknowledge"):
        consumer.poll()

    consumer.ack(received)
    assert client.commits == [(message, False)]
    assert consumer.poll() is None


def test_kafka_consumer_blocks_on_invalid_or_misrouted_event() -> None:
    source_event = event()
    invalid_topic = FakeMessage("custodian.v1.flow_update", source_event.to_bytes())
    client = FakeConsumer([invalid_topic])
    dead_letters = RecordingPublisher()
    consumer = KafkaEventConsumer(
        KafkaSettings(), consumer=client, dead_letter_publisher=dead_letters
    )

    assert consumer.poll() is None
    assert consumer.dead_letter_count == 1
    assert client.commits == [(invalid_topic, False)]
    dead_letter = dead_letters.events[0]
    assert dead_letter.event_type == EventType.DEAD_LETTER
    assert dead_letter.payload["source_topic"] == "custodian.v1.flow_update"
    assert dead_letter.payload["error_summary"] == "schema validation failed"
    assert dead_letter.payload["original_event_id"] is None


def test_kafka_worker_retries_then_acknowledges_success() -> None:
    source_event = event()
    message = FakeMessage(source_event.topic, source_event.to_bytes())
    client = FakeConsumer([message])
    consumer = KafkaEventConsumer(KafkaSettings(), consumer=client)
    attempts = []
    delays = []

    def handler(received):
        attempts.append(received.event_id)
        if len(attempts) < 3:
            raise RuntimeError("temporary storage error")

    worker = KafkaEventWorker(
        consumer,
        handler,
        RecordingPublisher(),
        max_attempts=3,
        initial_backoff_seconds=0.25,
        sleep_fn=delays.append,
    )

    assert worker.process_next()
    assert attempts == [source_event.event_id] * 3
    assert delays == [0.25, 0.5]
    assert worker.retry_count == 2
    assert worker.processed_count == 1
    assert client.commits == [(message, False)]
    assert worker.healthy


def test_kafka_worker_dead_letters_after_bounded_retries() -> None:
    source_event = event()
    message = FakeMessage(source_event.topic, source_event.to_bytes())
    client = FakeConsumer([message])
    consumer = KafkaEventConsumer(KafkaSettings(), consumer=client)
    dead_letters = RecordingPublisher()
    calls = []

    def fail_handler(_event):
        calls.append(1)
        raise ValueError("private data")

    worker = KafkaEventWorker(
        consumer,
        fail_handler,
        dead_letters,
        max_attempts=2,
        initial_backoff_seconds=0,
        sleep_fn=lambda _delay: None,
    )

    assert worker.process_next()
    assert len(calls) == 2
    assert worker.retry_count == 1
    assert worker.dead_letter_count == 1
    assert client.commits == [(message, False)]
    dead_letter = dead_letters.events[0]
    assert dead_letter.event_type == EventType.DEAD_LETTER
    assert dead_letter.causation_id == source_event.event_id
    assert dead_letter.payload["original_event_id"] == str(source_event.event_id)
    assert dead_letter.payload["attempt_count"] == 2
    assert "private data" not in str(dead_letter.model_dump())
    assert not worker.healthy


def test_kafka_worker_does_not_ack_when_dead_letter_publish_fails() -> None:
    source_event = event()
    message = FakeMessage(source_event.topic, source_event.to_bytes())
    client = FakeConsumer([message])
    consumer = KafkaEventConsumer(KafkaSettings(), consumer=client)

    class UnavailablePublisher(RecordingPublisher):
        def publish(self, pipeline_event, *, timeout_seconds=0.0):
            raise RuntimeError("broker unavailable")

    def failed_handler(_event):
        raise ValueError("failed")

    worker = KafkaEventWorker(
        consumer,
        failed_handler,
        UnavailablePublisher(),
        max_attempts=1,
        initial_backoff_seconds=0,
        sleep_fn=lambda _delay: None,
    )

    with pytest.raises(EventBusError, match="dead-letter topic"):
        worker.process_next()
    assert client.commits == []
    assert not worker.healthy
    assert "broker unavailable" not in str(worker.diagnostics)


def test_replay_session_propagates_capture_run_and_correlation_ids() -> None:
    config_dir = Path(__file__).resolve().parents[2] / "configs"
    config = load_config_bundle(config_dir)
    publisher = RecordingPublisher()
    session = ReplaySession(
        CustodianEngine(config),
        config,
        pipeline_publisher=publisher,
    )
    session.engine.capture_id = "capture-test"

    session._publish_pipeline_event(
        EventType.PACKET_OBSERVATION,
        {
            "timestamp": datetime.now(UTC).isoformat(),
            "src_ip": "192.0.2.1",
            "dst_ip": "192.0.2.2",
            "src_port": 12345,
            "dst_port": 53,
            "protocol": TransportProtocol.UDP.value,
            "packet_length": 128,
            "payload_length": 100,
        },
        datetime.now(UTC),
    )

    received = publisher.events[0]
    assert received.capture_id == "capture-test"
    assert received.run_id == session.pipeline_run_id
    assert received.correlation_id == session.pipeline_correlation_id
    assert received.sequence == 1
