"""Transactional inbox tests using a small PostgreSQL connection fake."""

from __future__ import annotations

import copy
import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from custodian.config import KafkaSettings
from custodian.core.enums import AlertDecision, EvidenceQuality, Severity, ThreatClass
from custodian.core.schemas import AlertRecord, Endpoint
from custodian.events import EventType, PipelineEvent
from custodian.events.kafka import KafkaEventConsumer, KafkaEventWorker
from custodian.storage import IdempotentEventHandler, PostgresEventStore


class FakePostgresState:
    def __init__(self):
        self.inbox = set()
        self.events = set()
        self.outbox = set()
        self.alerts = {}
        self.domain_writes = []


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.rowcount = -1

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, params=None):
        query = " ".join(query.split())
        state = self.connection.state
        if query.startswith("INSERT INTO custodian_event_inbox"):
            key = tuple(params)
            if key in state.inbox:
                self.rowcount = 0
            else:
                state.inbox.add(key)
                self.rowcount = 1
        elif query.startswith("INSERT INTO custodian_projection_outbox"):
            state.outbox.add(params[0])
        elif query.startswith("INSERT INTO custodian_pipeline_events"):
            state.events.add(params[0])
        elif query.startswith("INSERT INTO custodian_alert_records"):
            state.alerts[params[0]] = json.loads(params[2])


class FakePostgresConnection:
    def __init__(self, state):
        self.state = state
        self.snapshot = None

    def __enter__(self):
        self.snapshot = copy.deepcopy(self.state.__dict__)
        return self

    def __exit__(self, exc_type, exc, traceback):
        if exc_type is not None:
            self.state.__dict__.clear()
            self.state.__dict__.update(self.snapshot)
        return False

    def cursor(self):
        return FakeCursor(self)


class FakeKafkaMessage:
    def __init__(self, topic, value, offset):
        self._topic = topic
        self._value = value
        self._offset = offset

    def error(self):
        return None

    def topic(self):
        return self._topic

    def value(self):
        return self._value

    def partition(self):
        return 0

    def offset(self):
        return self._offset


class FakeKafkaConsumer:
    def __init__(self, messages):
        self.messages = list(messages)
        self.commits = []

    def subscribe(self, _topics):
        pass

    def poll(self, _timeout):
        return self.messages.pop(0) if self.messages else None

    def commit(self, *, message, asynchronous):
        self.commits.append((message, asynchronous))
        return []

    def close(self):
        pass


class FakeEventPublisher:
    def publish(self, _event, *, timeout_seconds=0.0):
        pass


def event() -> PipelineEvent:
    return PipelineEvent(
        event_id=uuid4(),
        event_type=EventType.RUNTIME_EVENT,
        schema_version="runtime_event.v1",
        occurred_at=datetime.now(UTC),
        run_id=uuid4(),
        capture_id="capture-inbox-test",
        correlation_id=uuid4(),
        sequence=1,
        payload={"name": "storage_test", "state": "ok", "details": {}},
    )


def test_postgres_inbox_suppresses_duplicate_event_per_consumer() -> None:
    state = FakePostgresState()
    store = PostgresEventStore(connection_factory=lambda: FakePostgresConnection(state))
    processed = []
    handler = IdempotentEventHandler(
        store,
        "dashboard-projector",
        lambda _connection, received: processed.append(received.event_id),
    )
    source = event()

    handler(source)
    handler(source)

    assert processed == [source.event_id]
    assert len(state.inbox) == 1
    assert len(state.events) == 1
    assert len(state.outbox) == 1
    assert store.process_once("alert-projector", source) is True
    assert len(state.inbox) == 2


def test_kafka_worker_acks_duplicate_only_after_postgres_noop() -> None:
    state = FakePostgresState()
    store = PostgresEventStore(connection_factory=lambda: FakePostgresConnection(state))
    source = event()
    messages = [
        FakeKafkaMessage(source.topic, source.to_bytes(), offset=0),
        FakeKafkaMessage(source.topic, source.to_bytes(), offset=1),
    ]
    client = FakeKafkaConsumer(messages)
    consumer = KafkaEventConsumer(KafkaSettings(), consumer=client)
    callback_events = []

    def domain_write(connection, received):
        connection.state.domain_writes.append(str(received.event_id))
        callback_events.append(received.event_id)

    handler = IdempotentEventHandler(store, "durable-writer", domain_write)
    worker = KafkaEventWorker(consumer, handler, dead_letter_publisher=FakeEventPublisher())

    assert worker.process_next()
    assert worker.process_next()

    assert callback_events == [source.event_id]
    assert len(client.commits) == 2
    assert len(state.inbox) == 1
    assert len(state.events) == 1
    assert len(state.outbox) == 1
    assert state.domain_writes == [str(source.event_id)]


def test_postgres_inbox_rolls_back_claim_and_writes_if_handler_fails() -> None:
    state = FakePostgresState()
    store = PostgresEventStore(connection_factory=lambda: FakePostgresConnection(state))
    source = event()

    def failing_handler(connection, received):
        connection.state.domain_writes.append((received.event_id, "partial"))
        raise RuntimeError("database write failed")

    with pytest.raises(RuntimeError, match="database write failed"):
        store.process_once("durable-projector", source, failing_handler)

    assert state.inbox == set()
    assert state.events == set()
    assert state.outbox == set()
    assert state.domain_writes == []
    assert store.process_once("durable-projector", source) is True


def test_postgres_store_requires_dsn_or_connection_factory() -> None:
    with pytest.raises(ValueError, match="DSN or connection factory"):
        PostgresEventStore()


def test_alert_delivery_upserts_stable_alert_id_without_incrementing_duplicates(
    observed_at,
) -> None:
    state = FakePostgresState()
    store = PostgresEventStore(connection_factory=lambda: FakePostgresConnection(state))
    alert = AlertRecord(
        alert_id="stable-alert",
        timestamp=observed_at,
        first_seen=observed_at,
        last_seen=observed_at,
        flow_id="flow-1",
        threat_class=ThreatClass.RECON,
        severity=Severity.MEDIUM,
        decision=AlertDecision.ACCEPT,
        calibrated_confidence=0.8,
        evidence_quality=EvidenceQuality.ADEQUATE,
        source=Endpoint(ip="10.0.0.1", port=1234),
        destination=Endpoint(ip="10.0.0.2", port=443),
        detector_id="behavior",
        model_version="test",
        feature_schema_version="behaviour.v1",
        inference_latency_ms=1,
        total_pipeline_latency_ms=2,
    )

    def alert_event(event_id):
        return PipelineEvent(
            event_id=event_id,
            event_type=EventType.ALERT,
            schema_version="alert.v1",
            occurred_at=observed_at,
            run_id=uuid4(),
            capture_id="capture-inbox-test",
            correlation_id=uuid4(),
            sequence=1,
            payload=alert.model_dump(mode="json"),
        )

    first = alert_event(uuid4())
    repeated_delivery = alert_event(first.event_id)
    store.process_once("alert-writer", first)
    store.process_once("alert-writer", repeated_delivery)

    assert len(state.alerts) == 1
    assert state.alerts["stable-alert"]["occurrence_count"] == 1
    assert len(state.events) == 1
    assert len(state.outbox) == 1
