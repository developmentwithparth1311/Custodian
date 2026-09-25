"""Event history and optional in-process transport contracts."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from custodian.core.enums import TransportProtocol
from custodian.events import (
    EVENT_TOPICS,
    EventBackpressureError,
    EventType,
    InProcessEventBus,
    PipelineEvent,
)
from custodian.runtime.events import EventHub


def _pipeline_event(**updates) -> PipelineEvent:
    values = {
        "event_id": uuid4(),
        "event_type": EventType.PACKET_OBSERVATION,
        "schema_version": "packet_observation.v1",
        "occurred_at": datetime.now(UTC),
        "run_id": uuid4(),
        "capture_id": "capture-test",
        "correlation_id": uuid4(),
        "sequence": 1,
        "payload": {
            "timestamp": datetime.now(UTC).isoformat(),
            "src_ip": "192.0.2.1",
            "dst_ip": "192.0.2.2",
            "src_port": 12345,
            "dst_port": 53,
            "protocol": TransportProtocol.UDP.value,
            "packet_length": 128,
            "payload_length": 100,
        },
    }
    values.update(updates)
    return PipelineEvent.model_validate(values)


def test_event_hub_is_bounded_and_resumable() -> None:
    hub = EventHub(max_events=2)
    first = hub.publish("one", {"value": 1}, run_id=0)
    hub.publish("two", {"value": 2}, run_id=0)
    third = hub.publish("three", {"value": 3}, run_id=1)

    assert [event.event_type for event in hub.since(first.sequence)] == ["two", "three"]
    assert hub.latest_sequence == third.sequence
    assert [event.event_type for event in hub.since(0)] == ["two", "three"]
    assert hub.earliest_sequence == 2
    assert hub.cursor_requires_resync(0) is True
    assert hub.cursor_requires_resync(99) is True
    assert hub.cursor_requires_resync(2) is False


def test_pipeline_event_validates_schema_and_maps_versioned_topic() -> None:
    event = _pipeline_event()

    assert event.topic == "custodian.v1.packet_observation"
    assert event.typed_payload().packet_length == 128
    assert set(EVENT_TOPICS.values()) == {
        f"custodian.v1.{event_type.value}" for event_type in EventType
    }

    with pytest.raises(ValidationError, match="requires schema"):
        _pipeline_event(schema_version="custodian.v1")


def test_pipeline_event_rejects_secrets_and_invalid_payloads() -> None:
    with pytest.raises(ValidationError, match="forbidden sensitive field"):
        _pipeline_event(payload={"raw_payload": "captured bytes"})

    with pytest.raises(ValidationError):
        _pipeline_event(payload={"timestamp": "not a timestamp"})


def test_pipeline_event_checks_serialized_size_before_parsing() -> None:
    event = _pipeline_event()
    encoded = event.to_bytes()

    with pytest.raises(ValueError, match="maximum size"):
        PipelineEvent.validate_serialized(encoded, max_event_bytes=16)


def test_in_process_bus_is_bounded_and_fifo() -> None:
    bus = InProcessEventBus(max_events=1)
    first = _pipeline_event()
    second = _pipeline_event(sequence=2)
    bus.publish(first)

    with pytest.raises(EventBackpressureError, match="queue is full"):
        bus.publish(second)

    assert bus.backlog == 1
    assert bus.poll() == first
    assert bus.poll() is None
