"""Versioned, metadata-only contracts for the optional event pipeline."""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Literal
from uuid import UUID

from pydantic import AwareDatetime, Field, JsonValue, model_validator

from custodian.core.schemas import (
    AlertRecord,
    ContractModel,
    DetectorVerdict,
    FeatureVector,
    FlowRecord,
    PacketObservation,
)

MAX_EVENT_BYTES = 262_144


class EventType(StrEnum):
    PACKET_OBSERVATION = "packet_observation"
    FLOW_UPDATE = "flow_update"
    FEATURE_VECTOR = "feature_vector"
    DETECTOR_VERDICT = "detector_verdict"
    ALERT = "alert"
    RUNTIME_EVENT = "runtime_event"
    DEAD_LETTER = "dead_letter"


EVENT_TOPICS: dict[EventType, str] = {
    event_type: f"custodian.v1.{event_type.value}" for event_type in EventType
}


class FlowUpdatePayload(ContractModel):
    update_kind: Annotated[str, Field(pattern=r"^(snapshot|closed)$")]
    flow: FlowRecord


class RuntimeEventPayload(ContractModel):
    name: Annotated[str, Field(min_length=1, max_length=128)]
    state: Annotated[str, Field(min_length=1, max_length=64)]
    details: dict[str, JsonValue] = Field(default_factory=dict)


class DeadLetterPayload(ContractModel):
    """Safe failure metadata; the original untrusted message is not copied here."""

    source_topic: Annotated[str, Field(min_length=1, max_length=249)]
    original_event_id: UUID | None = None
    failure_code: Annotated[str, Field(min_length=1, max_length=96)]
    error_summary: Literal[
        "schema validation failed",
        "payload exceeded maximum size",
        "event processing failed",
        "unsupported event type",
    ]
    attempt_count: Annotated[int, Field(ge=1)]


_PAYLOAD_MODELS = {
    EventType.PACKET_OBSERVATION: PacketObservation,
    EventType.FLOW_UPDATE: FlowUpdatePayload,
    EventType.FEATURE_VECTOR: FeatureVector,
    EventType.DETECTOR_VERDICT: DetectorVerdict,
    EventType.ALERT: AlertRecord,
    EventType.RUNTIME_EVENT: RuntimeEventPayload,
    EventType.DEAD_LETTER: DeadLetterPayload,
}

_FORBIDDEN_KEYS = {
    "raw_packet",
    "raw_packets",
    "raw_payload",
    "packet_bytes",
    "extracted_file",
    "extracted_files",
    "password",
    "passwords",
    "secret",
    "secrets",
    "jwt",
    "authorization",
    "access_token",
    "refresh_token",
    "model_binary",
    "model_binaries",
}


def _validate_metadata_only(value: object, path: str = "payload") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = str(key).strip().lower()
            sensitive_name = any(
                fragment in normalized for fragment in ("secret", "password", "token")
            )
            raw_content_name = "raw" in normalized and any(
                fragment in normalized for fragment in ("packet", "payload", "frame")
            )
            if normalized in _FORBIDDEN_KEYS or sensitive_name or raw_content_name:
                raise ValueError(f"forbidden sensitive field at {path}.{key}")
            _validate_metadata_only(child, f"{path}.{key}")
    elif isinstance(value, list | tuple):
        for index, child in enumerate(value):
            _validate_metadata_only(child, f"{path}[{index}]")


class PipelineEvent(ContractModel):
    """Validated transport envelope shared by local and future broker buses."""

    event_id: UUID
    event_type: EventType
    schema_version: Annotated[str, Field(min_length=1, max_length=64)]
    occurred_at: AwareDatetime
    run_id: UUID
    capture_id: Annotated[str, Field(min_length=1, max_length=128)] | None
    correlation_id: UUID
    sequence: Annotated[int, Field(ge=1)]
    causation_id: UUID | None = None
    payload: dict[str, JsonValue]

    @model_validator(mode="after")
    def validate_payload_and_size(self) -> PipelineEvent:
        expected_version = f"{self.event_type.value}.v1"
        if self.schema_version != expected_version:
            raise ValueError(f"{self.event_type.value} requires schema {expected_version}")
        _validate_metadata_only(self.payload)
        payload_model = _PAYLOAD_MODELS[self.event_type].model_validate(self.payload)
        # Normalize enums, datetimes, and IP values into the canonical JSON representation.
        object.__setattr__(self, "payload", payload_model.model_dump(mode="json"))
        return self

    @classmethod
    def validate_serialized(
        cls, data: bytes | str, *, max_event_bytes: int = MAX_EVENT_BYTES
    ) -> PipelineEvent:
        """Validate broker bytes with a caller-selected limit before JSON parsing."""
        encoded = data.encode("utf-8") if isinstance(data, str) else data
        if len(encoded) > max_event_bytes:
            raise ValueError(f"event exceeds maximum size of {max_event_bytes} bytes")
        return cls.model_validate_json(encoded)

    def to_bytes(self, *, max_event_bytes: int = MAX_EVENT_BYTES) -> bytes:
        encoded = self.model_dump_json().encode("utf-8")
        if len(encoded) > max_event_bytes:
            raise ValueError(f"event exceeds maximum size of {max_event_bytes} bytes")
        return encoded

    @property
    def topic(self) -> str:
        return EVENT_TOPICS[self.event_type]

    def typed_payload(self):
        """Return the payload parsed to the event type's concrete Pydantic contract."""
        return _PAYLOAD_MODELS[self.event_type].model_validate(self.payload)


__all__ = [
    "EVENT_TOPICS",
    "MAX_EVENT_BYTES",
    "DeadLetterPayload",
    "EventType",
    "FlowUpdatePayload",
    "PipelineEvent",
    "RuntimeEventPayload",
]
