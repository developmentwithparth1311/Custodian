"""Optional Confluent Kafka adapters for validated Custodian events."""

from __future__ import annotations

from time import monotonic
from typing import Any

from custodian.config import KafkaSettings
from custodian.events.bus import EventBackpressureError, EventBusError
from custodian.events.contracts import EventType, PipelineEvent


def _topic(prefix: str, event_type: EventType) -> str:
    return f"{prefix.rstrip('.')}.{event_type.value}"


def _client_import_error() -> ImportError:
    return ImportError(
        "Kafka mode requires the optional dependency; install Custodian with `.[kafka]`"
    )


class KafkaEventPublisher:
    """Asynchronous idempotent producer for typed, size-checked metadata events."""

    def __init__(self, settings: KafkaSettings, *, producer: Any | None = None) -> None:
        self.settings = settings
        if producer is None:
            try:
                from confluent_kafka import Producer
            except ImportError as exc:
                raise _client_import_error() from exc
            producer = Producer(
                {
                    "bootstrap.servers": ",".join(settings.bootstrap_servers),
                    "enable.idempotence": True,
                    "acks": "all",
                    "max.in.flight.requests.per.connection": 5,
                    "message.max.bytes": settings.max_event_bytes,
                    "message.timeout.ms": 5000,
                }
            )
        self._producer = producer
        self._last_error: str | None = None

    def verify(self, timeout_seconds: float = 1.0) -> None:
        """Boundedly verify broker metadata before a replay enters Kafka mode."""
        try:
            self._producer.list_topics(timeout=timeout_seconds)
        except Exception as exc:
            self._last_error = str(exc)
            raise EventBusError("Kafka broker is unavailable") from exc
        self._last_error = None

    def _delivery_report(self, error, _message) -> None:
        self._last_error = str(error) if error else None

    def publish(self, event: PipelineEvent, *, timeout_seconds: float = 0.0) -> None:
        if timeout_seconds < 0:
            raise ValueError("timeout_seconds must not be negative")
        if self._last_error:
            raise EventBusError(f"Kafka producer is degraded: {self._last_error}")
        value = event.to_bytes(max_event_bytes=self.settings.max_event_bytes)
        partition_key = f"{event.capture_id or 'process'}:{event.run_id}".encode()
        deadline = monotonic() + timeout_seconds
        while True:
            try:
                self._producer.produce(
                    _topic(self.settings.topic_prefix, event.event_type),
                    key=partition_key,
                    value=value,
                    on_delivery=self._delivery_report,
                )
                self._producer.poll(0)
                if self._last_error:
                    raise EventBusError(f"Kafka delivery failed: {self._last_error}")
                return
            except BufferError as exc:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise EventBackpressureError("Kafka producer queue is full") from exc
                self._producer.poll(min(0.1, remaining))

    def flush(self, timeout_seconds: float = 5.0) -> None:
        remaining = self._producer.flush(timeout_seconds)
        if remaining:
            raise TimeoutError(f"Kafka producer still has {remaining} queued event(s)")
        if self._last_error:
            raise EventBusError(f"Kafka delivery failed: {self._last_error}")

    @property
    def backlog(self) -> int:
        return len(self._producer)

    @property
    def healthy(self) -> bool:
        return self._last_error is None

    @property
    def last_error(self) -> str | None:
        return self._last_error


class KafkaEventConsumer:
    """Manual-ack consumer that validates the envelope before returning messages."""

    def __init__(
        self,
        settings: KafkaSettings,
        *,
        event_types: tuple[EventType, ...] = tuple(EventType),
        consumer: Any | None = None,
    ) -> None:
        self.settings = settings
        self.event_types = event_types
        if consumer is None:
            try:
                from confluent_kafka import Consumer
            except ImportError as exc:
                raise _client_import_error() from exc
            consumer = Consumer(
                {
                    "bootstrap.servers": ",".join(settings.bootstrap_servers),
                    "group.id": settings.consumer_group,
                    "enable.auto.commit": False,
                    "enable.auto.offset.store": False,
                    "auto.offset.reset": "earliest",
                }
            )
        self._consumer = consumer
        self._pending: dict[str, Any] = {}
        self._last_error: str | None = None
        self._blocked = False
        topics = [_topic(settings.topic_prefix, event_type) for event_type in event_types]
        self._consumer.subscribe(topics)

    def poll(self, *, timeout_seconds: float = 0.0) -> PipelineEvent | None:
        if timeout_seconds < 0:
            raise ValueError("timeout_seconds must not be negative")
        if self._blocked:
            raise EventBusError(
                "consumer is blocked on an invalid event; operator handling is required"
            )
        if self._pending:
            raise EventBusError("acknowledge the outstanding event before polling again")
        message = self._consumer.poll(timeout_seconds)
        if message is None:
            return None
        error = message.error()
        if error:
            self._last_error = str(error)
            raise EventBusError(f"Kafka consume failed: {self._last_error}")
        try:
            event = PipelineEvent.validate_serialized(
                message.value(), max_event_bytes=self.settings.max_event_bytes
            )
            expected_topic = _topic(self.settings.topic_prefix, event.event_type)
            if message.topic() != expected_topic:
                raise ValueError("Kafka topic does not match the event type")
        except Exception as exc:
            self._last_error = str(exc)
            self._blocked = True
            raise EventBusError("invalid Kafka event; offset was not acknowledged") from exc
        self._pending[str(event.event_id)] = message
        self._last_error = None
        return event

    def ack(self, event: PipelineEvent) -> None:
        message = self._pending.get(str(event.event_id))
        if message is None:
            raise EventBusError("cannot acknowledge an event not returned by this consumer")
        committed = self._consumer.commit(message=message, asynchronous=False)
        if committed and any(getattr(partition, "error", None) for partition in committed):
            raise EventBusError("Kafka consumer offset commit failed")
        del self._pending[str(event.event_id)]

    def close(self) -> None:
        self._consumer.close()

    @property
    def healthy(self) -> bool:
        return self._last_error is None

    @property
    def last_error(self) -> str | None:
        return self._last_error


class KafkaEventBus:
    """Convenience holder for the independently managed Kafka producer/consumer."""

    def __init__(self, settings: KafkaSettings) -> None:
        self.publisher = KafkaEventPublisher(settings)
        self.consumer = KafkaEventConsumer(settings)

    def close(self, timeout_seconds: float = 5.0) -> None:
        try:
            self.publisher.flush(timeout_seconds)
        finally:
            self.consumer.close()


__all__ = ["KafkaEventBus", "KafkaEventConsumer", "KafkaEventPublisher"]
