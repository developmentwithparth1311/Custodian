"""Optional Confluent Kafka adapters for validated Custodian events."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from time import monotonic, sleep
from typing import Any
from uuid import NAMESPACE_URL, uuid4, uuid5

from custodian.config import KafkaSettings
from custodian.events.bus import EventBackpressureError, EventBusError, EventPublisher
from custodian.events.contracts import DeadLetterPayload, EventType, PipelineEvent

MAX_HANDLER_ATTEMPTS = 10


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
            self._last_error = "Kafka broker unavailable"
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
        dead_letter_publisher: EventPublisher | None = None,
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
        self._dead_letter_publisher = dead_letter_publisher
        self._pending: dict[str, Any] = {}
        self._last_error: str | None = None
        self._blocked = False
        self.dead_letter_count = 0
        topics = [_topic(settings.topic_prefix, event_type) for event_type in event_types]
        self._consumer.subscribe(topics)

    def poll(self, *, timeout_seconds: float = 0.0) -> PipelineEvent | None:
        if timeout_seconds < 0:
            raise ValueError("timeout_seconds must not be negative")
        if self._blocked:
            raise EventBusError(
                "consumer is blocked after dead-letter or offset commit failure; restart after recovery"
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
            self._last_error = (
                "event exceeds configured size limit"
                if "maximum size" in str(exc)
                else "invalid event schema or topic"
            )
            try:
                self._route_invalid_message(message, exc)
            except Exception as dead_letter_error:
                self._blocked = True
                self._last_error = "dead-letter publish or source offset commit failed"
                raise EventBusError(self._last_error) from dead_letter_error
            return None
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

    def _route_invalid_message(self, message: Any, error: Exception) -> None:
        """Publish sanitized failure metadata before committing a poison message."""
        if self._dead_letter_publisher is None:
            self._dead_letter_publisher = KafkaEventPublisher(self.settings)
        too_large = "maximum size" in str(error)
        dead_letter = PipelineEvent(
            event_id=uuid5(
                NAMESPACE_URL,
                f"{message.topic()}:{getattr(message, 'partition', lambda: 0)()}:{getattr(message, 'offset', lambda: 0)()}",
            ),
            event_type=EventType.DEAD_LETTER,
            schema_version="dead_letter.v1",
            occurred_at=datetime.now(UTC),
            run_id=uuid4(),
            capture_id=None,
            correlation_id=uuid4(),
            sequence=1,
            payload=DeadLetterPayload(
                source_topic=message.topic(),
                original_event_id=None,
                failure_code="payload_too_large" if too_large else "invalid_event",
                error_summary=(
                    "payload exceeded maximum size" if too_large else "schema validation failed"
                ),
                attempt_count=1,
            ).model_dump(mode="json"),
        )
        self._dead_letter_publisher.publish(dead_letter, timeout_seconds=5.0)
        flush = getattr(self._dead_letter_publisher, "flush", None)
        if flush is not None:
            flush(timeout_seconds=5.0)
        self._commit_message(message)
        self.dead_letter_count += 1

    def _commit_message(self, message: Any) -> None:
        committed = self._consumer.commit(message=message, asynchronous=False)
        if committed and any(getattr(partition, "error", None) for partition in committed):
            raise EventBusError("Kafka consumer offset commit failed")

    def close(self) -> None:
        self._consumer.close()

    @property
    def healthy(self) -> bool:
        return self._last_error is None

    @property
    def last_error(self) -> str | None:
        return self._last_error


class KafkaEventWorker:
    """Process one event at a time with bounded retries and a sanitized dead letter."""

    def __init__(
        self,
        consumer: KafkaEventConsumer,
        handler: Callable[[PipelineEvent], None],
        dead_letter_publisher: EventPublisher,
        *,
        max_attempts: int = 3,
        initial_backoff_seconds: float = 0.1,
        max_backoff_seconds: float = 5.0,
        sleep_fn: Callable[[float], None] = sleep,
    ) -> None:
        if not 1 <= max_attempts <= MAX_HANDLER_ATTEMPTS:
            raise ValueError(f"max_attempts must be between 1 and {MAX_HANDLER_ATTEMPTS}")
        if initial_backoff_seconds < 0:
            raise ValueError("initial_backoff_seconds must not be negative")
        if max_backoff_seconds < initial_backoff_seconds:
            raise ValueError("max_backoff_seconds must be at least the initial backoff")
        self.consumer = consumer
        self.handler = handler
        self.dead_letter_publisher = dead_letter_publisher
        self.max_attempts = max_attempts
        self.initial_backoff_seconds = initial_backoff_seconds
        self.max_backoff_seconds = max_backoff_seconds
        self._sleep = sleep_fn
        self.processed_count = 0
        self.retry_count = 0
        self.dead_letter_count = 0
        self.last_error: str | None = None

    def process_next(self, *, timeout_seconds: float = 0.0) -> bool:
        """Process one event. Return False on timeout; raise on transport/DLQ failure."""
        event = self.consumer.poll(timeout_seconds=timeout_seconds)
        if event is None:
            return False

        failed = False
        for attempt in range(1, self.max_attempts + 1):
            try:
                self.handler(event)
            except Exception:
                failed = True
                self.last_error = f"event processing failed after attempt {attempt}"
                if attempt < self.max_attempts:
                    self.retry_count += 1
                    self._sleep(
                        min(
                            self.max_backoff_seconds,
                            self.initial_backoff_seconds * (2 ** (attempt - 1)),
                        )
                    )
                    continue
            else:
                try:
                    self.consumer.ack(event)
                except Exception as exc:
                    self.last_error = "event processed but Kafka offset commit failed"
                    raise EventBusError(self.last_error) from exc
                self.processed_count += 1
                self.last_error = None
                return True

        assert failed
        dead_letter = PipelineEvent(
            event_id=uuid5(event.event_id, "custodian.dead_letter.v1"),
            event_type=EventType.DEAD_LETTER,
            schema_version="dead_letter.v1",
            occurred_at=datetime.now(UTC),
            run_id=event.run_id,
            capture_id=event.capture_id,
            correlation_id=event.correlation_id,
            sequence=event.sequence,
            causation_id=event.event_id,
            payload=DeadLetterPayload(
                source_topic=_topic(self.consumer.settings.topic_prefix, event.event_type),
                original_event_id=event.event_id,
                failure_code="processing_failed",
                error_summary="event processing failed",
                attempt_count=self.max_attempts,
            ).model_dump(mode="json"),
        )
        try:
            self.dead_letter_publisher.publish(dead_letter, timeout_seconds=5.0)
            flush = getattr(self.dead_letter_publisher, "flush", None)
            if flush is not None:
                flush(timeout_seconds=5.0)
            self.consumer.ack(event)
        except Exception as exc:
            self.last_error = "failed to route event to dead-letter topic or commit source offset"
            raise EventBusError(self.last_error) from exc

        self.dead_letter_count += 1
        self.last_error = "event moved to dead-letter topic after bounded retries"
        return True

    @property
    def healthy(self) -> bool:
        return self.last_error is None and self.consumer.healthy

    @property
    def diagnostics(self) -> dict[str, int | bool | str | None]:
        return {
            "healthy": self.healthy,
            "consumer_healthy": self.consumer.healthy,
            "processed_count": self.processed_count,
            "retry_count": self.retry_count,
            "dead_letter_count": self.dead_letter_count + self.consumer.dead_letter_count,
            "last_error": self.last_error or self.consumer.last_error,
        }


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


__all__ = [
    "KafkaEventBus",
    "KafkaEventConsumer",
    "KafkaEventPublisher",
    "KafkaEventWorker",
]
