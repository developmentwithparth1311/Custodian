"""Transport-neutral event publisher/consumer interfaces and local bus."""

from __future__ import annotations

from queue import Empty, Full, Queue
from typing import Protocol, runtime_checkable

from custodian.events.contracts import MAX_EVENT_BYTES, PipelineEvent


class EventBusError(RuntimeError):
    """Base exception for event transport failures."""


class EventBackpressureError(EventBusError):
    """Raised when the bounded in-process event queue cannot accept an event."""


@runtime_checkable
class EventPublisher(Protocol):
    def publish(self, event: PipelineEvent, *, timeout_seconds: float = 0.0) -> None:
        """Publish a validated event or raise when it cannot be accepted."""


@runtime_checkable
class EventConsumer(Protocol):
    def poll(self, *, timeout_seconds: float = 0.0) -> PipelineEvent | None:
        """Return the next event, or None when the poll times out."""


class InProcessEventBus:
    """Bounded FIFO transport for a single process; no broker or network is used."""

    def __init__(self, *, max_events: int = 2000, max_event_bytes: int = MAX_EVENT_BYTES) -> None:
        if max_events <= 0:
            raise ValueError("max_events must be positive")
        if max_event_bytes <= 0:
            raise ValueError("max_event_bytes must be positive")
        self._queue: Queue[PipelineEvent] = Queue(maxsize=max_events)
        self.max_event_bytes = max_event_bytes

    def publish(self, event: PipelineEvent, *, timeout_seconds: float = 0.0) -> None:
        if timeout_seconds < 0:
            raise ValueError("timeout_seconds must not be negative")
        # Recheck at the transport boundary so alternate model construction paths cannot
        # bypass the configured serialization limit.
        event.to_bytes(max_event_bytes=self.max_event_bytes)
        try:
            self._queue.put(event, block=timeout_seconds > 0, timeout=timeout_seconds)
        except Full as exc:
            raise EventBackpressureError("in-process event queue is full") from exc

    def poll(self, *, timeout_seconds: float = 0.0) -> PipelineEvent | None:
        if timeout_seconds < 0:
            raise ValueError("timeout_seconds must not be negative")
        try:
            return self._queue.get(block=timeout_seconds > 0, timeout=timeout_seconds)
        except Empty:
            return None

    @property
    def backlog(self) -> int:
        return self._queue.qsize()


__all__ = [
    "EventBackpressureError",
    "EventBusError",
    "EventConsumer",
    "EventPublisher",
    "InProcessEventBus",
]
