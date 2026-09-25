"""Optional PostgreSQL inbox and durable event records for Kafka consumers."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from custodian.events.contracts import EventType, PipelineEvent

ConnectionFactory = Callable[[], Any]
EventWriteHandler = Callable[[Any, PipelineEvent], None]

POSTGRES_EVENT_SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS custodian_event_inbox (
        consumer_name TEXT NOT NULL,
        event_id UUID NOT NULL,
        processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (consumer_name, event_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS custodian_pipeline_events (
        event_id UUID PRIMARY KEY,
        event_type TEXT NOT NULL,
        schema_version TEXT NOT NULL,
        run_id UUID NOT NULL,
        capture_id TEXT,
        correlation_id UUID NOT NULL,
        sequence BIGINT NOT NULL,
        occurred_at TIMESTAMPTZ NOT NULL,
        payload JSONB NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS custodian_alert_records (
        alert_id TEXT PRIMARY KEY,
        capture_id TEXT,
        payload JSONB NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS custodian_projection_outbox (
        outbox_id BIGSERIAL PRIMARY KEY,
        event_id UUID NOT NULL UNIQUE,
        event JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        delivered_at TIMESTAMPTZ
    )
    """,
)


class PostgresEventStore:
    """Persist a consumer inbox claim and event/domain writes atomically.

    The psycopg dependency is imported only when no connection factory is injected,
    keeping PostgreSQL optional for local mode and deterministic unit tests.
    """

    def __init__(
        self,
        dsn: str | None = None,
        *,
        connection_factory: ConnectionFactory | None = None,
    ) -> None:
        if connection_factory is None and not dsn:
            raise ValueError("a PostgreSQL DSN or connection factory is required")
        if connection_factory is None:
            try:
                import psycopg
            except ImportError as exc:
                raise ImportError(
                    "PostgreSQL Kafka consumers require the optional dependency; "
                    "install Custodian with `.[postgres]`"
                ) from exc

            def connect():
                return psycopg.connect(dsn)

            connection_factory = connect
        self._connection_factory = connection_factory

    def initialize(self) -> None:
        """Create the small set of tables owned by the Kafka event store."""
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                for statement in POSTGRES_EVENT_SCHEMA:
                    cursor.execute(statement)

    def process_once(
        self,
        consumer_name: str,
        event: PipelineEvent,
        handler: EventWriteHandler | None = None,
    ) -> bool:
        """Atomically claim and persist an event; return False for duplicate delivery.

        ``handler`` may apply additional domain writes using the supplied connection.
        It runs in the same transaction as the inbox claim, event record, and alert
        upsert, so raising from it rolls every write back for Kafka redelivery.
        """
        if not consumer_name or len(consumer_name) > 128:
            raise ValueError("consumer_name must contain 1 to 128 characters")
        envelope = event.model_dump(mode="json")
        payload_json = json.dumps(envelope["payload"], sort_keys=True, separators=(",", ":"))
        envelope_json = json.dumps(envelope, sort_keys=True, separators=(",", ":"))
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO custodian_event_inbox(consumer_name, event_id)
                       VALUES (%s, %s) ON CONFLICT (consumer_name, event_id) DO NOTHING""",
                    (consumer_name, str(event.event_id)),
                )
                if cursor.rowcount == 0:
                    return False

                cursor.execute(
                    """INSERT INTO custodian_projection_outbox(event_id, event)
                       VALUES (%s, %s::jsonb) ON CONFLICT (event_id) DO NOTHING""",
                    (str(event.event_id), envelope_json),
                )

                cursor.execute(
                    """INSERT INTO custodian_pipeline_events(
                           event_id, event_type, schema_version, run_id, capture_id,
                           correlation_id, sequence, occurred_at, payload
                       ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                       ON CONFLICT (event_id) DO NOTHING""",
                    (
                        str(event.event_id),
                        event.event_type.value,
                        event.schema_version,
                        str(event.run_id),
                        event.capture_id,
                        str(event.correlation_id),
                        event.sequence,
                        event.occurred_at,
                        payload_json,
                    ),
                )

                if event.event_type == EventType.ALERT:
                    alert_id = str(event.payload["alert_id"])
                    cursor.execute(
                        """INSERT INTO custodian_alert_records(alert_id, capture_id, payload)
                           VALUES (%s, %s, %s::jsonb)
                           ON CONFLICT (alert_id) DO UPDATE SET
                               capture_id = EXCLUDED.capture_id,
                               payload = EXCLUDED.payload,
                               updated_at = CURRENT_TIMESTAMP""",
                        (alert_id, event.capture_id, payload_json),
                    )

                if handler is not None:
                    handler(connection, event)
        return True


class IdempotentEventHandler:
    """Adapt the PostgreSQL event store to the Kafka worker handler interface."""

    def __init__(
        self,
        store: PostgresEventStore,
        consumer_name: str,
        handler: EventWriteHandler | None = None,
    ) -> None:
        self.store = store
        self.consumer_name = consumer_name
        self.handler = handler

    def __call__(self, event: PipelineEvent) -> None:
        self.store.process_once(self.consumer_name, event, self.handler)


__all__ = ["IdempotentEventHandler", "PostgresEventStore"]
