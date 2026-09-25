# Kafka event pipeline: Phase 1 contracts

**Status:** Phase 1 contract baseline for the optional pilot

**Scope:** Event boundaries, Redis live-state contract, and PostgreSQL idempotency contract. No broker or storage integration is implemented here.

This proposal is based on the existing replay runtime, `ApplicationEvent`, alert/flow contracts, SQLite repository, and API diagnostics. It keeps local replay as the default and does not activate live capture.

## Existing project boundaries

- `CustodianEngine` currently processes PCAP observations, updates flows and temporal state, extracts features, runs detectors, applies evidence policy, and emits alerts.
- `ReplaySession` owns the replay lifecycle, numeric in-process `run_id`, `capture_id`, current event hub, and persistence calls.
- `EventHub` holds bounded `ApplicationEvent` history for API streaming. Its current event envelope has `event_id`, `sequence`, `event_type`, `created_at`, and numeric `run_id`, but no `capture_id` or `correlation_id`.
- `SQLiteRepository` is the current durable repository. Alerts and flows are upserted by their domain IDs; application events are inserted by event ID.
- The API and frontend already consume run-scoped state and event records. Kafka transport events should map into these application-facing records rather than making Kafka a frontend dependency.
- There is no Redis, PostgreSQL, or Compose integration in the current repository.

## Proposed event envelope

Use a transport envelope independent of `ApplicationEvent`:

```json
{
  "event_id": "UUID",
  "event_type": "flow_update",
  "schema_version": "flow_update.v1",
  "occurred_at": "RFC3339 UTC timestamp",
  "run_id": "UUID for this replay execution",
  "capture_id": "capture-… or null for process-level diagnostics",
  "correlation_id": "UUID stable for this replay execution",
  "sequence": 12,
  "causation_id": "UUID of the event that directly caused this event, or null",
  "payload": {}
}
```

`sequence` and `causation_id` are part of the pilot contract for replay ordering and traceability. `event_id` is unique per emitted event; retries reuse the same event ID. `schema_version` identifies the payload contract independently of the topic namespace. Transport `run_id` is a UUID to avoid collisions across process restarts. The existing numeric run counter remains an internal/UI counter and is included as `local_run_number` in runtime-event payloads. `correlation_id` is created once per replay run and propagated unchanged through derived events. For startup/shutdown and broker-health diagnostics that are not associated with a capture, `capture_id` is present as `null`; `run_id` is a process-session UUID and `correlation_id` identifies the diagnostic operation.

All event schemas must be validated before publish and after consume. Enforce the configured byte limit against the serialized UTF-8 envelope. Payloads contain metadata only: no captured packet bytes, application payload, extracted files, passwords, JWTs, secrets, or model binaries. Invalid or oversized events are rejected before publishing; invalid consumed records are dead-lettered with bounded, sanitized error metadata.

## Topics and stage ownership

| Topic | Producer boundary | Consumer boundary | Partition key |
|---|---|---|---|
| `custodian.v1.packet_observation` | Validated passive parser output | Flow reconstruction | `capture_id:run_id` |
| `custodian.v1.flow_update` | Flow manager snapshot/update | Feature extraction and inference | `capture_id:run_id` |
| `custodian.v1.feature_vector` | Shared feature extractor | Detector inference | `capture_id:run_id` |
| `custodian.v1.detector_verdict` | Detector output | Evidence and alert policy | `capture_id:run_id` |
| `custodian.v1.alert` | Evidence-approved alert | PostgreSQL persistence and Redis live state | `capture_id:run_id` |
| `custodian.v1.runtime_event` | Replay lifecycle and operational diagnostics | PostgreSQL persistence and Redis live state | `capture_id:run_id` |
| `custodian.v1.dead_letter` | Exhausted/invalid consumer messages | Operator inspection only; no automatic re-consumption | Original key when available |

Kafka ordering is guaranteed only within a partition of a single topic. The shared key keeps a capture run on one partition per topic, but does not establish cross-topic ordering. Consumers must use `sequence`, `causation_id`, and idempotent stage transitions where causal ordering matters. The initial pilot uses one replay producer per run and a run-global monotonic sequence. If flow reconstruction is distributed later, both directions of a canonical flow must map to the same partition; that scale-out change must preserve the run-level ordering contract or explicitly version it.

## Redis live-state contract

Redis is a rebuildable dashboard projection, not the durable source of truth. PostgreSQL owns durable records. The pilot adopts this key namespace:

- `custodian:v1:run:{run_id}:status` — current replay state, capture ID, progress, timestamps, and latest error summary.
- `custodian:v1:run:{run_id}:metrics` — latest measured counters and rates.
- `custodian:v1:run:{run_id}:alerts` — sorted set of alert IDs ordered by event time; alert bodies live at `custodian:v1:run:{run_id}:alert:{alert_id}`.
- `custodian:v1:run:{run_id}:flows` — bounded sorted set of recent flow IDs; flow snapshots live at `custodian:v1:run:{run_id}:flow:{flow_id}`.
- `custodian:v1:runtime:readiness` — latest component readiness snapshot, including Kafka producer/consumer state, backlog, and dead-letter count.

Each run-scoped value includes `run_id`, `capture_id`, and last applied `sequence`. Projectors ignore events for a superseded run and ignore updates whose sequence is not newer for the same projection. Replay start creates a new run namespace; prior run state is retained for a configurable TTL (pilot default: 24 hours), then expires. Alert and flow collections have explicit maximum counts (pilot defaults: latest 1,000 alerts and 5,000 flow summaries per run); updates trim the oldest entries. Redis loss must be recoverable by rebuilding the current projection from PostgreSQL and/or replaying retained Kafka events. Redis must not be used to decide whether a durable alert has already been committed.

The 24-hour TTL and collection bounds are defaults and must be configurable. Kafka work depends on this logical contract, not on a particular Redis client or key encoding. Redis is not made a required API read path by this contract: existing API reads remain backed by the configured repository until the Redis pilot explicitly changes them. Redis is used for current dashboard projection and live fan-out; PostgreSQL remains the source for durable history.

## PostgreSQL durability and idempotency contract

Kafka-mode consumers write durable records to PostgreSQL; existing local mode continues using SQLite. Use a transactional inbox table keyed by `(consumer_name, event_id)`. In one database transaction, a consumer claims the event, applies its domain write, records a Redis projection outbox item where required, and marks the inbox item processed. A duplicate claim is a successful no-op. If the transaction fails, the claim, domain write, and outbox item roll back so Kafka can retry. A separate idempotent projector applies outbox items to Redis and records completion; this avoids pretending PostgreSQL and Redis can participate in one atomic transaction.

Domain records also retain unique IDs and use upserts:

- alerts: unique `alert_id`; duplicate *event delivery* must not increase `occurrence_count`;
- flows: unique `(capture_id, run_id, flow_id)` (or a documented globally unique flow ID);
- detector results: unique `(capture_id, run_id, result_id)`;
- runtime events: unique `event_id`.

Alert repetition is a distinct domain event/update and must carry a stable alert ID plus an explicit occurrence/revision value. It must not be inferred from receiving the same Kafka event again. Retention and migrations remain explicit; consumer offsets alone are not the idempotency mechanism.

## Mode and outage behavior

- Default mode is in-process and requires no Kafka, Redis, or PostgreSQL services.
- Kafka mode is explicitly selected. A broker outage marks readiness degraded and preserves the failure for diagnostics; it does not silently switch transports.
- A separately selected in-process mode continues without Kafka. No mode opens a live network capture device or adds packet transmission.
- Kafka and Redis contain metadata only. Local broker access binds to loopback.

## Resolved Phase 1 decisions

These defaults are selected for this issue so implementation can proceed without blocking on further design questions:

1. **Redis contract:** adopt the run-scoped namespace above, configurable 24-hour TTL, bounded 1,000-alert/5,000-flow collections, and rebuildable projection. Redis does not become the durable record store or silently replace repository-backed API reads.
2. **PostgreSQL:** required for durable persistence in Kafka mode. SQLite remains the local-mode default. Use a transactional inbox and projection outbox for idempotency and eventual Redis updates.
3. **Topic scope:** define and populate all seven required topics in the pilot. The initial vertical slice can wire packet/flow/runtime events first, but all remaining topics and schemas must exist and be covered before calling the issue complete.
4. **Run identity:** Kafka envelope `run_id` is a UUID unique per replay execution; preserve the existing numeric API/UI counter as `local_run_number`. `capture_id` is nullable only for process-level diagnostics; the field is always present.
5. **Ordering:** one replay producer assigns a run-global increasing sequence. Kafka partition key is `capture_id:run_id` for capture events and `process:run_id` for process diagnostics. Ordering is per topic/partition; consumers do not assume cross-topic ordering.
6. **Event schemas:** topic names retain the `custodian.v1` namespace; envelope `schema_version` is event-specific (`packet_observation.v1`, `flow_update.v1`, etc.) so payload evolution is explicit.

## Phase 2 implementation baseline

The transport-neutral contracts live in `src/custodian/events/`:

- `contracts.py` defines the seven event types, topic mapping, envelope validation, payload model validation, sensitive-key rejection, and UTF-8 size checks at serialization/deserialization boundaries.
- `bus.py` defines the `EventPublisher` and `EventConsumer` protocols plus `InProcessEventBus`.
- Packet observations reuse `PacketObservation`; flow updates wrap a `FlowRecord`; feature vectors, detector verdicts, and alerts reuse the current core contracts; runtime and dead-letter events use bounded metadata-only payloads.

The local bus is a bounded FIFO queue. Queue saturation raises an explicit backpressure error; it never silently drops an event. Empty polls return `None`. This adapter is available for upcoming pipeline seams but does not replace or alter the existing `EventHub`, replay engine, or SQLite-backed API flow. Kafka selection/configuration and runtime stage wiring are later phases. The default local demo therefore continues on its current path without importing a broker client.

## Phase 3 implementation baseline

`configs/kafka.yaml` adds disabled-by-default settings for loopback bootstrap servers, topic prefix, consumer group, and maximum event size. `configs/kafka.local.yaml` is Git-ignored. `CUSTODIAN_KAFKA_CONFIG` can select an additional YAML override, and `CUSTODIAN_KAFKA_*` variables override file values. Configuration validation rejects non-loopback broker endpoints for this local-only pilot.

`compose.kafka.yaml` defines one opt-in Redpanda broker with a health check, one CPU core, persistent named data, and only the Kafka host port bound to `127.0.0.1:9092`. See `docs/kafka-local-pilot.md` for start/stop instructions. The broker and setting do not change runtime mode or enable live capture; Kafka transport implementation remains a later phase.

## Phase 4 implementation baseline

`KafkaEventPublisher` and `KafkaEventConsumer` use the optional `confluent-kafka` dependency, loaded only when Kafka is enabled. The publisher enables broker idempotence, validates serialized size, uses `capture_id:run_id` as its partition key, and surfaces producer queue or delivery errors. The consumer disables automatic offset commits, validates the envelope and topic before returning it, and commits only after the caller explicitly acknowledges the event. It permits one outstanding message per consumer to preserve processing order. Invalid and oversized records are sent to the dead-letter topic with sanitized metadata before source offsets are committed. `KafkaEventWorker` retries handler errors a bounded number of times with capped exponential backoff; after exhaustion it publishes a dead-letter event and acknowledges the source only after dead-letter delivery succeeds. A dead-letter publishing failure leaves the original event uncommitted.

When `kafka.enabled` is true, the API creates and probes the Kafka producer. Replay starts fail safely if the configured producer or broker is unavailable. The replay session emits packet observations, flow snapshots/closures, feature vectors, detector verdicts, alerts, and replay lifecycle events through a transport-neutral engine callback. All receive per-run UUIDs, capture IDs, correlation IDs, and a sequence assigned under the session lock. The existing EventHub, SQLite persistence, and synchronous inference path continue running locally; this phase publishes stage outputs for future consumers and does not yet replace local stage execution. Readiness and diagnostics report Kafka producer state and backlog. With Kafka disabled, no Kafka client is imported or required.

## Phase 5 consumer reliability baseline

`KafkaEventWorker` runs one validated event at a time and requires a synchronous handler. Handler failures receive a bounded number of attempts with capped exponential backoff. Exhausted failures become metadata-only `dead_letter.v1` events that carry the source event ID and `causation_id`, but never the original message or exception text. Invalid schema/topic and over-limit messages also produce sanitized dead-letter records. Source offsets are committed only after successful processing or successful dead-letter delivery. Worker diagnostics expose health, processed count, retry count, dead-letter count, and the last sanitized failure summary.

## Phase 6 durable idempotency baseline

`PostgresEventStore` adds a PostgreSQL inbox keyed by `(consumer_name, event_id)`. The inbox claim, durable pipeline-event record, alert upsert, projection-outbox insert, and optional consumer-specific database callback run in one PostgreSQL transaction. Duplicate delivery for one consumer is a successful no-op. A callback failure rolls back the inbox claim and all database writes so Kafka can redeliver safely. Stable `alert_id` values are upserted without incrementing `occurrence_count` from duplicate delivery. Each consumer callback must use the supplied connection for any side effects that must share this transaction.

Install the optional database adapter with `pip install -e '.[postgres]'`. Construct `PostgresEventStore` with a DSN supplied from a local secret/environment variable, call `initialize()` during service setup, and pass `IdempotentEventHandler(store, consumer_name, callback)` as the `KafkaEventWorker` handler. The adapter is not selected by the FastAPI replay process yet; its default SQLite-only demo remains unchanged. PostgreSQL-backed consumer deployment must use this handler (or an equivalent transactional inbox) before enabling durable Kafka consumers.

`custodian_projection_outbox` records the validated event in the same PostgreSQL transaction. Delivering those outbox records to Redis is a separate idempotent projection worker; it is intentionally not performed inside the PostgreSQL transaction. Redis remains a rebuildable live-state cache.

## Phase 1 exit criteria

- Runtime event boundaries and current local persistence are mapped to the proposed stages.
- Event envelope, topic names, ordering limits, and identifier semantics are defined.
- Redis is specified as a bounded, rebuildable live projection with per-run isolation.
- PostgreSQL idempotency semantics cover duplicate delivery and alert occurrence updates.
- Defaults are resolved before transport implementation begins; future changes require an explicit contract/version update.
