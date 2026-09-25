# Local Kafka-compatible broker pilot

Custodian's standard local PCAP replay does not need Kafka. The broker setup is opt-in and is not started by the application. Phase 4 adds optional Kafka transport adapters and publishes validated replay-stage metadata when Kafka is explicitly enabled. Phase 5 adds a bounded consumer worker with retry and dead-letter handling. Phase 6 adds an optional PostgreSQL inbox for transactional event idempotency and a projection outbox. The existing in-process event hub, replay, and SQLite persistence remain active; the API does not automatically start Kafka consumers or require PostgreSQL.

## Start the local broker

Prerequisites: Docker Engine and the Docker Compose plugin.

From the repository root:

```sh
docker compose -f compose.kafka.yaml up -d
docker compose -f compose.kafka.yaml ps
```

The single Redpanda service exposes only the Kafka listener at `127.0.0.1:9092`. Its health check must pass before it is ready. No console, HTTP proxy, schema registry, or host-visible admin port is included.

Stop the service while preserving its local broker data:

```sh
docker compose -f compose.kafka.yaml down
```

To remove the broker's named data volume as well:

```sh
docker compose -f compose.kafka.yaml down --volumes
```

## Configuration

`configs/kafka.yaml` is the tracked, disabled-by-default configuration. The loader overlays `configs/kafka.local.yaml` when present; that file is ignored by Git. `CUSTODIAN_KAFKA_CONFIG` selects an additional YAML override file. Environment values take precedence over both files:

| Variable | Example | Default |
|---|---|---|
| `CUSTODIAN_KAFKA_ENABLED` | `true` | `false` |
| `CUSTODIAN_KAFKA_BOOTSTRAP_SERVERS` | `127.0.0.1:9092` | `127.0.0.1:9092` |
| `CUSTODIAN_KAFKA_TOPIC_PREFIX` | `custodian.v1` | `custodian.v1` |
| `CUSTODIAN_KAFKA_CONSUMER_GROUP` | `custodian-pilot` | `custodian-pilot` |
| `CUSTODIAN_KAFKA_MAX_EVENT_BYTES` | `262144` | `262144` |

For example, enable the pilot setting in a shell after starting the broker:

```sh
export CUSTODIAN_KAFKA_ENABLED=true
export CUSTODIAN_KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
```

Current configuration validation accepts loopback bootstrap addresses only, including `localhost` and loopback IPs. It rejects remote broker endpoints for this local pilot. Kafka mode needs the optional client extra:

```sh
.venv/bin/python -m pip install -e '.[kafka]'
```

The API probes the broker at startup and again before replay. When Kafka is enabled, the existing replay stages publish validated packet, flow, feature, verdict, alert, and lifecycle events. The in-process runtime and SQLite persistence remain active. A Kafka consumer can be created by a separate processing component. `KafkaEventWorker` retries a handler a bounded number of times with capped exponential backoff. On exhaustion it publishes a sanitized dead-letter envelope and only then acknowledges the source event. Invalid or oversized input is dead-lettered without copying the original message; if dead-letter publishing fails, its source offset is not committed. Worker diagnostics expose processing, retry, and dead-letter counts. Handlers that write durable records must still use `event_id` as an idempotency key; the PostgreSQL inbox transaction is not implemented by this local pilot.

## Live broker integration check

The normal test suite uses Kafka client fakes. To run the opt-in end-to-end adapter check, start the local Redpanda service above and install the optional Kafka extra, then run:

```sh
CUSTODIAN_KAFKA_INTEGRATION=1 .venv/bin/python -m pytest tests/integration/test_kafka_live.py
```

The tests provision their versioned topics if needed, publish and consume a metadata-only runtime event, and send a malformed schema-only record through the sanitized dead-letter path. Set `CUSTODIAN_KAFKA_BOOTSTRAP_SERVERS` to another loopback Kafka-compatible address if the broker uses a non-default port. The tests do not run in the default suite and reject non-loopback broker addresses through normal Kafka settings validation.

## PostgreSQL-backed consumer idempotency

Install the optional PostgreSQL adapter with:

```sh
.venv/bin/python -m pip install -e '.[postgres]'
```

Pass a DSN from a local environment variable or secret store to `PostgresEventStore`; do not commit DSNs or passwords. Call `initialize()` during consumer setup, then wrap the domain handler with `IdempotentEventHandler(store, consumer_name, handler)` and use that wrapper with `KafkaEventWorker`. Inbox claims, event records, alert upserts, the Redis projection outbox item, and callback writes share one PostgreSQL transaction. A duplicate `(consumer_name, event_id)` is acknowledged as an already-processed no-op. If the callback fails, the transaction rolls back and the worker can retry the Kafka record. The API does not enable this consumer mode automatically; the regular demo remains SQLite-backed.

Do not put credentials, tokens, secrets, or remote broker addresses in the pilot configuration. The Compose file is for one-laptop development, not production deployment.
