# Local Kafka-compatible broker pilot

Custodian's standard local PCAP replay does not need Kafka. The broker setup is opt-in and is not started by the application. Phase 4 adds optional Kafka transport adapters and publishes validated replay-stage metadata when Kafka is explicitly enabled. The existing in-process event hub, replay, and SQLite persistence remain active. Kafka consumers expose validated events to a separate processing component; PostgreSQL/Redis projection, idempotent consumer processing, and dead-letter retry handling are later work.

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

The API probes the broker at startup and again before replay. When Kafka is enabled, the existing replay stages publish validated packet, flow, feature, verdict, alert, and lifecycle events. The in-process runtime and SQLite persistence remain active. A Kafka consumer can be created by a separate processing component; it must acknowledge a validated event after processing. Invalid messages are not committed and block that consumer until dead-letter handling is added in Phase 5.

## Live broker integration check

The normal test suite uses Kafka client fakes. To run the opt-in end-to-end adapter check, start the local Redpanda service above and install the optional Kafka extra, then run:

```sh
CUSTODIAN_KAFKA_INTEGRATION=1 .venv/bin/python -m pytest tests/integration/test_kafka_live.py
```

The test provisions the versioned runtime-event topic if needed, publishes one metadata-only event, consumes it with a unique consumer group, validates its run/capture/correlation identifiers, and acknowledges it. Set `CUSTODIAN_KAFKA_BOOTSTRAP_SERVERS` to another loopback Kafka-compatible address if the broker uses a non-default port. The test does not run in the default suite and rejects non-loopback broker addresses through normal Kafka settings validation.

Do not put credentials, tokens, secrets, or remote broker addresses in the pilot configuration. The Compose file is for one-laptop development, not production deployment.
