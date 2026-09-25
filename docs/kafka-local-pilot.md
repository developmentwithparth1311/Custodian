# Local Kafka-compatible broker pilot

Custodian's standard local PCAP replay does not need Kafka. The broker setup is opt-in and is not started by the application. Phase 3 adds local broker configuration and infrastructure only; Kafka transport selection and runtime publishing/consuming are implemented in later phases.

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

Current configuration validation accepts loopback bootstrap addresses only, including `localhost` and loopback IPs. It rejects remote broker endpoints for this local pilot. The setting does not yet connect a producer or consumer; the default application remains on its existing in-process replay path.

Do not put credentials, tokens, secrets, or remote broker addresses in the pilot configuration. The Compose file is for one-laptop development, not production deployment.
