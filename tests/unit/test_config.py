"""Tests for strict loading of the Phase 0 configuration bundle."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from custodian.config import DefaultSettings, load_config_bundle
from custodian.core.enums import FeatureFamily, ReplayMode


def test_repository_config_bundle_loads() -> None:
    config_dir = Path(__file__).resolve().parents[2] / "configs"

    bundle = load_config_bundle(config_dir)

    assert bundle.defaults.temporal_windows_seconds == (10, 60, 300)
    assert bundle.replay.mode is ReplayMode.PACED
    assert set(bundle.models.models) == set(FeatureFamily)
    assert bundle.models.models[FeatureFamily.BEHAVIOUR].artifact_path.name == "behaviour-xgb-v1"
    assert not bundle.models.models[FeatureFamily.BEHAVIOUR].trusted
    assert not bundle.models.models[FeatureFamily.DNS].enabled
    assert "dga" in bundle.models.models[FeatureFamily.DNS].variants
    assert not bundle.models.models[FeatureFamily.DNS].variants["dga"].enabled
    assert not bundle.models.models[FeatureFamily.TLS_QUIC].enabled
    assert bundle.storage.enabled
    assert bundle.storage.database_path.name == "custodian.sqlite3"
    assert not bundle.kafka.enabled
    assert bundle.kafka.bootstrap_servers == ("127.0.0.1:9092",)
    assert bundle.kafka.topic_prefix == "custodian.v1"
    assert bundle.kafka.max_event_bytes == 262144


def test_local_models_config_can_be_selected_explicitly(tmp_path, monkeypatch) -> None:
    config_dir = Path(__file__).resolve().parents[2] / "configs"
    override = tmp_path / "models.local.yaml"
    override.write_text((config_dir / "models.yaml").read_text(encoding="utf-8"), encoding="utf-8")
    monkeypatch.setenv("CUSTODIAN_MODELS_CONFIG", str(override))

    bundle = load_config_bundle(config_dir)

    assert bundle.models.models[FeatureFamily.BEHAVIOUR].artifact_path.name == "behaviour-xgb-v1"


def test_temporal_windows_must_be_sorted_and_unique() -> None:
    with pytest.raises(ValidationError, match="unique and sorted"):
        DefaultSettings(
            project_name="Custodian",
            environment="test",
            log_level="INFO",
            flow_idle_timeout_seconds=60,
            temporal_windows_seconds=(60, 10, 60),
        )


def test_kafka_environment_overrides_are_applied(monkeypatch) -> None:
    config_dir = Path(__file__).resolve().parents[2] / "configs"
    monkeypatch.setenv("CUSTODIAN_KAFKA_ENABLED", "true")
    monkeypatch.setenv("CUSTODIAN_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092,127.0.0.1:19092")
    monkeypatch.setenv("CUSTODIAN_KAFKA_TOPIC_PREFIX", "custodian.test.v1")
    monkeypatch.setenv("CUSTODIAN_KAFKA_CONSUMER_GROUP", "test-consumers")
    monkeypatch.setenv("CUSTODIAN_KAFKA_MAX_EVENT_BYTES", "4096")

    bundle = load_config_bundle(config_dir)

    assert bundle.kafka.enabled
    assert bundle.kafka.bootstrap_servers == ("localhost:9092", "127.0.0.1:19092")
    assert bundle.kafka.topic_prefix == "custodian.test.v1"
    assert bundle.kafka.consumer_group == "test-consumers"
    assert bundle.kafka.max_event_bytes == 4096


def test_kafka_local_override_is_loaded_and_remote_servers_are_rejected(
    tmp_path, monkeypatch
) -> None:
    config_dir = Path(__file__).resolve().parents[2] / "configs"
    local_override = tmp_path / "kafka.local.yaml"
    local_override.write_text("enabled: true\nmax_event_bytes: 8192\n", encoding="utf-8")
    target = tmp_path / "configs"
    target.mkdir()
    for filename in (
        "default.yaml",
        "replay.yaml",
        "evidence.yaml",
        "severity.yaml",
        "models.yaml",
        "storage.yaml",
        "kafka.yaml",
    ):
        (target / filename).write_text(
            (config_dir / filename).read_text(encoding="utf-8"), encoding="utf-8"
        )
    (target / "kafka.local.yaml").write_text(
        local_override.read_text(encoding="utf-8"), encoding="utf-8"
    )

    bundle = load_config_bundle(target)
    assert bundle.kafka.enabled
    assert bundle.kafka.max_event_bytes == 8192

    monkeypatch.setenv("CUSTODIAN_KAFKA_BOOTSTRAP_SERVERS", "broker.example.com:9092")
    with pytest.raises(ValidationError, match="loopback addresses"):
        load_config_bundle(target)
