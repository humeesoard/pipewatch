"""Tests for configuration loading and saving."""

import json
import os
import pytest
from pipewatch.config import (
    load_config,
    save_config,
    PipewatchConfig,
    ThresholdConfig,
)


def test_load_config_defaults_when_no_file(tmp_path):
    config = load_config(path=str(tmp_path / "nonexistent.json"))
    assert isinstance(config, PipewatchConfig)
    assert config.thresholds.max_error_rate == 0.05
    assert config.thresholds.min_records_per_second == 1.0
    assert config.pipelines == []
    assert config.refresh_interval_seconds == 5.0


def test_load_config_from_file(tmp_path):
    config_data = {
        "pipelines": ["etl_daily", "stream_events"],
        "thresholds": {
            "max_error_rate": 0.02,
            "min_records_per_second": 5.0,
        },
        "refresh_interval_seconds": 10.0,
        "alert_log_path": "/var/log/pipewatch.log",
    }
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps(config_data))

    config = load_config(path=str(config_file))
    assert config.pipelines == ["etl_daily", "stream_events"]
    assert config.thresholds.max_error_rate == 0.02
    assert config.thresholds.min_records_per_second == 5.0
    assert config.refresh_interval_seconds == 10.0
    assert config.alert_log_path == "/var/log/pipewatch.log"


def test_save_and_reload_config(tmp_path):
    config = PipewatchConfig(
        pipelines=["my_pipeline"],
        thresholds=ThresholdConfig(max_error_rate=0.1, min_records_per_second=2.0),
        alert_log_path="/tmp/alerts.log",
        refresh_interval_seconds=3.0,
    )
    config_path = str(tmp_path / "saved_config.json")
    save_config(config, path=config_path)

    loaded = load_config(path=config_path)
    assert loaded.pipelines == ["my_pipeline"]
    assert loaded.thresholds.max_error_rate == 0.1
    assert loaded.thresholds.min_records_per_second == 2.0
    assert loaded.alert_log_path == "/tmp/alerts.log"
    assert loaded.refresh_interval_seconds == 3.0


def test_save_config_creates_directory(tmp_path):
    nested_path = str(tmp_path / "subdir" / "config.json")
    config = PipewatchConfig()
    save_config(config, path=nested_path)
    assert os.path.exists(nested_path)


def test_load_config_partial_thresholds(tmp_path):
    config_data = {"thresholds": {"max_error_rate": 0.01}}
    config_file = tmp_path / "partial.json"
    config_file.write_text(json.dumps(config_data))

    config = load_config(path=str(config_file))
    assert config.thresholds.max_error_rate == 0.01
    assert config.thresholds.min_records_per_second == 1.0  # default preserved
