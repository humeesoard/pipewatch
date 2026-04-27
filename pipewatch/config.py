"""Configuration loading for pipewatch alert rules and thresholds."""

import json
import os
from dataclasses import dataclass, field
from typing import List, Optional

DEFAULT_CONFIG_PATH = os.path.join(os.path.expanduser("~"), ".pipewatch", "config.json")


@dataclass
class ThresholdConfig:
    max_error_rate: float = 0.05
    min_records_per_second: float = 1.0


@dataclass
class PipewatchConfig:
    pipelines: List[str] = field(default_factory=list)
    thresholds: ThresholdConfig = field(default_factory=ThresholdConfig)
    alert_log_path: Optional[str] = None
    refresh_interval_seconds: float = 5.0


def load_config(path: Optional[str] = None) -> PipewatchConfig:
    """Load configuration from a JSON file, falling back to defaults."""
    config_path = path or DEFAULT_CONFIG_PATH
    if not os.path.exists(config_path):
        return PipewatchConfig()

    with open(config_path, "r") as f:
        raw = json.load(f)

    thresholds_raw = raw.get("thresholds", {})
    thresholds = ThresholdConfig(
        max_error_rate=thresholds_raw.get("max_error_rate", 0.05),
        min_records_per_second=thresholds_raw.get("min_records_per_second", 1.0),
    )

    return PipewatchConfig(
        pipelines=raw.get("pipelines", []),
        thresholds=thresholds,
        alert_log_path=raw.get("alert_log_path"),
        refresh_interval_seconds=raw.get("refresh_interval_seconds", 5.0),
    )


def save_config(config: PipewatchConfig, path: Optional[str] = None) -> None:
    """Persist a PipewatchConfig to disk as JSON."""
    config_path = path or DEFAULT_CONFIG_PATH
    os.makedirs(os.path.dirname(config_path), exist_ok=True)
    data = {
        "pipelines": config.pipelines,
        "thresholds": {
            "max_error_rate": config.thresholds.max_error_rate,
            "min_records_per_second": config.thresholds.min_records_per_second,
        },
        "alert_log_path": config.alert_log_path,
        "refresh_interval_seconds": config.refresh_interval_seconds,
    }
    with open(config_path, "w") as f:
        json.dump(data, f, indent=2)
