"""Baseline comparison: compare current metrics against a saved baseline snapshot."""

from dataclasses import dataclass, field
from typing import Optional
import json
import os

from pipewatch.metrics import PipelineMetric, error_rate


@dataclass
class BaselineEntry:
    pipeline_name: str
    error_rate: float
    throughput: float
    latency_p99: float


@dataclass
class BaselineComparison:
    pipeline_name: str
    baseline: BaselineEntry
    current: PipelineMetric
    error_rate_delta: float
    throughput_delta: float
    latency_delta: float
    has_regressed: bool


def _entry_from_metric(metric: PipelineMetric) -> BaselineEntry:
    return BaselineEntry(
        pipeline_name=metric.pipeline_name,
        error_rate=error_rate(metric),
        throughput=metric.throughput,
        latency_p99=metric.latency_p99,
    )


def save_baseline(metrics: list[PipelineMetric], path: str) -> None:
    """Persist a list of metrics as a baseline JSON file."""
    os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
    entries = [vars(_entry_from_metric(m)) for m in metrics]
    with open(path, "w") as f:
        json.dump(entries, f, indent=2)


def load_baseline(path: str) -> dict[str, BaselineEntry]:
    """Load baseline entries keyed by pipeline name."""
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        raw = json.load(f)
    return {
        item["pipeline_name"]: BaselineEntry(**item)
        for item in raw
    }


def compare_to_baseline(
    metric: PipelineMetric,
    baseline_map: dict[str, BaselineEntry],
    regression_threshold: float = 0.05,
) -> Optional[BaselineComparison]:
    """Compare a metric against its baseline entry, if available."""
    entry = baseline_map.get(metric.pipeline_name)
    if entry is None:
        return None

    current_error_rate = error_rate(metric)
    er_delta = current_error_rate - entry.error_rate
    tp_delta = metric.throughput - entry.throughput
    lat_delta = metric.latency_p99 - entry.latency_p99

    regressed = (
        er_delta > regression_threshold
        or tp_delta < -regression_threshold * entry.throughput
        or lat_delta > regression_threshold * entry.latency_p99
    )

    return BaselineComparison(
        pipeline_name=metric.pipeline_name,
        baseline=entry,
        current=metric,
        error_rate_delta=er_delta,
        throughput_delta=tp_delta,
        latency_delta=lat_delta,
        has_regressed=regressed,
    )
