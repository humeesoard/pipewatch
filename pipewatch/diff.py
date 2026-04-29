"""Metric diff module: compare two snapshots and report changes."""

from dataclasses import dataclass
from typing import Optional

from pipewatch.history import MetricSnapshot


@dataclass
class MetricDiff:
    pipeline_name: str
    prev_error_rate: float
    curr_error_rate: float
    prev_throughput: int
    curr_throughput: int
    prev_latency_ms: float
    curr_latency_ms: float

    @property
    def error_rate_delta(self) -> float:
        return self.curr_error_rate - self.prev_error_rate

    @property
    def throughput_delta(self) -> int:
        return self.curr_throughput - self.prev_throughput

    @property
    def latency_delta(self) -> float:
        return self.curr_latency_ms - self.prev_latency_ms

    @property
    def has_degraded(self) -> bool:
        return self.error_rate_delta > 0.01 or self.latency_delta > 50

    @property
    def has_improved(self) -> bool:
        return self.error_rate_delta < -0.01 and self.latency_delta <= 0


def diff_snapshots(
    prev: MetricSnapshot, curr: MetricSnapshot
) -> Optional[MetricDiff]:
    """Compare two snapshots for the same pipeline and return a MetricDiff.

    Returns None if the snapshots belong to different pipelines.
    """
    if prev.metric.pipeline_name != curr.metric.pipeline_name:
        return None

    return MetricDiff(
        pipeline_name=curr.metric.pipeline_name,
        prev_error_rate=prev.metric.error_rate,
        curr_error_rate=curr.metric.error_rate,
        prev_throughput=prev.metric.records_processed,
        curr_throughput=curr.metric.records_processed,
        prev_latency_ms=prev.metric.latency_ms,
        curr_latency_ms=curr.metric.latency_ms,
    )


def diff_to_dict(d: MetricDiff) -> dict:
    return {
        "pipeline_name": d.pipeline_name,
        "error_rate": {"prev": d.prev_error_rate, "curr": d.curr_error_rate, "delta": d.error_rate_delta},
        "throughput": {"prev": d.prev_throughput, "curr": d.curr_throughput, "delta": d.throughput_delta},
        "latency_ms": {"prev": d.prev_latency_ms, "curr": d.curr_latency_ms, "delta": d.latency_delta},
        "has_degraded": d.has_degraded,
        "has_improved": d.has_improved,
    }
