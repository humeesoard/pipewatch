"""Aggregate statistics computed from a PipelineHistory buffer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pipewatch.history import PipelineHistory
from pipewatch.metrics import error_rate


@dataclass
class AggregatedStats:
    """Summary statistics over a window of metric snapshots."""

    pipeline_name: str
    sample_count: int
    avg_error_rate: float
    max_error_rate: float
    min_throughput: float
    avg_throughput: float
    max_throughput: float
    total_records_processed: int
    total_records_failed: int

    @property
    def overall_error_rate(self) -> float:
        if self.total_records_processed == 0:
            return 0.0
        return self.total_records_failed / self.total_records_processed


def compute_stats(history: PipelineHistory) -> Optional[AggregatedStats]:
    """Compute aggregated stats from a history buffer.

    Returns None if the history contains no snapshots.
    """
    snapshots = history.all_snapshots()
    if not snapshots:
        return None

    error_rates = [error_rate(s.metric) for s in snapshots]
    throughputs = [s.metric.throughput_per_second for s in snapshots]
    total_processed = sum(s.metric.records_processed for s in snapshots)
    total_failed = sum(s.metric.records_failed for s in snapshots)

    return AggregatedStats(
        pipeline_name=history.pipeline_name,
        sample_count=len(snapshots),
        avg_error_rate=sum(error_rates) / len(error_rates),
        max_error_rate=max(error_rates),
        min_throughput=min(throughputs),
        avg_throughput=sum(throughputs) / len(throughputs),
        max_throughput=max(throughputs),
        total_records_processed=total_processed,
        total_records_failed=total_failed,
    )
