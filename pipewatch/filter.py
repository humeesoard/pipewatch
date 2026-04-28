"""Filtering utilities for pipeline metrics and history snapshots."""

from dataclasses import dataclass
from typing import Optional, List

from pipewatch.metrics import PipelineMetric
from pipewatch.history import MetricSnapshot


@dataclass
class FilterCriteria:
    """Criteria used to filter metrics or snapshots."""
    pipeline_name: Optional[str] = None
    min_throughput: Optional[float] = None
    max_error_rate: Optional[float] = None
    healthy_only: bool = False


def matches_metric(metric: PipelineMetric, criteria: FilterCriteria) -> bool:
    """Return True if a metric satisfies all filter criteria."""
    if criteria.pipeline_name is not None:
        if metric.pipeline_name != criteria.pipeline_name:
            return False

    if criteria.min_throughput is not None:
        if metric.throughput < criteria.min_throughput:
            return False

    if criteria.max_error_rate is not None:
        total = metric.record_count
        rate = (metric.error_count / total) if total > 0 else 0.0
        if rate > criteria.max_error_rate:
            return False

    if criteria.healthy_only:
        from pipewatch.metrics import is_healthy
        if not is_healthy(metric):
            return False

    return True


def filter_metrics(
    metrics: List[PipelineMetric],
    criteria: FilterCriteria,
) -> List[PipelineMetric]:
    """Return metrics that match the given criteria."""
    return [m for m in metrics if matches_metric(m, criteria)]


def filter_snapshots(
    snapshots: List[MetricSnapshot],
    criteria: FilterCriteria,
) -> List[MetricSnapshot]:
    """Return snapshots whose embedded metric matches the given criteria."""
    return [s for s in snapshots if matches_metric(s.metric, criteria)]
