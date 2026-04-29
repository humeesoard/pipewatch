"""Summary module for aggregating pipeline health across all tracked pipelines."""

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.aggregation import AggregatedStats, compute_stats
from pipewatch.history import PipelineHistory


@dataclass
class PipelineSummary:
    pipeline_name: str
    stats: Optional[AggregatedStats]
    is_healthy: bool
    snapshot_count: int


@dataclass
class OverallSummary:
    total_pipelines: int
    healthy_count: int
    unhealthy_count: int
    no_data_count: int
    pipeline_summaries: List[PipelineSummary]


def summarize_pipeline(name: str, history: PipelineHistory) -> PipelineSummary:
    """Produce a summary for a single named pipeline from its history."""
    snapshots = history.snapshots
    stats = compute_stats(history)
    snapshot_count = len(snapshots)

    if stats is None:
        healthy = False
    else:
        healthy = stats.avg_error_rate < 0.05 and stats.avg_throughput > 0

    return PipelineSummary(
        pipeline_name=name,
        stats=stats,
        is_healthy=healthy,
        snapshot_count=snapshot_count,
    )


def build_overall_summary(
    histories: dict[str, PipelineHistory],
) -> OverallSummary:
    """Build a cross-pipeline summary from a mapping of name -> PipelineHistory."""
    summaries: List[PipelineSummary] = []
    healthy = 0
    unhealthy = 0
    no_data = 0

    for name, history in histories.items():
        ps = summarize_pipeline(name, history)
        summaries.append(ps)
        if ps.stats is None:
            no_data += 1
        elif ps.is_healthy:
            healthy += 1
        else:
            unhealthy += 1

    return OverallSummary(
        total_pipelines=len(histories),
        healthy_count=healthy,
        unhealthy_count=unhealthy,
        no_data_count=no_data,
        pipeline_summaries=summaries,
    )
