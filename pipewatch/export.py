"""Export pipeline metrics and history to common formats (JSON, CSV)."""

from __future__ import annotations

import csv
import json
import io
from typing import List

from pipewatch.history import MetricSnapshot, PipelineHistory
from pipewatch.aggregation import AggregatedStats


def snapshot_to_dict(snapshot: MetricSnapshot) -> dict:
    """Convert a MetricSnapshot to a plain dictionary."""
    return {
        "pipeline_id": snapshot.metric.pipeline_id,
        "timestamp": snapshot.timestamp,
        "total_records": snapshot.metric.total_records,
        "failed_records": snapshot.metric.failed_records,
        "throughput": snapshot.metric.throughput,
        "latency_ms": snapshot.metric.latency_ms,
        "error_rate": round(snapshot.metric.error_rate, 6),
        "is_healthy": snapshot.metric.is_healthy,
    }


def export_history_json(history: PipelineHistory) -> str:
    """Export all snapshots in a PipelineHistory as a JSON string."""
    snapshots = [snapshot_to_dict(s) for s in history.snapshots]
    return json.dumps(snapshots, indent=2)


def export_history_csv(history: PipelineHistory) -> str:
    """Export all snapshots in a PipelineHistory as a CSV string."""
    output = io.StringIO()
    fieldnames = [
        "pipeline_id",
        "timestamp",
        "total_records",
        "failed_records",
        "throughput",
        "latency_ms",
        "error_rate",
        "is_healthy",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for snapshot in history.snapshots:
        writer.writerow(snapshot_to_dict(snapshot))
    return output.getvalue()


def export_stats_json(pipeline_id: str, stats: AggregatedStats) -> str:
    """Serialize AggregatedStats for a pipeline to a JSON string."""
    data = {
        "pipeline_id": pipeline_id,
        "snapshot_count": stats.snapshot_count,
        "avg_error_rate": round(stats.avg_error_rate, 6),
        "avg_throughput": round(stats.avg_throughput, 4),
        "avg_latency_ms": round(stats.avg_latency_ms, 4),
        "total_records": stats.total_records,
        "total_failed": stats.total_failed,
        "overall_error_rate": round(stats.overall_error_rate, 6),
    }
    return json.dumps(data, indent=2)
