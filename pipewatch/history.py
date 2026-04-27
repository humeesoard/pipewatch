"""Metric history tracking for pipewatch pipelines."""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional

from pipewatch.metrics import PipelineMetric

DEFAULT_MAX_ENTRIES = 100


@dataclass
class MetricSnapshot:
    """A timestamped snapshot of a pipeline metric."""

    timestamp: float
    metric: PipelineMetric

    def age_seconds(self) -> float:
        """Return how many seconds ago this snapshot was taken."""
        return time.time() - self.timestamp


@dataclass
class PipelineHistory:
    """Circular buffer of metric snapshots for a single pipeline."""

    pipeline_name: str
    max_entries: int = DEFAULT_MAX_ENTRIES
    _snapshots: Deque[MetricSnapshot] = field(default_factory=deque, repr=False)

    def record(self, metric: PipelineMetric) -> None:
        """Append a new snapshot, evicting oldest if at capacity."""
        snapshot = MetricSnapshot(timestamp=time.time(), metric=metric)
        if len(self._snapshots) >= self.max_entries:
            self._snapshots.popleft()
        self._snapshots.append(snapshot)

    def latest(self) -> Optional[MetricSnapshot]:
        """Return the most recent snapshot, or None if empty."""
        return self._snapshots[-1] if self._snapshots else None

    def all_snapshots(self) -> List[MetricSnapshot]:
        """Return all snapshots in chronological order."""
        return list(self._snapshots)

    def __len__(self) -> int:
        return len(self._snapshots)


class HistoryStore:
    """In-memory store of per-pipeline history buffers."""

    def __init__(self, max_entries: int = DEFAULT_MAX_ENTRIES) -> None:
        self.max_entries = max_entries
        self._store: Dict[str, PipelineHistory] = {}

    def record(self, metric: PipelineMetric) -> None:
        """Record a metric snapshot for its pipeline."""
        name = metric.pipeline_name
        if name not in self._store:
            self._store[name] = PipelineHistory(
                pipeline_name=name, max_entries=self.max_entries
            )
        self._store[name].record(metric)

    def get(self, pipeline_name: str) -> Optional[PipelineHistory]:
        """Retrieve history for a pipeline, or None if unknown."""
        return self._store.get(pipeline_name)

    def all_pipelines(self) -> List[str]:
        """Return sorted list of tracked pipeline names."""
        return sorted(self._store.keys())

    def clear(self, pipeline_name: Optional[str] = None) -> None:
        """Clear history for one pipeline or all pipelines."""
        if pipeline_name is not None:
            self._store.pop(pipeline_name, None)
        else:
            self._store.clear()
