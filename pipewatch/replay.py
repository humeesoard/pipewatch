"""Replay historical metric snapshots for debugging and analysis."""

from dataclasses import dataclass, field
from typing import List, Optional, Iterator
from pipewatch.history import PipelineHistory, MetricSnapshot


@dataclass
class ReplaySession:
    """Represents a replay session over a pipeline's history."""
    pipeline_name: str
    snapshots: List[MetricSnapshot]
    current_index: int = field(default=0, init=False)

    def __post_init__(self):
        self.snapshots = sorted(self.snapshots, key=lambda s: s.timestamp)

    @property
    def total(self) -> int:
        return len(self.snapshots)

    @property
    def current(self) -> Optional[MetricSnapshot]:
        if 0 <= self.current_index < self.total:
            return self.snapshots[self.current_index]
        return None

    @property
    def is_finished(self) -> bool:
        return self.current_index >= self.total

    def step(self) -> Optional[MetricSnapshot]:
        """Advance to the next snapshot and return it."""
        snapshot = self.current
        if snapshot is not None:
            self.current_index += 1
        return snapshot

    def reset(self) -> None:
        """Reset replay to the beginning."""
        self.current_index = 0

    def iter_remaining(self) -> Iterator[MetricSnapshot]:
        """Iterate over all remaining snapshots from current position."""
        while not self.is_finished:
            snapshot = self.step()
            if snapshot is not None:
                yield snapshot


def create_replay_session(history: PipelineHistory, pipeline_name: str) -> Optional[ReplaySession]:
    """Create a ReplaySession from a PipelineHistory for a given pipeline."""
    snapshots = history.snapshots_for(pipeline_name)
    if not snapshots:
        return None
    return ReplaySession(pipeline_name=pipeline_name, snapshots=list(snapshots))


def replay_all(history: PipelineHistory, pipeline_name: str) -> List[MetricSnapshot]:
    """Return all snapshots for a pipeline in chronological order."""
    session = create_replay_session(history, pipeline_name)
    if session is None:
        return []
    return list(session.iter_remaining())
