"""Scheduler for periodically collecting pipeline metrics."""

import time
import threading
from typing import Callable, Dict, Optional
from dataclasses import dataclass, field


@dataclass
class ScheduledPipeline:
    pipeline_id: str
    interval_seconds: float
    collector: Callable[[], None]
    _last_run: float = field(default=0.0, init=False)
    _running: bool = field(default=False, init=False)


def is_due(pipeline: ScheduledPipeline, now: Optional[float] = None) -> bool:
    """Return True if the pipeline is due for collection."""
    now = now if now is not None else time.time()
    return (now - pipeline._last_run) >= pipeline.interval_seconds


def mark_run(pipeline: ScheduledPipeline, now: Optional[float] = None) -> None:
    """Update the last run timestamp."""
    pipeline._last_run = now if now is not None else time.time()


class MetricScheduler:
    """Manages periodic metric collection for multiple pipelines."""

    def __init__(self, tick_interval: float = 1.0):
        self.tick_interval = tick_interval
        self._pipelines: Dict[str, ScheduledPipeline] = {}
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def register(self, pipeline: ScheduledPipeline) -> None:
        """Register a pipeline for scheduled collection."""
        self._pipelines[pipeline.pipeline_id] = pipeline

    def unregister(self, pipeline_id: str) -> None:
        """Remove a pipeline from the scheduler."""
        self._pipelines.pop(pipeline_id, None)

    def _tick(self) -> None:
        while not self._stop_event.is_set():
            now = time.time()
            for pipeline in list(self._pipelines.values()):
                if is_due(pipeline, now):
                    try:
                        pipeline.collector()
                    except Exception:
                        pass
                    mark_run(pipeline, now)
            self._stop_event.wait(self.tick_interval)

    def start(self) -> None:
        """Start the background scheduler thread."""
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._tick, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop the background scheduler thread."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None

    @property
    def pipeline_ids(self):
        return list(self._pipelines.keys())
