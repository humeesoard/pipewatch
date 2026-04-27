"""Tests for pipewatch.scheduler."""

import time
import pytest
from unittest.mock import MagicMock

from pipewatch.scheduler import (
    MetricScheduler,
    ScheduledPipeline,
    is_due,
    mark_run,
)


def make_pipeline(pipeline_id="pipe-1", interval=10.0):
    collector = MagicMock()
    return ScheduledPipeline(
        pipeline_id=pipeline_id,
        interval_seconds=interval,
        collector=collector,
    )


def test_is_due_when_never_run():
    pipeline = make_pipeline(interval=5.0)
    assert is_due(pipeline) is True


def test_is_due_false_immediately_after_mark_run():
    pipeline = make_pipeline(interval=60.0)
    mark_run(pipeline)
    assert is_due(pipeline) is False


def test_is_due_true_after_interval_elapsed():
    pipeline = make_pipeline(interval=1.0)
    past = time.time() - 2.0
    pipeline._last_run = past
    assert is_due(pipeline) is True


def test_mark_run_updates_last_run():
    pipeline = make_pipeline()
    before = time.time()
    mark_run(pipeline)
    assert pipeline._last_run >= before


def test_register_and_unregister():
    scheduler = MetricScheduler()
    pipeline = make_pipeline("pipe-x")
    scheduler.register(pipeline)
    assert "pipe-x" in scheduler.pipeline_ids
    scheduler.unregister("pipe-x")
    assert "pipe-x" not in scheduler.pipeline_ids


def test_scheduler_calls_collector():
    scheduler = MetricScheduler(tick_interval=0.05)
    pipeline = make_pipeline("pipe-fast", interval=0.05)
    scheduler.register(pipeline)
    scheduler.start()
    time.sleep(0.2)
    scheduler.stop()
    assert pipeline.collector.call_count >= 1


def test_scheduler_stop_is_idempotent():
    scheduler = MetricScheduler()
    scheduler.start()
    scheduler.stop()
    scheduler.stop()  # should not raise


def test_scheduler_does_not_run_unregistered_pipeline():
    scheduler = MetricScheduler(tick_interval=0.05)
    pipeline = make_pipeline("pipe-removed", interval=0.05)
    scheduler.register(pipeline)
    scheduler.unregister("pipe-removed")
    scheduler.start()
    time.sleep(0.15)
    scheduler.stop()
    pipeline.collector.assert_not_called()


def test_pipeline_ids_returns_registered():
    scheduler = MetricScheduler()
    scheduler.register(make_pipeline("a"))
    scheduler.register(make_pipeline("b"))
    ids = scheduler.pipeline_ids
    assert set(ids) == {"a", "b"}
