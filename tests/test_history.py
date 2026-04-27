"""Tests for pipewatch.history module."""

from __future__ import annotations

import time

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.history import (
    MetricSnapshot,
    PipelineHistory,
    HistoryStore,
    DEFAULT_MAX_ENTRIES,
)


def make_metric(name: str = "pipe_a", processed: int = 100, failed: int = 2) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=name,
        records_processed=processed,
        records_failed=failed,
        throughput_per_second=50.0,
        latency_ms=10.0,
    )


def test_snapshot_age_is_recent():
    m = make_metric()
    snap = MetricSnapshot(timestamp=time.time(), metric=m)
    assert snap.age_seconds() < 1.0


def test_pipeline_history_records_snapshot():
    history = PipelineHistory(pipeline_name="pipe_a")
    history.record(make_metric())
    assert len(history) == 1
    assert history.latest() is not None


def test_pipeline_history_latest_is_most_recent():
    history = PipelineHistory(pipeline_name="pipe_a")
    history.record(make_metric(processed=10))
    history.record(make_metric(processed=99))
    assert history.latest().metric.records_processed == 99


def test_pipeline_history_evicts_oldest_at_capacity():
    history = PipelineHistory(pipeline_name="pipe_a", max_entries=3)
    for i in range(4):
        history.record(make_metric(processed=i))
    assert len(history) == 3
    snaps = history.all_snapshots()
    assert snaps[0].metric.records_processed == 1


def test_pipeline_history_latest_none_when_empty():
    history = PipelineHistory(pipeline_name="pipe_a")
    assert history.latest() is None


def test_history_store_records_and_retrieves():
    store = HistoryStore()
    store.record(make_metric(name="alpha"))
    store.record(make_metric(name="alpha", processed=200))
    history = store.get("alpha")
    assert history is not None
    assert len(history) == 2


def test_history_store_get_unknown_returns_none():
    store = HistoryStore()
    assert store.get("nonexistent") is None


def test_history_store_all_pipelines_sorted():
    store = HistoryStore()
    for name in ["z_pipe", "a_pipe", "m_pipe"]:
        store.record(make_metric(name=name))
    assert store.all_pipelines() == ["a_pipe", "m_pipe", "z_pipe"]


def test_history_store_clear_single_pipeline():
    store = HistoryStore()
    store.record(make_metric(name="pipe_a"))
    store.record(make_metric(name="pipe_b"))
    store.clear("pipe_a")
    assert store.get("pipe_a") is None
    assert store.get("pipe_b") is not None


def test_history_store_clear_all():
    store = HistoryStore()
    store.record(make_metric(name="pipe_a"))
    store.record(make_metric(name="pipe_b"))
    store.clear()
    assert store.all_pipelines() == []
