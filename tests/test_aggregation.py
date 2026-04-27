"""Tests for pipewatch.aggregation module."""

from __future__ import annotations

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.history import PipelineHistory
from pipewatch.aggregation import compute_stats


def make_metric(
    name: str = "pipe_a",
    processed: int = 100,
    failed: int = 0,
    throughput: float = 50.0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=name,
        records_processed=processed,
        records_failed=failed,
        throughput_per_second=throughput,
        latency_ms=5.0,
    )


def test_compute_stats_returns_none_for_empty_history():
    history = PipelineHistory(pipeline_name="pipe_a")
    assert compute_stats(history) is None


def test_compute_stats_single_snapshot():
    history = PipelineHistory(pipeline_name="pipe_a")
    history.record(make_metric(processed=100, failed=5, throughput=40.0))
    stats = compute_stats(history)
    assert stats is not None
    assert stats.sample_count == 1
    assert stats.avg_error_rate == pytest.approx(0.05)
    assert stats.max_error_rate == pytest.approx(0.05)
    assert stats.avg_throughput == pytest.approx(40.0)


def test_compute_stats_multiple_snapshots():
    history = PipelineHistory(pipeline_name="pipe_a")
    history.record(make_metric(processed=100, failed=10, throughput=30.0))
    history.record(make_metric(processed=200, failed=0, throughput=70.0))
    stats = compute_stats(history)
    assert stats.sample_count == 2
    assert stats.avg_error_rate == pytest.approx(0.05)  # (0.10 + 0.0) / 2
    assert stats.max_error_rate == pytest.approx(0.10)
    assert stats.min_throughput == pytest.approx(30.0)
    assert stats.max_throughput == pytest.approx(70.0)
    assert stats.avg_throughput == pytest.approx(50.0)


def test_compute_stats_totals():
    history = PipelineHistory(pipeline_name="pipe_a")
    history.record(make_metric(processed=100, failed=3))
    history.record(make_metric(processed=200, failed=7))
    stats = compute_stats(history)
    assert stats.total_records_processed == 300
    assert stats.total_records_failed == 10


def test_overall_error_rate_property():
    history = PipelineHistory(pipeline_name="pipe_a")
    history.record(make_metric(processed=200, failed=20))
    history.record(make_metric(processed=200, failed=0))
    stats = compute_stats(history)
    assert stats.overall_error_rate == pytest.approx(20 / 400)


def test_overall_error_rate_zero_when_no_processed():
    history = PipelineHistory(pipeline_name="pipe_a")
    history.record(make_metric(processed=0, failed=0))
    stats = compute_stats(history)
    assert stats.overall_error_rate == 0.0
