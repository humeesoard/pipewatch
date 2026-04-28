"""Tests for pipewatch.filter module."""

from pipewatch.metrics import PipelineMetric
from pipewatch.history import MetricSnapshot
from pipewatch.filter import FilterCriteria, matches_metric, filter_metrics, filter_snapshots

import datetime


def make_metric(
    name: str = "pipe",
    record_count: int = 100,
    error_count: int = 2,
    throughput: float = 50.0,
    latency_ms: float = 120.0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=name,
        record_count=record_count,
        error_count=error_count,
        throughput=throughput,
        latency_ms=latency_ms,
    )


def make_snapshot(metric: PipelineMetric) -> MetricSnapshot:
    return MetricSnapshot(metric=metric, timestamp=datetime.datetime.utcnow())


def test_matches_metric_no_criteria():
    m = make_metric()
    assert matches_metric(m, FilterCriteria()) is True


def test_matches_metric_by_name_pass():
    m = make_metric(name="alpha")
    assert matches_metric(m, FilterCriteria(pipeline_name="alpha")) is True


def test_matches_metric_by_name_fail():
    m = make_metric(name="alpha")
    assert matches_metric(m, FilterCriteria(pipeline_name="beta")) is False


def test_matches_metric_min_throughput_pass():
    m = make_metric(throughput=80.0)
    assert matches_metric(m, FilterCriteria(min_throughput=50.0)) is True


def test_matches_metric_min_throughput_fail():
    m = make_metric(throughput=30.0)
    assert matches_metric(m, FilterCriteria(min_throughput=50.0)) is False


def test_matches_metric_max_error_rate_pass():
    m = make_metric(record_count=100, error_count=3)
    assert matches_metric(m, FilterCriteria(max_error_rate=0.05)) is True


def test_matches_metric_max_error_rate_fail():
    m = make_metric(record_count=100, error_count=20)
    assert matches_metric(m, FilterCriteria(max_error_rate=0.05)) is False


def test_matches_metric_healthy_only_pass():
    m = make_metric(record_count=1000, error_count=1, throughput=60.0)
    assert matches_metric(m, FilterCriteria(healthy_only=True)) is True


def test_matches_metric_healthy_only_fail():
    m = make_metric(record_count=100, error_count=60, throughput=1.0)
    assert matches_metric(m, FilterCriteria(healthy_only=False)) is True


def test_filter_metrics_returns_matching_only():
    metrics = [
        make_metric(name="a", throughput=80.0),
        make_metric(name="b", throughput=20.0),
        make_metric(name="a", throughput=60.0),
    ]
    result = filter_metrics(metrics, FilterCriteria(pipeline_name="a", min_throughput=70.0))
    assert len(result) == 1
    assert result[0].throughput == 80.0


def test_filter_snapshots_returns_matching_only():
    snapshots = [
        make_snapshot(make_metric(name="x", throughput=100.0)),
        make_snapshot(make_metric(name="y", throughput=10.0)),
    ]
    result = filter_snapshots(snapshots, FilterCriteria(min_throughput=50.0))
    assert len(result) == 1
    assert result[0].metric.pipeline_name == "x"


def test_filter_metrics_empty_input():
    result = filter_metrics([], FilterCriteria(pipeline_name="any"))
    assert result == []
