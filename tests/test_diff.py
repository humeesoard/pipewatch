"""Tests for pipewatch.diff module."""

import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.history import MetricSnapshot
from pipewatch.diff import MetricDiff, diff_snapshots, diff_to_dict


def make_snapshot(name="pipeline-a", error_rate=0.02, records=1000, latency=120.0):
    total = records
    failed = int(total * error_rate)
    metric = PipelineMetric(
        pipeline_name=name,
        records_processed=total,
        records_failed=failed,
        latency_ms=latency,
    )
    return MetricSnapshot(metric=metric)


def test_diff_snapshots_returns_metric_diff():
    prev = make_snapshot(error_rate=0.02, records=1000, latency=100.0)
    curr = make_snapshot(error_rate=0.05, records=950, latency=160.0)
    result = diff_snapshots(prev, curr)
    assert isinstance(result, MetricDiff)


def test_diff_snapshots_none_for_different_pipelines():
    prev = make_snapshot(name="pipeline-a")
    curr = make_snapshot(name="pipeline-b")
    assert diff_snapshots(prev, curr) is None


def test_error_rate_delta_positive_on_degradation():
    prev = make_snapshot(error_rate=0.01)
    curr = make_snapshot(error_rate=0.06)
    diff = diff_snapshots(prev, curr)
    assert diff.error_rate_delta == pytest.approx(0.05, abs=1e-4)


def test_error_rate_delta_negative_on_improvement():
    prev = make_snapshot(error_rate=0.08)
    curr = make_snapshot(error_rate=0.02)
    diff = diff_snapshots(prev, curr)
    assert diff.error_rate_delta < 0


def test_throughput_delta():
    prev = make_snapshot(records=1000)
    curr = make_snapshot(records=1200)
    diff = diff_snapshots(prev, curr)
    assert diff.throughput_delta == 200


def test_latency_delta():
    prev = make_snapshot(latency=100.0)
    curr = make_snapshot(latency=175.0)
    diff = diff_snapshots(prev, curr)
    assert diff.latency_delta == pytest.approx(75.0)


def test_has_degraded_true_when_error_rate_spikes():
    prev = make_snapshot(error_rate=0.01, latency=100.0)
    curr = make_snapshot(error_rate=0.05, latency=100.0)
    diff = diff_snapshots(prev, curr)
    assert diff.has_degraded is True


def test_has_degraded_false_for_stable_metrics():
    prev = make_snapshot(error_rate=0.02, latency=100.0)
    curr = make_snapshot(error_rate=0.02, latency=110.0)
    diff = diff_snapshots(prev, curr)
    assert diff.has_degraded is False


def test_has_improved_true():
    prev = make_snapshot(error_rate=0.08, latency=200.0)
    curr = make_snapshot(error_rate=0.01, latency=90.0)
    diff = diff_snapshots(prev, curr)
    assert diff.has_improved is True


def test_diff_to_dict_keys():
    prev = make_snapshot(error_rate=0.03, records=500, latency=80.0)
    curr = make_snapshot(error_rate=0.04, records=480, latency=95.0)
    diff = diff_snapshots(prev, curr)
    d = diff_to_dict(diff)
    assert "pipeline_name" in d
    assert "error_rate" in d
    assert "throughput" in d
    assert "latency_ms" in d
    assert "has_degraded" in d
    assert "has_improved" in d


def test_diff_to_dict_delta_values():
    prev = make_snapshot(error_rate=0.02, records=1000, latency=100.0)
    curr = make_snapshot(error_rate=0.04, records=900, latency=130.0)
    diff = diff_snapshots(prev, curr)
    d = diff_to_dict(diff)
    assert d["throughput"]["delta"] == -100
    assert d["latency_ms"]["delta"] == pytest.approx(30.0)
