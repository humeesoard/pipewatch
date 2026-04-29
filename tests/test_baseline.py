"""Tests for pipewatch.baseline module."""

import json
import os
import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.baseline import (
    BaselineEntry,
    save_baseline,
    load_baseline,
    compare_to_baseline,
    _entry_from_metric,
)


def make_metric(
    name="pipeline_a",
    total=1000,
    errors=10,
    throughput=500.0,
    latency=120.0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=name,
        total_records=total,
        error_count=errors,
        throughput=throughput,
        latency_p99=latency,
    )


def test_entry_from_metric_captures_values():
    m = make_metric(total=1000, errors=50)
    entry = _entry_from_metric(m)
    assert entry.pipeline_name == "pipeline_a"
    assert entry.error_rate == pytest.approx(0.05)
    assert entry.throughput == 500.0
    assert entry.latency_p99 == 120.0


def test_save_and_load_baseline(tmp_path):
    path = str(tmp_path / "baseline.json")
    metrics = [make_metric("pipe_x"), make_metric("pipe_y", throughput=200.0)]
    save_baseline(metrics, path)

    assert os.path.exists(path)
    loaded = load_baseline(path)
    assert "pipe_x" in loaded
    assert "pipe_y" in loaded
    assert loaded["pipe_y"].throughput == 200.0


def test_load_baseline_returns_empty_when_missing(tmp_path):
    result = load_baseline(str(tmp_path / "nonexistent.json"))
    assert result == {}


def test_compare_to_baseline_no_entry_returns_none():
    m = make_metric()
    result = compare_to_baseline(m, {})
    assert result is None


def test_compare_to_baseline_ok_when_no_regression():
    m = make_metric(total=1000, errors=10, throughput=500.0, latency=120.0)
    baseline_map = {"pipeline_a": _entry_from_metric(m)}
    result = compare_to_baseline(m, baseline_map)
    assert result is not None
    assert not result.has_regressed
    assert result.error_rate_delta == pytest.approx(0.0)
    assert result.throughput_delta == pytest.approx(0.0)


def test_compare_to_baseline_detects_error_rate_regression():
    baseline_metric = make_metric(total=1000, errors=10)
    current = make_metric(total=1000, errors=200)
    baseline_map = {"pipeline_a": _entry_from_metric(baseline_metric)}
    result = compare_to_baseline(current, baseline_map)
    assert result is not None
    assert result.has_regressed
    assert result.error_rate_delta > 0


def test_compare_to_baseline_detects_latency_regression():
    baseline_metric = make_metric(latency=100.0)
    current = make_metric(latency=250.0)
    baseline_map = {"pipeline_a": _entry_from_metric(baseline_metric)}
    result = compare_to_baseline(current, baseline_map)
    assert result is not None
    assert result.has_regressed
    assert result.latency_delta == pytest.approx(150.0)


def test_compare_to_baseline_detects_throughput_regression():
    baseline_metric = make_metric(throughput=1000.0)
    current = make_metric(throughput=500.0)
    baseline_map = {"pipeline_a": _entry_from_metric(baseline_metric)}
    result = compare_to_baseline(current, baseline_map)
    assert result is not None
    assert result.has_regressed
    assert result.throughput_delta == pytest.approx(-500.0)
