"""Tests for the pipewatch.metrics module."""

from datetime import datetime

import pytest

from pipewatch.metrics import PipelineMetric, collect_metric


def make_metric(**kwargs) -> PipelineMetric:
    defaults = dict(
        pipeline_name="test-pipeline",
        records_processed=1000,
        records_failed=10,
        throughput_per_sec=250.0,
        latency_ms=120.0,
    )
    defaults.update(kwargs)
    return PipelineMetric(**defaults)


def test_error_rate_calculation():
    metric = make_metric(records_processed=900, records_failed=100)
    assert metric.error_rate == pytest.approx(10.0)


def test_error_rate_zero_when_no_records():
    metric = make_metric(records_processed=0, records_failed=0)
    assert metric.error_rate == 0.0


def test_is_healthy_true_for_good_metrics():
    metric = make_metric(records_processed=1000, records_failed=1, latency_ms=200.0)
    assert metric.is_healthy is True


def test_is_healthy_false_for_high_error_rate():
    metric = make_metric(records_processed=900, records_failed=100, latency_ms=200.0)
    assert metric.is_healthy is False


def test_is_healthy_false_for_high_latency():
    metric = make_metric(records_processed=1000, records_failed=0, latency_ms=1500.0)
    assert metric.is_healthy is False


def test_to_dict_contains_required_keys():
    metric = make_metric()
    result = metric.to_dict()
    expected_keys = {
        "pipeline_name", "records_processed", "records_failed",
        "throughput_per_sec", "latency_ms", "error_rate",
        "is_healthy", "timestamp", "error_message",
    }
    assert expected_keys == set(result.keys())


def test_to_dict_timestamp_is_iso_string():
    metric = make_metric()
    result = metric.to_dict()
    datetime.fromisoformat(result["timestamp"])  # should not raise


def test_collect_metric_factory():
    metric = collect_metric(
        pipeline_name="etl-job",
        records_processed=500,
        records_failed=5,
        throughput_per_sec=100.0,
        latency_ms=80.0,
        error_message=None,
    )
    assert isinstance(metric, PipelineMetric)
    assert metric.pipeline_name == "etl-job"
    assert metric.records_processed == 500
