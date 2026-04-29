"""Tests for pipewatch.summary module."""

import time

import pytest

from pipewatch.history import PipelineHistory
from pipewatch.metrics import PipelineMetric
from pipewatch.summary import (
    OverallSummary,
    PipelineSummary,
    build_overall_summary,
    summarize_pipeline,
)


def make_metric(
    name: str = "pipeline_a",
    records: int = 1000,
    errors: int = 10,
    throughput: float = 50.0,
    latency: float = 0.2,
) -> PipelineMetric:
    return PipelineMetric(
        name=name,
        records_processed=records,
        error_count=errors,
        throughput=throughput,
        latency_seconds=latency,
    )


def make_history_with_metrics(metrics: list[PipelineMetric]) -> PipelineHistory:
    history = PipelineHistory(pipeline_name=metrics[0].name if metrics else "test")
    for m in metrics:
        history.record(m)
    return history


def test_summarize_pipeline_no_data():
    history = PipelineHistory(pipeline_name="empty")
    summary = summarize_pipeline("empty", history)
    assert summary.pipeline_name == "empty"
    assert summary.stats is None
    assert summary.is_healthy is False
    assert summary.snapshot_count == 0


def test_summarize_pipeline_healthy():
    m = make_metric(errors=5, records=1000, throughput=100.0)
    history = make_history_with_metrics([m])
    summary = summarize_pipeline("pipeline_a", history)
    assert summary.is_healthy is True
    assert summary.snapshot_count == 1
    assert summary.stats is not None


def test_summarize_pipeline_unhealthy_high_error_rate():
    m = make_metric(errors=200, records=1000, throughput=10.0)
    history = make_history_with_metrics([m])
    summary = summarize_pipeline("pipeline_a", history)
    assert summary.is_healthy is False


def test_build_overall_summary_all_healthy():
    histories = {
        "a": make_history_with_metrics([make_metric("a", errors=0)]),
        "b": make_history_with_metrics([make_metric("b", errors=0)]),
    }
    overall = build_overall_summary(histories)
    assert overall.total_pipelines == 2
    assert overall.healthy_count == 2
    assert overall.unhealthy_count == 0
    assert overall.no_data_count == 0


def test_build_overall_summary_mixed():
    histories = {
        "healthy": make_history_with_metrics([make_metric("healthy", errors=1)]),
        "sick": make_history_with_metrics([make_metric("sick", errors=900, records=1000)]),
        "empty": PipelineHistory(pipeline_name="empty"),
    }
    overall = build_overall_summary(histories)
    assert overall.total_pipelines == 3
    assert overall.healthy_count == 1
    assert overall.unhealthy_count == 1
    assert overall.no_data_count == 1
    assert len(overall.pipeline_summaries) == 3


def test_build_overall_summary_empty_input():
    overall = build_overall_summary({})
    assert overall.total_pipelines == 0
    assert overall.healthy_count == 0
    assert overall.pipeline_summaries == []
