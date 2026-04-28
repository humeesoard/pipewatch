"""Tests for pipewatch.export — JSON and CSV export of metrics history."""

from __future__ import annotations

import csv
import io
import json

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.history import PipelineHistory
from pipewatch.aggregation import compute_stats
from pipewatch.export import (
    snapshot_to_dict,
    export_history_json,
    export_history_csv,
    export_stats_json,
)


def make_metric(pipeline_id="pipe-1", total=100, failed=5, throughput=50.0, latency=120.0):
    return PipelineMetric(
        pipeline_id=pipeline_id,
        total_records=total,
        failed_records=failed,
        throughput=throughput,
        latency_ms=latency,
    )


def make_history(pipeline_id="pipe-1", count=3):
    history = PipelineHistory(pipeline_id=pipeline_id, max_snapshots=10)
    for i in range(count):
        history.record(make_metric(pipeline_id=pipeline_id, total=100 + i * 10, failed=i))
    return history


def test_snapshot_to_dict_keys():
    history = make_history(count=1)
    snap = history.snapshots[0]
    d = snapshot_to_dict(snap)
    expected_keys = {
        "pipeline_id", "timestamp", "total_records", "failed_records",
        "throughput", "latency_ms", "error_rate", "is_healthy",
    }
    assert set(d.keys()) == expected_keys


def test_snapshot_to_dict_values():
    history = make_history(count=1)
    snap = history.snapshots[0]
    d = snapshot_to_dict(snap)
    assert d["pipeline_id"] == "pipe-1"
    assert d["total_records"] == 100
    assert isinstance(d["error_rate"], float)
    assert isinstance(d["is_healthy"], bool)


def test_export_history_json_valid():
    history = make_history(count=3)
    result = export_history_json(history)
    parsed = json.loads(result)
    assert isinstance(parsed, list)
    assert len(parsed) == 3
    assert all("pipeline_id" in item for item in parsed)


def test_export_history_json_empty():
    history = PipelineHistory(pipeline_id="empty", max_snapshots=5)
    result = export_history_json(history)
    assert json.loads(result) == []


def test_export_history_csv_has_header():
    history = make_history(count=2)
    result = export_history_csv(history)
    reader = csv.DictReader(io.StringIO(result))
    rows = list(reader)
    assert len(rows) == 2
    assert "pipeline_id" in reader.fieldnames
    assert "error_rate" in reader.fieldnames


def test_export_history_csv_empty():
    history = PipelineHistory(pipeline_id="empty", max_snapshots=5)
    result = export_history_csv(history)
    reader = csv.DictReader(io.StringIO(result))
    rows = list(reader)
    assert rows == []


def test_export_stats_json_valid():
    history = make_history(count=4)
    stats = compute_stats(history)
    result = export_stats_json("pipe-1", stats)
    parsed = json.loads(result)
    assert parsed["pipeline_id"] == "pipe-1"
    assert parsed["snapshot_count"] == 4
    assert "overall_error_rate" in parsed
    assert "avg_throughput" in parsed
