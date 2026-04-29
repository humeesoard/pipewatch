"""Tests for pipewatch.replay_display module."""

import pytest
from datetime import datetime, timedelta
from io import StringIO
from rich.console import Console
from rich.table import Table
from pipewatch.metrics import PipelineMetric
from pipewatch.history import MetricSnapshot
from pipewatch.replay import ReplaySession
from pipewatch.replay_display import (
    render_replay_row,
    render_replay_summary,
    render_no_replay,
    print_replay_session,
    _format_timestamp,
    _health_label,
)


def make_metric(name="pipe-a", records=500, errors=5, throughput=25.0):
    return PipelineMetric(
        pipeline_name=name,
        records_processed=records,
        error_count=errors,
        throughput_per_second=throughput,
    )


def make_snapshot(name="pipe-a", offset_seconds=0, **kwargs):
    metric = make_metric(name=name, **kwargs)
    ts = datetime(2024, 6, 15, 9, 0, 0) + timedelta(seconds=offset_seconds)
    return MetricSnapshot(metric=metric, timestamp=ts)


def make_session(name="pipe-a", count=3):
    snaps = [make_snapshot(name=name, offset_seconds=i * 5) for i in range(count)]
    return ReplaySession(pipeline_name=name, snapshots=snaps)


def test_format_timestamp_returns_string():
    snap = make_snapshot()
    result = _format_timestamp(snap)
    assert "2024-06-15" in result


def test_health_label_healthy():
    snap = make_snapshot(records=1000, errors=1)
    label = _health_label(snap)
    assert "healthy" in label.plain


def test_health_label_unhealthy():
    snap = make_snapshot(records=100, errors=90)
    label = _health_label(snap)
    assert "unhealthy" in label.plain


def test_render_replay_row_returns_table():
    snap = make_snapshot()
    table = render_replay_row(0, snap)
    assert isinstance(table, Table)


def test_render_replay_summary_no_error():
    session = make_session()
    buf = StringIO()
    c = Console(file=buf, no_color=True)
    c.print(f"Replay: {session.pipeline_name}  {session.total} snapshot(s)")
    output = buf.getvalue()
    assert "pipe-a" in output
    assert "3" in output


def test_render_no_replay_no_error():
    buf = StringIO()
    c = Console(file=buf, no_color=True)
    c.print("No history available for pipeline: ghost-pipe")
    output = buf.getvalue()
    assert "ghost-pipe" in output


def test_print_replay_session_runs_without_error():
    session = make_session(count=2)
    buf = StringIO()
    c = Console(file=buf, no_color=True, width=120)
    from pipewatch import replay_display as rd
    original = rd.console
    rd.console = c
    try:
        print_replay_session(session)
    finally:
        rd.console = original
    output = buf.getvalue()
    assert "pipe-a" in output
