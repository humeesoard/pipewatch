"""Tests for pipewatch.scheduler_display."""

import time
import pytest
from unittest.mock import MagicMock
from rich.table import Table
from rich.text import Text

from pipewatch.scheduler import MetricScheduler, ScheduledPipeline, mark_run
from pipewatch.scheduler_display import (
    _status_text,
    render_scheduler_row,
    render_scheduler_table,
    render_no_pipelines,
)


def make_pipeline(pipeline_id="pipe-1", interval=30.0):
    return ScheduledPipeline(
        pipeline_id=pipeline_id,
        interval_seconds=interval,
        collector=MagicMock(),
    )


def test_status_text_pending_when_never_run():
    pipeline = make_pipeline()
    result = _status_text(pipeline)
    assert "pending" in result.plain


def test_status_text_overdue_when_past_interval():
    pipeline = make_pipeline(interval=1.0)
    pipeline._last_run = time.time() - 5.0
    result = _status_text(pipeline)
    assert "overdue" in result.plain


def test_status_text_next_in_when_recently_run():
    pipeline = make_pipeline(interval=60.0)
    mark_run(pipeline)
    result = _status_text(pipeline)
    assert "next in" in result.plain


def test_render_scheduler_row_structure():
    pipeline = make_pipeline("my-pipe", interval=15.0)
    row = render_scheduler_row(pipeline)
    assert row[0] == "my-pipe"
    assert "15.0s" in row[1]
    assert isinstance(row[2], Text)


def test_render_scheduler_table_returns_table():
    scheduler = MetricScheduler()
    scheduler.register(make_pipeline("p1"))
    scheduler.register(make_pipeline("p2"))
    table = render_scheduler_table(scheduler)
    assert isinstance(table, Table)


def test_render_scheduler_table_empty_scheduler():
    scheduler = MetricScheduler()
    table = render_scheduler_table(scheduler)
    assert isinstance(table, Table)


def test_render_no_pipelines_returns_text():
    result = render_no_pipelines()
    assert isinstance(result, Text)
    assert len(result.plain) > 0
