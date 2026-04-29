"""Tests for pipewatch.replay module."""

import pytest
from datetime import datetime, timedelta
from pipewatch.metrics import PipelineMetric
from pipewatch.history import PipelineHistory, MetricSnapshot
from pipewatch.replay import (
    ReplaySession,
    create_replay_session,
    replay_all,
)


def make_metric(name="pipe-a", records=1000, errors=10, throughput=50.0):
    return PipelineMetric(
        pipeline_name=name,
        records_processed=records,
        error_count=errors,
        throughput_per_second=throughput,
    )


def make_snapshot(name="pipe-a", offset_seconds=0, **kwargs):
    metric = make_metric(name=name, **kwargs)
    ts = datetime(2024, 1, 1, 12, 0, 0) + timedelta(seconds=offset_seconds)
    return MetricSnapshot(metric=metric, timestamp=ts)


def make_history_with_snapshots(name="pipe-a", count=3):
    history = PipelineHistory(max_snapshots=10)
    for i in range(count):
        snap = make_snapshot(name=name, offset_seconds=i * 10)
        history.snapshots.setdefault(name, []).append(snap)
    return history


def test_replay_session_total():
    snaps = [make_snapshot(offset_seconds=i) for i in range(5)]
    session = ReplaySession(pipeline_name="pipe-a", snapshots=snaps)
    assert session.total == 5


def test_replay_session_step_advances_index():
    snaps = [make_snapshot(offset_seconds=i) for i in range(3)]
    session = ReplaySession(pipeline_name="pipe-a", snapshots=snaps)
    first = session.step()
    assert first is not None
    assert session.current_index == 1


def test_replay_session_is_finished_after_all_steps():
    snaps = [make_snapshot(offset_seconds=i) for i in range(2)]
    session = ReplaySession(pipeline_name="pipe-a", snapshots=snaps)
    session.step()
    session.step()
    assert session.is_finished


def test_replay_session_step_returns_none_when_finished():
    snaps = [make_snapshot()]
    session = ReplaySession(pipeline_name="pipe-a", snapshots=snaps)
    session.step()
    result = session.step()
    assert result is None


def test_replay_session_reset_restarts():
    snaps = [make_snapshot(offset_seconds=i) for i in range(3)]
    session = ReplaySession(pipeline_name="pipe-a", snapshots=snaps)
    session.step()
    session.step()
    session.reset()
    assert session.current_index == 0


def test_replay_session_snapshots_sorted_by_timestamp():
    snaps = [make_snapshot(offset_seconds=i) for i in [30, 10, 20]]
    session = ReplaySession(pipeline_name="pipe-a", snapshots=snaps)
    timestamps = [s.timestamp for s in session.snapshots]
    assert timestamps == sorted(timestamps)


def test_create_replay_session_returns_none_for_empty_history():
    history = PipelineHistory(max_snapshots=10)
    result = create_replay_session(history, "missing-pipe")
    assert result is None


def test_create_replay_session_returns_session():
    history = make_history_with_snapshots(name="pipe-a", count=3)
    session = create_replay_session(history, "pipe-a")
    assert session is not None
    assert session.pipeline_name == "pipe-a"
    assert session.total == 3


def test_replay_all_returns_ordered_snapshots():
    history = make_history_with_snapshots(name="pipe-b", count=4)
    snapshots = replay_all(history, "pipe-b")
    assert len(snapshots) == 4
    timestamps = [s.timestamp for s in snapshots]
    assert timestamps == sorted(timestamps)


def test_replay_all_empty_for_unknown_pipeline():
    history = PipelineHistory(max_snapshots=10)
    result = replay_all(history, "nonexistent")
    assert result == []
