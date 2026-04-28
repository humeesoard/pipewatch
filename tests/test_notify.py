"""Tests for pipewatch.notify dispatch module."""

from datetime import datetime
from unittest.mock import MagicMock
from pipewatch.alerts import Alert
from pipewatch.notify import (
    NotificationRecord,
    console_handler,
    dispatch_alerts,
    filter_by_severity,
)


def make_alert(pipeline="pipe1", rule="error_rate", severity="critical", message="Error rate too high"):
    return Alert(
        pipeline_name=pipeline,
        rule_name=rule,
        severity=severity,
        message=message,
        triggered_at=datetime.utcnow(),
    )


def test_console_handler_returns_record(capsys):
    alert = make_alert()
    record = console_handler(alert)
    assert isinstance(record, NotificationRecord)
    assert record.pipeline_name == "pipe1"
    assert record.severity == "critical"
    assert record.channel == "console"
    captured = capsys.readouterr()
    assert "pipe1" in captured.out
    assert "CRITICAL" in captured.out


def test_notification_record_to_dict():
    alert = make_alert()
    record = console_handler(alert)
    d = record.to_dict()
    assert "alert_id" in d
    assert "pipeline_name" in d
    assert "severity" in d
    assert "sent_at" in d
    assert d["channel"] == "console"


def test_dispatch_alerts_calls_all_handlers():
    alerts = [make_alert(pipeline="a"), make_alert(pipeline="b")]
    handler1 = MagicMock(return_value=NotificationRecord(
        alert_id="x", pipeline_name="a", severity="critical", message="m"
    ))
    handler2 = MagicMock(return_value=None)
    records = dispatch_alerts(alerts, handlers=[handler1, handler2])
    assert handler1.call_count == 2
    assert handler2.call_count == 2
    assert len(records) == 2  # handler2 returns None, so only handler1 records


def test_dispatch_alerts_default_handler(capsys):
    alerts = [make_alert()]
    records = dispatch_alerts(alerts)
    assert len(records) == 1
    assert records[0].channel == "console"


def test_dispatch_alerts_empty_list():
    records = dispatch_alerts([])
    assert records == []


def test_filter_by_severity_warning_includes_both():
    alerts = [
        make_alert(severity="warning"),
        make_alert(severity="critical"),
    ]
    result = filter_by_severity(alerts, min_severity="warning")
    assert len(result) == 2


def test_filter_by_severity_critical_only():
    alerts = [
        make_alert(severity="warning"),
        make_alert(severity="critical"),
    ]
    result = filter_by_severity(alerts, min_severity="critical")
    assert len(result) == 1
    assert result[0].severity == "critical"


def test_filter_by_severity_empty():
    result = filter_by_severity([], min_severity="warning")
    assert result == []
