"""Tests for alert evaluation logic."""

import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.alerts import (
    Alert,
    AlertRule,
    evaluate_rules,
    DEFAULT_RULES,
)


def make_metric(
    pipeline_name="test_pipeline",
    total_records=1000,
    error_count=0,
    records_per_second=10.0,
    is_running=True,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=pipeline_name,
        total_records=total_records,
        error_count=error_count,
        records_per_second=records_per_second,
        is_running=is_running,
    )


def test_no_alerts_for_healthy_metric():
    metric = make_metric(error_count=0, records_per_second=50.0)
    alerts = evaluate_rules(metric)
    assert alerts == []


def test_high_error_rate_triggers_critical_alert():
    metric = make_metric(total_records=100, error_count=10)  # 10% error rate
    alerts = evaluate_rules(metric)
    rule_names = [a.rule_name for a in alerts]
    assert "high_error_rate" in rule_names
    alert = next(a for a in alerts if a.rule_name == "high_error_rate")
    assert alert.severity == "critical"
    assert alert.pipeline == "test_pipeline"


def test_low_throughput_triggers_warning():
    metric = make_metric(records_per_second=0.5)
    alerts = evaluate_rules(metric)
    rule_names = [a.rule_name for a in alerts]
    assert "low_throughput" in rule_names
    alert = next(a for a in alerts if a.rule_name == "low_throughput")
    assert alert.severity == "warning"


def test_unhealthy_pipeline_triggers_alert():
    metric = make_metric(is_running=False, error_count=50, total_records=100)
    alerts = evaluate_rules(metric)
    rule_names = [a.rule_name for a in alerts]
    assert "pipeline_unhealthy" in rule_names


def test_custom_rule_is_evaluated():
    custom_rule = AlertRule(
        name="zero_records",
        condition=lambda m: m.total_records == 0,
        message="No records processed",
        severity="warning",
    )
    metric = make_metric(total_records=0, error_count=0, records_per_second=0.0)
    alerts = evaluate_rules(metric, rules=[custom_rule])
    assert len(alerts) == 1
    assert alerts[0].rule_name == "zero_records"


def test_alert_to_dict():
    metric = make_metric(total_records=100, error_count=10)
    alerts = evaluate_rules(metric)
    assert len(alerts) > 0
    d = alerts[0].to_dict()
    assert "rule" in d
    assert "pipeline" in d
    assert "severity" in d
    assert "message" in d


def test_broken_condition_does_not_raise():
    bad_rule = AlertRule(
        name="broken",
        condition=lambda m: 1 / 0,  # will raise ZeroDivisionError
        message="Should not appear",
        severity="critical",
    )
    metric = make_metric()
    alerts = evaluate_rules(metric, rules=[bad_rule])
    assert alerts == []
