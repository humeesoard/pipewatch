"""Alert rules and notification logic for pipeline health monitoring."""

from dataclasses import dataclass, field
from typing import Callable, List, Optional
from pipewatch.metrics import PipelineMetric, is_healthy


@dataclass
class AlertRule:
    """Defines a condition and message for triggering an alert."""
    name: str
    condition: Callable[[PipelineMetric], bool]
    message: str
    severity: str = "warning"  # "warning" or "critical"


@dataclass
class Alert:
    """A triggered alert instance."""
    rule_name: str
    pipeline: str
    message: str
    severity: str
    metric: PipelineMetric

    def to_dict(self) -> dict:
        return {
            "rule": self.rule_name,
            "pipeline": self.pipeline,
            "message": self.message,
            "severity": self.severity,
        }


DEFAULT_RULES: List[AlertRule] = [
    AlertRule(
        name="high_error_rate",
        condition=lambda m: m.error_count / m.total_records > 0.05 if m.total_records > 0 else False,
        message="Error rate exceeds 5%",
        severity="critical",
    ),
    AlertRule(
        name="low_throughput",
        condition=lambda m: m.records_per_second is not None and m.records_per_second < 1.0,
        message="Throughput below 1 record/sec",
        severity="warning",
    ),
    AlertRule(
        name="pipeline_unhealthy",
        condition=lambda m: not is_healthy(m),
        message="Pipeline is in an unhealthy state",
        severity="critical",
    ),
]


def evaluate_rules(
    metric: PipelineMetric,
    rules: Optional[List[AlertRule]] = None,
) -> List[Alert]:
    """Evaluate alert rules against a metric and return triggered alerts."""
    if rules is None:
        rules = DEFAULT_RULES

    triggered: List[Alert] = []
    for rule in rules:
        try:
            if rule.condition(metric):
                triggered.append(
                    Alert(
                        rule_name=rule.name,
                        pipeline=metric.pipeline_name,
                        message=rule.message,
                        severity=rule.severity,
                        metric=metric,
                    )
                )
        except Exception:
            pass
    return triggered
