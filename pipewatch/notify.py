"""Notification dispatch for pipewatch alerts."""

from dataclasses import dataclass, field
from typing import List, Optional, Callable
from datetime import datetime
from pipewatch.alerts import Alert


@dataclass
class NotificationRecord:
    alert_id: str
    pipeline_name: str
    severity: str
    message: str
    sent_at: datetime = field(default_factory=datetime.utcnow)
    channel: str = "console"

    def to_dict(self) -> dict:
        return {
            "alert_id": self.alert_id,
            "pipeline_name": self.pipeline_name,
            "severity": self.severity,
            "message": self.message,
            "sent_at": self.sent_at.isoformat(),
            "channel": self.channel,
        }


NotificationHandler = Callable[[Alert], Optional[NotificationRecord]]


def console_handler(alert: Alert) -> NotificationRecord:
    """Default handler: prints alert to stdout and returns a record."""
    timestamp = datetime.utcnow().isoformat()
    print(f"[{timestamp}] ALERT [{alert.severity.upper()}] {alert.pipeline_name}: {alert.message}")
    return NotificationRecord(
        alert_id=f"{alert.pipeline_name}:{alert.rule_name}",
        pipeline_name=alert.pipeline_name,
        severity=alert.severity,
        message=alert.message,
        channel="console",
    )


def dispatch_alerts(
    alerts: List[Alert],
    handlers: Optional[List[NotificationHandler]] = None,
) -> List[NotificationRecord]:
    """Dispatch a list of alerts through all registered handlers."""
    if handlers is None:
        handlers = [console_handler]

    records: List[NotificationRecord] = []
    for alert in alerts:
        for handler in handlers:
            record = handler(alert)
            if record is not None:
                records.append(record)
    return records


def filter_by_severity(
    alerts: List[Alert], min_severity: str = "warning"
) -> List[Alert]:
    """Return only alerts at or above the given severity level."""
    order = {"warning": 0, "critical": 1}
    min_level = order.get(min_severity, 0)
    return [a for a in alerts if order.get(a.severity, 0) >= min_level]
