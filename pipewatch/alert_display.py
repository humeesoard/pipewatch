"""Rendering helpers for displaying alerts in the CLI."""

from typing import List
from pipewatch.alerts import Alert

SEVERITY_COLORS = {
    "critical": "\033[91m",  # bright red
    "warning": "\033[93m",   # bright yellow
}
RESET = "\033[0m"
BOLD = "\033[1m"


def _severity_label(severity: str) -> str:
    color = SEVERITY_COLORS.get(severity, "")
    label = severity.upper().ljust(8)
    return f"{color}{BOLD}{label}{RESET}"


def render_alert_row(alert: Alert) -> str:
    """Render a single alert as a formatted string."""
    label = _severity_label(alert.severity)
    pipeline = alert.pipeline.ljust(20)
    rule = alert.rule_name.ljust(22)
    return f"  {label}  {pipeline}  {rule}  {alert.message}"


def render_alerts_table(alerts: List[Alert]) -> str:
    """Render a table of alerts with a header."""
    if not alerts:
        return render_no_alerts()

    header = (
        f"  {'SEVERITY'.ljust(8)}  {'PIPELINE'.ljust(20)}  "
        f"{'RULE'.ljust(22)}  MESSAGE"
    )
    divider = "  " + "-" * 74
    rows = [render_alert_row(a) for a in alerts]
    title = f"{BOLD}Active Alerts ({len(alerts)}){RESET}"
    return "\n".join([title, header, divider] + rows)


def render_no_alerts() -> str:
    """Render a message when no alerts are active."""
    return f"  \033[92m{BOLD}✓ No active alerts{RESET}"
