"""Rich-based display helpers for pipeline metric history."""

from __future__ import annotations

from typing import List

from rich.table import Table
from rich import box
from rich.console import Console

from pipewatch.history import MetricSnapshot, PipelineHistory
from pipewatch.metrics import error_rate, is_healthy

_console = Console()


def _format_age(seconds: float) -> str:
    if seconds < 60:
        return f"{int(seconds)}s ago"
    if seconds < 3600:
        return f"{int(seconds // 60)}m ago"
    return f"{int(seconds // 3600)}h ago"


def render_snapshot_row(snapshot: MetricSnapshot) -> List[str]:
    """Return a list of string cells for a single snapshot row."""
    m = snapshot.metric
    rate = error_rate(m)
    healthy = is_healthy(m)
    status = "[green]OK[/green]" if healthy else "[red]FAIL[/red]"
    return [
        _format_age(snapshot.age_seconds()),
        str(m.records_processed),
        str(m.records_failed),
        f"{rate:.2%}",
        f"{m.throughput_per_second:.1f}",
        status,
    ]


def render_history_table(history: PipelineHistory) -> Table:
    """Build a Rich table showing recent snapshots for a pipeline."""
    table = Table(
        title=f"History — {history.pipeline_name}",
        box=box.SIMPLE_HEAVY,
        show_lines=False,
    )
    table.add_column("Age", style="dim", min_width=8)
    table.add_column("Processed", justify="right")
    table.add_column("Failed", justify="right")
    table.add_column("Error Rate", justify="right")
    table.add_column("Throughput/s", justify="right")
    table.add_column("Status", justify="center")

    snapshots = history.all_snapshots()
    if not snapshots:
        table.add_row("—", "—", "—", "—", "—", "—")
        return table

    for snapshot in reversed(snapshots):
        table.add_row(*render_snapshot_row(snapshot))

    return table


def render_no_history(pipeline_name: str) -> str:
    """Return a plain message when no history exists for a pipeline."""
    return f"[dim]No history recorded for pipeline '{pipeline_name}'.[/dim]"
