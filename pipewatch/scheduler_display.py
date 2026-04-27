"""Display helpers for scheduler status."""

from typing import List
from rich.table import Table
from rich.console import Console
from rich.text import Text

from pipewatch.scheduler import MetricScheduler, ScheduledPipeline, is_due
import time


def _status_text(pipeline: ScheduledPipeline) -> Text:
    now = time.time()
    elapsed = now - pipeline._last_run
    if pipeline._last_run == 0.0:
        return Text("pending", style="yellow")
    if is_due(pipeline, now):
        return Text("overdue", style="bold red")
    remaining = pipeline.interval_seconds - elapsed
    return Text(f"next in {remaining:.1f}s", style="green")


def render_scheduler_row(pipeline: ScheduledPipeline) -> List:
    """Return column values for a single scheduled pipeline row."""
    return [
        pipeline.pipeline_id,
        f"{pipeline.interval_seconds:.1f}s",
        _status_text(pipeline),
    ]


def render_scheduler_table(scheduler: MetricScheduler) -> Table:
    """Render a Rich table showing all scheduled pipelines."""
    table = Table(title="Scheduled Pipelines", show_lines=True)
    table.add_column("Pipeline ID", style="cyan", no_wrap=True)
    table.add_column("Interval", justify="right")
    table.add_column("Status", justify="left")

    pipelines = list(scheduler._pipelines.values())
    if not pipelines:
        table.add_row("—", "—", Text("no pipelines registered", style="dim"))
        return table

    for pipeline in pipelines:
        row = render_scheduler_row(pipeline)
        table.add_row(*[r if isinstance(r, str) else r for r in row])

    return table


def render_no_pipelines() -> Text:
    """Render a message when no pipelines are scheduled."""
    return Text("No pipelines are currently scheduled.", style="dim italic")


def print_scheduler_table(scheduler: MetricScheduler) -> None:
    console = Console()
    table = render_scheduler_table(scheduler)
    console.print(table)
