"""Display helpers for filter criteria and filtered results."""

from typing import List

from rich.console import Console
from rich.table import Table
from rich import box

from pipewatch.filter import FilterCriteria
from pipewatch.metrics import PipelineMetric

console = Console()


def render_criteria_summary(criteria: FilterCriteria) -> str:
    """Return a human-readable summary of active filter criteria."""
    parts: List[str] = []
    if criteria.pipeline_name:
        parts.append(f"pipeline={criteria.pipeline_name!r}")
    if criteria.min_throughput is not None:
        parts.append(f"min_throughput={criteria.min_throughput}")
    if criteria.max_error_rate is not None:
        parts.append(f"max_error_rate={criteria.max_error_rate:.2%}")
    if criteria.healthy_only:
        parts.append("healthy_only=True")
    return "Filters: " + (" | ".join(parts) if parts else "none")


def render_filtered_table(metrics: List[PipelineMetric]) -> Table:
    """Render a Rich table of filtered pipeline metrics."""
    table = Table(
        title="Filtered Metrics",
        box=box.SIMPLE_HEAVY,
        show_lines=False,
    )
    table.add_column("Pipeline", style="cyan", no_wrap=True)
    table.add_column("Records", justify="right")
    table.add_column("Errors", justify="right")
    table.add_column("Throughput", justify="right")

    for m in metrics:
        table.add_row(
            m.pipeline_name,
            str(m.record_count),
            str(m.error_count),
            f"{m.throughput:.1f}/s",
        )
    return table


def print_no_results() -> None:
    """Print a message when no metrics match the active filters."""
    console.print("[yellow]No metrics match the current filter criteria.[/yellow]")
