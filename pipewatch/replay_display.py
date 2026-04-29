"""Display utilities for replay sessions."""

from rich.console import Console
from rich.table import Table
from rich.text import Text
from pipewatch.replay import ReplaySession
from pipewatch.history import MetricSnapshot
from pipewatch.metrics import error_rate, is_healthy

console = Console()


def _format_timestamp(snapshot: MetricSnapshot) -> str:
    return snapshot.timestamp.strftime("%Y-%m-%d %H:%M:%S")


def _health_label(snapshot: MetricSnapshot) -> Text:
    healthy = is_healthy(snapshot.metric)
    if healthy:
        return Text("healthy", style="bold green")
    return Text("unhealthy", style="bold red")


def render_replay_row(index: int, snapshot: MetricSnapshot) -> Table:
    """Render a single snapshot as a labeled row table."""
    table = Table(show_header=False, box=None, padding=(0, 1))
    table.add_column("Field", style="dim")
    table.add_column("Value")
    metric = snapshot.metric
    er = error_rate(metric)
    table.add_row("Step", str(index + 1))
    table.add_row("Time", _format_timestamp(snapshot))
    table.add_row("Pipeline", metric.pipeline_name)
    table.add_row("Records", str(metric.records_processed))
    table.add_row("Errors", str(metric.error_count))
    table.add_row("Error Rate", f"{er:.2%}")
    table.add_row("Throughput", f"{metric.throughput_per_second:.1f}/s")
    table.add_row("Status", _health_label(snapshot))
    return table


def render_replay_summary(session: ReplaySession) -> None:
    """Print a summary header for a replay session."""
    console.print(f"[bold cyan]Replay:[/bold cyan] {session.pipeline_name}  "
                  f"[dim]{session.total} snapshot(s)[/dim]")


def render_no_replay(pipeline_name: str) -> None:
    """Print a message when no replay data is available."""
    console.print(f"[yellow]No history available for pipeline:[/yellow] {pipeline_name}")


def print_replay_session(session: ReplaySession) -> None:
    """Print all snapshots in a replay session to the console."""
    render_replay_summary(session)
    session.reset()
    for index, snapshot in enumerate(session.iter_remaining()):
        console.rule(f"[dim]Snapshot {index + 1}[/dim]")
        console.print(render_replay_row(index, snapshot))
    console.rule("[dim]End of replay[/dim]")
