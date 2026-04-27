"""CLI display utilities for rendering pipeline metrics in the terminal."""

from typing import List

from pipewatch.metrics import PipelineMetric

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"
BOLD = "\033[1m"


def _colorize(text: str, color: str) -> str:
    return f"{color}{text}{RESET}"


def _status_label(metric: PipelineMetric) -> str:
    if metric.is_healthy:
        return _colorize("HEALTHY", GREEN)
    return _colorize("UNHEALTHY", RED)


def render_metric_row(metric: PipelineMetric) -> str:
    """Render a single metric as a formatted terminal row."""
    status = _status_label(metric)
    error_color = RED if metric.error_rate >= 5.0 else GREEN
    latency_color = RED if metric.latency_ms >= 1000.0 else GREEN

    return (
        f"{BOLD}{metric.pipeline_name:<24}{RESET} "
        f"Status: {status:<20} "
        f"Throughput: {_colorize(f'{metric.throughput_per_sec:>8.1f}/s', GREEN)} "
        f"Latency: {_colorize(f'{metric.latency_ms:>7.1f}ms', latency_color)} "
        f"Errors: {_colorize(f'{metric.error_rate:>5.1f}%', error_color)}"
    )


def render_metrics_table(metrics: List[PipelineMetric]) -> str:
    """Render a full table of pipeline metrics for CLI output."""
    header = (
        f"\n{BOLD}{'PIPELINE':<24} {'STATUS':<18} "
        f"{'THROUGHPUT':>14} {'LATENCY':>14} {'ERROR RATE':>12}{RESET}"
    )
    separator = "-" * 88
    rows = [render_metric_row(m) for m in metrics]
    timestamp = metrics[0].timestamp.strftime("%Y-%m-%d %H:%M:%S UTC") if metrics else ""
    footer = f"\n  Last updated: {timestamp}\n"
    return "\n".join([header, separator] + rows + [footer])


def render_empty_state() -> str:
    """Render a message when no metrics are available."""
    return _colorize("\n  No pipeline metrics available. Check your configuration.\n", YELLOW)
