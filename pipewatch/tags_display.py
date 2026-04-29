"""Display helpers for tag-grouped pipeline metrics."""

from typing import Dict, List
from pipewatch.metrics import PipelineMetric, error_rate, is_healthy

_RESET = "\033[0m"
_BOLD = "\033[1m"
_CYAN = "\033[96m"
_GREEN = "\033[92m"
_RED = "\033[91m"
_DIM = "\033[2m"


def _health_indicator(metric: PipelineMetric) -> str:
    if is_healthy(metric):
        return f"{_GREEN}✔{_RESET}"
    return f"{_RED}✘{_RESET}"


def render_tag_header(tag: str, count: int) -> str:
    """Render a section header for a tag group."""
    return f"{_BOLD}{_CYAN}[{tag}]{_RESET} {_DIM}({count} pipeline{'s' if count != 1 else ''}){_RESET}"


def render_tag_metric_row(metric: PipelineMetric) -> str:
    """Render a single metric row within a tag group."""
    rate = error_rate(metric)
    indicator = _health_indicator(metric)
    return (
        f"  {indicator} {metric.pipeline_name:<28} "
        f"err={rate:.1%}  "
        f"throughput={metric.records_processed:>6}  "
        f"latency={metric.latency_ms:>7.1f}ms"
    )


def render_tagged_groups(
    grouped: Dict[str, List[PipelineMetric]],
) -> None:
    """Print all tag groups with their metrics to stdout."""
    if not grouped:
        print(f"{_DIM}No tagged metrics to display.{_RESET}")
        return

    for tag in sorted(grouped.keys()):
        metrics = grouped[tag]
        print(render_tag_header(tag, len(metrics)))
        for metric in metrics:
            print(render_tag_metric_row(metric))
        print()


def render_tag_summary(tag_index_tags: List[str]) -> None:
    """Print a summary list of all known tags."""
    if not tag_index_tags:
        print(f"{_DIM}No tags registered.{_RESET}")
        return
    print(f"{_BOLD}Known tags:{_RESET}")
    for tag in tag_index_tags:
        print(f"  • {tag}")
