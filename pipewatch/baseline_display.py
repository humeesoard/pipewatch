"""Display helpers for baseline comparison results."""

from typing import Optional
from pipewatch.baseline import BaselineComparison


def _delta_str(value: float, unit: str = "") -> str:
    sign = "+" if value >= 0 else "-"
    return f"{sign}{abs(value):.3f}{unit}"


def _regression_label(has_regressed: bool) -> str:
    if has_regressed:
        return "\033[91m REGRESSED\033[0m"
    return "\033[92m OK\033[0m"


def render_comparison_row(comparison: BaselineComparison) -> str:
    """Render a single baseline comparison as a formatted string row."""
    label = _regression_label(comparison.has_regressed)
    er = _delta_str(comparison.error_rate_delta)
    tp = _delta_str(comparison.throughput_delta, " rec/s")
    lat = _delta_str(comparison.latency_delta, " ms")
    return (
        f"  {comparison.pipeline_name:<24} "
        f"err_rate: {er:<12} "
        f"throughput: {tp:<16} "
        f"latency_p99: {lat:<12} "
        f"{label}"
    )


def render_comparisons_table(comparisons: list[BaselineComparison]) -> str:
    """Render a full table of baseline comparisons."""
    if not comparisons:
        return render_no_comparisons()

    header = (
        f"  {'Pipeline':<24} "
        f"{'err_rate Δ':<22} "
        f"{'throughput Δ':<26} "
        f"{'latency_p99 Δ':<22} "
        f"Status"
    )
    divider = "  " + "-" * 90
    rows = [render_comparison_row(c) for c in comparisons]
    return "\n".join(["\n", header, divider] + rows + [""])


def render_no_comparisons() -> str:
    return "\n  No baseline comparisons available. Run 'pipewatch baseline save' first.\n"


def print_baseline_saved(path: str, count: int) -> None:
    print(f"\n  ✔ Baseline saved: {count} pipeline(s) → {path}\n")


def print_baseline_load_error(path: str) -> None:
    print(f"\n  ✘ Could not load baseline from: {path}\n")
