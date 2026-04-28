"""CLI display helpers for the export feature."""

from __future__ import annotations

from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.text import Text

console = Console()


def print_export_success(path: str, fmt: str) -> None:
    """Print a success message after writing an export file."""
    label = Text()
    label.append("✔ Exported ", style="bold green")
    label.append(fmt.upper(), style="bold cyan")
    label.append(" → ", style="dim")
    label.append(path, style="underline white")
    console.print(label)


def print_export_preview(content: str, fmt: str, max_lines: int = 20) -> None:
    """Print a syntax-highlighted preview of exported content."""
    lines = content.splitlines()
    preview = "\n".join(lines[:max_lines])
    truncated = len(lines) > max_lines

    lexer = "json" if fmt == "json" else "text"
    syntax = Syntax(preview, lexer, theme="monokai", line_numbers=True)

    title = f"Export Preview ({fmt.upper()})"
    if truncated:
        title += f" — first {max_lines} of {len(lines)} lines"

    console.print(Panel(syntax, title=title, border_style="dim"))


def print_export_error(message: str) -> None:
    """Print an error message for a failed export."""
    console.print(Text(f"✖ Export failed: {message}", style="bold red"))
