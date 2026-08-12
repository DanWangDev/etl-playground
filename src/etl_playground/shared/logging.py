"""Rich-based structured logging with progress bars, stage tracking, and JSON run logs.

This is the observability backbone of the entire playground. Every module uses
get_logger() for consistent, informative output that shows exactly what's happening
behind the scenes — critical for learning, debugging, and interview demonstrations.

Core components:
- get_logger(name) → Rich-enhanced logger with emoji level indicators
- ETLContext(module_name) → Context manager with spinner, timing, stage tracking
- PipelineRun(run_id) → Tracks all stages, writes structured JSON run log
- StageResult → Dataclass with metrics dict, auto-formatted on completion
"""

import json
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.rule import Rule
from rich.table import Table

console = Console()


# ── Log-level emoji indicators ──
_LEVEL_STYLES = {
    "INFO": "[bold cyan]ℹ[/]",
    "DETAIL": "[dim cyan]  ⤷[/]",
    "WARN": "[bold yellow]⚠[/]",
    "ERROR": "[bold red]✘[/]",
    "SUCCESS": "[bold green]✔[/]",
    "METRICS": "[bold blue]📊[/]",
    "STAGE": "[bold magenta]▶[/]",
    "SPARK": "[bold yellow]⚡[/]",
}


def get_logger(name: str) -> "ETLLogger":
    """Get a Rich-enhanced logger for an ETL module."""
    return ETLLogger(name)


class ETLLogger:
    """Logger that uses Rich for beautiful, structured console output."""

    def __init__(self, name: str):
        self.name = name
        self._metrics: dict[str, Any] = {}

    # ── Leveled log methods ──

    def info(self, message: str) -> None:
        console.print(f"  {_LEVEL_STYLES['INFO']} {message}")

    def detail(self, message: str) -> None:
        console.print(f"  {_LEVEL_STYLES['DETAIL']} {message}")

    def warn(self, message: str) -> None:
        console.print(f"  {_LEVEL_STYLES['WARN']} {message}")

    def error(self, message: str) -> None:
        console.print(f"  {_LEVEL_STYLES['ERROR']} {message}")

    def success(self, message: str) -> None:
        console.print(f"  {_LEVEL_STYLES['SUCCESS']} {message}")

    def metrics(self, message: str) -> None:
        console.print(f"  {_LEVEL_STYLES['METRICS']} {message}")

    def stage(self, message: str) -> None:
        """Log a pipeline stage header."""
        console.print()
        console.print(f"  {_LEVEL_STYLES['STAGE']} {message}")

    def spark(self, message: str) -> None:
        """Log Spark-specific information (jobs, shuffle, partitions)."""
        console.print(f"  {_LEVEL_STYLES['SPARK']} {message}")

    # ── Structured output ──

    def header(self, title: str) -> None:
        """Print a module header panel."""
        console.print()
        console.print(
            Panel.fit(
                f"[bold white]{title}[/]",
                border_style="cyan",
                padding=(1, 4),
            )
        )

    def section(self, title: str) -> None:
        """Print a section divider."""
        console.print()
        console.print(Rule(f"[dim cyan]{title}[/]", style="dim cyan"))

    def key_value(self, key: str, value: str) -> None:
        """Print a key-value pair."""
        console.print(f"  [dim]{key}:[/] [white]{value}[/]")

    def table(self, title: str, rows: list[tuple[str, str]]) -> None:
        """Print a simple two-column table."""
        t = Table(title=title, show_header=True, header_style="bold cyan")
        t.add_column("Metric", style="dim")
        t.add_column("Value", style="white")
        for key, val in rows:
            t.add_row(key, val)
        console.print(t)

    def table_triple(self, title: str, headers: tuple[str, str, str], rows: list[tuple[str, str, str]]) -> None:
        """Print a three-column table."""
        t = Table(title=title, show_header=True, header_style="bold cyan")
        for h in headers:
            t.add_column(h)
        for row in rows:
            t.add_row(*row)
        console.print(t)

    def spark_job(self, job_id: int, name: str, stages: int, tasks: int,
                  duration_s: float, shuffle_read: str = "-", shuffle_write: str = "-",
                  input_size: str = "-", output_rows: str = "-") -> None:
        """Log a Spark job summary line."""
        shuffle_info = ""
        if shuffle_read != "-" or shuffle_write != "-":
            shuffle_info = f" | Shuffle R/W: {shuffle_read}/{shuffle_write}"

        console.print(
            f"  {_LEVEL_STYLES['SPARK']} [bold]Job {job_id}[/]: {name}\n"
            f"  [dim]     Stages: {stages} | Tasks: {tasks} | "
            f"Input: {input_size} | Output: {output_rows} rows | "
            f"{duration_s:.1f}s{shuffle_info}[/]"
        )

    def collect_metric(self, key: str, value: Any) -> None:
        """Accumulate a metric for the run log."""
        self._metrics[key] = value

    def get_metrics(self) -> dict[str, Any]:
        return dict(self._metrics)


# ── Progress context managers ──

@contextmanager
def spinner_context(message: str):
    """Show a Rich spinner while work is happening."""
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True,
        console=console,
    ) as progress:
        task = progress.add_task(description=message, total=None)
        yield
        progress.update(task, completed=True)


@contextmanager
def progress_bar(message: str, total: int):
    """Show a determinate progress bar."""
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task(description=message, total=total)
        yield lambda advance=1: progress.update(task, advance=advance)


# ── Pipeline run tracking ──

@dataclass
class StageResult:
    """Result of a single pipeline stage with metrics."""

    name: str
    status: str = "success"  # success | warning | error
    input_rows: int = 0
    output_rows: int = 0
    duration_ms: float = 0
    metrics: dict[str, Any] = field(default_factory=dict)


class PipelineRun:
    """Tracks a full pipeline execution, collects stage results, writes JSON run log."""

    def __init__(self, run_id: str | None = None, mode: str = "full"):
        self.run_id = run_id or datetime.now(UTC).strftime("run_%Y%m%d_%H%M%S")
        self.mode = mode
        self.timestamp = datetime.now(UTC).isoformat()
        self.stages: list[StageResult] = []
        self._start_time = time.monotonic()
        self._stage_times: dict[str, float] = {}

    def start_stage(self, name: str) -> float:
        """Mark the start of a stage; returns start time."""
        self._stage_times[name] = time.monotonic()
        return self._stage_times[name]

    def end_stage(self, name: str, **metrics: Any) -> StageResult:
        """Complete a stage with metrics."""
        start = self._stage_times.pop(name, time.monotonic())
        duration_ms = (time.monotonic() - start) * 1000

        result = StageResult(
            name=name,
            input_rows=metrics.pop("input_rows", 0),
            output_rows=metrics.pop("output_rows", 0),
            duration_ms=round(duration_ms, 1),
            metrics=metrics,
        )
        self.stages.append(result)
        return result

    @property
    def total_duration_ms(self) -> float:
        return (time.monotonic() - self._start_time) * 1000

    def save(self) -> Path:
        """Write structured JSON run log to runs/ directory."""
        from etl_playground.shared.config import get_settings

        settings = get_settings()
        run_dir = settings.runs_base
        run_dir.mkdir(parents=True, exist_ok=True)

        run_data = {
            "run_id": self.run_id,
            "timestamp": self.timestamp,
            "mode": self.mode,
            "total_duration_ms": round(self.total_duration_ms, 1),
            "stages": [
                {
                    "name": s.name,
                    "status": s.status,
                    "input_rows": s.input_rows,
                    "output_rows": s.output_rows,
                    "duration_ms": s.duration_ms,
                    **s.metrics,
                }
                for s in self.stages
            ],
            "totals": {
                "input": sum(s.input_rows for s in self.stages),
                "output": sum(s.output_rows for s in self.stages),
            },
        }

        run_path = run_dir / f"{self.run_id}.json"
        run_path.write_text(json.dumps(run_data, indent=2, default=str))
        return run_path

    def print_summary(self, logger: ETLLogger) -> None:
        """Print a formatted run summary table."""
        rows = []
        for s in self.stages:
            status_icon = {
                "success": "✔",
                "warning": "⚠",
                "error": "✘",
            }.get(s.status, "?")
            rows.append(
                (
                    f"{status_icon} {s.name}",
                    f"{s.input_rows:,}",
                    f"{s.output_rows:,}",
                    f"{s.duration_ms / 1000:.1f}s",
                )
            )

        logger.header("Pipeline Run Summary")
        t = Table(title=f"Run: {self.run_id} | Mode: {self.mode}", show_header=True)
        t.add_column("Stage", style="bold")
        t.add_column("Input", justify="right")
        t.add_column("Output", justify="right")
        t.add_column("Time", justify="right")
        for row in rows:
            t.add_row(*row)
        t.add_section()
        t.add_row(
            "[bold]TOTAL[/]",
            f"[bold]{sum(s.input_rows for s in self.stages):,}[/]",
            f"[bold]{sum(s.output_rows for s in self.stages):,}[/]",
            f"[bold]{self.total_duration_ms / 1000:.1f}s[/]",
        )
        console.print(t)


@contextmanager
def etl_context(module_name: str, run_id: str | None = None, mode: str = "full"):
    """Context manager for a full module execution.

    Usage:
        with etl_context("M03_Transform") as (log, run):
            log.header("ETL Playground — Full Transform")
            run.start_stage("dedup")
            # ... do work ...
            run.end_stage("dedup", input_rows=500000, output_rows=498742)
            run.print_summary(log)
            run.save()
    """
    log = get_logger(module_name)
    run = PipelineRun(run_id=run_id, mode=mode)

    try:
        yield log, run
    except Exception as exc:
        log.error(f"Pipeline failed: {exc}")
        run.end_stage("ERROR", status="error", error=str(exc))
        raise
    finally:
        try:
            run.save()
            log.info(f"Run log saved: runs/{run.run_id}.json")
        except Exception:
            pass
