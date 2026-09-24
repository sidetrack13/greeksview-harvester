"""Real-time, resilient progress tracking and monitoring for GreeksView Harvester.

Maintains live ingestion metrics across tickers and trading days, writes periodic
clean table snapshots to logs/terminals, persists checkpoints to disk and database,
and enables seamless resumption from prior database watermarks upon process restart.
"""

from __future__ import annotations

import contextlib
import json
import logging
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from rich import box
from rich.console import Console, Group
from rich.live import Live
from rich.table import Table

logger = logging.getLogger("harvester.progress")

DEFAULT_CHECKPOINT_PATH = Path(".harvester_progress.json")


def format_duration(seconds: float | None) -> str:
    """Format duration in seconds to a human-readable HH:MM:SS or string."""
    if seconds is None:
        return "Estimating..."
    if seconds < 0:
        return "00:00:00"
    secs = int(seconds)
    hours = secs // 3600
    minutes = (secs % 3600) // 60
    rem_secs = secs % 60
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{rem_secs:02d}"
    return f"{minutes:02d}:{rem_secs:02d}"


@dataclass
class ProgressState:
    """Serializable snapshot of harvester progress."""

    dataset: str = "options"
    worker_name: str = "alphavantage"
    total_tickers: int = 0
    tickers_done: int = 0
    total_days: int = 0
    current_ticker: str = ""
    current_trading_day: str = ""
    completed_units: int = 0
    total_units: int = 0
    pct_complete: float = 0.0
    pct_remaining: float = 100.0
    rate: float = 0.0
    elapsed_seconds: float = 0.0
    eta_seconds: float | None = None
    eta_display: str = "Estimating..."
    records_harvested: int = 0
    records_upserted: int = 0
    errors_count: int = 0
    status: str = "running"
    updated_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ProgressState:
        valid_keys = cls.__dataclass_fields__.keys()
        filtered = {k: v for k, v in data.items() if k in valid_keys}
        return cls(**filtered)


class HarvestProgressTracker:
    """Tracks, logs, persists, and visualizes ingestion progress across tickers and trading days.

    Supports stop/restart resilience by inspecting database state at startup and resuming
    with accurate completion percentages and ETA estimates.
    """

    def __init__(
        self,
        dataset: str = "options",
        total_tickers: int = 0,
        total_days: int = 0,
        symbols: list[str] | None = None,
        console: Console | None = None,
        checkpoint_path: Path | str = DEFAULT_CHECKPOINT_PATH,
        render_interval: float = 5.0,
        log_interval: float = 5.0,
        interactive: bool | None = None,
    ):
        self.dataset = dataset
        self.total_tickers = total_tickers
        self.total_days = total_days
        self.symbols = [s.strip().upper() for s in (symbols or []) if s.strip()]
        if self.symbols and self.total_tickers == 0:
            self.total_tickers = len(self.symbols)
        self.console = console or Console(highlight=False)
        self.checkpoint_path = Path(checkpoint_path)
        self.render_interval = render_interval
        self.log_interval = log_interval

        if interactive is not None:
            self._is_interactive = interactive
        else:
            self._is_interactive = bool(self.console.is_terminal and sys.stdout.isatty())
        self._live: Live | None = None

        self.tickers_done = 0
        self.current_ticker = ""
        self.current_trading_day = ""
        self.completed_units = 0
        self.total_units = self.total_tickers * max(1, self.total_days)
        self.newly_completed_units = 0
        self.records_harvested = 0
        self.records_upserted = 0
        self.errors_count = 0
        self.status = "starting"

        self.start_time = time.monotonic()
        self.last_render_time = 0.0
        self.last_log_time = 0.0
        self.completed_tickers_set: set[str] = set()
        self._rate_override: float | None = None
        self._eta_display_override: str | None = None
        self._elapsed_override: float | None = None

    @property
    def pct_complete(self) -> float:
        if self.total_units <= 0:
            return 100.0 if self.completed_units > 0 else 0.0
        return min(100.0, max(0.0, (self.completed_units / self.total_units) * 100.0))

    @property
    def pct_remaining(self) -> float:
        return max(0.0, 100.0 - self.pct_complete)

    @property
    def elapsed(self) -> float:
        if self._elapsed_override is not None:
            return self._elapsed_override
        return max(0.0, time.monotonic() - self.start_time)

    @property
    def rate(self) -> float:
        if self._rate_override is not None:
            return self._rate_override
        el = self.elapsed
        if el <= 0.0 or self.newly_completed_units <= 0:
            return 0.0
        return self.newly_completed_units / el

    @property
    def remaining_units(self) -> int:
        return max(0, self.total_units - self.completed_units)

    @property
    def eta_seconds(self) -> float | None:
        if self.remaining_units <= 0:
            return 0.0
        r = self.rate
        if r > 0.0:
            return self.remaining_units / r
        return None

    @property
    def eta_display(self) -> str:
        if self._eta_display_override is not None:
            return self._eta_display_override
        if self.remaining_units <= 0:
            return "Complete"
        return format_duration(self.eta_seconds)

    def snapshot(self) -> ProgressState:
        return ProgressState(
            dataset=self.dataset,
            worker_name="alphavantage",
            total_tickers=self.total_tickers,
            tickers_done=self.tickers_done,
            total_days=self.total_days,
            current_ticker=self.current_ticker,
            current_trading_day=self.current_trading_day,
            completed_units=self.completed_units,
            total_units=self.total_units,
            pct_complete=round(self.pct_complete, 2),
            pct_remaining=round(self.pct_remaining, 2),
            rate=round(self.rate, 2),
            elapsed_seconds=round(self.elapsed, 2),
            eta_seconds=round(self.eta_seconds, 2) if self.eta_seconds is not None else None,
            eta_display=self.eta_display,
            records_harvested=self.records_harvested,
            records_upserted=self.records_upserted,
            errors_count=self.errors_count,
            status=self.status,
            updated_at=datetime.now(UTC).isoformat(),
        )

    async def scan_initial_state(
        self,
        db: Any,
        symbols: list[str],
        target_dates: list[str] | None = None,
        skip_existing: bool = True,
    ) -> None:
        """Scan database to detect previously completed work for stop/restart resilience."""
        self.symbols = [s.strip().upper() for s in symbols if s.strip()]
        self.total_tickers = len(self.symbols)
        dates_list = list(target_dates or [])
        self.total_days = len(dates_list) if dates_list else max(1, self.total_days)
        self.total_units = self.total_tickers * max(1, self.total_days)

        if not skip_existing or not self.symbols or db is None:
            self.status = "running"
            self.save_checkpoint()
            self.render(force=True)
            return

        target_dates_set = set(dates_list)
        already_stored_units = 0
        fully_completed_tickers = 0

        try:
            if self.dataset == "options" and hasattr(db, "get_stored_options_dates_for_symbols"):
                stored_map = await db.get_stored_options_dates_for_symbols(self.symbols)
                for sym in self.symbols:
                    stored_dates = stored_map.get(sym, set())
                    if target_dates_set:
                        overlap = len(stored_dates.intersection(target_dates_set))
                    else:
                        overlap = len(stored_dates)
                    already_stored_units += overlap
                    if target_dates_set and overlap >= len(target_dates_set) and len(target_dates_set) > 0:
                        fully_completed_tickers += 1
                        self.completed_tickers_set.add(sym)

            elif self.dataset == "daily" and hasattr(db, "get_stored_daily_symbols"):
                stored_syms = await db.get_stored_daily_symbols(self.symbols)
                already_stored_units = len(stored_syms)
                fully_completed_tickers = len(stored_syms)
                self.completed_tickers_set = set(stored_syms)
        except Exception as exc:
            logger.warning("Error during initial progress database scan: %s", exc)

        self.completed_units = min(self.total_units, already_stored_units)
        self.tickers_done = min(self.total_tickers, fully_completed_tickers)
        self.status = "resumed" if self.completed_units > 0 else "running"

        if self.completed_units > 0:
            logger.info(
                "Resuming %s harvest: %d/%d units (%0.1f%%) already stored across %d/%d tickers.",
                self.dataset,
                self.completed_units,
                self.total_units,
                self.pct_complete,
                self.tickers_done,
                self.total_tickers,
            )

        self.save_checkpoint()
        if hasattr(db, "upsert_harvester_progress"):
            with contextlib.suppress(Exception):
                await db.upsert_harvester_progress(self.snapshot().to_dict())
        self.render(force=True)

    def update_session(
        self,
        ticker: str,
        trading_day: str,
        records_count: int = 0,
        upserted: int = 0,
        is_error: bool = False,
    ) -> None:
        """Called whenever a single ticker trading day session is completed."""
        self.status = "running"
        self.current_ticker = ticker.upper()
        self.current_trading_day = trading_day
        self.completed_units = min(self.total_units, self.completed_units + 1)
        self.newly_completed_units += 1
        self.records_harvested += records_count
        self.records_upserted += upserted
        if is_error:
            self.errors_count += 1

        now = time.monotonic()
        if (now - self.last_log_time) >= self.log_interval:
            self.last_log_time = now
            logger.info(
                "[%s | %s] Progress: %d/%d (%0.1f%%) | Rate: %0.1f req/s | ETA: %s",
                self.current_ticker,
                self.current_trading_day,
                self.completed_units,
                self.total_units,
                self.pct_complete,
                self.rate,
                self.eta_display,
            )

        self.render(force=False)

    def mark_ticker_done(self, ticker: str) -> None:
        """Mark a ticker as completed."""
        sym = ticker.upper()
        if sym not in self.completed_tickers_set:
            self.completed_tickers_set.add(sym)
            self.tickers_done = min(self.total_tickers, self.tickers_done + 1)
        logger.info(
            "Ticker %s complete (%d/%d tickers done, %0.1f%% total progress)",
            sym,
            self.tickers_done,
            self.total_tickers,
            self.pct_complete,
        )
        self.render(force=False)

    def finish(self, status: str = "completed") -> None:
        """Mark run as completed/finished."""
        self.status = status
        if status == "completed":
            self.tickers_done = self.total_tickers
            self.completed_units = self.total_units
        self.save_checkpoint()

        if self._is_interactive:
            if self._live is not None:
                self._live.update(self.build_renderable(), refresh=True)
                self._live.stop()
                self._live = None
            else:
                self.console.print(self.build_progress_table())
                self.console.print(self.build_sub_panel())
                self.console.print()
        else:
            self.render(force=True)

    def close(self) -> None:
        """Clean up live display resources."""
        if self._live is not None:
            with contextlib.suppress(Exception):
                self._live.stop()
            self._live = None

    def __enter__(self) -> HarvestProgressTracker:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()

    def save_checkpoint(self) -> None:
        """Atomically persist progress state to JSON checkpoint file."""
        try:
            tmp = self.checkpoint_path.with_suffix(".tmp")
            data = json.dumps(self.snapshot().to_dict(), indent=2)
            tmp.write_text(data, encoding="utf-8")
            tmp.replace(self.checkpoint_path)
        except Exception as e:
            logger.debug("Failed to write checkpoint file: %s", e)

    @classmethod
    def load_checkpoint(cls, path: Path | str = DEFAULT_CHECKPOINT_PATH) -> ProgressState | None:
        """Load progress state from JSON checkpoint file."""
        p = Path(path)
        if not p.is_file():
            return None
        try:
            content = json.loads(p.read_text(encoding="utf-8"))
            return ProgressState.from_dict(content)
        except Exception:
            return None

    def build_progress_table(self) -> Table:
        """Build Rich Table with formatted progress metrics."""
        table = Table(
            title=f"GreeksView Harvester Ingestion Progress [{self.dataset.upper()}]",
            box=box.ROUNDED,
            header_style="bold cyan",
            show_lines=False,
        )
        table.add_column("Tickers", justify="center", style="bold yellow")
        table.add_column("Days", justify="center", style="bold yellow")
        table.add_column("Ticker", justify="center", style="bold green")
        table.add_column("Trading Day", justify="center", style="bold cyan", no_wrap=True)
        table.add_column("% Complete", justify="right", style="bold green")
        table.add_column("% Remaining", justify="right", style="bold magenta")
        table.add_column("Rate", justify="right", style="cyan")
        table.add_column("Elapsed", justify="center", style="white")
        table.add_column("ETA", justify="center", style="bold white")

        tickers_display = f"{self.tickers_done}/{self.total_tickers}"
        if self.total_tickers > 0:
            tickers_display += f" ({(self.tickers_done / self.total_tickers) * 100:.0f}%)"

        days_display = f"{self.total_days}d" if self.total_days > 0 else "—"
        curr_ticker = self.current_ticker or "—"
        curr_day = self.current_trading_day or "—"
        comp_pct = f"{self.pct_complete:.1f}%"
        rem_pct = f"{self.pct_remaining:.1f}%"
        rate_str = f"{self.rate:.1f} r/s" if self.rate > 0 else "—"
        elapsed_str = format_duration(self.elapsed)

        table.add_row(
            tickers_display,
            days_display,
            curr_ticker,
            curr_day,
            comp_pct,
            rem_pct,
            rate_str,
            elapsed_str,
            self.eta_display,
        )
        return table

    def build_sub_panel(self) -> str:
        """Build formatted sub-panel summary string."""
        return (
            f"[bold cyan]Sessions:[/bold cyan] {self.completed_units:,} / {self.total_units:,} "
            f"([bold green]{self.pct_complete:.1f}%[/bold green]) | "
            f"[bold cyan]Upserted Rows:[/bold cyan] {self.records_upserted:,} | "
            f"[bold cyan]Errors:[/bold cyan] {self.errors_count} | "
            f"[bold cyan]Status:[/bold cyan] [bold yellow]{self.status.upper()}[/bold yellow]"
        )

    def build_renderable(self) -> Group:
        """Combine progress table and sub-panel into a single Rich renderable."""
        return Group(self.build_progress_table(), self.build_sub_panel())

    def render(self, force: bool = False) -> None:
        """Render table to console with in-place refresh in interactive mode, throttled in non-interactive."""
        now = time.monotonic()

        if self._is_interactive:
            renderable = self.build_renderable()
            if self._live is None:
                self._live = Live(
                    renderable,
                    console=self.console,
                    refresh_per_second=4,
                    auto_refresh=False,
                    transient=False,
                )
                self._live.start()
            else:
                self._live.update(renderable, refresh=True)
            self.last_render_time = now
            self.save_checkpoint()
            return

        # In non-interactive mode, throttle table rendering so it prints every render_interval seconds
        if not force and (now - self.last_render_time) < self.render_interval:
            return

        self.last_render_time = now
        table = self.build_progress_table()

        self.console.print(table)
        self.console.print(self.build_sub_panel())
        self.console.print()

        self.save_checkpoint()
