"""Unit and integration tests for Harvester Progress Tracker and Monitor System."""

import json
import time

import pytest
from typer.testing import CliRunner

from harvester.cli import app
from harvester.config import Settings
from harvester.core.db import DatabaseManager
from harvester.core.progress import (
    HarvestProgressTracker,
    ProgressState,
    format_duration,
)
from harvester.workers.alphavantage.worker import AlphaVantageWorker

runner = CliRunner()


def test_format_duration():
    assert format_duration(None) == "Estimating..."
    assert format_duration(-5) == "00:00:00"
    assert format_duration(0) == "00:00"
    assert format_duration(45) == "00:45"
    assert format_duration(125) == "02:05"
    assert format_duration(3665) == "01:01:05"


def test_progress_state_serialization():
    state = ProgressState(
        dataset="options",
        total_tickers=10,
        tickers_done=3,
        total_days=550,
        current_ticker="AAPL",
        current_trading_day="2024-03-15",
        completed_units=1650,
        total_units=5500,
        pct_complete=30.0,
        pct_remaining=70.0,
        rate=22.5,
        elapsed_seconds=75.0,
        eta_seconds=171.1,
        eta_display="02:51",
        records_harvested=50000,
        records_upserted=50000,
        errors_count=0,
        status="running",
    )
    d = state.to_dict()
    assert d["current_ticker"] == "AAPL"
    assert d["pct_complete"] == 30.0

    # Extra keys ignored safely
    d["unknown_key"] = "test"
    restored = ProgressState.from_dict(d)
    assert restored.current_ticker == "AAPL"
    assert restored.total_units == 5500


def test_progress_tracker_initial_metrics(tmp_path):
    ckpt = tmp_path / "test.json"
    tracker = HarvestProgressTracker(
        dataset="options",
        total_tickers=4,
        total_days=10,
        symbols=["SPY", "QQQ", "AAPL", "MSFT"],
        checkpoint_path=ckpt,
    )
    assert tracker.total_units == 40
    assert tracker.completed_units == 0
    assert tracker.pct_complete == 0.0
    assert tracker.pct_remaining == 100.0
    assert tracker.eta_seconds is None
    assert tracker.eta_display == "Estimating..."

    # Table build
    tbl = tracker.build_progress_table()
    assert tbl.title == "GreeksView Harvester Ingestion Progress [OPTIONS]"


@pytest.mark.asyncio
async def test_progress_tracker_resumption_from_db(tmp_path):
    db_file = tmp_path / "test_resume.db"
    settings = Settings(database_url=f"sqlite:///{db_file}")

    async with DatabaseManager(settings=settings) as db:
        await db.initialize_tables()

        # Seed SPY with 5 dates and QQQ with 3 dates
        mock_spy = [
            {
                "contract_id": f"SPY_2024-01-0{d}_450_CALL",
                "symbol": "SPY",
                "trade_date": f"2024-01-0{d}",
                "expiration": "2024-02-15",
                "strike": 450.0,
                "option_type": "call",
            }
            for d in range(1, 6)
        ]
        mock_qqq = [
            {
                "contract_id": f"QQQ_2024-01-0{d}_380_CALL",
                "symbol": "QQQ",
                "trade_date": f"2024-01-0{d}",
                "expiration": "2024-02-15",
                "strike": 380.0,
                "option_type": "call",
            }
            for d in range(1, 4)
        ]
        await db.upsert_options_chains_eod(mock_spy + mock_qqq)

        target_dates = [f"2024-01-0{d}" for d in range(1, 6)]  # 5 dates
        symbols = ["SPY", "QQQ", "AAPL"]  # 3 symbols -> 15 total units

        ckpt = tmp_path / "resume.json"
        tracker = HarvestProgressTracker(
            dataset="options",
            total_tickers=3,
            total_days=5,
            symbols=symbols,
            checkpoint_path=ckpt,
        )

        # Scan initial DB state
        await tracker.scan_initial_state(db, symbols=symbols, target_dates=target_dates, skip_existing=True)

        # SPY has 5/5 -> fully completed ticker (1)
        # QQQ has 3/5
        # AAPL has 0/5
        # Total units stored: 5 + 3 = 8 out of 15 (53.33%)
        assert tracker.completed_units == 8
        assert tracker.total_units == 15
        assert round(tracker.pct_complete, 1) == 53.3
        assert round(tracker.pct_remaining, 1) == 46.7
        assert tracker.tickers_done == 1
        assert "SPY" in tracker.completed_tickers_set
        assert tracker.status == "resumed"


@pytest.mark.asyncio
async def test_progress_tracker_updates_and_checkpoint(tmp_path):
    ckpt = tmp_path / "updates.json"
    tracker = HarvestProgressTracker(
        dataset="options",
        total_tickers=2,
        total_days=5,
        symbols=["SPY", "QQQ"],
        checkpoint_path=ckpt,
    )

    tracker.update_session("SPY", "2024-01-01", records_count=100, upserted=100)
    assert tracker.completed_units == 1
    assert tracker.records_harvested == 100
    assert tracker.records_upserted == 100
    assert tracker.current_ticker == "SPY"
    assert tracker.current_trading_day == "2024-01-01"

    # Simulate passing time and rate
    tracker.start_time = time.monotonic() - 10.0
    tracker.newly_completed_units = 5
    assert tracker.rate > 0
    assert tracker.eta_seconds is not None

    tracker.mark_ticker_done("SPY")
    assert tracker.tickers_done == 1

    tracker.finish("completed")
    assert tracker.status == "completed"
    assert tracker.completed_units == tracker.total_units
    assert tracker.tickers_done == tracker.total_tickers

    # Verify checkpoint on disk
    loaded = HarvestProgressTracker.load_checkpoint(ckpt)
    assert loaded is not None
    assert loaded.status == "completed"
    assert loaded.total_tickers == 2


@pytest.mark.asyncio
async def test_db_progress_methods(tmp_path):
    db_file = tmp_path / "test_db_progress.db"
    settings = Settings(database_url=f"sqlite:///{db_file}")

    async with DatabaseManager(settings=settings) as db:
        await db.initialize_tables()

        # Test batch options query
        dates_map = await db.get_stored_options_dates_for_symbols(["AAPL", "MSFT"])
        assert "AAPL" in dates_map
        assert "MSFT" in dates_map

        # Test daily symbols query
        syms = await db.get_stored_daily_symbols(["AAPL", "MSFT"])
        assert len(syms) == 0

        # Test progress upsert and query
        state = ProgressState(
            dataset="options",
            worker_name="alphavantage",
            total_tickers=5,
            tickers_done=2,
            total_days=100,
            current_ticker="AAPL",
            current_trading_day="2024-03-15",
            completed_units=250,
            total_units=500,
            pct_complete=50.0,
            pct_remaining=50.0,
            rate=15.0,
            elapsed_seconds=20.0,
            eta_display="00:16",
            records_upserted=25000,
            errors_count=0,
            status="running",
        )
        await db.upsert_harvester_progress(state.to_dict())

        rows = await db.get_harvester_progress("options")
        assert len(rows) == 1
        assert rows[0]["dataset"] == "options"
        assert rows[0]["current_ticker"] == "AAPL"
        assert rows[0]["completed_units"] == 250


@pytest.mark.asyncio
async def test_worker_options_progress_mock_integration(tmp_path):
    db_file = tmp_path / "test_worker_progress.db"
    settings = Settings(database_url=f"sqlite:///{db_file}")
    worker = AlphaVantageWorker(settings=settings)

    ckpt = tmp_path / "worker_ckpt.json"
    tracker = HarvestProgressTracker(
        dataset="options",
        total_tickers=2,
        total_days=3,
        symbols=["SPY", "QQQ"],
        checkpoint_path=ckpt,
    )

    async with DatabaseManager(settings=settings) as db:
        await db.initialize_tables()
        target_dates = ["2024-01-02", "2024-01-03", "2024-01-04"]

        await tracker.scan_initial_state(db, symbols=["SPY", "QQQ"], target_dates=target_dates)

        h, u = await worker.download_historical_options(
            db=db,
            symbols=["SPY", "QQQ"],
            trade_dates=target_dates,
            use_mock=True,
            progress_tracker=tracker,
        )

        assert h > 0
        assert u > 0
        assert tracker.completed_units == 6
        assert tracker.tickers_done == 2
        assert tracker.pct_complete == 100.0


def test_cli_monitor_command(tmp_path, monkeypatch):
    ckpt = tmp_path / ".harvester_progress.json"
    state = ProgressState(
        dataset="options",
        worker_name="alphavantage",
        total_tickers=5,
        tickers_done=2,
        total_days=550,
        current_ticker="NVDA",
        current_trading_day="2024-03-15",
        completed_units=1375,
        total_units=2750,
        pct_complete=50.0,
        pct_remaining=50.0,
        rate=22.0,
        elapsed_seconds=60.0,
        eta_display="01:02",
        records_upserted=125000,
        errors_count=0,
        status="running",
    )
    ckpt.write_text(json.dumps(state.to_dict()))

    import harvester.core.progress

    monkeypatch.setattr(harvester.core.progress, "DEFAULT_CHECKPOINT_PATH", ckpt)
    import harvester.cli

    monkeypatch.setattr(harvester.cli, "DEFAULT_CHECKPOINT_PATH", ckpt)

    res = runner.invoke(app, ["monitor"])
    assert res.exit_code == 0
    assert "GreeksView Harvester Ingestion Progress [OPTIONS]" in res.stdout
    assert "NVDA" in res.stdout
    assert "2024-03-15" in res.stdout
    assert "50.0%" in res.stdout


def test_build_renderable_and_sub_panel(tmp_path):
    tracker = HarvestProgressTracker(
        dataset="options",
        total_tickers=2,
        total_days=10,
        symbols=["AAPL", "MSFT"],
        checkpoint_path=tmp_path / "panel.json",
    )
    tracker.current_ticker = "AAPL"
    tracker.current_trading_day = "2024-01-10"
    tracker.completed_units = 5
    tracker.records_upserted = 1200
    tracker.status = "running"

    sub = tracker.build_sub_panel()
    assert "Sessions:" in sub
    assert "5 / 20" in sub
    assert "Upserted Rows:" in sub
    assert "1,200" in sub
    assert "RUNNING" in sub

    from rich.console import Group

    renderable = tracker.build_renderable()
    assert isinstance(renderable, Group)
    assert len(renderable.renderables) == 2


def test_interactive_live_lifecycle(tmp_path):
    import io

    from rich.console import Console

    out = io.StringIO()
    con = Console(file=out, force_terminal=True, highlight=False)

    tracker = HarvestProgressTracker(
        dataset="options",
        total_tickers=2,
        total_days=5,
        symbols=["AAPL", "GOOG"],
        console=con,
        checkpoint_path=tmp_path / "live.json",
        interactive=True,
    )
    assert tracker._is_interactive is True
    assert tracker._live is None

    # Render triggers Live.start() in-place
    tracker.render()
    assert tracker._live is not None

    # Update session refreshes Live in-place
    tracker.update_session("AAPL", "2024-02-01", records_count=50, upserted=50)
    assert tracker._live is not None

    # Finish cleanly updates and stops Live
    tracker.finish("completed")
    assert tracker._live is None
    assert tracker.status == "completed"


def test_progress_tracker_context_manager(tmp_path):
    import io

    from rich.console import Console

    out = io.StringIO()
    con = Console(file=out, force_terminal=True)

    with HarvestProgressTracker(
        dataset="daily",
        total_tickers=1,
        total_days=1,
        symbols=["SPY"],
        console=con,
        checkpoint_path=tmp_path / "cm.json",
        interactive=True,
    ) as tracker:
        tracker.render()
        assert tracker._live is not None

    # Exiting context manager must invoke close() and stop _live
    assert tracker._live is None


def test_non_interactive_throttling(tmp_path):
    import io

    from rich.console import Console

    out = io.StringIO()
    con = Console(file=out, highlight=False)

    tracker = HarvestProgressTracker(
        dataset="options",
        total_tickers=2,
        total_days=2,
        symbols=["SPY", "QQQ"],
        console=con,
        checkpoint_path=tmp_path / "throttle.json",
        interactive=False,
        render_interval=10.0,
    )
    # Initial scan / force render prints table
    tracker.render(force=True)
    first_len = len(out.getvalue())
    assert first_len > 0

    # Unforced immediate render within interval should be throttled (no output added)
    tracker.update_session("SPY", "2024-01-02", records_count=10, upserted=10)
    assert len(out.getvalue()) == first_len

    # finish prints final table
    tracker.finish("completed")
    assert len(out.getvalue()) > first_len
