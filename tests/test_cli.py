"""Unit tests for the Typer CLI interface."""

from unittest.mock import AsyncMock, patch

import pytest
from typer.testing import CliRunner

from harvester.cli import app
from harvester.core.models import CrawlReport

runner = CliRunner()


def test_cli_version() -> None:
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert "GreeksView Harvester Suite" in result.stdout


def test_cli_house_mock_dry_run() -> None:
    result = runner.invoke(
        app,
        ["house", "--year", "2024", "--mock", "--dry-run", "--db-url", "sqlite:///:memory:"],
    )
    assert result.exit_code == 0
    assert "Crawl Ingestion Summary" in result.stdout
    assert "Filtered PTR Disclosures" in result.stdout


def test_cli_house_mock_live_run() -> None:
    result = runner.invoke(
        app,
        ["house", "--year", "2024", "--mock", "--limit", "2", "--db-url", "sqlite:///:memory:"],
    )
    assert result.exit_code == 0
    assert "Crawl Ingestion Summary" in result.stdout


def test_cli_house_with_errors() -> None:
    mock_report = CrawlReport(
        year=2024,
        filtered_ptrs=1,
        new_filings_to_crawl=1,
        filings_parsed=0,
        transactions_extracted=0,
        errors_count=1,
        duration_seconds=0.1,
        error_details=["Simulated parse failure on DocID 999"],
    )
    with patch("harvester.cli.HousePipeline.run", new_callable=AsyncMock) as mock_run:
        mock_run.return_value = mock_report
        result = runner.invoke(
            app,
            ["house", "--year", "2024", "--mock", "--db-url", "sqlite:///:memory:"],
        )
        assert result.exit_code == 0
        assert "Errors encountered during run" in result.stdout
        assert "Simulated parse failure" in result.stdout


def test_cli_stats() -> None:
    result = runner.invoke(app, ["stats", "--db-url", "sqlite:///:memory:"])
    assert result.exit_code == 0
    assert "Total Filings" in result.stdout
    assert "Total Transactions" in result.stdout


def test_cli_stats_with_data_and_default_db() -> None:
    mock_stats = {
        "total_filings": 10,
        "total_transactions": 25,
        "distinct_tickers": 8,
        "by_type": {"BUY": 15, "SALE_FULL": 10},
        "by_chamber": {"house": 15, "senate": 10},
    }
    with patch("harvester.cli.DatabaseManager.get_stats", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_stats
        result = runner.invoke(app, ["stats"])
        assert result.exit_code == 0
        assert "Direction: BUY" in result.stdout
        assert "Direction: SALE_FULL" in result.stdout
        assert "Chamber: HOUSE" in result.stdout
        assert "Chamber: SENATE" in result.stdout


def test_cli_stats_still_counts_retired_feeds_and_labels_them_retired() -> None:
    """`stats` keeps counting the two retired tables so an operator can see rows still stored.

    The counts are deliberately kept: they are the only place the stored rows are
    visible before the owner purges them. Each is labelled retired so nobody reads
    the row as a feed that is still collecting.
    """
    mock_stats = {
        "total_filings": 1,
        "total_transactions": 2,
        "distinct_tickers": 3,
        "by_type": {},
        "by_chamber": {},
        "finra_otc": 1234,
        "cboe_options": 567,
        "macro_indicators": 89,
    }
    with patch("harvester.cli.DatabaseManager.get_stats", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_stats
        result = runner.invoke(app, ["stats", "--db-url", "sqlite:///:memory:"])

    assert result.exit_code == 0
    out = " ".join(result.stdout.split())
    # The stored counts are still reported.
    assert "1234" in out
    assert "567" in out
    # And each of the two rows says it is retired.
    for label in ("FINRA OTC / Dark Pool Records", "CBOE Daily Options Records"):
        assert label in out
        tail = out.split(label, 1)[1]
        assert tail.startswith(" (retired, no longer collected)"), f"{label} is not labelled retired"
    # A live feed's row carries no such label, so the label distinguishes something.
    live_tail = out.split("FRED Macro Indicators", 1)[1]
    assert not live_tail.startswith(" (retired")


def test_cli_house_default_db_url_and_no_mock() -> None:
    mock_report = CrawlReport(year=2024, filtered_ptrs=0)
    with patch("harvester.cli.HousePipeline.run", new_callable=AsyncMock) as mock_run:
        mock_run.return_value = mock_report
        result = runner.invoke(app, ["house", "--year", "2024"])
        assert result.exit_code == 0
        assert "LIVE" in result.stdout


def test_cli_senate_mock_dry_run() -> None:
    result = runner.invoke(
        app,
        ["senate", "--year", "2024", "--mock", "--dry-run", "--db-url", "sqlite:///:memory:"],
    )
    assert result.exit_code == 0
    assert "Senate Crawl Ingestion Summary" in result.stdout


def test_cli_senate_mock_live_run() -> None:
    result = runner.invoke(
        app,
        ["senate", "--year", "2024", "--mock", "--limit", "2", "--db-url", "sqlite:///:memory:"],
    )
    assert result.exit_code == 0
    assert "Senate Crawl Ingestion Summary" in result.stdout


def test_cli_senate_with_errors() -> None:
    mock_report = CrawlReport(
        year=2024,
        chamber="senate",
        filtered_ptrs=1,
        new_filings_to_crawl=1,
        filings_parsed=0,
        transactions_extracted=0,
        errors_count=1,
        duration_seconds=0.1,
        error_details=["Simulated Senate parse error on DocID 999"],
    )
    with patch("harvester.cli.SenatePipeline.run", new_callable=AsyncMock) as mock_run:
        mock_run.return_value = mock_report
        result = runner.invoke(
            app,
            ["senate", "--year", "2024", "--mock", "--db-url", "sqlite:///:memory:"],
        )
        assert result.exit_code == 0
        assert "Errors encountered during run" in result.stdout
        assert "Simulated Senate parse error" in result.stdout


def test_cli_senate_default_db_url_and_no_mock() -> None:
    mock_report = CrawlReport(year=2024, chamber="senate", filtered_ptrs=0)
    with patch("harvester.cli.SenatePipeline.run", new_callable=AsyncMock) as mock_run:
        mock_run.return_value = mock_report
        result = runner.invoke(app, ["senate", "--year", "2024"])
        assert result.exit_code == 0
        assert "LIVE" in result.stdout


def test_cli_daemon_run_once() -> None:
    result = runner.invoke(
        app,
        ["daemon", "--run-once", "--mock", "--dry-run", "--db-url", "sqlite:///:memory:"],
    )
    assert result.exit_code == 0
    assert "RUN-ONCE" in result.stdout
    assert "Run-once pass completed successfully" in result.stdout


def test_cli_daemon_no_server() -> None:
    result = runner.invoke(
        app,
        [
            "daemon",
            "--run-once",
            "--mock",
            "--dry-run",
            "--no-server",
            "--initial-sync",
            "--db-url",
            "sqlite:///:memory:",
        ],
    )
    assert result.exit_code == 0
    assert "Disabled" in result.stdout


def test_cli_daemon_default_db_url_and_active_mode() -> None:
    with patch("harvester.cli.CrawlerDaemon.start", new_callable=AsyncMock) as mock_start:
        mock_start.return_value = {"mode": "daemon"}
        result = runner.invoke(app, ["daemon", "--dry-run"])
        assert result.exit_code == 0
        assert "ACTIVE DAEMON" in result.stdout


def test_cli_no_args_shows_help() -> None:
    result = runner.invoke(app, [])
    assert result.exit_code == 2 or result.exit_code == 0
    assert "Usage" in result.stdout or "help" in result.stdout.lower()


def test_cli_main_block(monkeypatch: pytest.MonkeyPatch) -> None:
    import runpy
    import sys

    monkeypatch.setattr(sys, "argv", ["harvester", "version"])
    with pytest.raises(SystemExit) as exc_info:
        runpy.run_module("harvester.cli", run_name="__main__")
    assert exc_info.value.code == 0


def test_cli_run_all_with_dataset_and_symbols(tmp_path) -> None:
    db_file = str(tmp_path / "run_all.db")
    result = runner.invoke(
        app,
        ["run-all", "--mock", "--dataset", "options", "--symbols", "SPY", "--db-url", f"sqlite:///{db_file}"],
    )
    assert result.exit_code == 0
    assert "Consolidated Harvesting Results" in result.stdout
    assert "alphavantage" in result.stdout
