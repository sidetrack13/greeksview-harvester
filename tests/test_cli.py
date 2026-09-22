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
