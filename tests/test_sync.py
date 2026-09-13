"""Unit tests for the SQLite-to-PostgreSQL synchronization module."""

import sqlite3
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from typer.testing import CliRunner

from harvester.cli import app
from harvester.core.sync import (
    _parse_date,
    sync_sqlite_to_postgres,
)

runner = CliRunner()


def test_parse_date():
    """Test date parsing helper across various string and object inputs."""
    assert _parse_date(None) is None
    assert _parse_date("") is None
    assert _parse_date(date(2024, 5, 15)) == date(2024, 5, 15)
    assert _parse_date("2024-05-15") == date(2024, 5, 15)
    assert _parse_date("2024-05-15 10:30:00") == date(2024, 5, 15)
    assert _parse_date("05/15/2024") == date(2024, 5, 15)
    assert _parse_date("invalid-date") is None


def test_sync_sqlite_to_postgres_missing_file():
    """Test FileNotFoundError when source SQLite file does not exist."""
    import asyncio
    with pytest.raises(FileNotFoundError):
        asyncio.run(sync_sqlite_to_postgres("postgresql://localhost:5432/test", sqlite_path="non_existent_db.sqlite"))


@pytest.mark.asyncio
async def test_sync_sqlite_to_postgres_execution(tmp_path):
    """Test end-to-end table synchronization with mocked PostgreSQL pool."""
    db_path = str(tmp_path / "test_harvester.db")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Create and populate sample tables
    cur.execute("""
    CREATE TABLE congressional_filings (
        filing_id TEXT PRIMARY KEY, chamber TEXT, member_name TEXT, member_id TEXT,
        filing_year INTEGER, filing_date TEXT, doc_url TEXT, raw_text TEXT,
        sha256_hash TEXT, status TEXT
    )
    """)
    cur.execute("""
    INSERT INTO congressional_filings VALUES (
        'FILING_H1', 'house', 'Jane Doe', 'D001', 2024, '2024-05-15', 'http://url', 'text', 'hash1', 'parsed'
    )
    """)

    cur.execute("""
    CREATE TABLE congressional_transactions (
        filing_id TEXT, member_name TEXT, chamber TEXT, party TEXT, state TEXT, district TEXT,
        ticker TEXT, asset_description TEXT, asset_type TEXT, transaction_type TEXT,
        amount_bracket TEXT, amount_min REAL, amount_max REAL, transaction_date TEXT,
        filing_date TEXT, owner TEXT, comment TEXT
    )
    """)
    cur.execute("""
    INSERT INTO congressional_transactions VALUES (
        'FILING_H1', 'Jane Doe', 'house', 'Democrat', 'CA', '12', 'NVDA', 'NVIDIA Corp',
        'stock', 'BUY', '$1,001 - $15,000', 1001.0, 15000.0, '2024-05-10', '2024-05-15', 'self', 'test'
    )
    """)

    cur.execute("""
    CREATE TABLE macro_indicators (
        id TEXT PRIMARY KEY, series_id TEXT, indicator_name TEXT, date TEXT,
        value REAL, frequency TEXT, units TEXT
    )
    """)
    cur.execute("INSERT INTO macro_indicators VALUES ('FEDFUNDS_2024-05-15', 'FEDFUNDS', 'Fed Funds', '2024-05-15', 5.33, 'daily', 'Percent')")

    cur.execute("""
    CREATE TABLE cboe_daily_options (
        id TEXT PRIMARY KEY, trade_date TEXT, total_call_volume REAL, total_put_volume REAL,
        total_volume REAL, equity_pc_ratio REAL, index_pc_ratio REAL, total_pc_ratio REAL, vix_volume REAL
    )
    """)
    cur.execute("INSERT INTO cboe_daily_options VALUES ('cboe_2024-05-15', '2024-05-15', 1000, 800, 1800, 0.65, 1.2, 0.8, 500)")

    cur.execute("""
    CREATE TABLE finra_otc_volume (
        id TEXT PRIMARY KEY, symbol TEXT, week_start_date TEXT, tier TEXT,
        otc_volume REAL, total_trades REAL, total_market_volume REAL, dark_pool_share_pct REAL
    )
    """)
    cur.execute("INSERT INTO finra_otc_volume VALUES ('finra_1', 'AAPL', '2024-05-13', 'Tier 1', 50000, 1200, 100000, 50.0)")

    cur.execute("""
    CREATE TABLE insider_trades (
        id TEXT PRIMARY KEY, symbol TEXT, filing_date TEXT, transaction_date TEXT,
        reporting_owner TEXT, owner_title TEXT, is_director INTEGER, is_officer INTEGER,
        is_ten_percent INTEGER, transaction_type TEXT, shares REAL, price_per_share REAL,
        shares_owned_following REAL, sec_form TEXT, filing_url TEXT
    )
    """)
    cur.execute("INSERT INTO insider_trades VALUES ('it_1', 'MSFT', '2024-05-15', '2024-05-14', 'Satya', 'CEO', 1, 1, 0, 'P', 100, 420.0, 5000, '4', 'url')")

    cur.execute("""
    CREATE TABLE institutional_holdings (
        id TEXT PRIMARY KEY, cik TEXT, institution_name TEXT, report_calendar_or_quarter TEXT,
        symbol TEXT, cusip TEXT, shares REAL, market_value REAL, investment_discretion TEXT,
        voting_authority_sole REAL, sec_form TEXT, filing_url TEXT
    )
    """)
    cur.execute("INSERT INTO institutional_holdings VALUES ('ih_1', '0001', 'Berkshire', '2024-03-31', 'AAPL', '037833100', 900000, 150000000.0, 'SOLE', 900000, '13F-HR', 'url')")

    conn.commit()
    conn.close()

    mock_pg_conn = AsyncMock()
    mock_pg_conn.execute = AsyncMock()
    mock_pg_conn.executemany = AsyncMock()

    class MockPoolAcquireContext:
        async def __aenter__(self):
            return mock_pg_conn

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

    mock_pool = MagicMock()
    mock_pool.acquire.return_value = MockPoolAcquireContext()
    mock_pool.close = AsyncMock()

    with patch("asyncpg.create_pool", new_callable=AsyncMock) as mock_create_pool:
        mock_create_pool.return_value = mock_pool
        summary = await sync_sqlite_to_postgres(
            pg_url="postgresql://user:pass@localhost:5432/testdb",
            sqlite_path=db_path,
        )

        assert "congressional_filings" in summary
        assert summary["congressional_filings"]["synced_count"] == 1
        assert summary["congressional_transactions"]["synced_count"] == 1
        assert summary["macro_indicators"]["synced_count"] == 1
        assert summary["cboe_daily_options"]["synced_count"] == 1
        assert summary["finra_otc_volume"]["synced_count"] == 1
        assert summary["insider_trades"]["synced_count"] == 1
        assert summary["institutional_holdings"]["synced_count"] == 1


def test_cli_sync_pg_missing_url():
    """Test CLI exit code 1 when no PostgreSQL URL is provided."""
    with patch("harvester.cli.get_settings") as mock_settings:
        mock_settings.return_value.database_url = ""
        res = runner.invoke(app, ["sync-pg"])
        assert res.exit_code == 1
        assert "Target PostgreSQL connection string required" in res.stdout


def test_cli_sync_pg_success(tmp_path):
    """Test CLI execution when valid arguments and mock sync are provided."""
    fake_db = str(tmp_path / "empty.db")
    conn = sqlite3.connect(fake_db)
    conn.close()

    with patch("harvester.core.sync.sync_sqlite_to_postgres", new_callable=AsyncMock) as mock_sync:
        mock_sync.return_value = {
            "congressional_filings": {"sqlite_count": 10, "synced_count": 10, "duration_seconds": 0.15},
            "macro_indicators": {"sqlite_count": 100, "synced_count": 100, "duration_seconds": 0.25},
        }

        res = runner.invoke(app, [
            "sync-pg",
            "--pg-url", "postgresql://user:secret@localhost:5432/greeksview",
            "--sqlite-path", fake_db,
        ])
        assert res.exit_code == 0
        assert "Synchronization Results Summary" in res.stdout
        assert "congressional_filings" in res.stdout
        assert "macro_indicators" in res.stdout


def test_cli_sync_pg_table_filter_and_batch_size(tmp_path):
    """Test CLI execution with specific table filter and batch size."""
    fake_db = str(tmp_path / "test.db")
    conn = sqlite3.connect(fake_db)
    conn.close()

    with patch("harvester.core.sync.sync_sqlite_to_postgres", new_callable=AsyncMock) as mock_sync:
        mock_sync.return_value = {
            "congressional_filings": {"sqlite_count": 5, "synced_count": 5, "duration_seconds": 0.05},
        }

        res = runner.invoke(app, [
            "sync-pg",
            "--pg-url", "postgresql://user:pass@localhost:5432/test",
            "--sqlite-path", fake_db,
            "--table", "congressional_filings",
            "--batch-size", "500",
        ])
        assert res.exit_code == 0
        assert "congressional_filings" in res.stdout
        mock_sync.assert_called_once()
        call_kwargs = mock_sync.call_args.kwargs
        assert call_kwargs["target_tables"] == ["congressional_filings"]
        assert call_kwargs["batch_size"] == 500


def test_cli_sync_pg_failure(tmp_path):
    """Test CLI handling when sync_sqlite_to_postgres raises an error."""
    fake_db = str(tmp_path / "test.db")
    conn = sqlite3.connect(fake_db)
    conn.close()

    with patch("harvester.core.sync.sync_sqlite_to_postgres", new_callable=AsyncMock) as mock_sync:
        mock_sync.side_effect = RuntimeError("PostgreSQL connection refused")

        res = runner.invoke(app, [
            "sync-pg",
            "--pg-url", "postgresql://user:pass@localhost:5432/test",
            "--sqlite-path", fake_db,
        ])
        assert res.exit_code == 1
        assert "Sync Failed:" in res.stdout


@pytest.mark.asyncio
async def test_sync_sqlite_to_postgres_filters_and_invalid_rows(tmp_path):
    """Test table filtering, empty table handling, and skipping rows with invalid dates."""
    db_path = str(tmp_path / "filter_test.db")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Create empty table
    cur.execute("""
    CREATE TABLE congressional_filings (
        filing_id TEXT PRIMARY KEY, chamber TEXT, member_name TEXT, member_id TEXT,
        filing_year INTEGER, filing_date TEXT, doc_url TEXT, raw_text TEXT,
        sha256_hash TEXT, status TEXT
    )
    """)

    # Table with invalid date to test branch coverage
    cur.execute("""
    CREATE TABLE macro_indicators (
        id TEXT PRIMARY KEY, series_id TEXT, indicator_name TEXT, date TEXT,
        value REAL, frequency TEXT, units TEXT
    )
    """)
    cur.execute("INSERT INTO macro_indicators VALUES ('FEDFUNDS_bad', 'FEDFUNDS', 'Fed Funds', 'bad-date', 5.0, 'daily', 'Percent')")

    cur.execute("""
    CREATE TABLE congressional_transactions (
        filing_id TEXT, member_name TEXT, chamber TEXT, party TEXT, state TEXT, district TEXT,
        ticker TEXT, asset_description TEXT, asset_type TEXT, transaction_type TEXT,
        amount_bracket TEXT, amount_min REAL, amount_max REAL, transaction_date TEXT,
        filing_date TEXT, owner TEXT, comment TEXT
    )
    """)
    cur.execute("""
    INSERT INTO congressional_transactions VALUES (
        'F1', 'Rep', 'house', 'D', 'CA', '1', 'AAPL', 'Apple', 'stock', 'BUY', '$1k', NULL, NULL, 'invalid-date', 'invalid-date', 'self', NULL
    )
    """)

    cur.execute("""
    CREATE TABLE institutional_holdings (
        id TEXT PRIMARY KEY, cik TEXT, institution_name TEXT, report_calendar_or_quarter TEXT,
        symbol TEXT, cusip TEXT, shares REAL, market_value REAL, investment_discretion TEXT,
        voting_authority_sole REAL, sec_form TEXT, filing_url TEXT
    )
    """)
    cur.execute("INSERT INTO institutional_holdings VALUES ('ih_bad', '0001', 'Fund', 'invalid-qtr', 'AAPL', '037833100', NULL, NULL, 'SOLE', NULL, '13F-HR', 'url')")

    cur.execute("""
    CREATE TABLE insider_trades (
        id TEXT PRIMARY KEY, symbol TEXT, filing_date TEXT, transaction_date TEXT,
        reporting_owner TEXT, owner_title TEXT, is_director INTEGER, is_officer INTEGER,
        is_ten_percent INTEGER, transaction_type TEXT, shares REAL, price_per_share REAL,
        shares_owned_following REAL, sec_form TEXT, filing_url TEXT
    )
    """)
    cur.execute("INSERT INTO insider_trades VALUES ('it_bad', 'MSFT', 'invalid-date', NULL, 'Satya', 'CEO', 0, 0, 0, 'P', NULL, NULL, NULL, '4', 'url')")

    cur.execute("""
    CREATE TABLE cboe_daily_options (
        id TEXT PRIMARY KEY, trade_date TEXT, total_call_volume REAL, total_put_volume REAL,
        total_volume REAL, equity_pc_ratio REAL, index_pc_ratio REAL, total_pc_ratio REAL, vix_volume REAL
    )
    """)
    cur.execute("INSERT INTO cboe_daily_options VALUES ('cboe_bad', 'invalid-date', NULL, NULL, NULL, NULL, NULL, NULL, NULL)")

    cur.execute("""
    CREATE TABLE finra_otc_volume (
        id TEXT PRIMARY KEY, symbol TEXT, week_start_date TEXT, tier TEXT,
        otc_volume REAL, total_trades REAL, total_market_volume REAL, dark_pool_share_pct REAL
    )
    """)
    cur.execute("INSERT INTO finra_otc_volume VALUES ('finra_bad', 'AAPL', 'invalid-date', 'Tier 1', NULL, NULL, NULL, NULL)")

    conn.commit()
    conn.close()

    mock_pg_conn = AsyncMock()
    mock_pg_conn.execute = AsyncMock()
    mock_pg_conn.executemany = AsyncMock()

    class MockPoolAcquireContext:
        async def __aenter__(self):
            return mock_pg_conn

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

    mock_pool = MagicMock()
    mock_pool.acquire.return_value = MockPoolAcquireContext()
    mock_pool.close = AsyncMock()

    with patch("asyncpg.create_pool", new_callable=AsyncMock) as mock_create_pool:
        mock_create_pool.return_value = mock_pool

        # 1. Target table filtering: only run macro_indicators
        summary1 = await sync_sqlite_to_postgres(
            pg_url="postgresql://user:pass@localhost:5432/testdb",
            sqlite_path=db_path,
            target_tables=["macro_indicators"],
        )
        assert "macro_indicators" in summary1
        assert "congressional_filings" not in summary1
        # Row had bad date so 0 synced
        assert summary1["macro_indicators"]["synced_count"] == 0

        # 2. Run all: empty table (filings has 0 rows), invalid rows in other tables
        summary2 = await sync_sqlite_to_postgres(
            pg_url="postgresql://user:pass@localhost:5432/testdb",
            sqlite_path=db_path,
        )
        assert summary2["congressional_filings"]["sqlite_count"] == 0
        assert summary2["congressional_transactions"]["synced_count"] == 0
        assert summary2["institutional_holdings"]["synced_count"] == 0
        assert summary2["insider_trades"]["synced_count"] == 0
        assert summary2["cboe_daily_options"]["synced_count"] == 0
        assert summary2["finra_otc_volume"]["synced_count"] == 0

