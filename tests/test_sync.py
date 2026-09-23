"""Unit tests for the SQLite-to-PostgreSQL synchronization module."""

import sqlite3
from datetime import UTC, date, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import pytest
from typer.testing import CliRunner

from harvester.cli import app
from harvester.core.sync import (
    RETIRED_SYNC_TABLES,
    RetiredSyncTableError,
    _clean_str,
    _parse_date,
    _parse_datetime,
    sync_sqlite_to_postgres,
)

runner = CliRunner()

RETIRED_TABLES = ("cboe_daily_options", "finra_otc_volume")


def test_clean_str():
    """Test null byte (0x00) stripping for PostgreSQL UTF-8 compatibility."""
    assert _clean_str(None) is None
    assert _clean_str("clean string") == "clean string"
    assert _clean_str("null\x00byte\x00test") == "nullbytetest"
    assert _clean_str(123) == "123"


def test_parse_date():
    """Test date parsing helper across various string and object inputs."""
    assert _parse_date(None) is None
    assert _parse_date("") is None
    assert _parse_date(datetime(2024, 5, 15, 10, 0, 0)) == date(2024, 5, 15)
    assert _parse_date(date(2024, 5, 15)) == date(2024, 5, 15)
    assert _parse_date("2024-05-15") == date(2024, 5, 15)
    assert _parse_date("2024-05-15 10:30:00") == date(2024, 5, 15)
    assert _parse_date("05/15/2024") == date(2024, 5, 15)
    assert _parse_date("invalid-date") is None


def test_parse_datetime():
    """Test datetime parsing helper across various string and object inputs."""
    assert _parse_datetime(None) is None
    assert _parse_datetime("") is None
    dt = datetime(2026, 9, 16, 2, 30, 45)
    assert _parse_datetime(dt) == dt
    assert _parse_datetime(date(2026, 9, 16)) == datetime(2026, 9, 16, 0, 0, 0)
    assert _parse_datetime("2026-09-16 02:30:45") == dt
    assert _parse_datetime("2026-09-16T02:30:45") == dt
    assert _parse_datetime("2026-09-16") == datetime(2026, 9, 16, 0, 0, 0)
    assert _parse_datetime("invalid-datetime") is None


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
    cur.execute(
        "INSERT INTO congressional_filings VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "FILING_H1",
            "house",
            "Jane Doe",
            "D001",
            2024,
            "2024-05-15",
            "http://url",
            "text\x00with\x00nulls",
            "hash1",
            "parsed",
        ),
    )

    cur.execute("""
    CREATE TABLE congressional_transactions (
        filing_id TEXT, member_name TEXT, chamber TEXT, party TEXT, state TEXT, district TEXT,
        ticker TEXT, asset_description TEXT, asset_type TEXT, transaction_type TEXT,
        amount_bracket TEXT, amount_min REAL, amount_max REAL, transaction_date TEXT,
        filing_date TEXT, owner TEXT, comment TEXT
    )
    """)
    cur.execute(
        "INSERT INTO congressional_transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "FILING_H1",
            "Jane Doe",
            "house",
            "Democrat",
            "CA",
            "12",
            "NVDA",
            "NVIDIA\x00Corp\x00",
            "stock",
            "BUY",
            "$1,001 - $15,000",
            1001.0,
            15000.0,
            "2024-05-10",
            "2024-05-15",
            "self",
            "test\x00",
        ),
    )

    cur.execute("""
    CREATE TABLE macro_indicators (
        id TEXT PRIMARY KEY, series_id TEXT, indicator_name TEXT, date TEXT,
        value REAL, frequency TEXT, units TEXT
    )
    """)
    cur.execute(
        "INSERT INTO macro_indicators VALUES ('FEDFUNDS_2024-05-15', 'FEDFUNDS', 'Fed Funds', '2024-05-15', 5.33, 'daily', 'Percent')"
    )

    cur.execute("""
    CREATE TABLE cboe_daily_options (
        id TEXT PRIMARY KEY, trade_date TEXT, total_call_volume REAL, total_put_volume REAL,
        total_volume REAL, equity_pc_ratio REAL, index_pc_ratio REAL, total_pc_ratio REAL, vix_volume REAL
    )
    """)
    cur.execute(
        "INSERT INTO cboe_daily_options VALUES ('cboe_2024-05-15', '2024-05-15', 1000, 800, 1800, 0.65, 1.2, 0.8, 500)"
    )

    cur.execute("""
    CREATE TABLE finra_otc_volume (
        id TEXT PRIMARY KEY, symbol TEXT, week_start_date TEXT, tier TEXT,
        otc_volume REAL, total_trades REAL, total_market_volume REAL, dark_pool_share_pct REAL
    )
    """)
    cur.execute(
        "INSERT INTO finra_otc_volume VALUES ('finra_1', 'AAPL', '2024-05-13', 'Tier 1', 50000, 1200, 100000, 50.0)"
    )

    cur.execute("""
    CREATE TABLE insider_trades (
        id TEXT PRIMARY KEY, symbol TEXT, filing_date TEXT, transaction_date TEXT,
        reporting_owner TEXT, owner_title TEXT, is_director INTEGER, is_officer INTEGER,
        is_ten_percent INTEGER, transaction_type TEXT, shares REAL, price_per_share REAL,
        shares_owned_following REAL, sec_form TEXT, filing_url TEXT
    )
    """)
    cur.execute(
        "INSERT INTO insider_trades VALUES ('it_1', 'MSFT', '2024-05-15', '2024-05-14', 'Satya', 'CEO', 1, 1, 0, 'P', 100, 420.0, 5000, '4', 'url')"
    )

    cur.execute("""
    CREATE TABLE institutional_holdings (
        id TEXT PRIMARY KEY, cik TEXT, institution_name TEXT, report_calendar_or_quarter TEXT,
        symbol TEXT, cusip TEXT, shares REAL, market_value REAL, investment_discretion TEXT,
        voting_authority_sole REAL, sec_form TEXT, filing_url TEXT
    )
    """)
    cur.execute(
        "INSERT INTO institutional_holdings VALUES ('ih_1', '0001', 'Berkshire', '2024-03-31', 'AAPL', '037833100', 900000, 150000000.0, 'SOLE', 900000, '13F-HR', 'url')"
    )

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
        assert summary["insider_trades"]["synced_count"] == 1
        assert summary["institutional_holdings"]["synced_count"] == 1

        # The two retired tables are populated in this same SQLite file (one row each,
        # seeded above) and a default sync must copy neither: not in the summary, and
        # no statement naming them reached PostgreSQL.
        assert "cboe_daily_options" not in summary
        assert "finra_otc_volume" not in summary
        executed_sql = [c.args[0] for c in mock_pg_conn.executemany.call_args_list]
        assert executed_sql  # presence: the live tables did reach PostgreSQL
        assert any("congressional_filings" in sql for sql in executed_sql)
        assert not any("cboe_daily_options" in sql for sql in executed_sql)
        assert not any("finra_otc_volume" in sql for sql in executed_sql)

        # Verify no null bytes reached PostgreSQL executemany calls
        for call in mock_pg_conn.executemany.call_args_list:
            for row in call.args[1]:
                for val in row:
                    assert "\x00" not in str(val)


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

        res = runner.invoke(
            app,
            [
                "sync-pg",
                "--pg-url",
                "postgresql://user:secret@localhost:5432/greeksview",
                "--sqlite-path",
                fake_db,
            ],
        )
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

        res = runner.invoke(
            app,
            [
                "sync-pg",
                "--pg-url",
                "postgresql://user:pass@localhost:5432/test",
                "--sqlite-path",
                fake_db,
                "--table",
                "congressional_filings",
                "--batch-size",
                "500",
            ],
        )
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

        res = runner.invoke(
            app,
            [
                "sync-pg",
                "--pg-url",
                "postgresql://user:pass@localhost:5432/test",
                "--sqlite-path",
                fake_db,
            ],
        )
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
    cur.execute(
        "INSERT INTO macro_indicators VALUES ('FEDFUNDS_bad', 'FEDFUNDS', 'Fed Funds', 'bad-date', 5.0, 'daily', 'Percent')"
    )

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
    cur.execute(
        "INSERT INTO institutional_holdings VALUES ('ih_bad', '0001', 'Fund', 'invalid-qtr', 'AAPL', '037833100', NULL, NULL, 'SOLE', NULL, '13F-HR', 'url')"
    )

    cur.execute("""
    CREATE TABLE insider_trades (
        id TEXT PRIMARY KEY, symbol TEXT, filing_date TEXT, transaction_date TEXT,
        reporting_owner TEXT, owner_title TEXT, is_director INTEGER, is_officer INTEGER,
        is_ten_percent INTEGER, transaction_type TEXT, shares REAL, price_per_share REAL,
        shares_owned_following REAL, sec_form TEXT, filing_url TEXT
    )
    """)
    cur.execute(
        "INSERT INTO insider_trades VALUES ('it_bad', 'MSFT', 'invalid-date', NULL, 'Satya', 'CEO', 0, 0, 0, 'P', NULL, NULL, NULL, '4', 'url')"
    )

    cur.execute("""
    CREATE TABLE cboe_daily_options (
        id TEXT PRIMARY KEY, trade_date TEXT, total_call_volume REAL, total_put_volume REAL,
        total_volume REAL, equity_pc_ratio REAL, index_pc_ratio REAL, total_pc_ratio REAL, vix_volume REAL
    )
    """)
    cur.execute(
        "INSERT INTO cboe_daily_options VALUES ('cboe_bad', 'invalid-date', NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
    )

    cur.execute("""
    CREATE TABLE finra_otc_volume (
        id TEXT PRIMARY KEY, symbol TEXT, week_start_date TEXT, tier TEXT,
        otc_volume REAL, total_trades REAL, total_market_volume REAL, dark_pool_share_pct REAL
    )
    """)
    cur.execute(
        "INSERT INTO finra_otc_volume VALUES ('finra_bad', 'AAPL', 'invalid-date', 'Tier 1', NULL, NULL, NULL, NULL)"
    )

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
        # Both retired tables hold a row here and are still skipped entirely.
        assert "cboe_daily_options" not in summary2
        assert "finra_otc_volume" not in summary2


@pytest.mark.asyncio
async def test_sync_alphavantage_tables(tmp_path):
    """Test sync for all 8 Alpha Vantage persistence tables."""
    db_path = str(tmp_path / "av_sync_test.db")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE stock_bars_daily (
        symbol TEXT, trade_date TEXT, open REAL, high REAL, low REAL, close REAL,
        adjusted_close REAL, volume INTEGER, dividend_amount REAL, split_coefficient REAL
    )
    """)
    cur.execute(
        "INSERT INTO stock_bars_daily VALUES ('SPY', '2024-06-14', 450.0, 455.0, 448.0, 452.0, 452.0, 50000000, 0.0, 1.0)"
    )
    cur.execute(
        "INSERT INTO stock_bars_daily VALUES ('SPY', 'bad-date', 450.0, 455.0, 448.0, 452.0, 452.0, 50000000, 0.0, 1.0)"
    )

    cur.execute("""
    CREATE TABLE stock_bars_intraday (
        symbol TEXT, bar_timestamp TEXT, interval TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER
    )
    """)
    cur.execute(
        "INSERT INTO stock_bars_intraday VALUES ('SPY', '2024-06-14 16:00:00', '5min', 450.0, 451.0, 449.5, 450.5, 15000)"
    )
    cur.execute(
        "INSERT INTO stock_bars_intraday VALUES ('SPY', 'bad-timestamp', '5min', 450.0, 451.0, 449.5, 450.5, 15000)"
    )

    cur.execute("""
    CREATE TABLE options_chains_eod (
        contract_id TEXT, symbol TEXT, trade_date TEXT, expiration TEXT, strike REAL, option_type TEXT,
        last_price REAL, mark_price REAL, bid REAL, ask REAL, volume INTEGER, open_interest INTEGER,
        implied_volatility REAL, delta REAL, gamma REAL, theta REAL, vega REAL, rho REAL
    )
    """)
    cur.execute(
        "INSERT INTO options_chains_eod VALUES ('C1', 'SPY', '2024-06-14', '2024-07-19', 450.0, 'call', 5.0, 5.0, 4.9, 5.1, 100, 500, 0.18, 0.5, 0.03, -0.04, 0.12, 0.05)"
    )
    cur.execute(
        "INSERT INTO options_chains_eod VALUES ('C2', 'SPY', 'bad-date', 'bad-date', 450.0, 'call', 5.0, 5.0, 4.9, 5.1, 100, 500, 0.18, 0.5, 0.03, -0.04, 0.12, 0.05)"
    )

    cur.execute("""
    CREATE TABLE company_fundamentals (
        symbol TEXT, fiscal_date_ending TEXT, report_type TEXT, period_type TEXT, data_json TEXT
    )
    """)
    cur.execute(
        "INSERT INTO company_fundamentals VALUES ('AAPL', '2024-03-31', 'OVERVIEW', 'annual', '{\"Symbol\":\"AAPL\"}')"
    )

    cur.execute("""
    CREATE TABLE corporate_dividends (
        symbol TEXT, ex_dividend_date TEXT, declaration_date TEXT, record_date TEXT, payment_date TEXT, amount REAL
    )
    """)
    cur.execute(
        "INSERT INTO corporate_dividends VALUES ('AAPL', '2024-05-10', '2024-05-01', '2024-05-13', '2024-05-16', 0.25)"
    )
    cur.execute("INSERT INTO corporate_dividends VALUES ('AAPL', 'bad-date', NULL, NULL, NULL, 0.25)")

    cur.execute("""
    CREATE TABLE corporate_splits (
        symbol TEXT, effective_date TEXT, split_factor REAL
    )
    """)
    cur.execute("INSERT INTO corporate_splits VALUES ('AAPL', '2020-08-31', 4.0)")
    cur.execute("INSERT INTO corporate_splits VALUES ('AAPL', 'bad-date', 4.0)")

    cur.execute("""
    CREATE TABLE etf_profiles (
        symbol TEXT, net_assets REAL, portfolio_turnover REAL, dividend_yield REAL, expense_ratio REAL, holdings_json TEXT, sectors_json TEXT
    )
    """)
    cur.execute("INSERT INTO etf_profiles VALUES ('SPY', 500000000.0, 0.02, 0.015, 0.0009, '[]', '[]')")

    cur.execute("""
    CREATE TABLE listing_status (
        symbol TEXT, name TEXT, exchange TEXT, asset_type TEXT, ipo_date TEXT, delisting_date TEXT, status TEXT
    )
    """)
    cur.execute(
        "INSERT INTO listing_status VALUES ('SPY', 'SPDR S&P 500', 'NYSE', 'ETF', '1993-01-22', NULL, 'Active')"
    )

    conn.commit()
    conn.close()

    mock_pg_conn = AsyncMock()
    mock_pg_conn.execute = AsyncMock()
    mock_pg_conn.executemany = AsyncMock()

    class MockPoolCtx:
        async def __aenter__(self):
            return mock_pg_conn

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

    mock_pool = MagicMock()
    mock_pool.acquire.return_value = MockPoolCtx()
    mock_pool.close = AsyncMock()

    with patch("asyncpg.create_pool", new_callable=AsyncMock) as mock_create_pool:
        mock_create_pool.return_value = mock_pool

        summary = await sync_sqlite_to_postgres(
            pg_url="postgresql://user:pass@localhost:5432/testdb",
            sqlite_path=db_path,
            target_tables=[
                "stock_bars_daily",
                "stock_bars_intraday",
                "options_chains_eod",
                "company_fundamentals",
                "corporate_dividends",
                "corporate_splits",
                "etf_profiles",
                "listing_status",
            ],
        )

        assert summary["stock_bars_daily"]["synced_count"] == 1
        assert summary["stock_bars_intraday"]["synced_count"] == 1
        assert summary["options_chains_eod"]["synced_count"] == 1
        assert summary["company_fundamentals"]["synced_count"] == 1
        assert summary["corporate_dividends"]["synced_count"] == 1
        assert summary["corporate_splits"]["synced_count"] == 1
        assert summary["etf_profiles"]["synced_count"] == 1
        assert summary["listing_status"]["synced_count"] == 1

        # Verify stock_bars_intraday passes a real datetime.datetime instance (not str)
        intraday_call = next(
            c.args[1] for c in mock_pg_conn.executemany.call_args_list if "stock_bars_intraday" in c.args[0]
        )
        assert len(intraday_call) == 1
        assert isinstance(intraday_call[0][1], datetime)
        assert not isinstance(intraday_call[0][1], str)


@pytest.mark.asyncio
async def test_sync_sqlite_to_postgres_days_back_filtering(tmp_path):
    """Test sync_sqlite_to_postgres with --days-back filtering on trade_date tables."""
    db_path = str(tmp_path / "days_back_test.db")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    today = datetime.now(UTC).date()
    recent_date = (today - timedelta(days=2)).isoformat()
    old_date = (today - timedelta(days=120)).isoformat()

    cur.execute("""
    CREATE TABLE stock_bars_daily (
        symbol TEXT, trade_date TEXT, open REAL, high REAL, low REAL, close REAL,
        adjusted_close REAL, volume INTEGER, dividend_amount REAL, split_coefficient REAL
    )
    """)
    cur.execute(
        "INSERT INTO stock_bars_daily VALUES ('SPY', ?, 450.0, 455.0, 448.0, 452.0, 452.0, 50000000, 0.0, 1.0)",
        (recent_date,),
    )
    cur.execute(
        "INSERT INTO stock_bars_daily VALUES ('SPY', ?, 400.0, 405.0, 398.0, 402.0, 402.0, 40000000, 0.0, 1.0)",
        (old_date,),
    )

    cur.execute("""
    CREATE TABLE options_chains_eod (
        contract_id TEXT, symbol TEXT, trade_date TEXT, expiration TEXT, strike REAL, option_type TEXT,
        last_price REAL, mark_price REAL, bid REAL, ask REAL, volume INTEGER, open_interest INTEGER,
        implied_volatility REAL, delta REAL, gamma REAL, theta REAL, vega REAL, rho REAL
    )
    """)
    cur.execute(
        """
    INSERT INTO options_chains_eod VALUES (
        'C_recent', 'SPY', ?, '2026-12-18', 450.0, 'call', 5.0, 5.0, 4.9, 5.1,
        100, 500, 0.18, 0.5, 0.03, -0.04, 0.12, 0.05
    )
    """,
        (recent_date,),
    )
    cur.execute(
        """
    INSERT INTO options_chains_eod VALUES (
        'C_old', 'SPY', ?, '2025-12-18', 400.0, 'call', 4.0, 4.0, 3.9, 4.1,
        50, 200, 0.22, 0.4, 0.02, -0.03, 0.10, 0.04
    )
    """,
        (old_date,),
    )

    cur.execute("""
    CREATE TABLE stock_bars_intraday (
        symbol TEXT, bar_timestamp TEXT, interval TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER
    )
    """)
    recent_ts = f"{recent_date} 15:30:00"
    old_ts = f"{old_date} 15:30:00"
    cur.execute(
        "INSERT INTO stock_bars_intraday VALUES ('SPY', ?, '5min', 450.0, 451.0, 449.5, 450.5, 15000)", (recent_ts,)
    )
    cur.execute(
        "INSERT INTO stock_bars_intraday VALUES ('SPY', ?, '5min', 400.0, 401.0, 399.5, 400.5, 12000)", (old_ts,)
    )

    conn.commit()
    conn.close()

    mock_pg_conn = AsyncMock()
    mock_pg_conn.execute = AsyncMock()
    mock_pg_conn.executemany = AsyncMock()

    class MockPoolCtx:
        async def __aenter__(self):
            return mock_pg_conn

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

    mock_pool = MagicMock()
    mock_pool.acquire.return_value = MockPoolCtx()
    mock_pool.close = AsyncMock()

    with patch("asyncpg.create_pool", new_callable=AsyncMock) as mock_create_pool:
        mock_create_pool.return_value = mock_pool

        # 1. Test with days_back=30: only recent_date (2 days ago) should be synced
        summary_filtered = await sync_sqlite_to_postgres(
            pg_url="postgresql://user:pass@localhost:5432/testdb",
            sqlite_path=db_path,
            target_tables=["stock_bars_daily", "options_chains_eod", "stock_bars_intraday"],
            days_back=30,
        )

        assert summary_filtered["stock_bars_daily"]["sqlite_count"] == 1
        assert summary_filtered["stock_bars_daily"]["synced_count"] == 1
        assert summary_filtered["options_chains_eod"]["sqlite_count"] == 1
        assert summary_filtered["options_chains_eod"]["synced_count"] == 1
        assert summary_filtered["stock_bars_intraday"]["sqlite_count"] == 1
        assert summary_filtered["stock_bars_intraday"]["synced_count"] == 1

        # Check that executed batches contain only recent records
        exec_calls = mock_pg_conn.executemany.call_args_list
        options_batch = next(c.args[1] for c in exec_calls if "options_chains_eod" in c.args[0])
        assert len(options_batch) == 1
        assert options_batch[0][0] == "C_recent"

        intraday_batch = next(c.args[1] for c in exec_calls if "stock_bars_intraday" in c.args[0])
        assert len(intraday_batch) == 1
        assert isinstance(intraday_batch[0][1], datetime)

        # 2. Test with days_back=None: all records should be synced
        mock_pg_conn.executemany.reset_mock()
        summary_all = await sync_sqlite_to_postgres(
            pg_url="postgresql://user:pass@localhost:5432/testdb",
            sqlite_path=db_path,
            target_tables=["stock_bars_daily", "options_chains_eod", "stock_bars_intraday"],
            days_back=None,
        )

        assert summary_all["stock_bars_daily"]["sqlite_count"] == 2
        assert summary_all["stock_bars_daily"]["synced_count"] == 2
        assert summary_all["options_chains_eod"]["sqlite_count"] == 2
        assert summary_all["options_chains_eod"]["synced_count"] == 2
        assert summary_all["stock_bars_intraday"]["sqlite_count"] == 2
        assert summary_all["stock_bars_intraday"]["synced_count"] == 2


def test_cli_sync_pg_days_back(tmp_path):
    """Test CLI execution with --days-back option."""
    fake_db = str(tmp_path / "test.db")
    conn = sqlite3.connect(fake_db)
    conn.close()

    with patch("harvester.core.sync.sync_sqlite_to_postgres", new_callable=AsyncMock) as mock_sync:
        mock_sync.return_value = {
            "options_chains_eod": {"sqlite_count": 50, "synced_count": 50, "duration_seconds": 0.12},
        }

        res = runner.invoke(
            app,
            [
                "sync-pg",
                "--pg-url",
                "postgresql://user:pass@localhost:5432/test",
                "--sqlite-path",
                fake_db,
                "--table",
                "options_chains_eod",
                "--days-back",
                "90",
            ],
        )
        assert res.exit_code == 0
        assert "options_chains_eod" in res.stdout
        assert "Days Back:" in res.stdout
        assert "90" in res.stdout
        mock_sync.assert_called_once()
        assert mock_sync.call_args.kwargs["days_back"] == 90


@pytest.mark.asyncio
async def test_sync_sqlite_to_postgres_insufficient_privilege(tmp_path):
    """Verify that sync_sqlite_to_postgres gracefully handles InsufficientPrivilegeError on DDL."""
    db_path = str(tmp_path / "test_empty.db")
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE congressional_filings (filing_id TEXT PRIMARY KEY, chamber TEXT, member_name TEXT, member_id TEXT, filing_year INTEGER, filing_date TEXT, doc_url TEXT, raw_text TEXT, sha256_hash TEXT, status TEXT)"
    )
    conn.commit()
    conn.close()

    mock_pg_conn = AsyncMock()

    async def mock_execute(query: str, *args: object) -> str:
        if "CREATE SCHEMA" in query or "CREATE TABLE" in query:
            raise asyncpg.exceptions.InsufficientPrivilegeError("permission denied for database postgres")
        return "OK"

    mock_pg_conn.execute.side_effect = mock_execute
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


def _seed_retired_and_live_tables(db_path: str) -> None:
    """Create a throwaway SQLite file holding a row in each retired table and a live one."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE macro_indicators (
        id TEXT PRIMARY KEY, series_id TEXT, indicator_name TEXT, date TEXT,
        value REAL, frequency TEXT, units TEXT
    )
    """)
    cur.execute(
        "INSERT INTO macro_indicators VALUES ('FAKESERIES_2024-05-15', 'FAKESERIES', 'Test Rate', '2024-05-15', 1.23, 'daily', 'Percent')"
    )
    cur.execute("""
    CREATE TABLE cboe_daily_options (
        id TEXT PRIMARY KEY, trade_date TEXT, total_call_volume REAL, total_put_volume REAL,
        total_volume REAL, equity_pc_ratio REAL, index_pc_ratio REAL, total_pc_ratio REAL, vix_volume REAL
    )
    """)
    cur.execute(
        "INSERT INTO cboe_daily_options VALUES ('cboe_2024-05-15', '2024-05-15', 11, 22, 33, 0.44, 0.55, 0.66, 77)"
    )
    cur.execute("""
    CREATE TABLE finra_otc_volume (
        id TEXT PRIMARY KEY, symbol TEXT, week_start_date TEXT, tier TEXT,
        otc_volume REAL, total_trades REAL, total_market_volume REAL, dark_pool_share_pct REAL
    )
    """)
    cur.execute("INSERT INTO finra_otc_volume VALUES ('finra_1', 'ZZTESTCO', '2024-05-13', 'Tier 1', 11, 22, 33, 44.0)")
    conn.commit()
    conn.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("table", RETIRED_TABLES)
async def test_sync_refuses_a_retired_table_asked_for_by_name(tmp_path, table):
    """Naming a retired table refuses with a reason, and never opens a database to do it."""
    db_path = str(tmp_path / "retired_by_name.db")
    _seed_retired_and_live_tables(db_path)

    with patch("asyncpg.create_pool", new_callable=AsyncMock) as mock_create_pool:
        with pytest.raises(RetiredSyncTableError) as exc:
            await sync_sqlite_to_postgres(
                pg_url="postgresql://user:pass@localhost:5432/testdb",
                sqlite_path=db_path,
                target_tables=[table],
            )
        # Refusing must not be a silent no-op, and must not dial PostgreSQL.
        mock_create_pool.assert_not_called()

    message = str(exc.value)
    assert table in message
    assert "Refusing to sync" in message
    assert RETIRED_SYNC_TABLES[table] in message


@pytest.mark.asyncio
async def test_sync_refuses_when_a_retired_table_rides_along_with_a_live_one(tmp_path):
    """One retired name in the list refuses the whole call rather than silently dropping it."""
    db_path = str(tmp_path / "retired_mixed.db")
    _seed_retired_and_live_tables(db_path)

    with patch("asyncpg.create_pool", new_callable=AsyncMock) as mock_create_pool:
        with pytest.raises(RetiredSyncTableError) as exc:
            await sync_sqlite_to_postgres(
                pg_url="postgresql://user:pass@localhost:5432/testdb",
                sqlite_path=db_path,
                target_tables=["macro_indicators", "finra_otc_volume"],
            )
        mock_create_pool.assert_not_called()
    assert "finra_otc_volume" in str(exc.value)


@pytest.mark.asyncio
async def test_default_sync_copies_a_live_table_and_neither_retired_one(tmp_path):
    """A default sync-pg over a file holding all three tables copies only the live one."""
    db_path = str(tmp_path / "default_sync.db")
    _seed_retired_and_live_tables(db_path)

    mock_pg_conn = AsyncMock()
    mock_pg_conn.execute = AsyncMock()
    mock_pg_conn.executemany = AsyncMock()

    class MockPoolCtx:
        async def __aenter__(self):
            return mock_pg_conn

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

    mock_pool = MagicMock()
    mock_pool.acquire.return_value = MockPoolCtx()
    mock_pool.close = AsyncMock()

    with patch("asyncpg.create_pool", new_callable=AsyncMock) as mock_create_pool:
        mock_create_pool.return_value = mock_pool
        summary = await sync_sqlite_to_postgres(
            pg_url="postgresql://user:pass@localhost:5432/testdb",
            sqlite_path=db_path,
        )

    # Presence: the live table was read and written.
    assert summary["macro_indicators"]["sqlite_count"] == 1
    assert summary["macro_indicators"]["synced_count"] == 1
    # Absence: neither retired table was reported or written, though both hold a row.
    executed_sql = [c.args[0] for c in mock_pg_conn.executemany.call_args_list]
    assert any("macro_indicators" in sql for sql in executed_sql)
    for table in RETIRED_TABLES:
        assert table not in summary
        assert not any(table in sql for sql in executed_sql)


@pytest.mark.parametrize("table", RETIRED_TABLES)
def test_cli_sync_pg_refuses_a_retired_table(tmp_path, table):
    """`sync-pg --table <retired>` exits 1 with the reason; the real sync function is called."""
    db_path = str(tmp_path / "cli_retired.db")
    _seed_retired_and_live_tables(db_path)

    with patch("asyncpg.create_pool", new_callable=AsyncMock) as mock_create_pool:
        res = runner.invoke(
            app,
            [
                "sync-pg",
                "--pg-url",
                "postgresql://user:pass@localhost:5432/test",
                "--sqlite-path",
                db_path,
                "--table",
                table,
            ],
        )
        mock_create_pool.assert_not_called()

    assert res.exit_code == 1
    assert "Sync Failed:" in res.stdout
    assert "Refusing to sync" in " ".join(res.stdout.split())
    assert "Synchronization Results Summary" not in res.stdout
