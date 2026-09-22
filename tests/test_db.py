"""Unit and integration tests for async database persistence and idempotency."""

from datetime import date, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import pytest

from harvester.config import Settings
from harvester.core.db import DatabaseManager, to_pg_date
from harvester.core.models import (
    CongressionalFiling,
    CongressionalTransaction,
    FilingStatus,
    OwnerType,
    TransactionType,
)


@pytest.mark.asyncio
async def test_sqlite_lifecycle_and_idempotency(test_db: DatabaseManager) -> None:
    # 1. Empty get_existing_filing_ids
    existing = await test_db.get_existing_filing_ids([])
    assert existing == set()

    # 2. Check non-existent IDs
    existing = await test_db.get_existing_filing_ids(["FILING_1", "FILING_2"])
    assert existing == set()

    # 3. Upsert filing
    f1 = CongressionalFiling(
        filing_id="FILING_1",
        chamber="house",
        member_name="Nancy Pelosi",
        filing_year=2024,
        filing_date=date(2024, 1, 15),
        sha256_hash="hash1",
        status=FilingStatus.PARSED,
    )
    await test_db.upsert_filing(f1)

    # Verify existing IDs now reports FILING_1
    existing = await test_db.get_existing_filing_ids(["FILING_1", "FILING_2"])
    assert existing == {"FILING_1"}

    # Re-upsert same filing (updates status/raw_text without error)
    f1.raw_text = "Updated text"
    await test_db.upsert_filing(f1)

    # 4. Insert transactions
    tx1 = CongressionalTransaction(
        filing_id="FILING_1",
        member_name="Nancy Pelosi",
        ticker="NVDA",
        asset_description="NVIDIA Corp",
        transaction_type=TransactionType.BUY,
        amount_bracket="$1,000,001 - $5,000,000",
        amount_min=1000001.0,
        amount_max=5000000.0,
        transaction_date=date(2024, 1, 10),
        filing_date=date(2024, 1, 15),
        owner=OwnerType.SPOUSE,
    )
    tx2 = CongressionalTransaction(
        filing_id="FILING_1",
        member_name="Nancy Pelosi",
        ticker="AAPL",
        asset_description="Apple Inc",
        transaction_type=TransactionType.SALE_FULL,
        amount_bracket="$250,001 - $500,000",
        amount_min=250001.0,
        amount_max=500000.0,
        transaction_date=date(2024, 1, 8),
        filing_date=date(2024, 1, 15),
        owner=OwnerType.SELF,
    )

    # Empty list
    count_empty = await test_db.insert_transactions([])
    assert count_empty == 0

    # Insert 2 transactions
    count1 = await test_db.insert_transactions([tx1, tx2])
    assert count1 == 2

    # Re-inserting exact duplicates is strictly idempotent (0 new rows)
    count2 = await test_db.insert_transactions([tx1, tx2])
    assert count2 == 0

    # 5. Stats
    stats = await test_db.get_stats()
    assert stats["total_filings"] == 1
    assert stats["total_transactions"] == 2
    assert stats["distinct_tickers"] == 2
    assert stats["by_type"]["BUY"] == 1
    assert stats["by_type"]["SALE_FULL"] == 1


@pytest.mark.asyncio
async def test_postgres_mode_mocked() -> None:
    settings = Settings(database_url="postgresql://user:pass@localhost:5432/db", pgssl="false")
    db = DatabaseManager(settings=settings)

    mock_pool = MagicMock()
    mock_conn = AsyncMock()

    class MockAcquireContext:
        async def __aenter__(self) -> AsyncMock:
            return mock_conn

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
            pass

    mock_pool.acquire.return_value = MockAcquireContext()
    mock_pool.close = AsyncMock()

    with patch("asyncpg.create_pool", new_callable=AsyncMock) as mock_create_pool:
        mock_create_pool.return_value = mock_pool

        await db.connect()
        assert mock_conn.execute.called

        # 1. get_existing_filing_ids
        mock_conn.fetch.return_value = [{"filing_id": "FILING_1"}]
        existing = await db.get_existing_filing_ids(["FILING_1", "FILING_2"])
        assert existing == {"FILING_1"}

        # 2. upsert_filing
        f1 = CongressionalFiling(
            filing_id="FILING_1",
            chamber="house",
            member_name="Nancy Pelosi",
            filing_year=2024,
            filing_date=date(2024, 1, 15),
            sha256_hash="hash1",
        )
        await db.upsert_filing(f1)

        # 3. insert_transactions
        tx1 = CongressionalTransaction(
            filing_id="FILING_1",
            member_name="Nancy Pelosi",
            ticker="NVDA",
            asset_description="NVIDIA Corp",
            transaction_type=TransactionType.BUY,
            amount_bracket="$1,000,001 - $5,000,000",
            amount_min=1000001.0,
            transaction_date=date(2024, 1, 10),
            filing_date=date(2024, 1, 15),
        )
        mock_conn.execute.return_value = "INSERT 0 1"
        ins_count = await db.insert_transactions([tx1])
        assert ins_count == 1

        # Conflict duplicate insert
        mock_conn.execute.return_value = "INSERT 0 0"
        dup_count = await db.insert_transactions([tx1])
        assert dup_count == 0

        # 4. get_stats
        mock_conn.fetchval.side_effect = [1, 1, 1, 0, 0, 0, 0, 0, 0]
        mock_conn.fetch.side_effect = [
            [{"transaction_type": "BUY", "count": 1}],
            [{"chamber": "house", "count": 1}],
        ]
        stats = await db.get_stats()
        assert stats["total_filings"] == 1
        assert stats["total_transactions"] == 1
        assert stats["by_type"]["BUY"] == 1
        assert stats["by_chamber"]["house"] == 1
        assert stats["options_chains"] == 0
        # 5. Harvester tables in Postgres mode (verify string dates converted to date objects)
        mock_conn.execute.return_value = "INSERT 0 1"
        assert (
            await db.upsert_insider_trades(
                [
                    {
                        "id": "it_1",
                        "symbol": "NVDA",
                        "filing_date": "2026-09-10",
                        "reporting_owner": "Jensen",
                        "transaction_type": "Sale",
                    }
                ]
            )
            == 1
        )
        assert mock_conn.execute.call_args[0][3] == date(2026, 9, 10)
        assert await db.upsert_insider_trades([]) == 0
        assert await db.upsert_insider_trades([{"id": "it_bad", "symbol": "NVDA", "filing_date": "invalid"}]) == 0

        assert (
            await db.upsert_institutional_holdings(
                [
                    {
                        "id": "ih_1",
                        "cik": "0001067983",
                        "institution_name": "Berkshire",
                        "report_calendar_or_quarter": "2026-06-30",
                        "symbol": "AAPL",
                        "shares": 100,
                    }
                ]
            )
            == 1
        )
        assert mock_conn.execute.call_args[0][4] == date(2026, 6, 30)
        assert await db.upsert_institutional_holdings([]) == 0
        assert (
            await db.upsert_institutional_holdings(
                [
                    {
                        "id": "ih_bad",
                        "cik": "1",
                        "institution_name": "b",
                        "report_calendar_or_quarter": "invalid",
                        "symbol": "A",
                    }
                ]
            )
            == 0
        )

        assert (
            await db.upsert_finra_otc_volume(
                [
                    {
                        "id": "otc_1",
                        "symbol": "NVDA",
                        "week_start_date": "2026-09-07",
                        "tier": "Tier 1",
                        "otc_volume": 1000,
                        "total_trades": 50,
                    }
                ]
            )
            == 1
        )
        assert mock_conn.execute.call_args[0][3] == date(2026, 9, 7)
        assert await db.upsert_finra_otc_volume([]) == 0
        assert (
            await db.upsert_finra_otc_volume(
                [{"id": "otc_bad", "symbol": "NVDA", "week_start_date": "invalid", "tier": "t"}]
            )
            == 0
        )

        assert (
            await db.upsert_cboe_daily_options(
                [
                    {
                        "id": "cboe_1",
                        "trade_date": "2026-09-11",
                        "total_call_volume": 100,
                        "total_put_volume": 80,
                        "total_volume": 180,
                    }
                ]
            )
            == 1
        )
        assert mock_conn.execute.call_args[0][2] == date(2026, 9, 11)
        assert await db.upsert_cboe_daily_options([]) == 0
        assert await db.upsert_cboe_daily_options([{"id": "cboe_bad", "trade_date": "invalid"}]) == 0

        assert (
            await db.upsert_macro_indicators(
                [{"id": "m_1", "series_id": "DGS10", "indicator_name": "10Y", "date": "2026-09-11", "value": 4.25}]
            )
            == 1
        )
        assert mock_conn.execute.call_args[0][4] == date(2026, 9, 11)
        assert await db.upsert_macro_indicators([]) == 0
        assert (
            await db.upsert_macro_indicators(
                [{"id": "m_bad", "series_id": "DGS10", "indicator_name": "10Y", "date": "invalid", "value": 1.0}]
            )
            == 0
        )

        # Stock bars daily
        assert (
            await db.upsert_stock_bars_daily(
                [
                    {
                        "symbol": "NVDA",
                        "trade_date": "2026-09-10",
                        "open": 100.0,
                        "high": 105.0,
                        "low": 99.0,
                        "close": 104.0,
                        "adjusted_close": 104.0,
                        "volume": 1000000,
                    }
                ]
            )
            == 1
        )
        assert mock_conn.execute.call_args[0][2] == date(2026, 9, 10)
        assert await db.upsert_stock_bars_daily([]) == 0
        assert await db.upsert_stock_bars_daily([{"symbol": "NVDA", "trade_date": "invalid"}]) == 0

        # Options chains EOD
        assert (
            await db.upsert_options_chains_eod(
                [
                    {
                        "contract_id": "NVDA260918C00100000",
                        "symbol": "NVDA",
                        "trade_date": "2026-09-10",
                        "expiration": "2026-09-18",
                        "strike": 100.0,
                        "option_type": "call",
                        "close": 5.0,
                    }
                ]
            )
            == 1
        )
        assert mock_conn.execute.call_args[0][3] == date(2026, 9, 10)
        assert mock_conn.execute.call_args[0][4] == date(2026, 9, 18)
        assert await db.upsert_options_chains_eod([]) == 0
        assert (
            await db.upsert_options_chains_eod(
                [{"contract_id": "x", "symbol": "NVDA", "trade_date": "invalid", "expiration": "2026-09-18"}]
            )
            == 0
        )

        # Corporate dividends
        assert (
            await db.upsert_corporate_dividends(
                [{"symbol": "NVDA", "ex_dividend_date": "2026-09-10", "declaration_date": "2026-08-20", "amount": 0.04}]
            )
            == 1
        )
        assert mock_conn.execute.call_args[0][2] == date(2026, 9, 10)
        assert mock_conn.execute.call_args[0][3] == date(2026, 8, 20)
        assert await db.upsert_corporate_dividends([]) == 0
        assert (
            await db.upsert_corporate_dividends([{"symbol": "NVDA", "ex_dividend_date": "invalid", "amount": 0.04}])
            == 0
        )

        # Corporate splits
        assert (
            await db.upsert_corporate_splits([{"symbol": "NVDA", "effective_date": "2026-09-10", "split_factor": 10.0}])
            == 1
        )
        assert mock_conn.execute.call_args[0][2] == date(2026, 9, 10)
        assert await db.upsert_corporate_splits([]) == 0
        assert (
            await db.upsert_corporate_splits([{"symbol": "NVDA", "effective_date": "invalid", "split_factor": 10.0}])
            == 0
        )

        # Listing status
        assert (
            await db.upsert_listing_status(
                [{"symbol": "NVDA", "name": "NVIDIA", "ipo_date": "1999-01-22", "delisting_date": "2030-01-01"}]
            )
            == 1
        )
        assert mock_conn.execute.call_args[0][5] == date(1999, 1, 22)
        assert mock_conn.execute.call_args[0][6] == date(2030, 1, 1)
        assert await db.upsert_listing_status([]) == 0

        # execute, fetch, fetchval in Postgres mode
        mock_conn.fetch.side_effect = None
        mock_conn.fetchval.side_effect = None
        mock_conn.fetch.return_value = [{"col": 1}]
        mock_conn.fetchval.return_value = 42
        assert await db.execute("SELECT 1") == "INSERT 0 1"
        assert await db.fetch("SELECT 1") == [{"col": 1}]
        assert await db.fetchval("SELECT 42") == 42

        # Storage optimization methods in Postgres mode
        mock_conn.fetchval.return_value = 450.5
        assert await db.get_stock_close("SPY", "2026-09-14") == 450.5
        assert mock_conn.fetchval.call_args[0][2] == date(2026, 9, 14)
        assert await db.get_stock_close("SPY", "invalid-date") is None

        mock_conn.execute.return_value = "DELETE 5"
        assert await db.prune_options_chains_older_than(30, symbol="SPY") == 5
        assert isinstance(mock_conn.execute.call_args[0][2], date)
        assert await db.prune_options_chains_older_than(30) == 5
        assert isinstance(mock_conn.execute.call_args[0][1], date)

        await db.close()
        assert mock_pool.close.called


@pytest.mark.asyncio
async def test_harvester_tables_sqlite(test_db: DatabaseManager) -> None:
    """Test all 5 harvester feed upsert methods in SQLite mode."""
    await test_db.initialize_tables()

    # 1. Insider Trades
    assert await test_db.upsert_insider_trades([]) == 0
    it_record = {
        "id": "it_test_1",
        "symbol": "NVDA",
        "filing_date": "2026-09-10",
        "transaction_date": "2026-09-08",
        "reporting_owner": "Jensen Huang",
        "owner_title": "CEO",
        "is_director": True,
        "is_officer": True,
        "is_ten_percent": False,
        "transaction_type": "Sale",
        "shares": 50000.0,
        "price_per_share": 125.5,
        "shares_owned_following": 1000000.0,
        "sec_form": "4",
        "filing_url": "https://sec.gov/form4.xml",
    }
    assert await test_db.upsert_insider_trades([it_record]) == 1
    # Update on conflict
    it_record["shares"] = 60000.0
    assert await test_db.upsert_insider_trades([it_record]) == 1
    row = await test_db.fetchval("SELECT shares FROM insider_trades WHERE id = ?", "it_test_1")
    assert row == 60000.0

    # 2. Institutional Holdings
    assert await test_db.upsert_institutional_holdings([]) == 0
    ih_record = {
        "id": "ih_test_1",
        "cik": "0001067983",
        "institution_name": "BERKSHIRE HATHAWAY INC",
        "report_calendar_or_quarter": "2026-06-30",
        "symbol": "AAPL",
        "cusip": "037833100",
        "shares": 400000000.0,
        "market_value": 88000000000.0,
        "investment_discretion": "SOLE",
        "voting_authority_sole": 400000000.0,
        "sec_form": "13F-HR",
        "filing_url": "https://sec.gov/13f.xml",
    }
    assert await test_db.upsert_institutional_holdings([ih_record]) == 1
    ih_record["market_value"] = 90000000000.0
    assert await test_db.upsert_institutional_holdings([ih_record]) == 1
    mv = await test_db.fetchval("SELECT market_value FROM institutional_holdings WHERE id = ?", "ih_test_1")
    assert mv == 90000000000.0

    # 3. FINRA OTC Volume
    assert await test_db.upsert_finra_otc_volume([]) == 0
    otc_record = {
        "id": "otc_test_1",
        "symbol": "NVDA",
        "week_start_date": "2026-09-07",
        "tier": "Tier 1 NMS",
        "otc_volume": 45000000.0,
        "total_trades": 320000.0,
        "total_market_volume": 105000000.0,
        "dark_pool_share_pct": 42.85,
    }
    assert await test_db.upsert_finra_otc_volume([otc_record]) == 1
    otc_record["dark_pool_share_pct"] = 43.10
    assert await test_db.upsert_finra_otc_volume([otc_record]) == 1
    pct = await test_db.fetchval("SELECT dark_pool_share_pct FROM finra_otc_volume WHERE id = ?", "otc_test_1")
    assert pct == 43.10

    # 4. CBOE Daily Options
    assert await test_db.upsert_cboe_daily_options([]) == 0
    cboe_record = {
        "id": "cboe_test_1",
        "trade_date": "2026-09-11",
        "total_call_volume": 12000000.0,
        "total_put_volume": 9600000.0,
        "total_volume": 21600000.0,
        "equity_pc_ratio": 0.65,
        "index_pc_ratio": 1.15,
        "total_pc_ratio": 0.80,
        "vix_volume": 1500000.0,
    }
    assert await test_db.upsert_cboe_daily_options([cboe_record]) == 1
    cboe_record["total_volume"] = 22000000.0
    assert await test_db.upsert_cboe_daily_options([cboe_record]) == 1
    tot = await test_db.fetchval("SELECT total_volume FROM cboe_daily_options WHERE trade_date = ?", "2026-09-11")
    assert tot == 22000000.0

    # 5. Macro Indicators
    assert await test_db.upsert_macro_indicators([]) == 0
    macro_record = {
        "id": "macro_test_1",
        "series_id": "DGS10",
        "indicator_name": "10-Year Treasury Constant Maturity Rate",
        "date": "2026-09-11",
        "value": 4.28,
        "frequency": "daily",
        "units": "Percent",
    }
    assert await test_db.upsert_macro_indicators([macro_record]) == 1
    macro_record["value"] = 4.30
    assert await test_db.upsert_macro_indicators([macro_record]) == 1
    val = await test_db.fetchval("SELECT value FROM macro_indicators WHERE id = ?", "macro_test_1")
    assert val == 4.30

    # Test fetch and execute
    rows = await test_db.fetch("SELECT * FROM macro_indicators WHERE series_id = ?", "DGS10")
    assert len(rows) == 1
    await test_db.execute("DELETE FROM macro_indicators WHERE id = ?", "macro_test_1")
    empty_val = await test_db.fetchval("SELECT value FROM macro_indicators WHERE id = ?", "macro_test_1")
    assert empty_val is None

    # Test get_stats returns feed counts
    stats = await test_db.get_stats()
    assert stats["insider_trades"] >= 1
    assert stats["institutional_holdings"] >= 1
    assert stats["finra_otc"] >= 1
    assert stats["cboe_options"] >= 1


def test_database_manager_path_resolution() -> None:
    """Verify DatabaseManager resolves sqlite_path from settings or defaults."""
    # 1. Explicit sqlite_path overrides everything
    db1 = DatabaseManager(sqlite_path=":memory:")
    assert db1.sqlite_path == ":memory:"

    # 2. sqlite:/// URL prefix
    s2 = Settings(database_url="sqlite:///custom_path.db")
    db2 = DatabaseManager(settings=s2)
    assert db2.sqlite_path == "custom_path.db"

    # 3. sqlite:// URL prefix
    s3 = Settings(database_url="sqlite://another_path.db")
    db3 = DatabaseManager(settings=s3)
    assert db3.sqlite_path == "another_path.db"

    # 4. Default empty database_url resolves to greeksview_harvester.db
    s4 = Settings(database_url="")
    db4 = DatabaseManager(settings=s4)
    assert db4.sqlite_path == "greeksview_harvester.db"
    assert db4.settings.is_sqlite is True

    # 5. PostgreSQL URL leaves sqlite_path empty and is_sqlite False
    s5 = Settings(database_url="postgresql://user:pass@localhost:5432/gv")
    db5 = DatabaseManager(settings=s5)
    assert db5.sqlite_path == ""
    assert db5.settings.is_sqlite is False


@pytest.mark.asyncio
async def test_postgres_connect_insufficient_privilege() -> None:
    """Verify that DatabaseManager.connect gracefully handles InsufficientPrivilegeError on DDL."""
    settings = Settings(database_url="postgresql://user:pass@localhost:5432/db", pgssl="false")
    db = DatabaseManager(settings=settings)

    mock_pool = MagicMock()
    mock_conn = AsyncMock()

    class MockAcquireContext:
        async def __aenter__(self) -> AsyncMock:
            return mock_conn

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
            pass

    mock_pool.acquire.return_value = MockAcquireContext()
    mock_pool.close = AsyncMock()

    async def mock_execute(query: str, *args: object) -> str:
        if "CREATE SCHEMA" in query or "CREATE TABLE" in query:
            raise asyncpg.exceptions.InsufficientPrivilegeError("permission denied for database postgres")
        return "OK"

    mock_conn.execute.side_effect = mock_execute

    with patch("asyncpg.create_pool", new_callable=AsyncMock) as mock_create_pool:
        mock_create_pool.return_value = mock_pool
        await db.connect()
        assert db._pg_pool is not None


def test_to_pg_date() -> None:
    """Verify to_pg_date parses dates, datetimes, and formats or safely returns None."""
    # 1. ISO string
    assert to_pg_date("2026-09-21") == date(2026, 9, 21)
    assert to_pg_date("2026-09-21 15:30:00") == date(2026, 9, 21)

    # 2. Slash formats
    assert to_pg_date("09/21/2026") == date(2026, 9, 21)
    assert to_pg_date("2026/09/21") == date(2026, 9, 21)

    # 3. datetime and date objects
    assert to_pg_date(date(2026, 9, 21)) == date(2026, 9, 21)
    assert to_pg_date(datetime(2026, 9, 21, 12, 0, 0)) == date(2026, 9, 21)

    # 4. None and empty
    assert to_pg_date(None) is None
    assert to_pg_date("") is None

    # 5. Malformed inputs
    assert to_pg_date("not-a-date") is None
    assert to_pg_date("2026-99-99") is None
    assert to_pg_date(12345) is None
