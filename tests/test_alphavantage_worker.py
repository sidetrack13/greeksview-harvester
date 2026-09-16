"""
Tests for Alpha Vantage Pacer, Client, Worker, and DB Persistence
=================================================================
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
from typer.testing import CliRunner

from harvester.cli import app
from harvester.config import Settings
from harvester.core.db import DatabaseManager
from harvester.workers.alphavantage.api_client import (
    AlphaVantageClient,
    AlphaVantageError,
    detect_failure,
    scrub,
)
from harvester.workers.alphavantage.pacer import (
    AlphaVantagePacer,
    is_burst_notice,
)
from harvester.workers.alphavantage.worker import AlphaVantageWorker

runner = CliRunner()

# ---------------------------------------------------------------------------
# 1. Pacer Unit Tests
# ---------------------------------------------------------------------------

def test_is_burst_notice():
    assert is_burst_notice("Burst pattern detected. Please consider spreading out your API requests...")
    assert is_burst_notice("query no more than 30 requests per second")
    assert not is_burst_notice("standard API call frequency is 5 calls per minute")
    assert not is_burst_notice(123)  # type: ignore


@pytest.mark.asyncio
async def test_pacer_acquire_and_stats():
    pacer = AlphaVantagePacer(max_per_second=20, cooldown_ms=500)
    assert pacer.max_per_second == 20
    assert pacer.spacing_ms > 0

    # Test background acquire
    granted, reason, retry_after = await pacer.acquire(is_background=True, max_wait_ms=1000)
    assert granted is True
    assert reason == "ok"

    # Test reader acquire
    granted_r, reason_r, _ = await pacer.acquire(is_background=False, max_wait_ms=1000)
    assert granted_r is True
    assert reason_r == "ok"

    stats = pacer.stats()
    assert stats["granted"] == 2
    assert stats["background_granted"] == 1


@pytest.mark.asyncio
async def test_pacer_burst_cooldown():
    pacer = AlphaVantagePacer(max_per_second=25, cooldown_ms=200)

    # First burst notice triggers cooldown
    res1 = pacer.burst_refused("TIME_SERIES_DAILY")
    assert res1["started"] is True
    assert pacer.cooling_for() > 0

    # Second immediate burst notice is absorbed
    res2 = pacer.burst_refused("TIME_SERIES_DAILY")
    assert res2["started"] is False

    stats = pacer.stats()
    assert stats["burst_refusals"] == 2
    assert stats["cooldowns"] == 1


@pytest.mark.asyncio
async def test_pacer_max_wait_refused():
    pacer = AlphaVantagePacer(max_per_second=10, cooldown_ms=2000)
    pacer.burst_refused("TEST")
    granted, reason, retry_after = await pacer.acquire(is_background=True, max_wait_ms=1.0)
    assert granted is False
    assert reason in ("cooldown", "queue")
    assert retry_after > 0


# ---------------------------------------------------------------------------
# 2. Client Unit Tests
# ---------------------------------------------------------------------------

def test_scrub_removes_api_key():
    secret = "MY_ALPHA_VANTAGE_SECRET_KEY_123"
    url = f"https://www.alphavantage.co/query?function=OVERVIEW&apikey={secret}&symbol=IBM"
    clean = scrub(url, secret)
    assert secret not in clean
    assert "[REDACTED]" in clean
    assert scrub(None) == ""  # type: ignore


def test_detect_failure():
    # 1. Unparseable
    assert detect_failure(None)["label"] == "unparseable"
    assert detect_failure(["not", "dict"])["label"] == "unparseable"

    # 2. Burst notice in Note
    burst_data = {"Note": "Burst pattern detected. Please spread out requests"}
    f_burst = detect_failure(burst_data)
    assert f_burst["status"] == 429
    assert f_burst["label"] == "burst"

    # 3. Throttle in Information
    info_data = {"Information": "Thank you for using Alpha Vantage. Our standard API call frequency is 5 calls per minute"}
    f_info = detect_failure(info_data)
    assert f_info["status"] == 429
    assert f_info["label"] == "throttle"

    # 4. Invalid symbol in Error Message
    err_data = {"Error Message": "Invalid API call. Please check documentation."}
    f_err = detect_failure(err_data)
    assert f_err["status"] == 404
    assert f_err["label"] == "invalid_symbol"

    # 5. Empty object
    assert detect_failure({})["status"] == 502
    assert detect_failure({}, function_name="OVERVIEW")["status"] == 404

    # 6. Valid data
    valid_data = {"Symbol": "AAPL", "Name": "Apple Inc"}
    assert detect_failure(valid_data) is None


@pytest.mark.asyncio
async def test_client_fetch_json_mock():
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"Meta Data": {"1. Information": "Daily Prices"}, "Time Series (Daily)": {}}

    mock_http = AsyncMock()
    mock_http.get.return_value = mock_resp

    client = AlphaVantageClient(api_key="TEST_KEY", client=mock_http)
    data = await client.fetch_json("TIME_SERIES_DAILY", {"symbol": "AAPL"})
    assert "Meta Data" in data
    await client.close()


@pytest.mark.asyncio
async def test_client_fetch_csv_mock():
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = "symbol,name,exchange,assetType,ipoDate,delistingDate,status\nAAPL,Apple Inc,NASDAQ,Stock,1980-12-12,null,Active\n"

    mock_http = AsyncMock()
    mock_http.get.return_value = mock_resp

    client = AlphaVantageClient(api_key="TEST_KEY", client=mock_http)
    rows = await client.fetch_csv("LISTING_STATUS")
    assert len(rows) == 1
    assert rows[0]["symbol"] == "AAPL"


@pytest.mark.asyncio
async def test_client_error_handling():
    mock_resp_404 = MagicMock()
    mock_resp_404.status_code = 404

    mock_http = AsyncMock()
    mock_http.get.return_value = mock_resp_404

    client = AlphaVantageClient(api_key="TEST_KEY", client=mock_http)
    with pytest.raises(AlphaVantageError) as exc_info:
        await client.fetch_json("UNKNOWN")
    assert exc_info.value.status == 404


# ---------------------------------------------------------------------------
# 3. Worker & DB Persistence Unit Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_worker_metadata_and_health(tmp_path):
    db_file = str(tmp_path / "health.db")
    settings = Settings(database_url=f"sqlite:///{db_file}", alphavantage_api_key="DEMO")
    worker = AlphaVantageWorker(settings=settings)

    assert worker.name == "alphavantage"
    assert len(worker.target_views) >= 10

    health = await worker.health()
    assert health["worker"] == "alphavantage"
    assert health["api_key_configured"] is True
    assert health["status"] in ("healthy", "degraded")


@pytest.mark.asyncio
async def test_worker_run_once_fails_fast_when_no_api_key(tmp_path) -> None:
    db_file = str(tmp_path / "missing_key.db")
    settings = Settings(database_url=f"sqlite:///{db_file}", alphavantage_api_key="")
    worker = AlphaVantageWorker(settings=settings)
    worker.client.api_key = ""

    result = await worker.run_once(use_mock=False)
    assert result.status == "failed"
    assert len(result.errors) == 1
    assert "Alpha Vantage API key is missing" in result.errors[0]


@pytest.mark.asyncio
async def test_worker_run_once_mock_all_datasets(tmp_path):
    db_file = str(tmp_path / "harvest.db")
    settings = Settings(database_url=f"sqlite:///{db_file}")
    worker = AlphaVantageWorker(settings=settings)

    # Execute mock run covering all datasets
    res = await worker.run_once(dataset="all", symbols=["SPY", "AAPL"], use_mock=True)

    assert res.is_success
    assert res.records_harvested > 0
    assert res.records_upserted > 0
    assert len(res.errors) == 0

    # Verify tables in database
    async with DatabaseManager(settings) as db:
        async with db._sqlite_conn.execute("SELECT COUNT(*) FROM stock_bars_daily") as cur:
            row = await cur.fetchone()
            assert row[0] > 0

        async with db._sqlite_conn.execute("SELECT COUNT(*) FROM stock_bars_intraday") as cur:
            row = await cur.fetchone()
            assert row[0] > 0

        async with db._sqlite_conn.execute("SELECT COUNT(*) FROM options_chains_eod") as cur:
            row = await cur.fetchone()
            assert row[0] > 0

        async with db._sqlite_conn.execute("SELECT COUNT(*) FROM company_fundamentals") as cur:
            row = await cur.fetchone()
            assert row[0] > 0

        async with db._sqlite_conn.execute("SELECT COUNT(*) FROM corporate_dividends") as cur:
            row = await cur.fetchone()
            assert row[0] > 0

        async with db._sqlite_conn.execute("SELECT COUNT(*) FROM corporate_splits") as cur:
            row = await cur.fetchone()
            assert row[0] > 0

        async with db._sqlite_conn.execute("SELECT COUNT(*) FROM etf_profiles") as cur:
            row = await cur.fetchone()
            assert row[0] > 0

        async with db._sqlite_conn.execute("SELECT COUNT(*) FROM listing_status") as cur:
            row = await cur.fetchone()
            assert row[0] > 0

        # Empty upsert methods tests
        assert await db.upsert_stock_bars_daily([]) == 0
        assert await db.upsert_stock_bars_intraday([]) == 0
        assert await db.upsert_options_chains_eod([]) == 0
        assert await db.upsert_company_fundamentals([]) == 0
        assert await db.upsert_corporate_dividends([]) == 0
        assert await db.upsert_corporate_splits([]) == 0
        assert await db.upsert_etf_profiles([]) == 0
        assert await db.upsert_listing_status([]) == 0


@pytest.mark.asyncio
async def test_worker_real_flow_with_json_mocks(tmp_path):
    db_file = str(tmp_path / "real_mock.db")
    settings = Settings(database_url=f"sqlite:///{db_file}")
    worker = AlphaVantageWorker(settings=settings)

    # Mock daily bars API response
    daily_mock = {
        "Time Series (Daily)": {
            "2024-06-14": {
                "1. open": "450.0",
                "2. high": "455.0",
                "3. low": "448.0",
                "4. close": "452.0",
                "5. adjusted close": "452.0",
                "6. volume": "50000000",
                "7. dividend amount": "0.0",
                "8. split coefficient": "1.0",
            }
        }
    }
    # Mock intraday bars API response
    intraday_mock = {
        "Time Series (5min)": {
            "2024-06-14 16:00:00": {
                "1. open": "451.0",
                "2. high": "452.0",
                "3. low": "450.5",
                "4. close": "451.8",
                "5. volume": "12000",
            }
        }
    }
    # Mock options API response
    options_mock = {
        "data": [
            {
                "contractID": "SPY240621C00450000",
                "symbol": "SPY",
                "expiration": "2024-06-21",
                "strike": "450",
                "type": "call",
                "last": "3.50",
                "mark": "3.55",
                "bid": "3.50",
                "ask": "3.60",
                "volume": "1500",
                "open_interest": "8500",
                "implied_volatility": "0.16",
                "delta": "0.55",
                "gamma": "0.04",
                "theta": "-0.05",
                "vega": "0.14",
                "rho": "0.06",
            }
        ]
    }
    # Mock fundamentals API response
    fundamentals_mock = {
        "Symbol": "AAPL",
        "FiscalDateEnding": "2024-03-31",
        "TotalRevenue": "90753000000",
    }
    # Mock corporate actions
    dividends_mock = {"data": [{"ex_dividend_date": "2024-05-10", "amount": "0.25"}]}
    splits_mock = {"data": [{"effective_date": "2020-08-31", "split_factor": "4.0"}]}

    async def mock_fetch_json(function, params=None, is_background=True):
        if function == "TIME_SERIES_DAILY_ADJUSTED":
            return daily_mock
        elif function == "TIME_SERIES_INTRADAY":
            return intraday_mock
        elif function == "HISTORICAL_OPTIONS":
            return options_mock
        elif function in ("OVERVIEW", "BALANCE_SHEET", "INCOME_STATEMENT", "CASH_FLOW", "EARNINGS"):
            return fundamentals_mock
        elif function == "DIVIDENDS":
            return dividends_mock
        elif function == "SPLITS":
            return splits_mock
        elif function == "ETF_PROFILE":
            return {"net_assets": 500000000, "portfolio_turnover": 0.05, "dividend_yield": 0.015, "expense_ratio": 0.001}
        return {}

    async def mock_fetch_csv(function, params=None, is_background=True):
        if function == "LISTING_STATUS":
            return [{"symbol": "SPY", "name": "SPDR S&P 500", "exchange": "NYSE", "assetType": "ETF", "status": "Active"}]
        return []

    worker.client.fetch_json = mock_fetch_json
    worker.client.fetch_csv = mock_fetch_csv

    async with DatabaseManager(settings) as db:
        await db.initialize_tables()
        h_d, u_d = await worker.download_daily_bars(db, ["SPY"], use_mock=False)
        assert h_d == 1 and u_d == 1

        h_i, u_i = await worker.download_intraday_bars(db, ["SPY"], use_mock=False)
        assert h_i == 1 and u_i == 1

        h_o, u_o = await worker.download_historical_options(db, ["SPY"], use_mock=False)
        assert h_o == 1 and u_o == 1

        h_f, u_f = await worker.download_fundamentals(db, ["AAPL"], use_mock=False)
        assert h_f >= 1 and u_f >= 1

        h_a, u_a = await worker.download_corporate_actions(db, ["AAPL"], use_mock=False)
        assert h_a == 2 and u_a == 2

        h_r, u_r = await worker.download_reference_data(db, ["SPY"], use_mock=False)
        assert h_r == 2 and u_r == 2


# ---------------------------------------------------------------------------
# 4. CLI Execution Test
# ---------------------------------------------------------------------------

def test_cli_run_alphavantage_mock():
    runner = CliRunner()
    result = runner.invoke(app, ["run", "alphavantage", "--mock", "--dataset", "daily", "--symbols", "SPY"])
    assert result.exit_code == 0
    assert "alphavantage" in result.output.lower()


@pytest.mark.asyncio
async def test_worker_dataset_individual_modes(tmp_path):
    db_file = str(tmp_path / "modes.db")
    settings = Settings(database_url=f"sqlite:///{db_file}")
    worker = AlphaVantageWorker(settings=settings)

    for ds in ["daily", "intraday", "options", "fundamentals", "corporate_actions", "reference"]:
        res = await worker.run_once(dataset=ds, symbols=["QQQ"], use_mock=True)
        assert res.is_success
        assert res.records_harvested > 0


@pytest.mark.asyncio
async def test_worker_error_recovery(tmp_path):
    db_file = str(tmp_path / "errors.db")
    settings = Settings(database_url=f"sqlite:///{db_file}")
    worker = AlphaVantageWorker(settings=settings)

    # Force client to throw
    async def mock_fetch_fail(*args, **kwargs):
        raise AlphaVantageError("API limit reached", status=429, label="throttle")

    worker.client.fetch_json = mock_fetch_fail
    worker.client.fetch_csv = mock_fetch_fail

    res = await worker.run_once(dataset="daily", symbols=["FAIL"], use_mock=False)
    assert not res.is_success
    assert len(res.errors) > 0


@pytest.mark.asyncio
async def test_client_internal_lifecycle_and_retry():
    client = AlphaVantageClient(api_key="TEST_KEY", timeout_seconds=10.0)
    
    # Test internal client creation & close
    http = await client.get_client()
    assert http is not None
    await client.close()
    assert client._internal_client is None


def test_cli_run_alphavantage_with_api_key(tmp_path) -> None:
    db_file = str(tmp_path / "cli_key.db")
    result = runner.invoke(
        app,
        ["run", "alphavantage", "--mock", "--symbols", "SPY", "--api-key", "CUSTOM_KEY", "--db-url", f"sqlite:///{db_file}"],
    )
    assert result.exit_code == 0
    assert "SUCCESS" in result.stdout


@pytest.mark.asyncio
async def test_download_historical_options_days_back(tmp_path):
    db_file = str(tmp_path / "days_back.db")
    settings = Settings(database_url=f"sqlite:///{db_file}")
    worker = AlphaVantageWorker(settings=settings)

    async with DatabaseManager(settings) as db:
        await db.initialize_tables()
        h, u = await worker.download_historical_options(db, ["SPY"], days_back=5, use_mock=True)
        assert h > 6
        assert u == h

        async with db._sqlite_conn.execute("SELECT COUNT(DISTINCT trade_date), COUNT(*) FROM options_chains_eod") as cur:
            row = await cur.fetchone()
            num_dates, total_rows = row[0], row[1]
            assert num_dates >= 3
            assert total_rows == num_dates * 6


def test_cli_run_alphavantage_days_back(tmp_path) -> None:
    db_file = str(tmp_path / "cli_days_back.db")
    result = runner.invoke(
        app,
        ["run", "alphavantage", "--mock", "--dataset", "options", "--symbols", "SPY", "--days-back", "5", "--db-url", f"sqlite:///{db_file}"],
    )
    assert result.exit_code == 0
    assert "SUCCESS" in result.stdout


