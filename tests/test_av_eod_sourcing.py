"""
Alpha Vantage end-of-day sourcing: honesty of stored rows, sessions, pacing, mock isolation.

Every vendor reply here is shaped like Alpha Vantage's documented payloads, for a
fake symbol (ZZZT) with hand-worked values. No test reaches the network: the
autouse guard in conftest.py blocks every non-loopback socket and DNS lookup, and
HTTP is answered by respx or by a stubbed ``fetch_json``.
"""

import asyncio
import socket
import sqlite3
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import respx
from typer.testing import CliRunner

import harvester.core.db as db_module
from harvester.cli import _resolve_settings, app
from harvester.config import Settings, get_settings
from harvester.core.db import (
    MOCK_SQLITE_PATH,
    POSTGRES_SCHEMA,
    SQLITE_SCHEMA,
    DatabaseManager,
    as_vendor_eastern,
    sqlite_file_is_mock,
)
from harvester.core.sync import MockDatabaseRefusedError, sync_sqlite_to_postgres
from harvester.workers.alphavantage.pacer import AlphaVantagePacer
from harvester.workers.alphavantage.worker import (
    AlphaVantageWorker,
    HarvestReport,
    parse_trade_dates,
)

runner = CliRunner()
AV_URL = "https://www.alphavantage.co/query"
SYM = "ZZZT"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def option_row(
    contract_id: str,
    strike: str,
    option_type: str | None,
    row_date: str | None,
    **overrides: Any,
) -> dict[str, Any]:
    """One HISTORICAL_OPTIONS row as Alpha Vantage documents it (all values are strings)."""
    row: dict[str, Any] = {
        "contractID": contract_id,
        "symbol": SYM,
        "expiration": "2026-08-21",
        "strike": strike,
        "type": option_type,
        "last": "1.10",
        "mark": "1.15",
        "bid": "1.10",
        "bid_size": "3",
        "ask": "1.20",
        "ask_size": "4",
        "volume": "12",
        "open_interest": "34",
        "date": row_date,
        "implied_volatility": "0.4100",
        "delta": "0.5000",
        "gamma": "0.0500",
        "theta": "-0.0200",
        "vega": "0.0300",
        "rho": "0.0100",
    }
    if option_type is None:
        del row["type"]
    if row_date is None:
        del row["date"]
    row.update(overrides)
    return {k: v for k, v in row.items() if v is not _ABSENT}


_ABSENT = object()


def options_payload(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {"endpoint": "Historical Options", "message": "success", "data": rows}


def daily_bar(close: str, **overrides: Any) -> dict[str, Any]:
    bar: dict[str, Any] = {
        "1. open": "19.00",
        "2. high": "21.00",
        "3. low": "18.00",
        "4. close": close,
        "5. adjusted close": close,
        "6. volume": "1000",
        "7. dividend amount": "0.0000",
        "8. split coefficient": "1.0",
    }
    bar.update(overrides)
    return {k: v for k, v in bar.items() if v is not _ABSENT}


def intraday_payload(interval: str, bars: dict[str, dict[str, Any]]) -> dict[str, Any]:
    return {
        "Meta Data": {
            "1. Information": f"Intraday ({interval}) open, high, low, close prices and volume",
            "2. Symbol": SYM,
            "3. Last Refreshed": "2026-06-30 19:59:00",
            "4. Interval": interval,
            "5. Output Size": "Full size",
            "6. Time Zone": "US/Eastern",
        },
        f"Time Series ({interval})": bars,
    }


def intraday_bar(close: str, **overrides: Any) -> dict[str, Any]:
    bar: dict[str, Any] = {"1. open": "20.00", "2. high": "20.50", "3. low": "19.50", "4. close": close, "5. volume": "700"}
    bar.update(overrides)
    return {k: v for k, v in bar.items() if v is not _ABSENT}


class RecordingFetch:
    """Stub for AlphaVantageClient.fetch_json that records (function, params) calls."""

    def __init__(self, responder: Any) -> None:
        self.responder = responder
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def __call__(self, function: str, params: dict[str, Any] | None = None, is_background: bool = True) -> Any:
        p = dict(params or {})
        self.calls.append((function, p))
        return self.responder(function, p)


def fast_settings(db_file: Path, **overrides: Any) -> Settings:
    base: dict[str, Any] = {
        "database_url": f"sqlite:///{db_file}",
        "alphavantage_api_key": "TESTKEY",
        "alphavantage_max_per_second": 30,
        "alphavantage_rpm": 1200,
    }
    base.update(overrides)
    return Settings(_env_file=None, **base)


def flat(text: str) -> str:
    """CLI output with Rich's line wrapping undone."""
    return " ".join(text.split())


def rows(db_file: Path, sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    conn = sqlite3.connect(db_file)
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


class FakePgConn:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[Any, ...]]] = []
        self.executemany_calls: list[tuple[str, list[tuple[Any, ...]]]] = []

    async def execute(self, query: str, *args: Any) -> str:
        self.executed.append((query, args))
        return "INSERT 0 1"

    async def executemany(self, query: str, batch: list[tuple[Any, ...]]) -> None:
        self.executemany_calls.append((query, list(batch)))

    async def fetch(self, query: str, *args: Any) -> list[tuple[Any, ...]]:
        self.executed.append((query, args))
        return [(date(2026, 7, 1),), (date(2026, 7, 2),), (date(2026, 7, 6),)]


class FakePgPool:
    def __init__(self) -> None:
        self.conn = FakePgConn()

    def acquire(self) -> Any:
        conn = self.conn

        class _Ctx:
            async def __aenter__(self) -> FakePgConn:
                return conn

            async def __aexit__(self, *exc: Any) -> None:
                return None

        return _Ctx()

    async def close(self) -> None:
        return None


@pytest.fixture
def fresh_cli_settings(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Run CLI commands from an empty directory with a fresh, fast-paced settings cache."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ALPHAVANTAGE_MAX_PER_SECOND", "30")
    monkeypatch.setenv("ALPHAVANTAGE_RPM", "1200")
    get_settings.cache_clear()
    yield tmp_path
    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# Fix 1: each options row is stored under its own date
# ---------------------------------------------------------------------------


async def test_options_row_stored_under_its_own_date_not_the_date_asked(tmp_path: Path) -> None:
    db_file = tmp_path / "own_date.db"
    worker = AlphaVantageWorker(settings=fast_settings(db_file))
    # Asked for a market holiday (2026-07-03); the vendor answers with the 2026-07-02 session.
    chain = [option_row("ZZZT260821C00020000", "20.00", "call", "2026-07-02")]
    fetch = RecordingFetch(lambda fn, p: options_payload(chain))
    worker.client.fetch_json = fetch
    report = HarvestReport()

    async with DatabaseManager(worker.settings) as db:
        await worker.download_historical_options(db, [SYM], trade_dates=["2026-07-02", "2026-07-03"], report=report)

    assert [p["date"] for _, p in fetch.calls] == ["2026-07-02", "2026-07-03"]
    stored = rows(db_file, "SELECT contract_id, trade_date FROM options_chains_eod")
    # One contract, one session: the stale answer did not create a second copy.
    assert stored == [("ZZZT260821C00020000", "2026-07-02")]
    assert report.date_substitutions == [{"symbol": SYM, "asked": "2026-07-03", "got": ["2026-07-02"]}]


async def test_options_row_without_date_is_skipped_and_counted(tmp_path: Path) -> None:
    db_file = tmp_path / "no_date.db"
    worker = AlphaVantageWorker(settings=fast_settings(db_file))
    chain = [
        option_row("ZZZT260821C00020000", "20.00", "call", None),
        option_row("ZZZT260821P00020000", "20.00", "put", "2026-07-02"),
    ]
    worker.client.fetch_json = RecordingFetch(lambda fn, p: options_payload(chain))
    report = HarvestReport()

    async with DatabaseManager(worker.settings) as db:
        await worker.download_historical_options(db, [SYM], trade_dates=["2026-07-02"], report=report)

    assert rows(db_file, "SELECT contract_id, trade_date FROM options_chains_eod") == [
        ("ZZZT260821P00020000", "2026-07-02")
    ]
    assert report.skipped["options_missing_date"] == 1


def test_cli_run_summary_reports_asked_and_got_dates(fresh_cli_settings: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["function"] == "HISTORICAL_OPTIONS"
        assert request.url.params["date"] == "2026-07-03"
        return httpx.Response(200, json=options_payload([option_row("ZZZT260821C00020000", "20.00", "call", "2026-07-02")]))

    db_file = fresh_cli_settings / "cli_dates.db"
    with respx.mock(assert_all_called=True) as router:
        router.get(AV_URL).mock(side_effect=handler)
        result = runner.invoke(
            app,
            [
                "run", "alphavantage", "--dataset", "options", "--symbols", SYM,
                "--trade-dates", "2026-07-03", "--api-key", "TESTKEY", "--db-url", f"sqlite:///{db_file}",
            ],
        )

    assert result.exit_code == 0, result.output
    assert "ZZZT: asked 2026-07-03, got 2026-07-02" in flat(result.stdout)
    assert rows(db_file, "SELECT trade_date FROM options_chains_eod") == [("2026-07-02",)]


async def test_refused_options_request_is_an_error_not_an_empty_session(tmp_path: Path) -> None:
    db_file = tmp_path / "refused.db"
    worker = AlphaVantageWorker(settings=fast_settings(db_file))

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503)

    with respx.mock(assert_all_called=True) as router:
        router.get(AV_URL).mock(side_effect=handler)
        res = await worker.run_once(dataset="options", symbols=[SYM], trade_dates=["2026-07-02"])
    await worker.client.close()

    assert res.status == "failed"
    assert res.errors == ["HISTORICAL_OPTIONS ZZZT 2026-07-02: status 503 (http_error)"]
    assert res.metadata["report"]["empty_responses"] == []


# ---------------------------------------------------------------------------
# Fix 2: missing volume / open interest stay NULL end to end; a real 0 stays 0
# ---------------------------------------------------------------------------


async def test_missing_volume_and_open_interest_are_stored_null_and_real_zero_stays_zero(tmp_path: Path) -> None:
    db_file = tmp_path / "null_counts.db"
    worker = AlphaVantageWorker(settings=fast_settings(db_file))
    chain = [
        option_row("ZZZT260821C00010000", "10.00", "call", "2026-07-02", volume=_ABSENT, open_interest=_ABSENT),
        option_row("ZZZT260821C00020000", "20.00", "call", "2026-07-02", volume="", open_interest=None),
        option_row("ZZZT260821C00030000", "30.00", "call", "2026-07-02", volume="0", open_interest="0"),
        option_row("ZZZT260821C00040000", "40.00", "call", "2026-07-02", volume="5", open_interest="9"),
    ]
    worker.client.fetch_json = RecordingFetch(lambda fn, p: options_payload(chain))

    async with DatabaseManager(worker.settings) as db:
        await worker.download_historical_options(db, [SYM], trade_dates=["2026-07-02"])

    assert rows(db_file, "SELECT strike, volume, open_interest FROM options_chains_eod ORDER BY strike") == [
        (10.0, None, None),
        (20.0, None, None),
        (30.0, 0, 0),
        (40.0, 5, 9),
    ]


async def test_fresh_schema_gives_options_volume_and_open_interest_no_default(tmp_path: Path) -> None:
    db_file = tmp_path / "ddl.db"
    async with DatabaseManager(Settings(_env_file=None, database_url=f"sqlite:///{db_file}")) as db:
        await db.execute(
            "INSERT INTO options_chains_eod (contract_id, symbol, trade_date, expiration, strike, option_type) "
            "VALUES ('ZZZT260821C00020000', 'ZZZT', '2026-07-02', '2026-08-21', 20.0, 'call')"
        )
    assert rows(db_file, "SELECT volume, open_interest FROM options_chains_eod") == [(None, None)]
    info = {r[1]: r[4] for r in rows(db_file, "PRAGMA table_info(options_chains_eod)")}
    assert info["volume"] is None and info["open_interest"] is None


def _create_table_block(schema: str, table: str) -> str:
    """The CREATE TABLE statement for one table, with SQL line comments stripped."""
    body = "\n".join(line.split("--", 1)[0] for line in schema.splitlines())
    start = body.index(f"CREATE TABLE IF NOT EXISTS {table} (")
    return body[start : body.index(");", start)]


def test_both_ddls_declare_options_volume_and_open_interest_without_default() -> None:
    for schema in (SQLITE_SCHEMA, POSTGRES_SCHEMA):
        block = _create_table_block(schema, "options_chains_eod")
        cols = {line.strip().split()[0]: line.strip() for line in block.splitlines()[1:] if line.strip()}
        # Presence first: the columns exist in this block.
        assert cols["volume"].startswith("volume ") and cols["open_interest"].startswith("open_interest ")
        assert "DEFAULT" not in cols["volume"].upper(), cols["volume"]
        assert "DEFAULT" not in cols["open_interest"].upper(), cols["open_interest"]


async def test_upsert_keeps_null_counts_on_a_table_created_with_the_old_default(tmp_path: Path) -> None:
    """Existing tables keep DEFAULT 0 (CREATE TABLE IF NOT EXISTS is a no-op), so the
    insert must pass NULL explicitly rather than rely on the new DDL."""
    db_file = tmp_path / "old_default.db"
    old_ddl = _create_table_block(SQLITE_SCHEMA, "options_chains_eod").replace(
        "volume INTEGER,", "volume INTEGER DEFAULT 0,"
    ).replace("open_interest INTEGER,", "open_interest INTEGER DEFAULT 0,") + ");"
    assert "DEFAULT 0" in old_ddl
    conn = sqlite3.connect(db_file)
    conn.execute(old_ddl)
    conn.commit()
    conn.close()

    record = {
        "contract_id": "ZZZT260821C00020000", "symbol": SYM, "trade_date": "2026-07-02",
        "expiration": "2026-08-21", "strike": 20.0, "option_type": "call",
    }
    async with DatabaseManager(Settings(_env_file=None, database_url=f"sqlite:///{db_file}")) as db:
        assert await db.upsert_options_chains_eod([record]) == 1
    assert rows(db_file, "SELECT volume, open_interest FROM options_chains_eod") == [(None, None)]


async def test_postgres_upsert_binds_null_counts() -> None:
    db = DatabaseManager(Settings(_env_file=None, database_url="postgresql://u:p@pg.example.invalid:5432/x"))
    pool = FakePgPool()
    db._pg_pool = pool  # type: ignore[assignment]
    records = [
        {"contract_id": "C_UNKNOWN", "symbol": SYM, "trade_date": "2026-07-02", "expiration": "2026-08-21",
         "strike": 20.0, "option_type": "call", "volume": None, "open_interest": None},
        {"contract_id": "C_ZERO", "symbol": SYM, "trade_date": "2026-07-02", "expiration": "2026-08-21",
         "strike": 30.0, "option_type": "put", "volume": 0, "open_interest": 0},
    ]
    assert await db.upsert_options_chains_eod(records) == 2
    bound = [(args[0], args[10], args[11]) for q, args in pool.conn.executed if "options_chains_eod" in q]
    assert bound == [("C_UNKNOWN", None, None), ("C_ZERO", 0, 0)]


def _sqlite_with_table(db_file: Path, table: str, columns: str, values: list[tuple[Any, ...]]) -> None:
    conn = sqlite3.connect(db_file)
    conn.execute(f"CREATE TABLE {table} ({columns})")
    placeholders = ",".join("?" for _ in values[0])
    conn.executemany(f"INSERT INTO {table} VALUES ({placeholders})", values)
    conn.commit()
    conn.close()


async def _sync_with_fake_pool(db_file: Path, tables: list[str]) -> FakePgPool:
    pool = FakePgPool()
    with patch("asyncpg.create_pool", new_callable=AsyncMock) as create_pool:
        create_pool.return_value = pool
        await sync_sqlite_to_postgres(
            pg_url="postgresql://u:p@pg.example.invalid:5432/x",
            sqlite_path=str(db_file),
            settings=Settings(_env_file=None),
            target_tables=tables,
        )
    return pool


async def test_sync_pushes_null_counts_as_null_and_zero_as_zero(tmp_path: Path) -> None:
    db_file = tmp_path / "sync_counts.db"
    cols = (
        "contract_id TEXT, symbol TEXT, trade_date TEXT, expiration TEXT, strike REAL, option_type TEXT, "
        "last_price REAL, mark_price REAL, bid REAL, ask REAL, volume INTEGER, open_interest INTEGER, "
        "implied_volatility REAL, delta REAL, gamma REAL, theta REAL, vega REAL, rho REAL"
    )
    base = (SYM, "2026-07-02", "2026-08-21", 20.0, "call", 1.1, 1.15, 1.1, 1.2)
    greeks = (0.41, 0.5, 0.05, -0.02, 0.03, 0.01)
    _sqlite_with_table(
        db_file, "options_chains_eod", cols,
        [("C_UNKNOWN", *base, None, None, *greeks), ("C_ZERO", *base, 0, 0, *greeks)],
    )
    pool = await _sync_with_fake_pool(db_file, ["options_chains_eod"])
    batch = next(b for q, b in pool.conn.executemany_calls if "options_chains_eod" in q)
    assert sorted((r[0], r[10], r[11]) for r in batch) == [("C_UNKNOWN", None, None), ("C_ZERO", 0, 0)]


# ---------------------------------------------------------------------------
# Fix 3: a row with no option type is skipped, never defaulted to 'call'
# ---------------------------------------------------------------------------


async def test_row_without_option_type_is_skipped_and_counted(tmp_path: Path) -> None:
    db_file = tmp_path / "no_type.db"
    worker = AlphaVantageWorker(settings=fast_settings(db_file))
    chain = [
        option_row("ZZZT260821X00020000", "20.00", None, "2026-07-02"),
        option_row("ZZZT260821P00020000", "20.00", "put", "2026-07-02"),
    ]
    worker.client.fetch_json = RecordingFetch(lambda fn, p: options_payload(chain))
    report = HarvestReport()

    async with DatabaseManager(worker.settings) as db:
        await worker.download_historical_options(db, [SYM], trade_dates=["2026-07-02"], report=report)

    assert rows(db_file, "SELECT contract_id, option_type FROM options_chains_eod") == [("ZZZT260821P00020000", "put")]
    assert report.skipped["options_missing_type"] == 1


async def test_rows_missing_identity_or_with_malformed_numbers_are_skipped_and_counted(tmp_path: Path) -> None:
    db_file = tmp_path / "identity.db"
    worker = AlphaVantageWorker(settings=fast_settings(db_file))
    chain = [
        option_row("", "20.00", "call", "2026-07-02"),
        option_row("C_NO_EXPIRY", "20.00", "call", "2026-07-02", expiration=_ABSENT),
        option_row("C_BAD_EXPIRY", "20.00", "call", "2026-07-02", expiration="21-08-2026"),
        option_row("C_NO_STRIKE", "", "call", "2026-07-02"),
        option_row("C_BAD_VOLUME", "20.00", "call", "2026-07-02", volume="12.5"),
        option_row("C_BAD_IV", "20.00", "call", "2026-07-02", implied_volatility="n/a"),
        option_row("C_GOOD", "20.00", "call", "2026-07-02"),
    ]
    worker.client.fetch_json = RecordingFetch(lambda fn, p: options_payload(chain))
    report = HarvestReport()

    async with DatabaseManager(worker.settings) as db:
        await worker.download_historical_options(db, [SYM], trade_dates=["2026-07-02"], report=report)

    assert rows(db_file, "SELECT contract_id FROM options_chains_eod") == [("C_GOOD",)]
    assert dict(report.skipped) == {
        "options_missing_contract_id": 1,
        "options_missing_expiration": 2,
        "options_missing_strike": 1,
        "options_malformed_number": 2,
    }


# ---------------------------------------------------------------------------
# Fix 4: spot is never invented; with no stored close the band is not applied
# ---------------------------------------------------------------------------

_WIDE_CHAIN = [
    option_row("ZZZT260821C00010000", "10.00", "call", "2026-07-02"),
    option_row("ZZZT260821C00020000", "20.00", "call", "2026-07-02"),
    option_row("ZZZT260821C00100000", "100.00", "call", "2026-07-02"),
]


async def test_unknown_spot_keeps_the_full_chain_and_is_reported(tmp_path: Path) -> None:
    db_file = tmp_path / "no_spot.db"
    worker = AlphaVantageWorker(settings=fast_settings(db_file))
    worker.client.fetch_json = RecordingFetch(lambda fn, p: options_payload(_WIDE_CHAIN))
    report = HarvestReport()

    async with DatabaseManager(worker.settings) as db:
        await worker.download_historical_options(
            db, [SYM], trade_dates=["2026-07-02"], moneyness_band_pct=10.0, report=report
        )

    # The old code took the median strike (20) as spot and kept only strike 20.
    assert [r[0] for r in rows(db_file, "SELECT strike FROM options_chains_eod ORDER BY strike")] == [10.0, 20.0, 100.0]
    assert report.spot_unknown == ["ZZZT 2026-07-02"]


async def test_band_uses_the_close_of_the_rows_own_session(tmp_path: Path) -> None:
    db_file = tmp_path / "spot_row_date.db"
    worker = AlphaVantageWorker(settings=fast_settings(db_file))
    worker.client.fetch_json = RecordingFetch(lambda fn, p: options_payload(_WIDE_CHAIN))
    report = HarvestReport()

    async with DatabaseManager(worker.settings) as db:
        # A close exists only for the session the vendor answered with (2026-07-02).
        await db.upsert_stock_bars_daily([{
            "symbol": SYM, "trade_date": "2026-07-02", "open": 19.0, "high": 21.0, "low": 18.0,
            "close": 20.0, "adjusted_close": 20.0, "volume": 1000,
        }])
        await worker.download_historical_options(
            db, [SYM], trade_dates=["2026-07-03"], moneyness_band_pct=10.0, report=report
        )

    # +/-10% of 20.0 keeps only strike 20.
    assert [r[0] for r in rows(db_file, "SELECT strike FROM options_chains_eod")] == [20.0]
    assert report.spot_unknown == []
    assert report.filtered["options_outside_band"] == 2


# ---------------------------------------------------------------------------
# Fix 5: --prune-inactive never drops a row whose volume or OI is unknown
# ---------------------------------------------------------------------------


async def test_prune_inactive_drops_only_reported_zero_zero_rows(tmp_path: Path) -> None:
    db_file = tmp_path / "prune.db"
    worker = AlphaVantageWorker(settings=fast_settings(db_file))
    chain = [
        option_row("C_ZERO_ZERO", "10.00", "call", "2026-07-02", volume="0", open_interest="0"),
        option_row("C_UNKNOWN", "20.00", "call", "2026-07-02", volume=_ABSENT, open_interest=_ABSENT),
        option_row("C_ZERO_UNKNOWN", "30.00", "call", "2026-07-02", volume="0", open_interest=_ABSENT),
        option_row("C_ACTIVE", "40.00", "call", "2026-07-02", volume="0", open_interest="7"),
    ]
    worker.client.fetch_json = RecordingFetch(lambda fn, p: options_payload(chain))
    report = HarvestReport()

    async with DatabaseManager(worker.settings) as db:
        await worker.download_historical_options(
            db, [SYM], trade_dates=["2026-07-02"], prune_inactive=True, report=report
        )

    kept = [r[0] for r in rows(db_file, "SELECT contract_id FROM options_chains_eod ORDER BY strike")]
    assert kept == ["C_UNKNOWN", "C_ZERO_UNKNOWN", "C_ACTIVE"]
    assert report.filtered["options_pruned_inactive"] == 1


# ---------------------------------------------------------------------------
# Fix 6: explicit session lists
# ---------------------------------------------------------------------------


def test_parse_trade_dates_accepts_list_and_file(tmp_path: Path) -> None:
    dates_file = tmp_path / "sessions.txt"
    dates_file.write_text("# sessions\n2026-07-06\n\n2026-07-02  # before the holiday\n")
    assert parse_trade_dates(f"2026-07-01, @{dates_file},2026-07-02") == ["2026-07-01", "2026-07-02", "2026-07-06"]


@pytest.mark.parametrize("bad", ["2026-02-30", "20260702", "2026-7-2", "07/02/2026", " , "])
def test_parse_trade_dates_rejects_non_dates(bad: str) -> None:
    with pytest.raises(ValueError):
        parse_trade_dates(bad)


async def test_stored_sessions_are_the_symbols_daily_bar_dates(tmp_path: Path) -> None:
    async with DatabaseManager(Settings(_env_file=None, database_url=f"sqlite:///{tmp_path / 's.db'}")) as db:
        bars = [
            {"symbol": s, "trade_date": d, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0,
             "adjusted_close": 1.0, "volume": 1}
            for s, d in [(SYM, "2026-07-06"), (SYM, "2026-07-01"), (SYM, "2026-07-02"), ("ZZZQ", "2026-07-03")]
        ]
        await db.upsert_stock_bars_daily(bars)
        assert await db.get_stored_sessions("zzzt") == ["2026-07-01", "2026-07-02", "2026-07-06"]
        assert await db.get_stored_sessions(SYM, limit=2) == ["2026-07-02", "2026-07-06"]
        assert await db.get_stored_sessions("NONE") == []


async def test_stored_sessions_on_postgres_come_back_as_iso_dates() -> None:
    db = DatabaseManager(Settings(_env_file=None, database_url="postgresql://u:p@pg.example.invalid:5432/x"))
    pool = FakePgPool()
    db._pg_pool = pool  # type: ignore[assignment]
    assert await db.get_stored_sessions("zzzt", limit=2) == ["2026-07-02", "2026-07-06"]
    assert pool.conn.executed[-1][1] == (SYM,)


async def test_mark_mock_written_refuses_postgres() -> None:
    db = DatabaseManager(Settings(_env_file=None, database_url="postgresql://u:p@pg.example.invalid:5432/x"))
    with pytest.raises(RuntimeError, match="never write to PostgreSQL"):
        await db.mark_mock_written()


async def test_run_once_sessions_from_asks_exactly_the_stored_sessions(tmp_path: Path) -> None:
    db_file = tmp_path / "sessions_from.db"
    worker = AlphaVantageWorker(settings=fast_settings(db_file))
    async with DatabaseManager(worker.settings) as db:
        await db.upsert_stock_bars_daily([
            {"symbol": SYM, "trade_date": d, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0,
             "adjusted_close": 1.0, "volume": 1}
            for d in ("2026-07-01", "2026-07-02", "2026-07-06")
        ])
    fetch = RecordingFetch(lambda fn, p: options_payload([option_row("C1", "20.00", "call", p["date"])]))
    worker.client.fetch_json = fetch

    res = await worker.run_once(dataset="options", symbols=[SYM], sessions_from=SYM, days_back=2)

    assert res.status == "success", res.errors
    assert [p["date"] for _, p in fetch.calls] == ["2026-07-02", "2026-07-06"]


async def test_run_once_sessions_from_without_stored_bars_fails_loudly(tmp_path: Path) -> None:
    worker = AlphaVantageWorker(settings=fast_settings(tmp_path / "empty.db"))
    fetch = RecordingFetch(lambda fn, p: options_payload([]))
    worker.client.fetch_json = fetch

    res = await worker.run_once(dataset="options", symbols=[SYM], sessions_from=SYM)

    assert res.status == "failed"
    assert "No stored daily bars for ZZZT" in res.errors[0]
    assert fetch.calls == []


def test_cli_trade_dates_from_file_are_the_dates_requested(fresh_cli_settings: Path) -> None:
    (fresh_cli_settings / "sessions.txt").write_text("2026-07-06\n2026-07-02\n")
    asked: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        asked.append(request.url.params["date"])
        return httpx.Response(200, json=options_payload([option_row("C1", "20.00", "call", request.url.params["date"])]))

    with respx.mock(assert_all_called=True) as router:
        router.get(AV_URL).mock(side_effect=handler)
        result = runner.invoke(
            app,
            ["run", "alphavantage", "--dataset", "options", "--symbols", SYM, "--trade-dates", "@sessions.txt",
             "--api-key", "TESTKEY", "--db-url", "sqlite:///cli_file.db"],
        )

    assert result.exit_code == 0, result.output
    assert asked == ["2026-07-02", "2026-07-06"]


def test_cli_rejects_a_malformed_trade_date(fresh_cli_settings: Path) -> None:
    result = runner.invoke(
        app, ["run", "alphavantage", "--dataset", "options", "--trade-dates", "2026-13-01", "--db-url", "sqlite:///x.db"]
    )
    assert result.exit_code == 1
    assert "Not a YYYY-MM-DD date: 2026-13-01" in flat(result.stdout)


# ---------------------------------------------------------------------------
# Fix 7: intraday months, parameters, and missing fields
# ---------------------------------------------------------------------------


async def test_intraday_requests_every_month_full_and_unadjusted(tmp_path: Path) -> None:
    db_file = tmp_path / "intraday.db"
    worker = AlphaVantageWorker(settings=fast_settings(db_file))

    def responder(fn: str, p: dict[str, Any]) -> dict[str, Any]:
        day = "2026-05-29" if p["month"] == "2026-05" else "2026-06-30"
        return intraday_payload("1min", {f"{day} 08:30:00": intraday_bar("20.10"), f"{day} 09:31:00": intraday_bar("20.20")})

    fetch = RecordingFetch(responder)
    worker.client.fetch_json = fetch

    async with DatabaseManager(worker.settings) as db:
        h, u = await worker.download_intraday_bars(db, [SYM], interval="1min", months=["2026-05", "2026-06"])

    assert fetch.calls == [
        ("TIME_SERIES_INTRADAY", {"symbol": SYM, "interval": "1min", "outputsize": "full", "adjusted": "false",
                                  "extended_hours": "true", "month": "2026-05"}),
        ("TIME_SERIES_INTRADAY", {"symbol": SYM, "interval": "1min", "outputsize": "full", "adjusted": "false",
                                  "extended_hours": "true", "month": "2026-06"}),
    ]
    assert (h, u) == (4, 4)
    stamps = [r[0] for r in rows(db_file, "SELECT bar_timestamp FROM stock_bars_intraday ORDER BY bar_timestamp")]
    # Pre-market 08:30 bars are kept; timestamps stay the vendor's Eastern wall-clock strings.
    assert stamps == ["2026-05-29 08:30:00", "2026-05-29 09:31:00", "2026-06-30 08:30:00", "2026-06-30 09:31:00"]


async def test_intraday_extended_hours_is_an_explicit_switch(tmp_path: Path) -> None:
    worker = AlphaVantageWorker(settings=fast_settings(tmp_path / "eh.db"))
    fetch = RecordingFetch(lambda fn, p: intraday_payload("5min", {}))
    worker.client.fetch_json = fetch
    async with DatabaseManager(worker.settings) as db:
        await worker.download_intraday_bars(db, [SYM], extended_hours=False)
    assert fetch.calls[0][1]["extended_hours"] == "false"
    assert "month" not in fetch.calls[0][1]


async def test_intraday_bar_missing_a_field_is_skipped_not_zeroed(tmp_path: Path) -> None:
    db_file = tmp_path / "intraday_missing.db"
    worker = AlphaVantageWorker(settings=fast_settings(db_file))
    bars = {
        "2026-06-30 09:31:00": intraday_bar("20.20", **{"1. open": _ABSENT}),
        "2026-06-30 09:32:00": intraday_bar("20.30", **{"5. volume": ""}),
        "2026-06-30 09:33:00": intraday_bar("20.40"),
        "2026-06-30T09:34": intraday_bar("20.50"),
    }
    worker.client.fetch_json = RecordingFetch(lambda fn, p: intraday_payload("1min", bars))
    report = HarvestReport()

    async with DatabaseManager(worker.settings) as db:
        h, _ = await worker.download_intraday_bars(db, [SYM], interval="1min", months=["2026-06"], report=report)

    assert h == 1
    assert rows(db_file, "SELECT bar_timestamp, open, volume FROM stock_bars_intraday") == [("2026-06-30 09:33:00", 20.0, 700)]
    assert report.skipped["intraday_missing_field"] == 2
    assert report.skipped["intraday_malformed_timestamp"] == 1


async def test_intraday_rejects_an_unknown_interval(tmp_path: Path) -> None:
    worker = AlphaVantageWorker(settings=fast_settings(tmp_path / "iv.db"))
    async with DatabaseManager(worker.settings) as db:
        with pytest.raises(ValueError):
            await worker.download_intraday_bars(db, [SYM], interval="2min")
        with pytest.raises(ValueError):
            await worker.download_intraday_bars(db, [SYM], months=["2026-6"])


def test_cli_intraday_options_reach_the_request(fresh_cli_settings: Path) -> None:
    seen: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        params = dict(request.url.params)
        seen.append(params)
        return httpx.Response(200, json=intraday_payload("1min", {"2026-06-30 09:31:00": intraday_bar("20.20")}))

    with respx.mock(assert_all_called=True) as router:
        router.get(AV_URL).mock(side_effect=handler)
        result = runner.invoke(
            app,
            ["run", "alphavantage", "--dataset", "intraday", "--symbols", SYM, "--interval", "1min",
             "--months", "2026-05,2026-06", "--no-extended-hours", "--api-key", "TESTKEY", "--db-url", "sqlite:///i.db"],
        )

    assert result.exit_code == 0, result.output
    assert [(p["month"], p["interval"], p["extended_hours"], p["outputsize"], p["adjusted"]) for p in seen] == [
        ("2026-05", "1min", "false", "full", "false"),
        ("2026-06", "1min", "false", "full", "false"),
    ]


# ---------------------------------------------------------------------------
# Fix 8 and the daily missing-field defect
# ---------------------------------------------------------------------------


async def test_daily_outputsize_full_reaches_the_request(tmp_path: Path) -> None:
    worker = AlphaVantageWorker(settings=fast_settings(tmp_path / "full.db"))
    fetch = RecordingFetch(lambda fn, p: {"Time Series (Daily)": {"2026-07-02": daily_bar("20.00")}})
    worker.client.fetch_json = fetch

    res = await worker.run_once(dataset="daily", symbols=[SYM], outputsize="full")

    assert res.status == "success", res.errors
    assert fetch.calls == [("TIME_SERIES_DAILY_ADJUSTED", {"symbol": SYM, "outputsize": "full"})]


def test_cli_daily_outputsize_full(fresh_cli_settings: Path) -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.params["outputsize"])
        return httpx.Response(200, json={"Time Series (Daily)": {"2026-07-02": daily_bar("20.00")}})

    with respx.mock(assert_all_called=True) as router:
        router.get(AV_URL).mock(side_effect=handler)
        result = runner.invoke(
            app,
            ["run", "alphavantage", "--dataset", "daily", "--symbols", SYM, "--outputsize", "full",
             "--api-key", "TESTKEY", "--db-url", "sqlite:///d.db"],
        )
    assert result.exit_code == 0, result.output
    assert seen == ["full"]


async def test_daily_rejects_an_unknown_outputsize(tmp_path: Path) -> None:
    worker = AlphaVantageWorker(settings=fast_settings(tmp_path / "os.db"))
    async with DatabaseManager(worker.settings) as db:
        with pytest.raises(ValueError):
            await worker.download_daily_bars(db, [SYM], outputsize="everything")


async def test_daily_bar_missing_close_is_skipped_not_stored_as_zero(tmp_path: Path) -> None:
    """stock_bars_daily.close settles expiring options; a 0 would settle every option against zero."""
    db_file = tmp_path / "daily_missing.db"
    worker = AlphaVantageWorker(settings=fast_settings(db_file))
    series = {
        "2026-07-01": daily_bar("20.00", **{"4. close": _ABSENT}),
        "2026-07-02": daily_bar("21.00", **{"5. adjusted close": ""}),
        "2026-07-06": daily_bar("22.00", **{"6. volume": _ABSENT}),
        "2026-07-07": daily_bar("23.00"),
        "07/08/2026": daily_bar("24.00"),
    }
    worker.client.fetch_json = RecordingFetch(lambda fn, p: {"Time Series (Daily)": series})
    report = HarvestReport()

    async with DatabaseManager(worker.settings) as db:
        h, _ = await worker.download_daily_bars(db, [SYM], report=report)

    assert h == 1
    assert rows(db_file, "SELECT trade_date, close, adjusted_close, volume FROM stock_bars_daily") == [
        ("2026-07-07", 23.0, 23.0, 1000)
    ]
    assert report.skipped["daily_missing_field"] == 3
    assert report.skipped["daily_malformed_date"] == 1


async def test_daily_response_without_a_series_is_an_error(tmp_path: Path) -> None:
    worker = AlphaVantageWorker(settings=fast_settings(tmp_path / "noseries.db"))
    worker.client.fetch_json = RecordingFetch(lambda fn, p: {"Meta Data": {"2. Symbol": SYM}})
    res = await worker.run_once(dataset="daily", symbols=[SYM])
    assert res.status == "failed"
    assert res.errors == ["TIME_SERIES_DAILY_ADJUSTED ZZZT: response had no 'Time Series (Daily)'"]


# ---------------------------------------------------------------------------
# Intraday timestamps are US/Eastern wall-clock; Postgres gets the right instant
# ---------------------------------------------------------------------------


def test_as_vendor_eastern_applies_the_dst_offset_of_the_date() -> None:
    summer = as_vendor_eastern(datetime(2026, 6, 2, 9, 31))
    winter = as_vendor_eastern(datetime(2026, 1, 6, 9, 31))
    assert summer.astimezone(UTC) == datetime(2026, 6, 2, 13, 31, tzinfo=UTC)
    assert winter.astimezone(UTC) == datetime(2026, 1, 6, 14, 31, tzinfo=UTC)
    aware = datetime(2026, 6, 2, 9, 31, tzinfo=UTC)
    assert as_vendor_eastern(aware) is aware


async def test_sync_sends_intraday_bars_as_eastern_instants(tmp_path: Path) -> None:
    db_file = tmp_path / "sync_tz.db"
    _sqlite_with_table(
        db_file, "stock_bars_intraday",
        "symbol TEXT, bar_timestamp TEXT, interval TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER",
        [
            (SYM, "2026-06-02 09:31:00", "1min", 20.0, 20.5, 19.5, 20.2, 700),
            (SYM, "2026-01-06 09:31:00", "1min", 20.0, 20.5, 19.5, 20.2, 700),
            (SYM, "2026-06-02T09:31:00+00:00", "1min", 20.0, 20.5, 19.5, 20.2, 700),
        ],
    )
    pool = await _sync_with_fake_pool(db_file, ["stock_bars_intraday"])
    batch = next(b for q, b in pool.conn.executemany_calls if "stock_bars_intraday" in q)
    assert [r[1].astimezone(UTC) for r in batch] == [
        datetime(2026, 6, 2, 13, 31, tzinfo=UTC),
        datetime(2026, 1, 6, 14, 31, tzinfo=UTC),
        datetime(2026, 6, 2, 9, 31, tzinfo=UTC),
    ]


async def test_direct_postgres_intraday_upsert_sends_eastern_instants() -> None:
    db = DatabaseManager(Settings(_env_file=None, database_url="postgresql://u:p@pg.example.invalid:5432/x"))
    pool = FakePgPool()
    db._pg_pool = pool  # type: ignore[assignment]
    bars = [
        {"symbol": SYM, "bar_timestamp": ts, "interval": "1min", "open": 20.0, "high": 20.5,
         "low": 19.5, "close": 20.2, "volume": 700}
        for ts in ("2026-06-02 09:31:00", "2026-01-06 09:31:00")
    ]
    assert await db.upsert_stock_bars_intraday(bars) == 2
    sent = [args[1].astimezone(UTC) for q, args in pool.conn.executed if "stock_bars_intraday" in q]
    assert sent == [datetime(2026, 6, 2, 13, 31, tzinfo=UTC), datetime(2026, 1, 6, 14, 31, tzinfo=UTC)]


# ---------------------------------------------------------------------------
# Fix 9: pacing enforces a per-minute cap from config
# ---------------------------------------------------------------------------


class FakeClock:
    """Millisecond clock that only moves when the pacer sleeps."""

    def __init__(self) -> None:
        self.ms = 1_000_000.0
        self.sleeps = 0

    def now_ms(self) -> float:
        return self.ms

    async def sleep(self, seconds: float) -> None:
        self.sleeps += 1
        self.ms += seconds * 1000.0
        await asyncio.sleep(0)


async def test_pacer_never_grants_more_than_the_per_minute_cap_in_any_60s_window() -> None:
    clock = FakeClock()
    pacer = AlphaVantagePacer(max_per_second=30, requests_per_minute=9, clock=clock.now_ms, sleep=clock.sleep)
    grants: list[float] = []
    for _ in range(20):
        granted, reason, at = await pacer.acquire(is_background=True)
        assert granted and reason == "ok"
        grants.append(at)

    # The per-second spacing alone would put 20 grants inside 1 second.
    assert all(later >= earlier for earlier, later in zip(grants, grants[1:], strict=False))
    for i in range(len(grants) - 9):
        assert grants[i + 9] - grants[i] >= 60000.0, (i, grants[i + 9] - grants[i])
    # ... and the first nine are only spaced per second.
    assert grants[8] - grants[0] < 2000.0
    assert pacer.stats()["requests_per_minute"] == 9


async def test_pacer_falls_back_to_the_conservative_defaults_on_invalid_limits() -> None:
    pacer = AlphaVantagePacer(max_per_second=0, requests_per_minute=5000)
    assert (pacer.max_per_second, pacer.requests_per_minute) == (1, 9)


def test_default_pacing_settings_are_conservative(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ALPHAVANTAGE_RPM", raising=False)
    monkeypatch.delenv("ALPHAVANTAGE_MAX_PER_SECOND", raising=False)
    settings = Settings(_env_file=None)
    assert (settings.alphavantage_rpm, settings.alphavantage_max_per_second) == (9, 1)


def test_worker_pacer_takes_its_limits_from_config(tmp_path: Path) -> None:
    worker = AlphaVantageWorker(
        settings=Settings(_env_file=None, database_url="sqlite:///:memory:", alphavantage_rpm=7, alphavantage_max_per_second=2)
    )
    assert (worker.pacer.requests_per_minute, worker.pacer.max_per_second) == (7, 2)
    assert worker.client.pacer is worker.pacer


# ---------------------------------------------------------------------------
# Fix 10: mock rows never share a file with real rows, and never reach Postgres
# ---------------------------------------------------------------------------


async def test_mock_run_without_a_db_url_writes_to_the_separate_mock_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    worker = AlphaVantageWorker(settings=Settings(_env_file=None, database_url=""))

    res = await worker.run_once(dataset="options", symbols=[SYM], use_mock=True, trade_dates=["2026-07-02"])

    assert res.status == "success", res.errors
    assert not (tmp_path / "greeksview_harvester.db").exists()
    assert sqlite_file_is_mock(str(tmp_path / MOCK_SQLITE_PATH))
    assert rows(tmp_path / MOCK_SQLITE_PATH, "SELECT COUNT(*) FROM options_chains_eod")[0][0] == 10


async def test_mock_run_into_an_explicit_sqlite_file_marks_it(tmp_path: Path) -> None:
    db_file = tmp_path / "explicit.db"
    worker = AlphaVantageWorker(settings=Settings(_env_file=None, database_url=f"sqlite:///{db_file}"))
    res = await worker.run_once(dataset="daily", symbols=[SYM], use_mock=True)
    assert res.status == "success", res.errors
    assert sqlite_file_is_mock(str(db_file))


async def test_live_run_does_not_mark_its_file(tmp_path: Path) -> None:
    db_file = tmp_path / "live.db"
    worker = AlphaVantageWorker(settings=fast_settings(db_file))
    worker.client.fetch_json = RecordingFetch(lambda fn, p: {"Time Series (Daily)": {"2026-07-02": daily_bar("20.00")}})
    res = await worker.run_once(dataset="daily", symbols=[SYM])
    assert res.status == "success", res.errors
    assert rows(db_file, "SELECT COUNT(*) FROM stock_bars_daily")[0][0] == 1
    assert not sqlite_file_is_mock(str(db_file))


async def test_mock_run_refuses_postgres_without_connecting() -> None:
    worker = AlphaVantageWorker(settings=Settings(_env_file=None, database_url="postgresql://u:p@pg.example.invalid:5432/x"))
    with patch("asyncpg.create_pool", new_callable=AsyncMock) as create_pool:
        res = await worker.run_once(dataset="daily", symbols=[SYM], use_mock=True)
    assert res.status == "failed"
    assert "Mock runs never write to PostgreSQL" in res.errors[0]
    create_pool.assert_not_called()


async def test_sync_refuses_a_mock_written_file_before_connecting(tmp_path: Path) -> None:
    db_file = tmp_path / "was_mock.db"
    worker = AlphaVantageWorker(settings=Settings(_env_file=None, database_url=f"sqlite:///{db_file}"))
    assert (await worker.run_once(dataset="daily", symbols=[SYM], use_mock=True)).status == "success"

    with (
        patch("asyncpg.create_pool", new_callable=AsyncMock) as create_pool,
        pytest.raises(MockDatabaseRefusedError),
    ):
        await sync_sqlite_to_postgres(
            pg_url="postgresql://u:p@pg.example.invalid:5432/x", sqlite_path=str(db_file), settings=Settings(_env_file=None)
        )
    create_pool.assert_not_called()


def test_cli_mock_run_ignores_database_url_and_sync_refuses_the_mock_file(
    fresh_cli_settings: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@pg.example.invalid:5432/x")
    get_settings.cache_clear()
    with patch("asyncpg.create_pool", new_callable=AsyncMock) as create_pool:
        run = runner.invoke(app, ["run", "alphavantage", "--mock", "--dataset", "daily", "--symbols", SYM])
        assert run.exit_code == 0, run.output
        assert "SUCCESS" in run.stdout
        create_pool.assert_not_called()

        mock_file = fresh_cli_settings / MOCK_SQLITE_PATH
        assert sqlite_file_is_mock(str(mock_file))
        assert rows(mock_file, "SELECT COUNT(*) FROM stock_bars_daily")[0][0] > 0
        assert not (fresh_cli_settings / "greeksview_harvester.db").exists()

        sync = runner.invoke(app, ["sync-pg", "--sqlite-path", MOCK_SQLITE_PATH])
        assert sync.exit_code == 1
        assert "written by a --mock run" in flat(sync.stdout)
        create_pool.assert_not_called()


def test_cli_mock_refuses_an_explicit_postgres_db_url(fresh_cli_settings: Path) -> None:
    result = runner.invoke(
        app, ["run", "alphavantage", "--mock", "--db-url", "postgresql://u:p@pg.example.invalid:5432/x"]
    )
    assert result.exit_code == 1
    assert "--mock never writes to PostgreSQL" in flat(result.stdout)


def test_cli_run_all_mock_writes_only_the_mock_file(fresh_cli_settings: Path) -> None:
    result = runner.invoke(app, ["run-all", "--mock", "--dataset", "options", "--symbols", SYM])
    assert result.exit_code == 0, result.output
    assert sqlite_file_is_mock(str(fresh_cli_settings / MOCK_SQLITE_PATH))
    assert rows(fresh_cli_settings / MOCK_SQLITE_PATH, "SELECT COUNT(*) FROM options_chains_eod")[0][0] > 0
    assert not (fresh_cli_settings / "greeksview_harvester.db").exists()


def test_cli_mock_settings_point_at_the_mock_file_without_repointing_the_cache(fresh_cli_settings: Path) -> None:
    routed = _resolve_settings(mock=True, db_url=None)
    assert routed.database_url == f"sqlite:///{MOCK_SQLITE_PATH}"
    assert sqlite_file_is_mock(str(fresh_cli_settings / MOCK_SQLITE_PATH))
    # The cached settings are not repointed, so a later live command in-process is unaffected.
    assert get_settings().database_url == ""
    live = _resolve_settings(mock=False, db_url=None)
    assert live.database_url == ""


def test_sqlite_file_is_mock_is_false_for_a_plain_file(tmp_path: Path) -> None:
    plain = tmp_path / "plain.db"
    sqlite3.connect(plain).close()
    assert sqlite_file_is_mock(str(plain)) is False
    db_module.mark_sqlite_file_mock(str(plain))
    assert sqlite_file_is_mock(str(plain)) is True


# ---------------------------------------------------------------------------
# Test harness: no outbound network
# ---------------------------------------------------------------------------


def test_outbound_network_is_blocked_in_tests() -> None:
    # 192.0.2.1 is TEST-NET-1 (RFC 5737): never a real host, even if the guard failed.
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(0.2)
    try:
        with pytest.raises(OSError, match="Outbound network is blocked"):
            sock.connect(("192.0.2.1", 9))
    finally:
        sock.close()
    with pytest.raises(OSError, match="DNS lookup is blocked"):
        socket.getaddrinfo("www.alphavantage.co", 443)
    # Loopback stays open (the daemon's health server tests use it).
    assert socket.getaddrinfo("127.0.0.1", 9)


def test_live_options_path_with_explicit_dates_never_reads_the_clock(tmp_path: Path) -> None:
    """Stored trade dates come from the vendor's rows, never from the current time."""
    worker = AlphaVantageWorker(settings=fast_settings(tmp_path / "clock.db"))
    frozen = MagicMock(side_effect=AssertionError("live options path must not read the clock"))

    async def go() -> None:
        worker.client.fetch_json = RecordingFetch(
            lambda fn, p: options_payload([option_row("C1", "20.00", "call", "2026-07-02")])
        )
        async with DatabaseManager(worker.settings) as db:
            with patch("harvester.workers.alphavantage.worker.datetime") as dt_mock:
                dt_mock.now = frozen
                await worker.download_historical_options(db, [SYM], trade_dates=["2026-07-02"])

    asyncio.run(go())
    frozen.assert_not_called()
    assert rows(tmp_path / "clock.db", "SELECT trade_date FROM options_chains_eod") == [("2026-07-02",)]
