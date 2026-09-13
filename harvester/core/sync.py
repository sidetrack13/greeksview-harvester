"""Data synchronization pipeline from local SQLite database to PostgreSQL."""

import logging
import sqlite3
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

from harvester.config import Settings, get_settings
from harvester.core.db import POSTGRES_SCHEMA, DatabaseManager

logger = logging.getLogger(__name__)


def _parse_date(val: Any) -> date | None:
    """Safely parse SQLite date string to datetime.date object."""
    if val is None or val == "":
        return None
    if isinstance(val, date):
        return val
    s = str(val).strip().split(" ")[0]
    try:
        return date.fromisoformat(s)
    except Exception:
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
    return None


def _clean_str(val: Any) -> str | None:
    """Sanitize strings by removing PostgreSQL-incompatible null bytes (0x00)."""
    if val is None:
        return None
    s = str(val)
    if "\x00" in s:
        s = s.replace("\x00", "")
    return s


async def sync_sqlite_to_postgres(
    pg_url: str,
    sqlite_path: str = "greeksview_harvester.db",
    batch_size: int = 1000,
    settings: Settings | None = None,
    target_tables: list[str] | None = None,
) -> dict[str, dict[str, Any]]:
    """Synchronize all or selected harvested tables from local SQLite to PostgreSQL.
    
    Returns a summary mapping table names to dicts with {sqlite_count, synced_count, duration_seconds}.
    """
    db_file = Path(sqlite_path)
    if not db_file.exists():
        raise FileNotFoundError(f"Source SQLite file not found: {sqlite_path}")

    app_settings = settings or get_settings()
    app_settings.database_url = pg_url

    pg_manager = DatabaseManager(settings=app_settings)
    await pg_manager.connect()

    # 1. Initialize PostgreSQL schema (idempotent)
    async with pg_manager._pg_pool.acquire() as conn:
        schema = pg_manager.settings.database_schema
        await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')
        await conn.execute(f'SET search_path = "{schema}", public;')
        await conn.execute(POSTGRES_SCHEMA)

    summary: dict[str, dict[str, Any]] = {}
    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_conn.row_factory = sqlite3.Row

    # Dependency order: filings must precede transactions for foreign key constraint
    all_sync_tables = [
        "congressional_filings",
        "congressional_transactions",
        "macro_indicators",
        "cboe_daily_options",
        "finra_otc_volume",
        "insider_trades",
        "institutional_holdings",
    ]

    active_tables = [t for t in all_sync_tables if target_tables is None or t in target_tables]

    try:
        for tbl in active_tables:
            # Check if table exists in SQLite
            cur = sqlite_conn.cursor()
            table_check = cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tbl,)
            ).fetchone()
            if not table_check:
                logger.info("Table %s does not exist in SQLite; skipping", tbl)
                continue

            row_count = cur.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            if row_count == 0:
                summary[tbl] = {"sqlite_count": 0, "synced_count": 0, "duration_seconds": 0.0}
                continue

            t0 = time.perf_counter()
            synced = 0

            if tbl == "congressional_filings":
                synced = await _sync_congressional_filings(sqlite_conn, pg_manager._pg_pool, batch_size)
            elif tbl == "congressional_transactions":
                synced = await _sync_congressional_transactions(sqlite_conn, pg_manager._pg_pool, batch_size)
            elif tbl == "macro_indicators":
                synced = await _sync_macro_indicators(sqlite_conn, pg_manager._pg_pool, batch_size)
            elif tbl == "cboe_daily_options":
                synced = await _sync_cboe_daily_options(sqlite_conn, pg_manager._pg_pool, batch_size)
            elif tbl == "finra_otc_volume":
                synced = await _sync_finra_otc_volume(sqlite_conn, pg_manager._pg_pool, batch_size)
            elif tbl == "insider_trades":
                synced = await _sync_insider_trades(sqlite_conn, pg_manager._pg_pool, batch_size)
            elif tbl == "institutional_holdings":
                synced = await _sync_institutional_holdings(sqlite_conn, pg_manager._pg_pool, batch_size)

            elapsed = round(time.perf_counter() - t0, 2)
            summary[tbl] = {
                "sqlite_count": row_count,
                "synced_count": synced,
                "duration_seconds": elapsed,
            }
            logger.info("Synced %s: %s/%s rows in %.2fs", tbl, synced, row_count, elapsed)

    finally:
        sqlite_conn.close()
        await pg_manager.close()

    return summary


async def _sync_congressional_filings(sqlite_conn: sqlite3.Connection, pg_pool: Any, batch_size: int) -> int:
    query = """
    INSERT INTO congressional_filings (
        filing_id, chamber, member_name, member_id, filing_year, filing_date,
        doc_url, raw_text, sha256_hash, status, updated_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
    ON CONFLICT (filing_id) DO UPDATE SET
        status = EXCLUDED.status,
        raw_text = EXCLUDED.raw_text,
        updated_at = NOW()
    """
    cur = sqlite_conn.cursor()
    cur.execute("SELECT filing_id, chamber, member_name, member_id, filing_year, filing_date, doc_url, raw_text, sha256_hash, status FROM congressional_filings")
    total = 0
    async with pg_pool.acquire() as conn:
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            batch = []
            for r in rows:
                f_date = _parse_date(r["filing_date"])
                if f_date is None:
                    continue
                batch.append((
                    _clean_str(r["filing_id"]),
                    _clean_str(r["chamber"]),
                    _clean_str(r["member_name"]),
                    _clean_str(r["member_id"]),
                    r["filing_year"],
                    f_date,
                    _clean_str(r["doc_url"]),
                    _clean_str(r["raw_text"]),
                    _clean_str(r["sha256_hash"]),
                    _clean_str(r["status"]),
                ))
            if batch:
                await conn.executemany(query, batch)
                total += len(batch)
    return total


async def _sync_congressional_transactions(sqlite_conn: sqlite3.Connection, pg_pool: Any, batch_size: int) -> int:
    query = """
    INSERT INTO congressional_transactions (
        filing_id, member_name, chamber, party, state, district, ticker,
        asset_description, asset_type, transaction_type, amount_bracket,
        amount_min, amount_max, transaction_date, filing_date, owner, comment
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
    ON CONFLICT (filing_id, ticker, transaction_date, transaction_type, amount_bracket, owner)
    DO NOTHING
    """
    cur = sqlite_conn.cursor()
    cur.execute("""
    SELECT filing_id, member_name, chamber, party, state, district, ticker,
           asset_description, asset_type, transaction_type, amount_bracket,
           amount_min, amount_max, transaction_date, filing_date, owner, comment
    FROM congressional_transactions
    """)
    total = 0
    async with pg_pool.acquire() as conn:
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            batch = []
            for r in rows:
                t_date = _parse_date(r["transaction_date"])
                f_date = _parse_date(r["filing_date"])
                if t_date is None or f_date is None:
                    continue
                batch.append((
                    _clean_str(r["filing_id"]),
                    _clean_str(r["member_name"]),
                    _clean_str(r["chamber"]),
                    _clean_str(r["party"]),
                    _clean_str(r["state"]),
                    _clean_str(r["district"]),
                    _clean_str(r["ticker"]),
                    _clean_str(r["asset_description"]),
                    _clean_str(r["asset_type"]),
                    _clean_str(r["transaction_type"]),
                    _clean_str(r["amount_bracket"]),
                    float(r["amount_min"]) if r["amount_min"] is not None else 0.0,
                    float(r["amount_max"]) if r["amount_max"] is not None else None,
                    t_date,
                    f_date,
                    _clean_str(r["owner"]),
                    _clean_str(r["comment"]),
                ))
            if batch:
                await conn.executemany(query, batch)
                total += len(batch)
    return total


async def _sync_macro_indicators(sqlite_conn: sqlite3.Connection, pg_pool: Any, batch_size: int) -> int:
    query = """
    INSERT INTO macro_indicators (id, series_id, indicator_name, date, value, frequency, units, created_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
    ON CONFLICT (id) DO UPDATE SET
        value = EXCLUDED.value,
        indicator_name = EXCLUDED.indicator_name,
        created_at = NOW()
    """
    cur = sqlite_conn.cursor()
    cur.execute("SELECT id, series_id, indicator_name, date, value, frequency, units FROM macro_indicators")
    total = 0
    async with pg_pool.acquire() as conn:
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            batch = []
            for r in rows:
                obs_date = _parse_date(r["date"])
                if obs_date is None:
                    continue
                val = float(r["value"]) if r["value"] is not None else 0.0
                batch.append((
                    _clean_str(r["id"]),
                    _clean_str(r["series_id"]),
                    _clean_str(r["indicator_name"]),
                    obs_date,
                    val,
                    _clean_str(r["frequency"]),
                    _clean_str(r["units"]),
                ))
            if batch:
                await conn.executemany(query, batch)
                total += len(batch)
    return total


async def _sync_cboe_daily_options(sqlite_conn: sqlite3.Connection, pg_pool: Any, batch_size: int) -> int:
    query = """
    INSERT INTO cboe_daily_options (
        id, trade_date, total_call_volume, total_put_volume, total_volume,
        equity_pc_ratio, index_pc_ratio, total_pc_ratio, vix_volume, created_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
    ON CONFLICT (trade_date) DO UPDATE SET
        total_call_volume = EXCLUDED.total_call_volume,
        total_put_volume = EXCLUDED.total_put_volume,
        total_volume = EXCLUDED.total_volume,
        equity_pc_ratio = EXCLUDED.equity_pc_ratio,
        index_pc_ratio = EXCLUDED.index_pc_ratio,
        total_pc_ratio = EXCLUDED.total_pc_ratio,
        vix_volume = EXCLUDED.vix_volume
    """
    cur = sqlite_conn.cursor()
    cur.execute("SELECT id, trade_date, total_call_volume, total_put_volume, total_volume, equity_pc_ratio, index_pc_ratio, total_pc_ratio, vix_volume FROM cboe_daily_options")
    total = 0
    async with pg_pool.acquire() as conn:
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            batch = []
            for r in rows:
                t_date = _parse_date(r["trade_date"])
                if t_date is None:
                    continue
                batch.append((
                    _clean_str(r["id"]),
                    t_date,
                    float(r["total_call_volume"] or 0),
                    float(r["total_put_volume"] or 0),
                    float(r["total_volume"] or 0),
                    float(r["equity_pc_ratio"] or 0) if r["equity_pc_ratio"] is not None else None,
                    float(r["index_pc_ratio"] or 0) if r["index_pc_ratio"] is not None else None,
                    float(r["total_pc_ratio"] or 0) if r["total_pc_ratio"] is not None else None,
                    float(r["vix_volume"] or 0) if r["vix_volume"] is not None else None,
                ))
            if batch:
                await conn.executemany(query, batch)
                total += len(batch)
    return total


async def _sync_finra_otc_volume(sqlite_conn: sqlite3.Connection, pg_pool: Any, batch_size: int) -> int:
    query = """
    INSERT INTO finra_otc_volume (
        id, symbol, week_start_date, tier, otc_volume, total_trades,
        total_market_volume, dark_pool_share_pct, created_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
    ON CONFLICT (id) DO UPDATE SET
        otc_volume = EXCLUDED.otc_volume,
        total_trades = EXCLUDED.total_trades,
        dark_pool_share_pct = EXCLUDED.dark_pool_share_pct
    """
    cur = sqlite_conn.cursor()
    cur.execute("SELECT id, symbol, week_start_date, tier, otc_volume, total_trades, total_market_volume, dark_pool_share_pct FROM finra_otc_volume")
    total = 0
    async with pg_pool.acquire() as conn:
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            batch = []
            for r in rows:
                w_date = _parse_date(r["week_start_date"])
                if w_date is None:
                    continue
                batch.append((
                    _clean_str(r["id"]),
                    _clean_str(r["symbol"]),
                    w_date,
                    _clean_str(r["tier"]),
                    float(r["otc_volume"] or 0),
                    float(r["total_trades"] or 0),
                    float(r["total_market_volume"] or 0) if r["total_market_volume"] is not None else None,
                    float(r["dark_pool_share_pct"] or 0) if r["dark_pool_share_pct"] is not None else None,
                ))
            if batch:
                await conn.executemany(query, batch)
                total += len(batch)
    return total


async def _sync_insider_trades(sqlite_conn: sqlite3.Connection, pg_pool: Any, batch_size: int) -> int:
    query = """
    INSERT INTO insider_trades (
        id, symbol, filing_date, transaction_date, reporting_owner, owner_title,
        is_director, is_officer, is_ten_percent, transaction_type, shares,
        price_per_share, shares_owned_following, sec_form, filing_url, created_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, NOW())
    ON CONFLICT (id) DO NOTHING
    """
    cur = sqlite_conn.cursor()
    cur.execute("""
    SELECT id, symbol, filing_date, transaction_date, reporting_owner, owner_title,
           is_director, is_officer, is_ten_percent, transaction_type, shares,
           price_per_share, shares_owned_following, sec_form, filing_url
    FROM insider_trades
    """)
    total = 0
    async with pg_pool.acquire() as conn:
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            batch = []
            for r in rows:
                f_date = _parse_date(r["filing_date"])
                t_date = _parse_date(r["transaction_date"])
                if f_date is None:
                    continue
                batch.append((
                    _clean_str(r["id"]),
                    _clean_str(r["symbol"]),
                    f_date,
                    t_date,
                    _clean_str(r["reporting_owner"]),
                    _clean_str(r["owner_title"]),
                    bool(r["is_director"]),
                    bool(r["is_officer"]),
                    bool(r["is_ten_percent"]),
                    _clean_str(r["transaction_type"]),
                    float(r["shares"]) if r["shares"] is not None else None,
                    float(r["price_per_share"]) if r["price_per_share"] is not None else None,
                    float(r["shares_owned_following"]) if r["shares_owned_following"] is not None else None,
                    _clean_str(r["sec_form"]),
                    _clean_str(r["filing_url"]),
                ))
            if batch:
                await conn.executemany(query, batch)
                total += len(batch)
    return total


async def _sync_institutional_holdings(sqlite_conn: sqlite3.Connection, pg_pool: Any, batch_size: int) -> int:
    query = """
    INSERT INTO institutional_holdings (
        id, cik, institution_name, report_calendar_or_quarter, symbol, cusip,
        shares, market_value, investment_discretion, voting_authority_sole,
        sec_form, filing_url, created_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
    ON CONFLICT (id) DO NOTHING
    """
    cur = sqlite_conn.cursor()
    cur.execute("""
    SELECT id, cik, institution_name, report_calendar_or_quarter, symbol, cusip,
           shares, market_value, investment_discretion, voting_authority_sole,
           sec_form, filing_url
    FROM institutional_holdings
    """)
    total = 0
    async with pg_pool.acquire() as conn:
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            batch = []
            for r in rows:
                q_date = _parse_date(r["report_calendar_or_quarter"])
                if q_date is None:
                    continue
                batch.append((
                    _clean_str(r["id"]),
                    _clean_str(r["cik"]),
                    _clean_str(r["institution_name"]),
                    q_date,
                    _clean_str(r["symbol"]),
                    _clean_str(r["cusip"]),
                    float(r["shares"] or 0),
                    float(r["market_value"]) if r["market_value"] is not None else None,
                    _clean_str(r["investment_discretion"]),
                    float(r["voting_authority_sole"]) if r["voting_authority_sole"] is not None else None,
                    _clean_str(r["sec_form"]),
                    _clean_str(r["filing_url"]),
                ))
            if batch:
                await conn.executemany(query, batch)
                total += len(batch)
    return total
