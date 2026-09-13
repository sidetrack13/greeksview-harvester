"""Asynchronous database manager supporting PostgreSQL and SQLite with idempotent migrations."""

import logging
from typing import Any

import aiosqlite
import asyncpg

from harvester.config import Settings, get_settings
from harvester.core.models import CongressionalFiling, CongressionalTransaction

logger = logging.getLogger(__name__)

# Schema DDL matching GreeksView GV-96 specifications
SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS congressional_filings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filing_id TEXT UNIQUE NOT NULL,
    chamber TEXT NOT NULL CHECK (chamber IN ('house', 'senate')),
    member_name TEXT NOT NULL,
    member_id TEXT,
    filing_year INTEGER,
    filing_date TEXT NOT NULL,
    doc_url TEXT,
    raw_text TEXT,
    sha256_hash TEXT UNIQUE NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'parsed', 'error', 'manual_review')),
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_cg_filings_date ON congressional_filings(filing_date);
CREATE INDEX IF NOT EXISTS idx_cg_filings_member ON congressional_filings(member_name);

CREATE TABLE IF NOT EXISTS congressional_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filing_id TEXT NOT NULL,
    member_name TEXT NOT NULL,
    chamber TEXT NOT NULL CHECK (chamber IN ('house', 'senate')),
    party TEXT,
    state TEXT,
    district TEXT,
    ticker TEXT NOT NULL,
    asset_description TEXT,
    asset_type TEXT NOT NULL DEFAULT 'stock',
    transaction_type TEXT NOT NULL CHECK (transaction_type IN ('BUY', 'SALE_FULL', 'SALE_PARTIAL', 'EXCHANGE')),
    amount_bracket TEXT NOT NULL,
    amount_min REAL NOT NULL,
    amount_max REAL,
    transaction_date TEXT NOT NULL,
    filing_date TEXT NOT NULL,
    disclosure_lag_days INTEGER GENERATED ALWAYS AS (CAST(julianday(filing_date) - julianday(transaction_date) AS INTEGER)) STORED,
    owner TEXT NOT NULL DEFAULT 'self' CHECK (owner IN ('self', 'spouse', 'dependent', 'joint')),
    comment TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (filing_id) REFERENCES congressional_filings(filing_id) ON DELETE CASCADE,
    CONSTRAINT uq_congressional_tx_dedup UNIQUE (filing_id, ticker, transaction_date, transaction_type, amount_bracket, owner)
);
CREATE INDEX IF NOT EXISTS idx_cg_tx_ticker ON congressional_transactions(ticker);
CREATE INDEX IF NOT EXISTS idx_cg_tx_date ON congressional_transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_cg_tx_member ON congressional_transactions(member_name);

CREATE VIEW IF NOT EXISTS v_ticker_congressional_summary AS
SELECT
    ticker,
    COUNT(*) AS total_trades,
    SUM(CASE WHEN transaction_type = 'BUY' THEN 1 ELSE 0 END) AS buy_count,
    SUM(CASE WHEN transaction_type IN ('SALE_FULL', 'SALE_PARTIAL') THEN 1 ELSE 0 END) AS sell_count,
    SUM(CASE WHEN transaction_type = 'EXCHANGE' THEN 1 ELSE 0 END) AS exchange_count,
    SUM(CASE WHEN transaction_type = 'BUY' THEN 1 WHEN transaction_type IN ('SALE_FULL', 'SALE_PARTIAL') THEN -1 ELSE 0 END) AS net_trades,
    COALESCE(SUM(amount_min), 0) AS est_volume_min,
    COALESCE(SUM(amount_max), 0) AS est_volume_max,
    COALESCE(SUM(CASE WHEN transaction_type = 'BUY' THEN amount_min WHEN transaction_type IN ('SALE_FULL', 'SALE_PARTIAL') THEN -amount_min ELSE 0 END), 0) AS net_volume_min,
    COALESCE(SUM(CASE WHEN transaction_type = 'BUY' THEN amount_max WHEN transaction_type IN ('SALE_FULL', 'SALE_PARTIAL') THEN -amount_max ELSE 0 END), 0) AS net_volume_max,
    COUNT(DISTINCT member_name) AS active_traders,
    SUM(CASE WHEN chamber = 'house' THEN 1 ELSE 0 END) AS house_trades,
    SUM(CASE WHEN chamber = 'senate' THEN 1 ELSE 0 END) AS senate_trades,
    SUM(CASE WHEN party = 'Democrat' THEN 1 ELSE 0 END) AS democrat_trades,
    SUM(CASE WHEN party = 'Republican' THEN 1 ELSE 0 END) AS republican_trades,
    MAX(transaction_date) AS last_transaction_date,
    MAX(filing_date) AS last_filing_date,
    AVG(disclosure_lag_days) AS avg_disclosure_lag_days
FROM congressional_transactions
GROUP BY ticker;

CREATE TABLE IF NOT EXISTS insider_trades (
    id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    filing_date TEXT NOT NULL,
    transaction_date TEXT,
    reporting_owner TEXT NOT NULL,
    owner_title TEXT,
    is_director INTEGER DEFAULT 0,
    is_officer INTEGER DEFAULT 0,
    is_ten_percent INTEGER DEFAULT 0,
    transaction_type TEXT NOT NULL,
    shares REAL,
    price_per_share REAL,
    shares_owned_following REAL,
    sec_form TEXT DEFAULT '4',
    filing_url TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_insider_trades_sym ON insider_trades(symbol);
CREATE INDEX IF NOT EXISTS idx_insider_trades_date ON insider_trades(filing_date);

CREATE TABLE IF NOT EXISTS institutional_holdings (
    id TEXT PRIMARY KEY,
    cik TEXT NOT NULL,
    institution_name TEXT NOT NULL,
    report_calendar_or_quarter TEXT NOT NULL,
    symbol TEXT NOT NULL,
    cusip TEXT,
    shares REAL NOT NULL,
    market_value REAL,
    investment_discretion TEXT,
    voting_authority_sole REAL,
    sec_form TEXT DEFAULT '13F-HR',
    filing_url TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_inst_holdings_sym ON institutional_holdings(symbol);
CREATE INDEX IF NOT EXISTS idx_inst_holdings_cik ON institutional_holdings(cik);

CREATE TABLE IF NOT EXISTS finra_otc_volume (
    id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    week_start_date TEXT NOT NULL,
    tier TEXT NOT NULL,
    otc_volume REAL NOT NULL,
    total_trades REAL NOT NULL,
    total_market_volume REAL,
    dark_pool_share_pct REAL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_finra_otc_sym ON finra_otc_volume(symbol);
CREATE INDEX IF NOT EXISTS idx_finra_otc_week ON finra_otc_volume(week_start_date);

CREATE TABLE IF NOT EXISTS cboe_daily_options (
    id TEXT PRIMARY KEY,
    trade_date TEXT UNIQUE NOT NULL,
    total_call_volume REAL NOT NULL,
    total_put_volume REAL NOT NULL,
    total_volume REAL NOT NULL,
    equity_pc_ratio REAL,
    index_pc_ratio REAL,
    total_pc_ratio REAL,
    vix_volume REAL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_cboe_daily_date ON cboe_daily_options(trade_date);

CREATE TABLE IF NOT EXISTS macro_indicators (
    id TEXT PRIMARY KEY,
    series_id TEXT NOT NULL,
    indicator_name TEXT NOT NULL,
    date TEXT NOT NULL,
    value REAL NOT NULL,
    frequency TEXT DEFAULT 'daily',
    units TEXT DEFAULT 'Percent',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_macro_series ON macro_indicators(series_id);
CREATE INDEX IF NOT EXISTS idx_macro_date ON macro_indicators(date);
"""

POSTGRES_SCHEMA = """
CREATE TABLE IF NOT EXISTS congressional_filings (
    id BIGSERIAL PRIMARY KEY,
    filing_id VARCHAR(64) UNIQUE NOT NULL,
    chamber VARCHAR(10) NOT NULL CHECK (chamber IN ('house', 'senate')),
    member_name TEXT NOT NULL,
    member_id VARCHAR(32),
    filing_year INTEGER,
    filing_date DATE NOT NULL,
    doc_url TEXT,
    raw_text TEXT,
    sha256_hash VARCHAR(64) UNIQUE NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'parsed', 'error', 'manual_review')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_cg_filings_date ON congressional_filings(filing_date);
CREATE INDEX IF NOT EXISTS idx_cg_filings_member ON congressional_filings(member_name);

CREATE TABLE IF NOT EXISTS congressional_transactions (
    id BIGSERIAL PRIMARY KEY,
    filing_id VARCHAR(64) NOT NULL,
    member_name TEXT NOT NULL,
    chamber VARCHAR(10) NOT NULL CHECK (chamber IN ('house', 'senate')),
    party VARCHAR(20),
    state VARCHAR(10),
    district VARCHAR(10),
    ticker VARCHAR(16) NOT NULL,
    asset_description TEXT,
    asset_type VARCHAR(32) NOT NULL DEFAULT 'stock',
    transaction_type VARCHAR(20) NOT NULL CHECK (transaction_type IN ('BUY', 'SALE_FULL', 'SALE_PARTIAL', 'EXCHANGE')),
    amount_bracket VARCHAR(64) NOT NULL,
    amount_min NUMERIC(14,2) NOT NULL,
    amount_max NUMERIC(14,2),
    transaction_date DATE NOT NULL,
    filing_date DATE NOT NULL,
    disclosure_lag_days INTEGER GENERATED ALWAYS AS (filing_date - transaction_date) STORED,
    owner VARCHAR(20) NOT NULL DEFAULT 'self' CHECK (owner IN ('self', 'spouse', 'dependent', 'joint')),
    comment TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (filing_id) REFERENCES congressional_filings(filing_id) ON DELETE CASCADE,
    CONSTRAINT uq_congressional_tx_dedup UNIQUE (filing_id, ticker, transaction_date, transaction_type, amount_bracket, owner)
);
CREATE INDEX IF NOT EXISTS idx_cg_tx_ticker ON congressional_transactions(ticker);
CREATE INDEX IF NOT EXISTS idx_cg_tx_date ON congressional_transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_cg_tx_member ON congressional_transactions(member_name);

CREATE OR REPLACE VIEW v_ticker_congressional_summary AS
SELECT
    ticker,
    COUNT(*) AS total_trades,
    SUM(CASE WHEN transaction_type = 'BUY' THEN 1 ELSE 0 END) AS buy_count,
    SUM(CASE WHEN transaction_type IN ('SALE_FULL', 'SALE_PARTIAL') THEN 1 ELSE 0 END) AS sell_count,
    SUM(CASE WHEN transaction_type = 'EXCHANGE' THEN 1 ELSE 0 END) AS exchange_count,
    SUM(CASE WHEN transaction_type = 'BUY' THEN 1 WHEN transaction_type IN ('SALE_FULL', 'SALE_PARTIAL') THEN -1 ELSE 0 END) AS net_trades,
    COALESCE(SUM(amount_min), 0) AS est_volume_min,
    COALESCE(SUM(amount_max), 0) AS est_volume_max,
    COALESCE(SUM(CASE WHEN transaction_type = 'BUY' THEN amount_min WHEN transaction_type IN ('SALE_FULL', 'SALE_PARTIAL') THEN -amount_min ELSE 0 END), 0) AS net_volume_min,
    COALESCE(SUM(CASE WHEN transaction_type = 'BUY' THEN amount_max WHEN transaction_type IN ('SALE_FULL', 'SALE_PARTIAL') THEN -amount_max ELSE 0 END), 0) AS net_volume_max,
    COUNT(DISTINCT member_name) AS active_traders,
    SUM(CASE WHEN chamber = 'house' THEN 1 ELSE 0 END) AS house_trades,
    SUM(CASE WHEN chamber = 'senate' THEN 1 ELSE 0 END) AS senate_trades,
    SUM(CASE WHEN party = 'Democrat' THEN 1 ELSE 0 END) AS democrat_trades,
    SUM(CASE WHEN party = 'Republican' THEN 1 ELSE 0 END) AS republican_trades,
    MAX(transaction_date) AS last_transaction_date,
    MAX(filing_date) AS last_filing_date,
    AVG(disclosure_lag_days) AS avg_disclosure_lag_days
FROM congressional_transactions
GROUP BY ticker;

CREATE TABLE IF NOT EXISTS insider_trades (
    id VARCHAR(128) PRIMARY KEY,
    symbol VARCHAR(16) NOT NULL,
    filing_date DATE NOT NULL,
    transaction_date DATE,
    reporting_owner TEXT NOT NULL,
    owner_title TEXT,
    is_director BOOLEAN DEFAULT FALSE,
    is_officer BOOLEAN DEFAULT FALSE,
    is_ten_percent BOOLEAN DEFAULT FALSE,
    transaction_type VARCHAR(32) NOT NULL,
    shares NUMERIC(16,4),
    price_per_share NUMERIC(14,4),
    shares_owned_following NUMERIC(16,4),
    sec_form VARCHAR(16) DEFAULT '4',
    filing_url TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_insider_trades_sym ON insider_trades(symbol);
CREATE INDEX IF NOT EXISTS idx_insider_trades_date ON insider_trades(filing_date);

CREATE TABLE IF NOT EXISTS institutional_holdings (
    id VARCHAR(128) PRIMARY KEY,
    cik VARCHAR(16) NOT NULL,
    institution_name TEXT NOT NULL,
    report_calendar_or_quarter DATE NOT NULL,
    symbol VARCHAR(16) NOT NULL,
    cusip VARCHAR(16),
    shares NUMERIC(16,4) NOT NULL,
    market_value NUMERIC(16,2),
    investment_discretion VARCHAR(32),
    voting_authority_sole NUMERIC(16,4),
    sec_form VARCHAR(16) DEFAULT '13F-HR',
    filing_url TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_inst_holdings_sym ON institutional_holdings(symbol);
CREATE INDEX IF NOT EXISTS idx_inst_holdings_cik ON institutional_holdings(cik);

CREATE TABLE IF NOT EXISTS finra_otc_volume (
    id VARCHAR(128) PRIMARY KEY,
    symbol VARCHAR(16) NOT NULL,
    week_start_date DATE NOT NULL,
    tier VARCHAR(32) NOT NULL,
    otc_volume NUMERIC(16,2) NOT NULL,
    total_trades NUMERIC(14,0) NOT NULL,
    total_market_volume NUMERIC(16,2),
    dark_pool_share_pct NUMERIC(6,3),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_finra_otc_sym ON finra_otc_volume(symbol);
CREATE INDEX IF NOT EXISTS idx_finra_otc_week ON finra_otc_volume(week_start_date);

CREATE TABLE IF NOT EXISTS cboe_daily_options (
    id VARCHAR(128) PRIMARY KEY,
    trade_date DATE UNIQUE NOT NULL,
    total_call_volume NUMERIC(16,0) NOT NULL,
    total_put_volume NUMERIC(16,0) NOT NULL,
    total_volume NUMERIC(16,0) NOT NULL,
    equity_pc_ratio NUMERIC(6,4),
    index_pc_ratio NUMERIC(6,4),
    total_pc_ratio NUMERIC(6,4),
    vix_volume NUMERIC(16,0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_cboe_daily_date ON cboe_daily_options(trade_date);

CREATE TABLE IF NOT EXISTS macro_indicators (
    id VARCHAR(128) PRIMARY KEY,
    series_id VARCHAR(32) NOT NULL,
    indicator_name TEXT NOT NULL,
    date DATE NOT NULL,
    value NUMERIC(12,4) NOT NULL,
    frequency VARCHAR(16) DEFAULT 'daily',
    units VARCHAR(32) DEFAULT 'Percent',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_macro_series ON macro_indicators(series_id);
CREATE INDEX IF NOT EXISTS idx_macro_date ON macro_indicators(date);
"""


class DatabaseManager:
    """Async database abstraction supporting SQLite and PostgreSQL."""

    def __init__(self, settings: Settings | None = None, sqlite_path: str | None = None) -> None:
        self.settings = settings or get_settings()
        if sqlite_path is not None:
            self.sqlite_path = sqlite_path
        elif self.settings.database_url.startswith("sqlite:///"):
            self.sqlite_path = self.settings.database_url.removeprefix("sqlite:///")
        elif self.settings.database_url.startswith("sqlite://"):
            self.sqlite_path = self.settings.database_url.removeprefix("sqlite://")
        elif self.settings.is_sqlite:
            self.sqlite_path = "greeksview_harvester.db"
        else:
            self.sqlite_path = ""
        self._sqlite_conn: aiosqlite.Connection | None = None
        self._pg_pool: asyncpg.Pool | None = None

    @property
    def is_connected(self) -> bool:
        if self.settings.is_sqlite:
            return self._sqlite_conn is not None
        return self._pg_pool is not None

    async def __aenter__(self) -> "DatabaseManager":
        if not self.is_connected:
            await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()

    async def initialize_tables(self) -> None:
        """Alias to ensure tables and views are initialized."""
        if not self.is_connected:
            await self.connect()

    async def execute(self, query: str, *args: Any) -> Any:
        """Execute a SQL statement against SQLite or PostgreSQL."""
        if not self.is_connected:
            await self.connect()
        if self.settings.is_sqlite:
            assert self._sqlite_conn is not None
            cursor = await self._sqlite_conn.execute(query, args)
            await self._sqlite_conn.commit()
            return cursor
        else:
            assert self._pg_pool is not None
            async with self._pg_pool.acquire() as conn:
                return await conn.execute(query, *args)

    async def fetch(self, query: str, *args: Any) -> list[Any]:
        """Fetch multiple rows."""
        if not self.is_connected:
            await self.connect()
        if self.settings.is_sqlite:
            assert self._sqlite_conn is not None
            async with self._sqlite_conn.execute(query, args) as cursor:
                return await cursor.fetchall()
        else:
            assert self._pg_pool is not None
            async with self._pg_pool.acquire() as conn:
                return await conn.fetch(query, *args)

    async def fetchval(self, query: str, *args: Any) -> Any:
        """Fetch single value."""
        if not self.is_connected:
            await self.connect()
        if self.settings.is_sqlite:
            assert self._sqlite_conn is not None
            async with self._sqlite_conn.execute(query, args) as cursor:
                row = await cursor.fetchone()
                return row[0] if row is not None else None
        else:
            assert self._pg_pool is not None
            async with self._pg_pool.acquire() as conn:
                return await conn.fetchval(query, *args)

    async def connect(self) -> None:
        """Establish database connection or pool and initialize schema."""
        if self.settings.is_sqlite:
            target = self.sqlite_path or "greeksview_harvester.db"
            self._sqlite_conn = await aiosqlite.connect(target)
            self._sqlite_conn.row_factory = aiosqlite.Row
            await self._sqlite_conn.executescript(SQLITE_SCHEMA)
            await self._sqlite_conn.commit()
            logger.info("Connected to SQLite database: %s", target)
        else:
            ssl_opt = self.settings.pgssl.lower() != "false"
            self._pg_pool = await asyncpg.create_pool(
                self.settings.database_url,
                ssl=ssl_opt,
                min_size=1,
                max_size=10,
            )
            async with self._pg_pool.acquire() as conn:
                await conn.execute(POSTGRES_SCHEMA)
            logger.info("Connected to PostgreSQL pool: %s", self.settings.database_url.split("@")[-1])

    async def close(self) -> None:
        """Close database connection or pool."""
        if self._sqlite_conn is not None:
            await self._sqlite_conn.close()
            self._sqlite_conn = None
        if self._pg_pool is not None:
            await self._pg_pool.close()
            self._pg_pool = None

    async def get_existing_filing_ids(self, filing_ids: list[str]) -> set[str]:
        """Return set of filing_ids that already exist in the database."""
        if not filing_ids:
            return set()

        if self.settings.is_sqlite:
            assert self._sqlite_conn is not None
            placeholders = ",".join("?" for _ in filing_ids)
            query = f"SELECT filing_id FROM congressional_filings WHERE filing_id IN ({placeholders})"
            async with self._sqlite_conn.execute(query, filing_ids) as cursor:
                rows = await cursor.fetchall()
                return {row[0] for row in rows}
        else:
            assert self._pg_pool is not None
            query = "SELECT filing_id FROM congressional_filings WHERE filing_id = ANY($1::text[])"
            async with self._pg_pool.acquire() as conn:
                rows = await conn.fetch(query, filing_ids)
                return {row["filing_id"] for row in rows}

    async def upsert_filing(self, filing: CongressionalFiling) -> None:
        """Insert or update a congressional filing record."""
        if self.settings.is_sqlite:
            assert self._sqlite_conn is not None
            query = """
            INSERT INTO congressional_filings (
                filing_id, chamber, member_name, member_id, filing_year, filing_date,
                doc_url, raw_text, sha256_hash, status, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT (filing_id) DO UPDATE SET
                status = excluded.status,
                raw_text = excluded.raw_text,
                updated_at = datetime('now')
            """
            await self._sqlite_conn.execute(
                query,
                (
                    filing.filing_id,
                    filing.chamber,
                    filing.member_name,
                    filing.member_id,
                    filing.filing_year,
                    filing.filing_date.isoformat(),
                    filing.doc_url,
                    filing.raw_text,
                    filing.sha256_hash,
                    filing.status.value,
                ),
            )
            await self._sqlite_conn.commit()
        else:
            assert self._pg_pool is not None
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
            async with self._pg_pool.acquire() as conn:
                await conn.execute(
                    query,
                    filing.filing_id,
                    filing.chamber,
                    filing.member_name,
                    filing.member_id,
                    filing.filing_year,
                    filing.filing_date,
                    filing.doc_url,
                    filing.raw_text,
                    filing.sha256_hash,
                    filing.status.value,
                )

    async def insert_transactions(self, transactions: list[CongressionalTransaction]) -> int:
        """Batch insert transactions with composite deduplication. Returns inserted count."""
        if not transactions:
            return 0

        inserted_count = 0
        if self.settings.is_sqlite:
            assert self._sqlite_conn is not None
            query = """
            INSERT OR IGNORE INTO congressional_transactions (
                filing_id, member_name, chamber, party, state, district, ticker,
                asset_description, asset_type, transaction_type, amount_bracket,
                amount_min, amount_max, transaction_date, filing_date, owner, comment
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            for tx in transactions:
                cur = await self._sqlite_conn.execute(
                    query,
                    (
                        tx.filing_id,
                        tx.member_name,
                        tx.chamber,
                        tx.party,
                        tx.state,
                        tx.district,
                        tx.ticker,
                        tx.asset_description,
                        tx.asset_type,
                        tx.transaction_type.value,
                        tx.amount_bracket,
                        tx.amount_min,
                        tx.amount_max,
                        tx.transaction_date.isoformat(),
                        tx.filing_date.isoformat(),
                        tx.owner.value,
                        tx.comment,
                    ),
                )
                if cur.rowcount > 0:
                    inserted_count += cur.rowcount
            await self._sqlite_conn.commit()
        else:
            assert self._pg_pool is not None
            query = """
            INSERT INTO congressional_transactions (
                filing_id, member_name, chamber, party, state, district, ticker,
                asset_description, asset_type, transaction_type, amount_bracket,
                amount_min, amount_max, transaction_date, filing_date, owner, comment
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
            ON CONFLICT (filing_id, ticker, transaction_date, transaction_type, amount_bracket, owner)
            DO NOTHING
            """
            async with self._pg_pool.acquire() as conn:
                for tx in transactions:
                    res = await conn.execute(
                        query,
                        tx.filing_id,
                        tx.member_name,
                        tx.chamber,
                        tx.party,
                        tx.state,
                        tx.district,
                        tx.ticker,
                        tx.asset_description,
                        tx.asset_type,
                        tx.transaction_type.value,
                        tx.amount_bracket,
                        tx.amount_min,
                        tx.amount_max,
                        tx.transaction_date,
                        tx.filing_date,
                        tx.owner.value,
                        tx.comment,
                    )
                    if "INSERT 0 1" in res:
                        inserted_count += 1

        return inserted_count

    async def get_stats(self) -> dict[str, Any]:
        """Return aggregate statistics of ingested filings and transactions."""
        if self.settings.is_sqlite:
            assert self._sqlite_conn is not None
            async with self._sqlite_conn.execute("SELECT COUNT(*) FROM congressional_filings") as c1:
                r1 = await c1.fetchone()
                total_filings = r1[0] if r1 is not None else 0
            async with self._sqlite_conn.execute("SELECT COUNT(*) FROM congressional_transactions") as c2:
                r2 = await c2.fetchone()
                total_transactions = r2[0] if r2 is not None else 0
            async with self._sqlite_conn.execute(
                "SELECT transaction_type, COUNT(*) FROM congressional_transactions GROUP BY transaction_type"
            ) as c3:
                by_type = {row[0]: row[1] for row in await c3.fetchall()}
            async with self._sqlite_conn.execute("SELECT COUNT(DISTINCT ticker) FROM congressional_transactions") as c4:
                r4 = await c4.fetchone()
                distinct_tickers = r4[0] if r4 is not None else 0
            async with self._sqlite_conn.execute(
                "SELECT chamber, COUNT(*) FROM congressional_transactions GROUP BY chamber"
            ) as c5:
                by_chamber = {row[0]: row[1] for row in await c5.fetchall()}
            async with self._sqlite_conn.execute("SELECT COUNT(*) FROM insider_trades") as c6:
                r6 = await c6.fetchone()
                insider_trades = r6[0] if r6 is not None else 0
            async with self._sqlite_conn.execute("SELECT COUNT(*) FROM institutional_holdings") as c7:
                r7 = await c7.fetchone()
                institutional_holdings = r7[0] if r7 is not None else 0
            async with self._sqlite_conn.execute("SELECT COUNT(*) FROM finra_otc_volume") as c8:
                r8 = await c8.fetchone()
                finra_otc = r8[0] if r8 is not None else 0
            async with self._sqlite_conn.execute("SELECT COUNT(*) FROM cboe_daily_options") as c9:
                r9 = await c9.fetchone()
                cboe_options = r9[0] if r9 is not None else 0
            async with self._sqlite_conn.execute("SELECT COUNT(*) FROM macro_indicators") as c10:
                r10 = await c10.fetchone()
                macro_indicators = r10[0] if r10 is not None else 0
        else:
            assert self._pg_pool is not None
            async with self._pg_pool.acquire() as conn:
                total_filings = await conn.fetchval("SELECT COUNT(*) FROM congressional_filings")
                total_transactions = await conn.fetchval("SELECT COUNT(*) FROM congressional_transactions")
                type_rows = await conn.fetch(
                    "SELECT transaction_type, COUNT(*) as count FROM congressional_transactions GROUP BY transaction_type"
                )
                by_type = {row["transaction_type"]: row["count"] for row in type_rows}
                chamber_rows = await conn.fetch(
                    "SELECT chamber, COUNT(*) as count FROM congressional_transactions GROUP BY chamber"
                )
                by_chamber = {row["chamber"]: row["count"] for row in chamber_rows}
                distinct_tickers = await conn.fetchval("SELECT COUNT(DISTINCT ticker) FROM congressional_transactions")
                insider_trades = await conn.fetchval("SELECT COUNT(*) FROM insider_trades")
                institutional_holdings = await conn.fetchval("SELECT COUNT(*) FROM institutional_holdings")
                finra_otc = await conn.fetchval("SELECT COUNT(*) FROM finra_otc_volume")
                cboe_options = await conn.fetchval("SELECT COUNT(*) FROM cboe_daily_options")
                macro_indicators = await conn.fetchval("SELECT COUNT(*) FROM macro_indicators")

        return {
            "total_filings": total_filings,
            "total_transactions": total_transactions,
            "distinct_tickers": distinct_tickers,
            "by_type": by_type,
            "by_chamber": by_chamber,
            "insider_trades": insider_trades,
            "institutional_holdings": institutional_holdings,
            "finra_otc": finra_otc,
            "cboe_options": cboe_options,
            "macro_indicators": macro_indicators,
        }

    async def upsert_insider_trades(self, records: list[dict[str, Any]]) -> int:
        """Upsert Form 4 insider trading disclosures."""
        if not records:
            return 0
        count = 0
        if self.settings.is_sqlite:
            assert self._sqlite_conn is not None
            query = """
            INSERT INTO insider_trades (
                id, symbol, filing_date, transaction_date, reporting_owner,
                owner_title, is_director, is_officer, is_ten_percent,
                transaction_type, shares, price_per_share, shares_owned_following,
                sec_form, filing_url, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT (id) DO UPDATE SET
                symbol = excluded.symbol,
                filing_date = excluded.filing_date,
                transaction_date = excluded.transaction_date,
                reporting_owner = excluded.reporting_owner,
                owner_title = excluded.owner_title,
                is_director = excluded.is_director,
                is_officer = excluded.is_officer,
                is_ten_percent = excluded.is_ten_percent,
                transaction_type = excluded.transaction_type,
                shares = excluded.shares,
                price_per_share = excluded.price_per_share,
                shares_owned_following = excluded.shares_owned_following,
                sec_form = excluded.sec_form,
                filing_url = excluded.filing_url
            """
            for r in records:
                cur = await self._sqlite_conn.execute(
                    query,
                    (
                        r["id"],
                        r["symbol"],
                        r["filing_date"],
                        r.get("transaction_date"),
                        r["reporting_owner"],
                        r.get("owner_title"),
                        1 if r.get("is_director") else 0,
                        1 if r.get("is_officer") else 0,
                        1 if r.get("is_ten_percent") else 0,
                        r["transaction_type"],
                        r.get("shares"),
                        r.get("price_per_share"),
                        r.get("shares_owned_following"),
                        r.get("sec_form", "4"),
                        r.get("filing_url"),
                    ),
                )
                if cur.rowcount > 0:
                    count += 1
            await self._sqlite_conn.commit()
        else:
            assert self._pg_pool is not None
            query = """
            INSERT INTO insider_trades (
                id, symbol, filing_date, transaction_date, reporting_owner,
                owner_title, is_director, is_officer, is_ten_percent,
                transaction_type, shares, price_per_share, shares_owned_following,
                sec_form, filing_url, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, NOW())
            ON CONFLICT (id) DO UPDATE SET
                symbol = EXCLUDED.symbol,
                filing_date = EXCLUDED.filing_date,
                transaction_date = EXCLUDED.transaction_date,
                reporting_owner = EXCLUDED.reporting_owner,
                owner_title = EXCLUDED.owner_title,
                is_director = EXCLUDED.is_director,
                is_officer = EXCLUDED.is_officer,
                is_ten_percent = EXCLUDED.is_ten_percent,
                transaction_type = EXCLUDED.transaction_type,
                shares = EXCLUDED.shares,
                price_per_share = EXCLUDED.price_per_share,
                shares_owned_following = EXCLUDED.shares_owned_following,
                sec_form = EXCLUDED.sec_form,
                filing_url = EXCLUDED.filing_url
            """
            async with self._pg_pool.acquire() as conn:
                for r in records:
                    await conn.execute(
                        query,
                        r["id"],
                        r["symbol"],
                        r["filing_date"],
                        r.get("transaction_date"),
                        r["reporting_owner"],
                        r.get("owner_title"),
                        bool(r.get("is_director")),
                        bool(r.get("is_officer")),
                        bool(r.get("is_ten_percent")),
                        r["transaction_type"],
                        r.get("shares"),
                        r.get("price_per_share"),
                        r.get("shares_owned_following"),
                        r.get("sec_form", "4"),
                        r.get("filing_url"),
                    )
                    count += 1
        return count

    async def upsert_institutional_holdings(self, records: list[dict[str, Any]]) -> int:
        """Upsert Form 13F institutional holdings."""
        if not records:
            return 0
        count = 0
        if self.settings.is_sqlite:
            assert self._sqlite_conn is not None
            query = """
            INSERT INTO institutional_holdings (
                id, cik, institution_name, report_calendar_or_quarter,
                symbol, cusip, shares, market_value, investment_discretion,
                voting_authority_sole, sec_form, filing_url, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT (id) DO UPDATE SET
                shares = excluded.shares,
                market_value = excluded.market_value,
                investment_discretion = excluded.investment_discretion,
                voting_authority_sole = excluded.voting_authority_sole,
                filing_url = excluded.filing_url
            """
            for r in records:
                cur = await self._sqlite_conn.execute(
                    query,
                    (
                        r["id"],
                        r["cik"],
                        r["institution_name"],
                        r["report_calendar_or_quarter"],
                        r["symbol"],
                        r.get("cusip"),
                        r["shares"],
                        r.get("market_value"),
                        r.get("investment_discretion"),
                        r.get("voting_authority_sole"),
                        r.get("sec_form", "13F-HR"),
                        r.get("filing_url"),
                    ),
                )
                if cur.rowcount > 0:
                    count += 1
            await self._sqlite_conn.commit()
        else:
            assert self._pg_pool is not None
            query = """
            INSERT INTO institutional_holdings (
                id, cik, institution_name, report_calendar_or_quarter,
                symbol, cusip, shares, market_value, investment_discretion,
                voting_authority_sole, sec_form, filing_url, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
            ON CONFLICT (id) DO UPDATE SET
                shares = EXCLUDED.shares,
                market_value = EXCLUDED.market_value,
                investment_discretion = EXCLUDED.investment_discretion,
                voting_authority_sole = EXCLUDED.voting_authority_sole,
                filing_url = EXCLUDED.filing_url
            """
            async with self._pg_pool.acquire() as conn:
                for r in records:
                    await conn.execute(
                        query,
                        r["id"],
                        r["cik"],
                        r["institution_name"],
                        r["report_calendar_or_quarter"],
                        r["symbol"],
                        r.get("cusip"),
                        r["shares"],
                        r.get("market_value"),
                        r.get("investment_discretion"),
                        r.get("voting_authority_sole"),
                        r.get("sec_form", "13F-HR"),
                        r.get("filing_url"),
                    )
                    count += 1
        return count

    async def upsert_finra_otc_volume(self, records: list[dict[str, Any]]) -> int:
        """Upsert weekly FINRA OTC volume records."""
        if not records:
            return 0
        count = 0
        if self.settings.is_sqlite:
            assert self._sqlite_conn is not None
            query = """
            INSERT INTO finra_otc_volume (
                id, symbol, week_start_date, tier, otc_volume,
                total_trades, total_market_volume, dark_pool_share_pct, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT (id) DO UPDATE SET
                otc_volume = excluded.otc_volume,
                total_trades = excluded.total_trades,
                total_market_volume = excluded.total_market_volume,
                dark_pool_share_pct = excluded.dark_pool_share_pct
            """
            for r in records:
                cur = await self._sqlite_conn.execute(
                    query,
                    (
                        r["id"],
                        r["symbol"],
                        r["week_start_date"],
                        r["tier"],
                        r["otc_volume"],
                        r["total_trades"],
                        r.get("total_market_volume"),
                        r.get("dark_pool_share_pct"),
                    ),
                )
                if cur.rowcount > 0:
                    count += 1
            await self._sqlite_conn.commit()
        else:
            assert self._pg_pool is not None
            query = """
            INSERT INTO finra_otc_volume (
                id, symbol, week_start_date, tier, otc_volume,
                total_trades, total_market_volume, dark_pool_share_pct, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
            ON CONFLICT (id) DO UPDATE SET
                otc_volume = EXCLUDED.otc_volume,
                total_trades = EXCLUDED.total_trades,
                total_market_volume = EXCLUDED.total_market_volume,
                dark_pool_share_pct = EXCLUDED.dark_pool_share_pct
            """
            async with self._pg_pool.acquire() as conn:
                for r in records:
                    await conn.execute(
                        query,
                        r["id"],
                        r["symbol"],
                        r["week_start_date"],
                        r["tier"],
                        r["otc_volume"],
                        r["total_trades"],
                        r.get("total_market_volume"),
                        r.get("dark_pool_share_pct"),
                    )
                    count += 1
        return count

    async def upsert_cboe_daily_options(self, records: list[dict[str, Any]]) -> int:
        """Upsert CBOE daily options statistics."""
        if not records:
            return 0
        count = 0
        if self.settings.is_sqlite:
            assert self._sqlite_conn is not None
            query = """
            INSERT INTO cboe_daily_options (
                id, trade_date, total_call_volume, total_put_volume,
                total_volume, equity_pc_ratio, index_pc_ratio,
                total_pc_ratio, vix_volume, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT (trade_date) DO UPDATE SET
                total_call_volume = excluded.total_call_volume,
                total_put_volume = excluded.total_put_volume,
                total_volume = excluded.total_volume,
                equity_pc_ratio = excluded.equity_pc_ratio,
                index_pc_ratio = excluded.index_pc_ratio,
                total_pc_ratio = excluded.total_pc_ratio,
                vix_volume = excluded.vix_volume
            """
            for r in records:
                cur = await self._sqlite_conn.execute(
                    query,
                    (
                        r["id"],
                        r["trade_date"],
                        r["total_call_volume"],
                        r["total_put_volume"],
                        r["total_volume"],
                        r.get("equity_pc_ratio"),
                        r.get("index_pc_ratio"),
                        r.get("total_pc_ratio"),
                        r.get("vix_volume"),
                    ),
                )
                if cur.rowcount > 0:
                    count += 1
            await self._sqlite_conn.commit()
        else:
            assert self._pg_pool is not None
            query = """
            INSERT INTO cboe_daily_options (
                id, trade_date, total_call_volume, total_put_volume,
                total_volume, equity_pc_ratio, index_pc_ratio,
                total_pc_ratio, vix_volume, created_at
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
            async with self._pg_pool.acquire() as conn:
                for r in records:
                    await conn.execute(
                        query,
                        r["id"],
                        r["trade_date"],
                        r["total_call_volume"],
                        r["total_put_volume"],
                        r["total_volume"],
                        r.get("equity_pc_ratio"),
                        r.get("index_pc_ratio"),
                        r.get("total_pc_ratio"),
                        r.get("vix_volume"),
                    )
                    count += 1
        return count

    async def upsert_macro_indicators(self, records: list[dict[str, Any]]) -> int:
        """Upsert macroeconomic indicator series."""
        if not records:
            return 0
        count = 0
        if self.settings.is_sqlite:
            assert self._sqlite_conn is not None
            query = """
            INSERT INTO macro_indicators (
                id, series_id, indicator_name, date, value,
                frequency, units, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT (id) DO UPDATE SET
                value = excluded.value,
                frequency = excluded.frequency,
                units = excluded.units
            """
            for r in records:
                cur = await self._sqlite_conn.execute(
                    query,
                    (
                        r["id"],
                        r["series_id"],
                        r["indicator_name"],
                        r["date"],
                        r["value"],
                        r.get("frequency", "daily"),
                        r.get("units", "Percent"),
                    ),
                )
                if cur.rowcount > 0:
                    count += 1
            await self._sqlite_conn.commit()
        else:
            assert self._pg_pool is not None
            query = """
            INSERT INTO macro_indicators (
                id, series_id, indicator_name, date, value,
                frequency, units, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
            ON CONFLICT (id) DO UPDATE SET
                value = EXCLUDED.value,
                frequency = EXCLUDED.frequency,
                units = EXCLUDED.units
            """
            async with self._pg_pool.acquire() as conn:
                for r in records:
                    await conn.execute(
                        query,
                        r["id"],
                        r["series_id"],
                        r["indicator_name"],
                        r["date"],
                        r["value"],
                        r.get("frequency", "daily"),
                        r.get("units", "Percent"),
                    )
                    count += 1
        return count
