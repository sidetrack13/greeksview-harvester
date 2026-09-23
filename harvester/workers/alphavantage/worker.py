"""
GreeksView Harvester — Alpha Vantage Autonomous Data Worker
===========================================================
Autonomous background worker ingesting:
  - Daily Adjusted Stock Timeseries (TIME_SERIES_DAILY_ADJUSTED) -> stock_bars_daily
  - Intraday Stock Bars (TIME_SERIES_INTRADAY) -> stock_bars_intraday
  - Settled End-of-Day Options Chains (HISTORICAL_OPTIONS) -> options_chains_eod
  - Corporate Fundamentals (OVERVIEW, BALANCE_SHEET, etc.) -> company_fundamentals
  - Corporate Actions (DIVIDENDS, SPLITS) -> corporate_dividends, corporate_splits
  - Market Discovery & Registry (LISTING_STATUS, ETF_PROFILE) -> listing_status, etf_profiles

Harvester is the Single Source of Truth for sourcing Alpha Vantage data into GreeksView.
Adheres to the BaseWorker contract with rate-limited, resilient execution.

Honesty rules for stored rows:
  - A field the vendor did not send is stored as NULL or the row is skipped and
    counted; it never becomes 0 and never borrows a neighbouring value.
  - An options row is stored under its own ``date`` field, never the date asked
    for: Alpha Vantage answers a non-session date with the latest session it has.
  - Intraday ``bar_timestamp`` is stored in SQLite as the vendor's US/Eastern
    wall-clock string ("YYYY-MM-DD HH:MM:SS"); the Eastern zone is attached when a
    bar is written to PostgreSQL (see harvester.core.db.as_vendor_eastern).
"""

import asyncio
import json
import logging
import re
import time
from collections import Counter
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from harvester.config import Settings, get_settings
from harvester.core.base_worker import BaseWorker, WorkerResult
from harvester.core.db import MOCK_SQLITE_PATH, DatabaseManager
from harvester.workers.alphavantage.api_client import AlphaVantageClient, AlphaVantageError
from harvester.workers.alphavantage.pacer import AlphaVantagePacer

logger = logging.getLogger("harvester.workers.alphavantage")

# HOW MANY CHAIN FETCHES MAY BE IN FLIGHT AT ONCE. Not a rate limit — the pacer
# is the only thing that decides requests per second and per minute. This only
# decides how many of them may be WAITING on the vendor together, and the loop
# below used to allow exactly one, so throughput was 1/latency whatever the
# pacer allowed. Settings may override it with `alphavantage_options_concurrency`;
# read with getattr so this module works with or without that entry.
DEFAULT_OPTIONS_CONCURRENCY = 8

DEFAULT_UNIVERSE = ["SPY", "QQQ", "IWM", "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA"]

INTRADAY_INTERVALS = ("1min", "5min", "15min", "30min", "60min")
DAILY_OUTPUTSIZES = ("compact", "full")

_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ISO_MONTH = re.compile(r"^\d{4}-\d{2}$")
_VENDOR_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")

# Every field a stored bar needs. A bar missing any of them is skipped and counted.
_DAILY_FIELDS = (
    ("open", "1. open"),
    ("high", "2. high"),
    ("low", "3. low"),
    ("close", "4. close"),
    ("adjusted_close", "5. adjusted close"),
    ("volume", "6. volume"),
    ("dividend_amount", "7. dividend amount"),
    ("split_coefficient", "8. split coefficient"),
)
_INTRADAY_FIELDS = (
    ("open", "1. open"),
    ("high", "2. high"),
    ("low", "3. low"),
    ("close", "4. close"),
    ("volume", "5. volume"),
)


def is_iso_date(value: Any) -> bool:
    """True for a real calendar date written exactly as YYYY-MM-DD."""
    if not isinstance(value, str) or not _ISO_DATE.match(value):
        return False
    try:
        date.fromisoformat(value)
    except ValueError:
        return False
    return True


def parse_trade_dates(spec: str) -> list[str]:
    """Parse ``--trade-dates``: comma-separated YYYY-MM-DD and/or ``@file`` (one date per line).

    Blank lines and ``#`` comments in a file are ignored. Returns sorted unique dates.
    Raises ValueError on any token that is not a real YYYY-MM-DD date, or when no
    date is given, and OSError when a file cannot be read.
    """
    tokens: list[str] = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if part.startswith("@"):
            for line in Path(part[1:]).read_text().splitlines():
                line = line.split("#", 1)[0].strip()
                if line:
                    tokens.append(line)
        else:
            tokens.append(part)
    bad = [t for t in tokens if not is_iso_date(t)]
    if bad:
        raise ValueError(f"Not a YYYY-MM-DD date: {', '.join(bad)}")
    if not tokens:
        raise ValueError("No trade dates given.")
    return sorted(set(tokens))


def _optional_float(value: Any) -> float | None:
    """None or blank means the vendor did not send it; anything else must parse."""
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    return float(text)


_UNKNOWN_METRIC_TOKENS = frozenset(("", "n/a", "none", "null", "-", "nan", "undefined", "unknown"))


def _parse_metric_float(value: Any) -> float | None:
    """Parse a financial/reference metric float where vendor placeholders ('n/a', 'None', '-') represent None."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if text.lower() in _UNKNOWN_METRIC_TOKENS:
        return None
    try:
        return float(text.replace(",", ""))
    except (ValueError, TypeError):
        return None


def _optional_count(value: Any) -> int | None:
    """Volume / open interest: None when not sent, a whole non-negative number otherwise."""
    number = _optional_float(value)
    if number is None:
        return None
    if not number.is_integer() or number < 0:
        raise ValueError(f"not a whole count: {value!r}")
    return int(number)


@dataclass
class HarvestReport:
    """What a run could not store, and why. Carried in WorkerResult.metadata["report"]."""

    skipped: Counter[str] = field(default_factory=Counter)
    filtered: Counter[str] = field(default_factory=Counter)
    date_substitutions: list[dict[str, Any]] = field(default_factory=list)
    spot_unknown: list[str] = field(default_factory=list)
    empty_responses: list[str] = field(default_factory=list)
    fetch_errors: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "skipped": dict(self.skipped),
            "filtered": dict(self.filtered),
            "date_substitutions": list(self.date_substitutions),
            "spot_unknown": list(self.spot_unknown),
            "empty_responses": list(self.empty_responses),
            "fetch_errors": list(self.fetch_errors),
        }


class AlphaVantageWorker(BaseWorker):
    """Autonomous background worker harvesting all Alpha Vantage financial feeds."""

    name: str = "alphavantage"
    description: str = "Alpha Vantage Autonomous Financial Feeds Harvester (Stocks, Options, Fundamentals)"
    frequency: str = "daily"
    target_views: list[str] = [
        "View 01 (Max Pain)",
        "View 03 (Gamma Exposure)",
        "View 05 (Options Chain Grid)",
        "View 15 (Key Levels)",
        "View 16 (Volatility Smile)",
        "View 18 (Expected Move)",
        "View 19 (Max Pain Chart)",
        "View 20 (Gamma Flip)",
        "View 21 (Dealer Positioning)",
        "View 25 (Term Structure)",
        "View 26 (Skew Parameterization)",
        "View 34 (Fundamentals Overview)",
        "View 35 (Financial Statements)",
        "View 36 (Earnings Calendar & History)",
        "View 37 (Corporate Actions)",
    ]

    def __init__(self, settings: Settings | None = None, config: Any | None = None):
        super().__init__(settings or config or get_settings())
        self.settings: Settings = self.config
        configured = getattr(self.settings, "alphavantage_options_concurrency", None)
        self.options_concurrency: int = max(1, int(configured)) if configured else DEFAULT_OPTIONS_CONCURRENCY
        self.pacer = AlphaVantagePacer(
            max_per_second=self.settings.alphavantage_max_per_second,
            cooldown_ms=self.settings.alphavantage_cooldown_ms,
            requests_per_minute=self.settings.alphavantage_rpm,
        )
        self.client = AlphaVantageClient(
            api_key=self.settings.alphavantage_api_key,
            pacer=self.pacer,
            base_url=self.settings.alphavantage_base_url,
            timeout_seconds=self.settings.request_timeout_seconds,
        )

    def _normalize_symbols(self, symbols: list[str] | str | None, limit: int | None = None) -> list[str]:
        if symbols is None:
            res = list(DEFAULT_UNIVERSE)
        elif isinstance(symbols, str):
            res = []
            for part in symbols.split(","):
                part = part.strip()
                if not part:
                    continue
                if part.startswith("@"):
                    file_path = Path(part[1:].strip())
                    if file_path.is_file():
                        for line in file_path.read_text().splitlines():
                            line = line.split("#", 1)[0].strip()
                            if line:
                                res.append(line.upper())
                        continue
                res.append(part.upper())
        else:
            res = [s.strip().upper() for s in symbols if s.strip()]
        if limit is not None and limit > 0:
            res = res[:limit]
        return res

    @staticmethod
    def _parse_bar(
        bar: Any,
        fields: tuple[tuple[str, str], ...],
        report: HarvestReport,
        dataset: str,
    ) -> dict[str, Any] | None:
        """Parse one vendor bar. A missing or unparseable field skips the bar (counted)."""
        if not isinstance(bar, dict):
            report.skipped[f"{dataset}_malformed_bar"] += 1
            return None
        parsed: dict[str, Any] = {}
        for name, key in fields:
            raw = bar.get(key)
            if raw is None or str(raw).strip() == "":
                report.skipped[f"{dataset}_missing_field"] += 1
                return None
            try:
                parsed[name] = _optional_count(raw) if name == "volume" else float(str(raw).strip())
            except ValueError:
                report.skipped[f"{dataset}_malformed_field"] += 1
                return None
        return parsed

    async def download_daily_bars(
        self,
        db: DatabaseManager,
        symbols: list[str],
        outputsize: str = "compact",
        use_mock: bool = False,
        report: HarvestReport | None = None,
    ) -> tuple[int, int]:
        """Download daily adjusted bars and persist to stock_bars_daily.

        ``outputsize="full"`` returns the whole history (for backfills); the vendor's
        default ``compact`` returns only the latest 100 sessions.
        """
        if outputsize not in DAILY_OUTPUTSIZES:
            raise ValueError(f"outputsize must be one of {', '.join(DAILY_OUTPUTSIZES)}, got {outputsize!r}")
        rep = report if report is not None else HarvestReport()
        harvested = 0
        upserted = 0

        for sym in symbols:
            records = []
            if use_mock:
                # Generate synthetic daily bars for testing
                today = datetime.now(UTC).date()
                for d in range(10):
                    t_date = (today - timedelta(days=d)).isoformat()
                    records.append(
                        {
                            "symbol": sym,
                            "trade_date": t_date,
                            "open": 450.0 + d,
                            "high": 455.0 + d,
                            "low": 448.0 + d,
                            "close": 452.0 + d,
                            "adjusted_close": 452.0 + d,
                            "volume": 50000000 + d * 100000,
                            "dividend_amount": 0.0,
                            "split_coefficient": 1.0,
                        }
                    )
            else:
                try:
                    data = await self.client.fetch_json(
                        "TIME_SERIES_DAILY_ADJUSTED",
                        {"symbol": sym, "outputsize": outputsize},
                    )
                except AlphaVantageError as exc:
                    logger.warning("Daily bars fetch error for %s: %s", sym, exc)
                    rep.fetch_errors.append(f"TIME_SERIES_DAILY_ADJUSTED {sym}: status {exc.status} ({exc.label})")
                    continue
                ts = data.get("Time Series (Daily)")
                if not isinstance(ts, dict):
                    rep.fetch_errors.append(f"TIME_SERIES_DAILY_ADJUSTED {sym}: response had no 'Time Series (Daily)'")
                    continue
                for t_date, bar in ts.items():
                    if not is_iso_date(t_date):
                        rep.skipped["daily_malformed_date"] += 1
                        continue
                    parsed = self._parse_bar(bar, _DAILY_FIELDS, rep, "daily")
                    if parsed is None:
                        logger.warning("Skipping incomplete daily bar for %s (%s)", sym, t_date)
                        continue
                    records.append({"symbol": sym, "trade_date": t_date, **parsed})

            harvested += len(records)
            if records:
                upserted += await db.upsert_stock_bars_daily(records)

        return harvested, upserted

    async def download_intraday_bars(
        self,
        db: DatabaseManager,
        symbols: list[str],
        interval: str = "5min",
        months: list[str] | None = None,
        use_mock: bool = False,
        extended_hours: bool = True,
        report: HarvestReport | None = None,
    ) -> tuple[int, int]:
        """Download intraday stock bars and persist to stock_bars_intraday.

        One request per (symbol, month). Every request asks for ``outputsize=full``
        (the whole month, or the trailing 30 days when no month is given) and
        ``adjusted=false`` (as-traded prices). ``extended_hours`` defaults to True so
        pre-market bars (e.g. around 08:30 ET releases) are kept; readers filter
        regular hours themselves. Timestamps are stored as the vendor's US/Eastern
        wall-clock string.
        """
        if interval not in INTRADAY_INTERVALS:
            raise ValueError(f"interval must be one of {', '.join(INTRADAY_INTERVALS)}, got {interval!r}")
        month_list: list[str | None] = list(months) if months else [None]
        bad_months = [m for m in month_list if m is not None and not _ISO_MONTH.match(m)]
        if bad_months:
            raise ValueError(f"months must be YYYY-MM, got {', '.join(str(m) for m in bad_months)}")
        rep = report if report is not None else HarvestReport()
        harvested = 0
        upserted = 0

        for sym in symbols:
            records: list[dict[str, Any]] = []
            if use_mock:
                now = datetime.now(UTC)
                for m in range(12):
                    ts_str = (now - timedelta(minutes=m * 5)).strftime("%Y-%m-%d %H:%M:%S")
                    records.append(
                        {
                            "symbol": sym,
                            "bar_timestamp": ts_str,
                            "interval": interval,
                            "open": 450.0 + m * 0.1,
                            "high": 450.5 + m * 0.1,
                            "low": 449.8 + m * 0.1,
                            "close": 450.2 + m * 0.1,
                            "volume": 25000 + m * 100,
                        }
                    )
                harvested += len(records)
                if records:
                    upserted += await db.upsert_stock_bars_intraday(records)
                continue

            for month in month_list:
                params: dict[str, Any] = {
                    "symbol": sym,
                    "interval": interval,
                    "outputsize": "full",
                    "adjusted": "false",
                    "extended_hours": "true" if extended_hours else "false",
                }
                if month is not None:
                    params["month"] = month
                label = f"TIME_SERIES_INTRADAY {sym} {interval} {month or 'latest'}"
                try:
                    data = await self.client.fetch_json("TIME_SERIES_INTRADAY", params)
                except AlphaVantageError as exc:
                    logger.warning("%s fetch error: %s", label, exc)
                    rep.fetch_errors.append(f"{label}: status {exc.status} ({exc.label})")
                    continue
                ts = data.get(f"Time Series ({interval})")
                if not isinstance(ts, dict):
                    rep.fetch_errors.append(f"{label}: response had no 'Time Series ({interval})'")
                    continue
                month_records: list[dict[str, Any]] = []
                for ts_str, bar in ts.items():
                    if not isinstance(ts_str, str) or not _VENDOR_TIMESTAMP.match(ts_str):
                        rep.skipped["intraday_malformed_timestamp"] += 1
                        continue
                    parsed = self._parse_bar(bar, _INTRADAY_FIELDS, rep, "intraday")
                    if parsed is None:
                        logger.warning("Skipping incomplete intraday bar for %s (%s)", sym, ts_str)
                        continue
                    month_records.append({"symbol": sym, "bar_timestamp": ts_str, "interval": interval, **parsed})
                harvested += len(month_records)
                if month_records:
                    upserted += await db.upsert_stock_bars_intraday(month_records)

        return harvested, upserted

    @staticmethod
    def resolve_target_dates(trade_dates: list[str] | None, days_back: int | None) -> list[str]:
        """Dates to ask the vendor for. Explicit ``trade_dates`` win over ``days_back``.

        ``days_back`` walks calendar weekdays (holidays included); a holiday is then
        answered with the previous session and stored under that session's date.
        """
        if trade_dates:
            return list(trade_dates)
        today = datetime.now(UTC).date()
        if days_back is not None and days_back > 1:
            return [
                (today - timedelta(days=i)).isoformat()
                for i in range(1, days_back + 1)
                if (today - timedelta(days=i)).weekday() < 5
            ]
        return [(today - timedelta(days=1)).isoformat()]

    @staticmethod
    def _options_record(sym: str, row: Any, rep: HarvestReport) -> dict[str, Any] | None:
        """Parse one HISTORICAL_OPTIONS row, or skip it (counted) when it cannot be stored honestly."""
        if not isinstance(row, dict):
            rep.skipped["options_malformed_row"] += 1
            return None
        row_date = row.get("date")
        if not is_iso_date(row_date):
            rep.skipped["options_missing_date"] += 1
            return None
        option_type = str(row.get("type") or "").strip().lower()
        if option_type not in ("call", "put"):
            rep.skipped["options_missing_type"] += 1
            return None
        contract_id = str(row.get("contractID") or "").strip()
        if not contract_id:
            rep.skipped["options_missing_contract_id"] += 1
            return None
        expiration = row.get("expiration")
        if not is_iso_date(expiration):
            rep.skipped["options_missing_expiration"] += 1
            return None
        try:
            strike = _optional_float(row.get("strike"))
            record = {
                "contract_id": contract_id,
                "symbol": sym,
                "trade_date": row_date,
                "expiration": expiration,
                "strike": strike,
                "option_type": option_type,
                "last_price": _optional_float(row.get("last")),
                "mark_price": _optional_float(row.get("mark")),
                "bid": _optional_float(row.get("bid")),
                "ask": _optional_float(row.get("ask")),
                "volume": _optional_count(row.get("volume")),
                "open_interest": _optional_count(row.get("open_interest")),
                "implied_volatility": _optional_float(row.get("implied_volatility")),
                "delta": _optional_float(row.get("delta")),
                "gamma": _optional_float(row.get("gamma")),
                "theta": _optional_float(row.get("theta")),
                "vega": _optional_float(row.get("vega")),
                "rho": _optional_float(row.get("rho")),
            }
        except ValueError:
            rep.skipped["options_malformed_number"] += 1
            return None
        if strike is None or strike <= 0:
            rep.skipped["options_missing_strike"] += 1
            return None
        return record

    async def _one_chain(
        self, sym: str, trade_date: str
    ) -> tuple[str, dict[str, Any] | None, AlphaVantageError | None]:
        """One HISTORICAL_OPTIONS call, never raising.

        A raise inside ``asyncio.gather`` cancels its siblings, and a cancelled
        sibling is a call already paid for at the vendor and thrown away. The
        error is carried back instead and re-raised as a report line by the
        caller, in date order.
        """
        try:
            data = await self.client.fetch_json("HISTORICAL_OPTIONS", {"symbol": sym, "date": trade_date})
        except AlphaVantageError as exc:
            return trade_date, None, exc
        return trade_date, data, None

    async def _chains_in_date_order(
        self, sym: str, dates: list[str], rep: HarvestReport
    ) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        """Yield ``(date, payload)`` in DATE ORDER, fetching a window at a time.

        ONLY THE NETWORK IS PARALLEL. This loop used to await one call per
        date, so however wide the pacer was opened the worker never had more
        than a single request in flight and ran at 1/latency — measured at
        ~3.5 a second against a pacer allowing 25. Parsing, the ``spots``
        cache, ``records`` and every ``rep`` counter stay in the caller,
        serial and in date order, so a widened window changes THROUGHPUT and
        nothing else about what gets stored or reported.

        The window is a window and not one big gather because each payload is
        a whole day's chain: 550 of them in flight would be held in memory at
        once. The pacer, not this number, is what bounds the request rate —
        the window only has to be wide enough to keep the pacer busy
        (rate x latency), and wider costs memory without buying calls.
        """
        width = max(1, int(self.options_concurrency))
        for start in range(0, len(dates), width):
            window = dates[start : start + width]
            settled = await asyncio.gather(*(self._one_chain(sym, dt) for dt in window))
            for trade_date, data, exc in settled:
                if exc is not None:
                    self._note_fetch_error(sym, trade_date, exc, rep)
                    continue
                if data is None:  # pragma: no cover - defensive; _one_chain pairs None with an exc
                    continue
                yield trade_date, data

    @staticmethod
    def _note_fetch_error(sym: str, trade_date: str, exc: AlphaVantageError, rep: HarvestReport) -> None:
        """One refused session, logged and reported. Shared so both fetch
        strategies below produce byte-identical report lines."""
        logger.warning("Options chain fetch error for %s on %s: %s", sym, trade_date, exc)
        rep.fetch_errors.append(f"HISTORICAL_OPTIONS {sym} {trade_date}: status {exc.status} ({exc.label})")

    async def _fetch_whole_symbol(
        self, sym: str, dates: list[str]
    ) -> tuple[str, list[tuple[str, dict[str, Any] | None, AlphaVantageError | None]]]:
        """Every session of ONE symbol, in flight together, settled in date order."""
        return sym, list(await asyncio.gather(*(self._one_chain(sym, dt) for dt in dates)))

    async def _symbols_in_order(
        self,
        db: DatabaseManager,
        symbols: list[str],
        target_dates: list[str],
        skip_existing: bool,
        use_mock: bool,
        rep: HarvestReport,
    ) -> AsyncIterator[tuple[str, list[str], list[tuple[str, dict[str, Any] | None, AlphaVantageError | None]] | None]]:
        """Yield ``(symbol, needed_dates, settled)`` in SYMBOL ORDER.

        TWO SHAPES OF RUN, ONE WINDOW. The 800-day backfill asks for ~550
        sessions of one symbol; the daily run asks for ONE session of ~5,350
        symbols. Widening dates alone fixes the first and does nothing for the
        second, where every symbol's date list is a single call and the symbol
        loop was what ran one at a time.

        So a symbol is only worth running beside another when its own dates do
        not already fill the window::

            symbol_width = options_concurrency // len(target_dates)

        One date a symbol gives the whole window to symbols; 550 gives it to
        dates and keeps symbols serial, which is exactly the behaviour the
        backfill already had. Either way at most ``options_concurrency``
        requests are in flight and at most that many payloads are held, which
        is the point: a window of symbols times a window of dates would be the
        product of the two, and a session's chain is megabytes.

        ``settled`` is the prefetched result when whole symbols were fetched
        together, and None when the caller should stream this symbol's dates
        through the window instead. Everything that touches ``rep`` happens
        here in symbol order, or in the caller — never inside a gather.
        """
        per_symbol = max(1, len(target_dates))
        symbol_width = max(1, int(self.options_concurrency) // per_symbol)

        for start in range(0, len(symbols), symbol_width):
            batch = symbols[start : start + symbol_width]
            plans: list[tuple[str, list[str]]] = []
            for sym in batch:
                existing_dates: set[str] = set()
                if skip_existing:
                    existing_dates = await db.get_stored_options_dates(sym)
                needed_dates = [dt for dt in target_dates if dt not in existing_dates]
                skipped_dates_count = len(target_dates) - len(needed_dates)
                if skipped_dates_count > 0:
                    rep.skipped["options_already_stored"] += skipped_dates_count
                    logger.info(
                        "Skipping %d already-stored options sessions for %s (%d remaining)",
                        skipped_dates_count,
                        sym,
                        len(needed_dates),
                    )
                if needed_dates:
                    plans.append((sym, needed_dates))

            prefetched: dict[str, list[tuple[str, dict[str, Any] | None, AlphaVantageError | None]]] = {}
            if plans and not use_mock and symbol_width > 1:
                prefetched = dict(await asyncio.gather(*(self._fetch_whole_symbol(s, d) for s, d in plans)))

            for sym, needed_dates in plans:
                yield sym, needed_dates, prefetched.get(sym)

    async def _chains_for(
        self,
        sym: str,
        dates: list[str],
        rep: HarvestReport,
        settled: list[tuple[str, dict[str, Any] | None, AlphaVantageError | None]] | None,
    ) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        """One symbol's chains in DATE ORDER, however they were fetched."""
        if settled is None:
            async for item in self._chains_in_date_order(sym, dates, rep):
                yield item
            return
        for trade_date, data, exc in settled:
            if exc is not None:
                self._note_fetch_error(sym, trade_date, exc, rep)
                continue
            if data is None:  # pragma: no cover - defensive; _one_chain pairs None with an exc
                continue
            yield trade_date, data

    async def download_historical_options(
        self,
        db: DatabaseManager,
        symbols: list[str],
        trade_dates: list[str] | None = None,
        days_back: int | None = None,
        moneyness_band_pct: float | None = None,
        prune_inactive: bool = False,
        skip_existing: bool = True,
        use_mock: bool = False,
        report: HarvestReport | None = None,
    ) -> tuple[int, int]:
        """Download end-of-day options chains and persist to options_chains_eod.

        Each row is stored under its own ``date``. When the vendor answers date D
        with a different session D' (a holiday, a weekend, a date not yet settled),
        the run records "asked D, got D'" in ``report.date_substitutions``.

        ``moneyness_band_pct`` keeps strikes within +/-N% of that session's stored
        close. When the close is unknown the band is NOT applied (the full chain is
        stored) and the session is listed in ``report.spot_unknown``.

        ``prune_inactive`` drops contracts whose reported volume AND open interest
        are both 0. It never drops a row whose volume or open interest is unknown
        (NULL). Pruning hides builds-from-zero: a contract dropped at 0 OI has no
        stored baseline on the day its open interest starts to build.

        ``skip_existing`` checks ``options_chains_eod`` for dates already stored for
        each symbol and skips querying the vendor for them, enabling fast auto-resume.
        """
        rep = report if report is not None else HarvestReport()
        harvested = 0
        upserted = 0
        target_dates = self.resolve_target_dates(trade_dates, days_back)
        band = moneyness_band_pct if moneyness_band_pct is not None and moneyness_band_pct > 0 else None

        async for sym, needed_dates, settled in self._symbols_in_order(
            db, symbols, target_dates, skip_existing, use_mock, rep
        ):
            records: list[dict[str, Any]] = []
            if use_mock:
                for dt in needed_dates:
                    mock_spot = 455.0
                    for mock_strike in (300.0, 450.0, 455.0, 460.0, 600.0):
                        if band is not None and abs(mock_strike - mock_spot) / mock_spot > (band / 100.0):
                            continue
                        for otype in ("call", "put"):
                            vol = 0 if mock_strike == 600.0 else (10 if mock_strike == 300.0 else 1200)
                            oi = 0 if mock_strike == 600.0 else (50 if mock_strike == 300.0 else 4500)
                            if prune_inactive and vol == 0 and oi == 0:
                                continue
                            cid = f"{sym}_{dt}_{mock_strike:.0f}_{otype.upper()}"
                            records.append(
                                {
                                    "contract_id": cid,
                                    "symbol": sym,
                                    "trade_date": dt,
                                    "expiration": (datetime.now(UTC).date() + timedelta(days=30)).isoformat(),
                                    "strike": mock_strike,
                                    "option_type": otype,
                                    "last_price": 5.25,
                                    "mark_price": 5.20,
                                    "bid": 5.15,
                                    "ask": 5.25,
                                    "volume": vol,
                                    "open_interest": oi,
                                    "implied_volatility": 0.185,
                                    "delta": 0.52 if otype == "call" else -0.48,
                                    "gamma": 0.035,
                                    "theta": -0.045,
                                    "vega": 0.12,
                                    "rho": 0.05,
                                }
                            )
            else:
                spots: dict[str, float | None] = {}
                async for dt, data in self._chains_for(sym, needed_dates, rep, settled):
                    chain = data.get("data")
                    if not isinstance(chain, list) or not chain:
                        rep.empty_responses.append(f"{sym} {dt}")
                        continue

                    got_dates: set[str] = set()
                    for row in chain:
                        rec = self._options_record(sym, row, rep)
                        if rec is None:
                            continue
                        row_date = rec["trade_date"]
                        got_dates.add(row_date)

                        vol = rec["volume"]
                        oi = rec["open_interest"]
                        if prune_inactive and vol is not None and oi is not None and vol == 0 and oi == 0:
                            rep.filtered["options_pruned_inactive"] += 1
                            continue

                        if band is not None:
                            if row_date not in spots:
                                spots[row_date] = await db.get_stock_close(sym, row_date)
                                if spots[row_date] is None:
                                    rep.spot_unknown.append(f"{sym} {row_date}")
                                    logger.warning(
                                        "No stored close for %s on %s: moneyness band not applied, full chain kept",
                                        sym,
                                        row_date,
                                    )
                            spot = spots[row_date]
                            if spot is not None and spot > 0 and abs(rec["strike"] - spot) / spot > (band / 100.0):
                                rep.filtered["options_outside_band"] += 1
                                continue

                        records.append(rec)

                    if got_dates and got_dates != {dt}:
                        rep.date_substitutions.append({"symbol": sym, "asked": dt, "got": sorted(got_dates)})
                        logger.warning("HISTORICAL_OPTIONS %s: asked %s, got %s", sym, dt, ", ".join(sorted(got_dates)))

            harvested += len(records)
            if records:
                upserted += await db.upsert_options_chains_eod(records)

        return harvested, upserted

    async def download_fundamentals(
        self,
        db: DatabaseManager,
        symbols: list[str],
        use_mock: bool = False,
    ) -> tuple[int, int]:
        """Download fundamental financial reports and persist to company_fundamentals."""
        harvested = 0
        upserted = 0
        report_types = ["OVERVIEW", "BALANCE_SHEET", "INCOME_STATEMENT", "CASH_FLOW", "EARNINGS"]

        for sym in symbols:
            records = []
            if use_mock:
                for rt in report_types:
                    records.append(
                        {
                            "symbol": sym,
                            "fiscal_date_ending": "2024-06-30",
                            "report_type": rt,
                            "period_type": "annual",
                            "data_json": json.dumps({"Symbol": sym, "FiscalYear": 2024, "Metric": "Mock"}),
                        }
                    )
            else:
                for rt in report_types:
                    try:
                        data = await self.client.fetch_json(rt, {"symbol": sym})
                        if not data:
                            continue
                        fiscal_date = data.get("FiscalDateEnding") or data.get("LatestQuarter") or "LATEST"
                        records.append(
                            {
                                "symbol": sym,
                                "fiscal_date_ending": fiscal_date,
                                "report_type": rt,
                                "period_type": "annual",
                                "data_json": json.dumps(data),
                            }
                        )
                    except AlphaVantageError as exc:
                        logger.warning("Fundamentals %s fetch error for %s: %s", rt, sym, exc)

            harvested += len(records)
            if records:
                upserted += await db.upsert_company_fundamentals(records)

        return harvested, upserted

    async def download_corporate_actions(
        self,
        db: DatabaseManager,
        symbols: list[str],
        use_mock: bool = False,
    ) -> tuple[int, int]:
        """Download dividends and splits, persisting to corporate_dividends and corporate_splits."""
        harvested = 0
        upserted = 0

        for sym in symbols:
            div_records = []
            split_records = []
            if use_mock:
                div_records.append(
                    {
                        "symbol": sym,
                        "ex_dividend_date": "2024-03-15",
                        "declaration_date": "2024-02-15",
                        "record_date": "2024-03-18",
                        "payment_date": "2024-03-29",
                        "amount": 1.78,
                    }
                )
                split_records.append(
                    {
                        "symbol": sym,
                        "effective_date": "2024-06-10",
                        "split_factor": 10.0,
                    }
                )
            else:
                try:
                    div_data = await self.client.fetch_json("DIVIDENDS", {"symbol": sym})
                    for item in div_data.get("data", []):
                        if item.get("ex_dividend_date") and item.get("amount") is not None:
                            amt = _parse_metric_float(item["amount"])
                            if amt is not None:
                                div_records.append(
                                    {
                                        "symbol": sym,
                                        "ex_dividend_date": item["ex_dividend_date"],
                                        "declaration_date": item.get("declaration_date"),
                                        "record_date": item.get("record_date"),
                                        "payment_date": item.get("payment_date"),
                                        "amount": amt,
                                    }
                                )
                except AlphaVantageError as exc:
                    logger.warning("Dividends fetch error for %s: %s", sym, exc)

                try:
                    split_data = await self.client.fetch_json("SPLITS", {"symbol": sym})
                    for item in split_data.get("data", []):
                        if item.get("effective_date") and item.get("split_factor") is not None:
                            factor = _parse_metric_float(item["split_factor"])
                            if factor is not None:
                                split_records.append(
                                    {
                                        "symbol": sym,
                                        "effective_date": item["effective_date"],
                                        "split_factor": factor,
                                    }
                                )
                except AlphaVantageError as exc:
                    logger.warning("Splits fetch error for %s: %s", sym, exc)

            harvested += len(div_records) + len(split_records)
            if div_records:
                upserted += await db.upsert_corporate_dividends(div_records)
            if split_records:
                upserted += await db.upsert_corporate_splits(split_records)

        return harvested, upserted

    async def download_reference_data(
        self,
        db: DatabaseManager,
        symbols: list[str] | None = None,
        use_mock: bool = False,
    ) -> tuple[int, int]:
        """Download market listing status and ETF profiles."""
        harvested = 0
        upserted = 0

        listing_records = []
        etf_records = []

        if use_mock:
            listing_records.append(
                {
                    "symbol": "SPY",
                    "name": "SPDR S&P 500 ETF Trust",
                    "exchange": "NYSE ARCA",
                    "asset_type": "ETF",
                    "ipo_date": "1993-01-22",
                    "delisting_date": None,
                    "status": "Active",
                }
            )
            etf_records.append(
                {
                    "symbol": "SPY",
                    "net_assets": 550000000000.0,
                    "portfolio_turnover": 0.02,
                    "dividend_yield": 0.0125,
                    "expense_ratio": 0.0009,
                    "holdings_json": json.dumps([{"symbol": "MSFT", "weight": 0.07}]),
                    "sectors_json": json.dumps([{"sector": "Technology", "weight": 0.32}]),
                }
            )
        else:
            try:
                listings = await self.client.fetch_csv("LISTING_STATUS")
                for row in listings:
                    if row.get("symbol"):
                        ipo = row.get("ipoDate")
                        delist = row.get("delistingDate")
                        listing_records.append(
                            {
                                "symbol": row["symbol"].strip().upper(),
                                "name": row.get("name"),
                                "exchange": row.get("exchange"),
                                "asset_type": row.get("assetType"),
                                "ipo_date": ipo if is_iso_date(ipo) else None,
                                "delisting_date": delist if is_iso_date(delist) else None,
                                "status": row.get("status", "Active"),
                            }
                        )
            except AlphaVantageError as exc:
                logger.warning("Listing status CSV fetch error: %s", exc)

            etf_targets = symbols or ["SPY", "QQQ", "IWM"]
            for etf_sym in etf_targets:
                try:
                    data = await self.client.fetch_json("ETF_PROFILE", {"symbol": etf_sym})
                    if data and isinstance(data, dict) and "net_assets" in data:
                        etf_records.append(
                            {
                                "symbol": etf_sym,
                                "net_assets": _parse_metric_float(data.get("net_assets")),
                                "portfolio_turnover": _parse_metric_float(data.get("portfolio_turnover")),
                                "dividend_yield": _parse_metric_float(data.get("dividend_yield")),
                                "expense_ratio": _parse_metric_float(data.get("expense_ratio")),
                                "holdings_json": json.dumps(data.get("holdings", [])),
                                "sectors_json": json.dumps(data.get("sectors", [])),
                            }
                        )
                except AlphaVantageError as exc:
                    logger.warning("ETF profile fetch error for %s: %s", etf_sym, exc)
                except (ValueError, TypeError) as exc:
                    logger.warning("ETF profile parse error for %s: %s", etf_sym, exc)

        harvested += len(listing_records) + len(etf_records)
        if listing_records:
            upserted += await db.upsert_listing_status(listing_records)
        if etf_records:
            upserted += await db.upsert_etf_profiles(etf_records)

        return harvested, upserted

    async def run_once(
        self,
        dataset: str = "daily",
        symbols: list[str] | str | None = None,
        limit: int | None = None,
        use_mock: bool = False,
        **kwargs,
    ) -> WorkerResult:
        """Execute Alpha Vantage harvesting pass."""
        start_time = time.time()
        harvested = 0
        upserted = 0
        errors: list[str] = []

        target_symbols = self._normalize_symbols(symbols or kwargs.get("symbols"), limit=limit or kwargs.get("limit"))
        selected_dataset = (dataset or kwargs.get("dataset", "daily")).lower().strip()

        logger.info(
            "Starting Alpha Vantage Harvester [Dataset=%s, Symbols=%d, Mock=%s]",
            selected_dataset,
            len(target_symbols),
            use_mock,
        )

        if not use_mock and not self.client.api_key:
            err_msg = (
                "Alpha Vantage API key is missing. Set ALPHAVANTAGE_API_KEY in your environment, "
                "configure it in .env (or ../greeksview/.env), or pass --api-key <key>. "
                "For offline simulation, use --mock."
            )
            logger.error(err_msg)
            return WorkerResult(
                worker=self.name,
                status="failed",
                records_harvested=0,
                records_upserted=0,
                duration_seconds=round(time.time() - start_time, 2),
                errors=[err_msg],
            )

        run_settings = self.settings
        if use_mock:
            # Fabricated rows never share a file with real ones, and never reach PostgreSQL.
            if not run_settings.is_sqlite:
                err_msg = (
                    "Mock runs never write to PostgreSQL. Omit --db-url (mock rows go to "
                    f"{MOCK_SQLITE_PATH}) or pass a sqlite:/// file."
                )
                logger.error(err_msg)
                return WorkerResult(
                    worker=self.name,
                    status="failed",
                    duration_seconds=round(time.time() - start_time, 2),
                    errors=[err_msg],
                )
            if not run_settings.database_url:
                run_settings = run_settings.model_copy(update={"database_url": f"sqlite:///{MOCK_SQLITE_PATH}"})

        report = HarvestReport()
        try:
            async with DatabaseManager(run_settings) as db:
                await db.initialize_tables()
                if use_mock:
                    await db.mark_mock_written()

                if selected_dataset in ("daily", "all"):
                    h, u = await self.download_daily_bars(
                        db,
                        target_symbols,
                        outputsize=kwargs.get("outputsize") or "compact",
                        use_mock=use_mock,
                        report=report,
                    )
                    harvested += h
                    upserted += u

                if selected_dataset in ("intraday", "all"):
                    h, u = await self.download_intraday_bars(
                        db,
                        target_symbols,
                        interval=kwargs.get("interval") or "5min",
                        months=kwargs.get("months"),
                        use_mock=use_mock,
                        extended_hours=bool(kwargs.get("extended_hours", True)),
                        report=report,
                    )
                    harvested += h
                    upserted += u

                if selected_dataset in ("options", "all"):
                    trade_dates = kwargs.get("trade_dates")
                    if isinstance(trade_dates, str):
                        trade_dates = parse_trade_dates(trade_dates)
                    sessions_from = kwargs.get("sessions_from")
                    days_back = kwargs.get("days_back")
                    if trade_dates and sessions_from:
                        raise ValueError("Pass --trade-dates or --sessions-from, not both.")
                    if sessions_from:
                        # Real sessions only: the dates the vendor reported daily bars for.
                        trade_dates = await db.get_stored_sessions(sessions_from, limit=days_back)
                        if not trade_dates:
                            raise ValueError(
                                f"No stored daily bars for {sessions_from.upper()}; "
                                "harvest --dataset daily for it first."
                            )
                        days_back = None
                    skip_existing = bool(kwargs.get("skip_existing", True))
                    h, u = await self.download_historical_options(
                        db,
                        target_symbols,
                        trade_dates=trade_dates,
                        days_back=days_back,
                        moneyness_band_pct=kwargs.get("moneyness_band_pct"),
                        prune_inactive=bool(kwargs.get("prune_inactive", False)),
                        skip_existing=skip_existing,
                        use_mock=use_mock,
                        report=report,
                    )
                    harvested += h
                    upserted += u

                if selected_dataset in ("fundamentals", "all"):
                    h, u = await self.download_fundamentals(db, target_symbols, use_mock=use_mock)
                    harvested += h
                    upserted += u

                if selected_dataset in ("actions", "corporate_actions", "all"):
                    h, u = await self.download_corporate_actions(db, target_symbols, use_mock=use_mock)
                    harvested += h
                    upserted += u

                if selected_dataset in ("reference", "all"):
                    h, u = await self.download_reference_data(db, target_symbols, use_mock=use_mock)
                    harvested += h
                    upserted += u

        except Exception as exc:
            logger.error("Alpha Vantage Harvester error: %s", exc, exc_info=True)
            errors.append(str(exc))

        # A request we could not make is an error, not an empty answer.
        errors.extend(report.fetch_errors)
        duration = round(time.time() - start_time, 2)
        status = "success" if not errors else ("partial" if upserted > 0 else "failed")

        return WorkerResult(
            worker=self.name,
            status=status,
            records_harvested=harvested,
            records_upserted=upserted,
            duration_seconds=duration,
            errors=errors,
            metadata={
                "dataset": selected_dataset,
                "symbols_count": len(target_symbols),
                "mock": use_mock,
                "pacer_stats": self.pacer.stats(),
                "report": report.as_dict(),
            },
        )

    async def health(self) -> dict[str, Any]:
        """Perform health check on pacer, db, and API configuration."""
        db_ok = False
        try:
            async with DatabaseManager(self.settings) as db:
                await db.connect()
                db_ok = True
        except Exception:
            db_ok = False

        return {
            "worker": self.name,
            "status": "healthy" if db_ok else "degraded",
            "database_connected": db_ok,
            "api_key_configured": bool(self.settings.alphavantage_api_key),
            "max_per_second": self.pacer.max_per_second,
            "requests_per_minute": self.pacer.requests_per_minute,
            "pacer_stats": self.pacer.stats(),
        }
