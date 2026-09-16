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
"""

import json
import logging
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from harvester.config import Settings, get_settings
from harvester.core.base_worker import BaseWorker, WorkerResult
from harvester.core.db import DatabaseManager
from harvester.workers.alphavantage.api_client import AlphaVantageClient, AlphaVantageError
from harvester.workers.alphavantage.pacer import AlphaVantagePacer

logger = logging.getLogger("harvester.workers.alphavantage")

DEFAULT_UNIVERSE = ["SPY", "QQQ", "IWM", "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA"]


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
        self.pacer = AlphaVantagePacer(
            max_per_second=self.settings.alphavantage_max_per_second,
            cooldown_ms=self.settings.alphavantage_cooldown_ms,
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
            res = [s.strip().upper() for s in symbols.split(",") if s.strip()]
        else:
            res = [s.strip().upper() for s in symbols if s.strip()]
        if limit is not None and limit > 0:
            res = res[:limit]
        return res

    async def download_daily_bars(
        self,
        db: DatabaseManager,
        symbols: list[str],
        outputsize: str = "compact",
        use_mock: bool = False,
    ) -> tuple[int, int]:
        """Download daily adjusted bars and persist to stock_bars_daily."""
        harvested = 0
        upserted = 0

        for sym in symbols:
            records = []
            if use_mock:
                # Generate synthetic daily bars for testing
                today = datetime.now(UTC).date()
                for d in range(10):
                    t_date = (today - timedelta(days=d)).isoformat()
                    records.append({
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
                    })
            else:
                data = await self.client.fetch_json(
                    "TIME_SERIES_DAILY_ADJUSTED",
                    {"symbol": sym, "outputsize": outputsize},
                )
                ts = data.get("Time Series (Daily)", {})
                for t_date, bar in ts.items():
                    try:
                        records.append({
                            "symbol": sym,
                            "trade_date": t_date,
                            "open": float(bar.get("1. open", 0)),
                            "high": float(bar.get("2. high", 0)),
                            "low": float(bar.get("3. low", 0)),
                            "close": float(bar.get("4. close", 0)),
                            "adjusted_close": float(bar.get("5. adjusted close", 0)),
                            "volume": int(bar.get("6. volume", 0)),
                            "dividend_amount": float(bar.get("7. dividend amount", 0)),
                            "split_coefficient": float(bar.get("8. split coefficient", 1)),
                        })
                    except (ValueError, TypeError) as exc:
                        logger.warning("Skipping malformed daily bar for %s (%s): %s", sym, t_date, exc)

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
    ) -> tuple[int, int]:
        """Download intraday stock bars and persist to stock_bars_intraday."""
        harvested = 0
        upserted = 0

        for sym in symbols:
            records = []
            if use_mock:
                now = datetime.now(UTC)
                for m in range(12):
                    ts_str = (now - timedelta(minutes=m * 5)).strftime("%Y-%m-%d %H:%M:%S")
                    records.append({
                        "symbol": sym,
                        "bar_timestamp": ts_str,
                        "interval": interval,
                        "open": 450.0 + m * 0.1,
                        "high": 450.5 + m * 0.1,
                        "low": 449.8 + m * 0.1,
                        "close": 450.2 + m * 0.1,
                        "volume": 25000 + m * 100,
                    })
            else:
                params: dict[str, Any] = {"symbol": sym, "interval": interval}
                if months and len(months) > 0:
                    params["month"] = months[0]
                data = await self.client.fetch_json("TIME_SERIES_INTRADAY", params)
                key = f"Time Series ({interval})"
                ts = data.get(key, {})
                for ts_str, bar in ts.items():
                    try:
                        records.append({
                            "symbol": sym,
                            "bar_timestamp": ts_str,
                            "interval": interval,
                            "open": float(bar.get("1. open", 0)),
                            "high": float(bar.get("2. high", 0)),
                            "low": float(bar.get("3. low", 0)),
                            "close": float(bar.get("4. close", 0)),
                            "volume": int(bar.get("5. volume", 0)),
                        })
                    except (ValueError, TypeError) as exc:
                        logger.warning("Skipping malformed intraday bar for %s (%s): %s", sym, ts_str, exc)

            harvested += len(records)
            if records:
                upserted += await db.upsert_stock_bars_intraday(records)

        return harvested, upserted

    async def download_historical_options(
        self,
        db: DatabaseManager,
        symbols: list[str],
        trade_dates: list[str] | None = None,
        days_back: int | None = None,
        moneyness_band_pct: float | None = None,
        prune_inactive: bool = False,
        use_mock: bool = False,
    ) -> tuple[int, int]:
        """Download end-of-day options chains and persist to options_chains_eod."""
        harvested = 0
        upserted = 0
        if trade_dates:
            target_dates = trade_dates
        elif days_back is not None and days_back > 1:
            today = datetime.now(UTC).date()
            target_dates = [
                (today - timedelta(days=i)).isoformat()
                for i in range(1, days_back + 1)
                if (today - timedelta(days=i)).weekday() < 5
            ]
        else:
            target_dates = [(datetime.now(UTC).date() - timedelta(days=1)).isoformat()]

        for sym in symbols:
            records = []
            if use_mock:
                for dt in target_dates:
                    mock_spot = 455.0
                    for strike in (300.0, 450.0, 455.0, 460.0, 600.0):
                        if (
                            moneyness_band_pct is not None
                            and moneyness_band_pct > 0
                            and abs(strike - mock_spot) / mock_spot > (moneyness_band_pct / 100.0)
                        ):
                            continue
                        for otype in ("call", "put"):
                            vol = 0 if strike == 600.0 else (10 if strike == 300.0 else 1200)
                            oi = 0 if strike == 600.0 else (50 if strike == 300.0 else 4500)
                            if prune_inactive and vol == 0 and oi == 0:
                                continue
                            cid = f"{sym}_{dt}_{strike:.0f}_{otype.upper()}"
                            records.append({
                                "contract_id": cid,
                                "symbol": sym,
                                "trade_date": dt,
                                "expiration": (datetime.now(UTC).date() + timedelta(days=30)).isoformat(),
                                "strike": strike,
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
                            })
            else:
                for dt in target_dates:
                    try:
                        data = await self.client.fetch_json("HISTORICAL_OPTIONS", {"symbol": sym, "date": dt})
                        chain = data.get("data", [])
                        spot: float | None = None
                        if moneyness_band_pct is not None and moneyness_band_pct > 0:
                            spot = await db.get_stock_close(sym, dt)
                            if spot is None and chain:
                                strikes_list = [float(r.get("strike", 0)) for r in chain if float(r.get("strike", 0)) > 0]
                                if strikes_list:
                                    strikes_list.sort()
                                    spot = strikes_list[len(strikes_list) // 2]

                        for row in chain:
                            vol = int(row.get("volume") or 0)
                            oi = int(row.get("open_interest") or 0)
                            if prune_inactive and vol == 0 and oi == 0:
                                continue

                            strike = float(row.get("strike", 0))
                            if (
                                moneyness_band_pct is not None
                                and moneyness_band_pct > 0
                                and spot is not None
                                and spot > 0
                                and abs(strike - spot) / spot > (moneyness_band_pct / 100.0)
                            ):
                                continue

                            cid = row.get("contractID") or f"{sym}_{dt}_{row.get('strike')}_{row.get('type')}"
                            records.append({
                                "contract_id": cid,
                                "symbol": sym,
                                "trade_date": dt,
                                "expiration": row.get("expiration"),
                                "strike": strike,
                                "option_type": str(row.get("type", "call")).lower(),
                                "last_price": float(row["last"]) if row.get("last") is not None else None,
                                "mark_price": float(row["mark"]) if row.get("mark") is not None else None,
                                "bid": float(row["bid"]) if row.get("bid") is not None else None,
                                "ask": float(row["ask"]) if row.get("ask") is not None else None,
                                "volume": vol,
                                "open_interest": oi,
                                "implied_volatility": float(row["implied_volatility"]) if row.get("implied_volatility") is not None else None,
                                "delta": float(row["delta"]) if row.get("delta") is not None else None,
                                "gamma": float(row["gamma"]) if row.get("gamma") is not None else None,
                                "theta": float(row["theta"]) if row.get("theta") is not None else None,
                                "vega": float(row["vega"]) if row.get("vega") is not None else None,
                                "rho": float(row["rho"]) if row.get("rho") is not None else None,
                            })
                    except AlphaVantageError as exc:
                        logger.warning("Options chain fetch error for %s on %s: %s", sym, dt, exc)

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
                    records.append({
                        "symbol": sym,
                        "fiscal_date_ending": "2024-06-30",
                        "report_type": rt,
                        "period_type": "annual",
                        "data_json": json.dumps({"Symbol": sym, "FiscalYear": 2024, "Metric": "Mock"}),
                    })
            else:
                for rt in report_types:
                    try:
                        data = await self.client.fetch_json(rt, {"symbol": sym})
                        if not data:
                            continue
                        fiscal_date = data.get("FiscalDateEnding") or data.get("LatestQuarter") or "LATEST"
                        records.append({
                            "symbol": sym,
                            "fiscal_date_ending": fiscal_date,
                            "report_type": rt,
                            "period_type": "annual",
                            "data_json": json.dumps(data),
                        })
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
                div_records.append({
                    "symbol": sym,
                    "ex_dividend_date": "2024-03-15",
                    "declaration_date": "2024-02-15",
                    "record_date": "2024-03-18",
                    "payment_date": "2024-03-29",
                    "amount": 1.78,
                })
                split_records.append({
                    "symbol": sym,
                    "effective_date": "2024-06-10",
                    "split_factor": 10.0,
                })
            else:
                try:
                    div_data = await self.client.fetch_json("DIVIDENDS", {"symbol": sym})
                    for item in div_data.get("data", []):
                        if item.get("ex_dividend_date") and item.get("amount"):
                            div_records.append({
                                "symbol": sym,
                                "ex_dividend_date": item["ex_dividend_date"],
                                "declaration_date": item.get("declaration_date"),
                                "record_date": item.get("record_date"),
                                "payment_date": item.get("payment_date"),
                                "amount": float(item["amount"]),
                            })
                except AlphaVantageError as exc:
                    logger.warning("Dividends fetch error for %s: %s", sym, exc)

                try:
                    split_data = await self.client.fetch_json("SPLITS", {"symbol": sym})
                    for item in split_data.get("data", []):
                        if item.get("effective_date") and item.get("split_factor"):
                            split_records.append({
                                "symbol": sym,
                                "effective_date": item["effective_date"],
                                "split_factor": float(item["split_factor"]),
                            })
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
            listing_records.append({
                "symbol": "SPY",
                "name": "SPDR S&P 500 ETF Trust",
                "exchange": "NYSE ARCA",
                "asset_type": "ETF",
                "ipo_date": "1993-01-22",
                "delisting_date": None,
                "status": "Active",
            })
            etf_records.append({
                "symbol": "SPY",
                "net_assets": 550000000000.0,
                "portfolio_turnover": 0.02,
                "dividend_yield": 0.0125,
                "expense_ratio": 0.0009,
                "holdings_json": json.dumps([{"symbol": "MSFT", "weight": 0.07}]),
                "sectors_json": json.dumps([{"sector": "Technology", "weight": 0.32}]),
            })
        else:
            try:
                listings = await self.client.fetch_csv("LISTING_STATUS")
                for row in listings:
                    if row.get("symbol"):
                        listing_records.append({
                            "symbol": row["symbol"].strip().upper(),
                            "name": row.get("name"),
                            "exchange": row.get("exchange"),
                            "asset_type": row.get("assetType"),
                            "ipo_date": row.get("ipoDate") if row.get("ipoDate") else None,
                            "delisting_date": row.get("delistingDate") if row.get("delistingDate") else None,
                            "status": row.get("status", "Active"),
                        })
            except AlphaVantageError as exc:
                logger.warning("Listing status CSV fetch error: %s", exc)

            etf_targets = symbols or ["SPY", "QQQ", "IWM"]
            for etf_sym in etf_targets:
                try:
                    data = await self.client.fetch_json("ETF_PROFILE", {"symbol": etf_sym})
                    if data and "net_assets" in data:
                        etf_records.append({
                            "symbol": etf_sym,
                            "net_assets": float(data.get("net_assets") or 0),
                            "portfolio_turnover": float(data.get("portfolio_turnover") or 0),
                            "dividend_yield": float(data.get("dividend_yield") or 0),
                            "expense_ratio": float(data.get("expense_ratio") or 0),
                            "holdings_json": json.dumps(data.get("holdings", [])),
                            "sectors_json": json.dumps(data.get("sectors", [])),
                        })
                except AlphaVantageError as exc:
                    logger.warning("ETF profile fetch error for %s: %s", etf_sym, exc)

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

        try:
            async with DatabaseManager(self.settings) as db:
                await db.initialize_tables()

                if selected_dataset in ("daily", "all"):
                    h, u = await self.download_daily_bars(db, target_symbols, use_mock=use_mock)
                    harvested += h
                    upserted += u

                if selected_dataset in ("intraday", "all"):
                    h, u = await self.download_intraday_bars(db, target_symbols, use_mock=use_mock)
                    harvested += h
                    upserted += u

                if selected_dataset in ("options", "all"):
                    h, u = await self.download_historical_options(
                        db,
                        target_symbols,
                        days_back=kwargs.get("days_back"),
                        moneyness_band_pct=kwargs.get("moneyness_band_pct"),
                        prune_inactive=bool(kwargs.get("prune_inactive", False)),
                        use_mock=use_mock,
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
            "pacer_stats": self.pacer.stats(),
        }
