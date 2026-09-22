# GreeksView Harvester Operational Runbook

This runbook details how to operate, configure, schedule, and troubleshoot all background data harvesting workers in the **GreeksView Harvester** repository.

---

## 1. System Architecture & Operating Principles

GreeksView Harvester is the **authoritative single source of truth** for sourcing, normalizing, and persisting all historical, fundamental, macroeconomic, and regulatory alpha datasets for the GreeksView analytics platform.

### Storage & Persistence Model
* **Local Storage (Tier 1)**: Embedded SQLite database (`greeksview_harvester.db`) optimized for high-throughput batch writes and zero-latency local analytical operations.
* **Production Synchronization (Tier 2)**: Bidirectional upsert synchronization engine (`harvester/core/sync.py`) replicating normalized records to production PostgreSQL.
* **Rate Pacing & Quota Governance**: Token-bucket rate pacers enforce provider quota ceilings with automated burst detection regex and cooldown backoffs. The Alpha Vantage pacer enforces both a per-second spacing and a per-minute cap, read from `ALPHAVANTAGE_MAX_PER_SECOND` and `ALPHAVANTAGE_RPM`. The key is shared with the GreeksView product, whose worst case already uses most of the key's budget, so the defaults are 1/second and 9/minute. Set them so the harvester plus the product stays inside the key's budget.

---

## 2. Worker Directory & Data Coverage

| Worker Name | Family / Source | Endpoints / Sources | Target Database Tables | Standard Ingestion Cadence |
| :--- | :--- | :--- | :--- | :--- |
| **`alphavantage`** | Alpha Vantage API | `TIME_SERIES_DAILY_ADJUSTED`<br>`TIME_SERIES_INTRADAY`<br>`HISTORICAL_OPTIONS`<br>`OVERVIEW`, `BALANCE_SHEET`, `INCOME_STATEMENT`, `CASH_FLOW`, `EARNINGS`<br>`DIVIDENDS`, `SPLITS`<br>`ETF_PROFILE`, `LISTING_STATUS` | `stock_bars_daily`<br>`stock_bars_intraday`<br>`options_chains_eod`<br>`company_fundamentals`<br>`corporate_dividends`<br>`corporate_splits`<br>`etf_profiles`<br>`listing_status` | **Nightly / Off-Market**<br>(8:00 PM – 7:00 AM ET)<br>Weekend fundamentals sweep |
| **`congressional`** | US House & Senate | House Clerk PDF Disclosures<br>Senate eFD Periodic Transaction Reports | `congressional_disclosures`<br>`congressional_trades` | **Every 4 Hours**<br>(Accelerated 15-min sweep Friday afternoons) |
| **`sec_edgar`** | SEC EDGAR Stream | Form 4 (Insider Transactions)<br>Form 8-K (Material Events) | `sec_insider_trades`<br>`sec_material_events` | **Every 15 Minutes**<br>(During market hours 9:30 AM – 4:30 PM ET) |
| **`fred_macro`** | St. Louis Fed FRED | CPI, Fed Funds Effective Rate, 10Y/2Y Yield Curves, GDP, Unemployment | `macro_indicators` | **Daily at 9:00 AM ET** |
| **`finra_darkpool`** | FINRA TRF | OTC Non-ATS Short Sale & Dark Pool Trading Volumes | `darkpool_volume_daily` | **Weekly on Monday morning** |
| **`cboe_options`** | CBOE Exchange | Total Exchange Options Volume & Put/Call Ratios | `cboe_options_volume` | **Daily at 5:00 PM ET** |

---

## 3. On-Demand CLI Invocations (`harvester run`)

All workers can be triggered manually via the command line for ad-hoc backfills, historical seeds, or local verification.

### Environment Setup
Before executing workers, ensure dependencies are installed and the virtual environment is active:
```bash
# Install dependencies using uv
uv sync

# Ensure environment variables are loaded (.env)
export DATABASE_URL="sqlite:///greeksview_harvester.db"
export ALPHAVANTAGE_API_KEY="<your_api_key>"
export POSTGRES_URL="postgresql://user:pass@host:5432/greeksview_prod"
```

### Inspecting System & Registered Workers
```bash
# Display CLI version
uv run harvester version

# List all registered workers and frequencies
uv run harvester list

# Perform health checks across all workers, databases, and upstream APIs
uv run harvester health
```

### Ingesting Alpha Vantage Datasets (`alphavantage`)
The Alpha Vantage worker supports dataset slicing via the `--dataset` option:

```bash
# 1. Daily Adjusted Stock Bars (20+ Years OHLCV, Cash Dividends, Splits)
uv run harvester run alphavantage --dataset daily --symbols SPY,QQQ,AAPL,NVDA,MSFT

# Daily backfill: --outputsize full returns the whole history (compact = latest 100 sessions)
uv run harvester run alphavantage --dataset daily --symbols SPY --outputsize full

# 2. Intraday Stock Bars: one request per month, outputsize=full, adjusted=false (as-traded).
#    Pre/post-market bars are kept by default (--no-extended-hours drops them).
#    bar_timestamp is the vendor's US/Eastern wall-clock time; sync-pg writes it to
#    PostgreSQL as the matching Eastern instant.
uv run harvester run alphavantage --dataset intraday --symbols SPY --interval 1min --months 2026-05,2026-06

# 3. Settled End-of-Day Options Chains
uv run harvester run alphavantage --dataset options --symbols SPY,QQQ --days-back 30

# Explicit sessions: a list and/or @file (one YYYY-MM-DD per line)
uv run harvester run alphavantage --dataset options --symbols SPY --trade-dates 2026-07-01,2026-07-02
uv run harvester run alphavantage --dataset options --symbols SPY --trade-dates @sessions.txt

# Real sessions only: the dates stored in stock_bars_daily for SPY (the 30 most recent)
uv run harvester run alphavantage --dataset options --symbols SPY,QQQ --sessions-from SPY --days-back 30

# Each options row is stored under its own `date`. Alpha Vantage answers a holiday or
# weekend with the previous session; the run summary prints "asked D, got D'".
# Rows missing a date, type, contract ID, expiration or strike are skipped and counted.
# Missing volume / open interest are stored as NULL, never 0.

# High-Density Options Ingestion with Storage Optimizations (Saves 60%-75% Disk)
# Keeps strikes within +/-40% of that session's stored close (not applied, and the full
# chain kept, when the close is unknown), and drops contracts whose reported volume AND
# open interest are both 0. Rows with unknown volume/OI are never dropped. Pruning hides
# builds-from-zero: a dropped contract has no baseline row on the day its OI starts to build.
uv run harvester run alphavantage --dataset options --symbols SPY,QQQ --days-back 252 --moneyness-band 40 --prune-inactive

# Auto-Resume & Interruption Protection (--skip-existing):
# Enabled by default (--skip-existing). Performs sub-millisecond B-Tree indexed lookups
# against options_chains_eod. If a symbol/date pair is already present in the database,
# the vendor API call is bypassed entirely, allowing large-scale backfills to be safely
# stopped and resumed without duplicate downloads. To force a full re-fetch: --no-skip-existing.
uv run harvester run alphavantage --dataset options --symbols SPY --days-back 800 --skip-existing

# Archive older options to compressed .csv.gz files and prune live database:
uv run harvester archive-options --days-to-keep 90 --output-dir ./archives --prune


# 4. Comprehensive Corporate Fundamentals & Earnings Statements
uv run harvester run alphavantage --dataset fundamentals --symbols AAPL,MSFT,NVDA

# 5. Corporate Actions (Cash Dividends & Stock Splits)
uv run harvester run alphavantage --dataset actions --symbols AAPL,TSLA

# 6. Market Reference & Discovery (Listing Status & ETF Profiles)
uv run harvester run alphavantage --dataset reference --symbols SPY,QQQ

# 7. Complete Multi-Dataset Pass (All Data Families for Universe)
uv run harvester run alphavantage --dataset all --symbols SPY

# Offline Sandbox Mode (Uses synthetic mock fixtures without burning API quota).
# Mock rows go to greeksview_harvester.mock.db, never to the file sync-pg pushes;
# --mock with a PostgreSQL --db-url is refused.
uv run harvester run alphavantage --dataset all --mock
```

### Universe Slicing & Large-Scale Ingestion (`@file` Syntax)

The Harvester CLI natively supports loading universe ticker lists from text files using the `@filename` syntax in the `--symbols` argument. This avoids OS command-line buffer limits (`ARG_MAX`) when orchestrating sweeps across thousands of tickers.

Pre-built, normalized universe reference files are provided in the repository root and `data/`:

| Universe File | Count | Scope & Asset Types | Sourcing Origin & Filtering |
| :--- | :---: | :--- | :--- |
| **`all_equities.txt`** | **14,439** | Full active US equity, ETF, and ADR universe | SEC & Alpha Vantage `LISTING_STATUS` |
| **`optionable_tickers.txt`** | **5,350** | Curated universe of equities (**3,710**) and ETFs (**1,640**) with listed options | Official Cboe Exchange Directories (`opt`, `cone`, `ctwo`, `exo`) |

Lines beginning with `#` and empty whitespace are automatically ignored.

```bash
# 1. Stocks & ETFs Universe (14,439 symbols) — Full 20+ Year Daily History
uv run harvester run alphavantage \
  --dataset daily \
  --outputsize full \
  --symbols @all_equities.txt

# 2. Stocks & ETFs Universe (14,439 symbols) — 5-Minute Intraday Bars
uv run harvester run alphavantage \
  --dataset intraday \
  --interval 5min \
  --outputsize full \
  --symbols @all_equities.txt

# 3. Optionable Universe (5,350 symbols) — Historical Options Chains with Moneyness & Activity Pruning
#    Keeps strikes within +/-40% of spot and skips inactive contracts (0 vol & 0 OI), saving 60-75% disk:
uv run harvester run alphavantage \
  --dataset options \
  --symbols @optionable_tickers.txt \
  --days-back 800 \
  --moneyness-band 40 \
  --prune-inactive \
  --skip-existing

# 4. Multi-Dataset Pass (All Data Families) for Optionable Universe (Authoritative 800-Day Ingestion)
uv run harvester run alphavantage \
  --dataset all \
  --symbols @optionable_tickers.txt \
  --days-back 800 \
  --interval 5min \
  --outputsize full \
  --moneyness-band 40 \
  --prune-inactive \
  --skip-existing \
  --db-url sqlite:///greeksview_harvester.db
```

### Auto-Resume & Idempotent Ingestion Engine

When running multi-day or multi-year historical sweeps (e.g. `--days-back 800`), processes may be paused, interrupted by rate-limit cooldowns, or interrupted by network reconnections.

* **Sub-Millisecond Verification**: The engine issues `SELECT DISTINCT trade_date FROM options_chains_eod WHERE symbol = ?` prior to fetching any options chain. Powered by composite index `idx_options_chains_sym_date(symbol, trade_date)`, this check executes in **< 1 ms**.
* **Zero Duplicate Downloads**: If a ticker already has all requested sessions stored in the database, the symbol is skipped immediately (0 network calls, 0 delay).
* **Seamless Partial Resumes**: If a symbol has partial coverage (e.g. 174 sessions stored out of 571), only the missing 397 sessions are queried from the vendor.
* **Audit & Honesty Accounting**: Skipped sessions are tracked in `HarvestReport.skipped["options_already_stored"]` and logged in the terminal summary.

### Storage & Infrastructure Sizing (Supabase Pro)

For an 800-calendar-day historical options load (~571 market sessions) across the 5,350 optionable tickers:
* **Average Pruned Contracts**: ~673,415 contracts per session across all symbols (~133 contracts/ticker).
* **Projected Database Volume**: ~384.5M options rows (~128.9 GB data + B-tree indexes) + ~19.3M daily bars (~3.2 GB) + ~10.1M intraday bars (~2.0 GB) = **~134.3 GB total footprint**.
* **Supabase Pro Operational Cost**: **~$40.75 / month** ($25.00 base + ~$15.75 for 126 GB disk overage at $0.125/GB).
* **Why Hot PostgreSQL is Preserved**: Keeping all 800 days in PostgreSQL preserves **sub-millisecond B-tree index traversal** (< 1ms per options chain, < 15ms for 52-week IV rank, < 50ms for multi-year vector backtests) without the multi-second latency penalties of fetching and decompressing cold S3 archives.

> **High-Throughput Commercial Pacer Tuning**:
> Alpha Vantage commercial tiers support up to **30 requests/second** and **1,200 requests/minute**. To maximize ingestion velocity while reserving headroom for concurrent GreeksView web services, configure the rate pacers in `.env`:
> ```ini
> ALPHAVANTAGE_RPM=1100
> ALPHAVANTAGE_MAX_PER_SECOND=20
> ```


### Ingesting Congressional Disclosures (`congressional`)
```bash
# Crawl House of Representatives PTRs for a specific year
uv run harvester house --year 2024 --limit 50

# Crawl Senate eFD filings for a specific year
uv run harvester senate --year 2024 --limit 50

# Generic worker execution for both chambers
uv run harvester run congressional --year 2024 --limit 100

# Dry-run preview without database writes
uv run harvester house --year 2024 --dry-run
```

### Ingesting Macro & Exchange Analytics
```bash
# SEC EDGAR Form 4 & 8-K Filings
uv run harvester run sec_edgar --days-back 7

# FRED Macroeconomic Indicators
uv run harvester run fred_macro --days-back 30

# FINRA Dark Pool & Short Volume Aggregates
uv run harvester run finra_darkpool --weeks-back 4

# CBOE Total Options Volume & Put/Call Ratios
uv run harvester run cboe_options --days-back 10

# Execute all registered workers sequentially
uv run harvester run-all
```

---

## 4. Automated Scheduling & Background Daemon

The Harvester daemon coordinates continuous background cron jobs using APScheduler and serves an HTTP health monitoring interface.

### Running the Daemon Locally
```bash
# Start the daemon on port 8080 with immediate startup sync
uv run harvester daemon --port 8080 --initial-sync

# Run single sync pass and shut down
uv run harvester daemon --run-once
```

### Recommended Crontab Configuration
On production servers, workers can be triggered via standard cron (`crontab -e`):

```cron
# ── GreeksView Harvester Automated Production Schedule ──
SHELL=/bin/bash
PATH=/usr/local/bin:/usr/bin:/bin
WORKDIR=/opt/greeksview-harvester

# 1. Market Open Universe & Reference Sync (8:30 AM ET, Mon-Fri)
30 8 * * 1-5 cd $WORKDIR && uv run harvester run alphavantage --dataset reference >> /var/log/harvester_ref.log 2>&1

# 2. Macro Indicators Ingestion (9:00 AM ET, Mon-Fri)
0 9 * * 1-5 cd $WORKDIR && uv run harvester run fred_macro --days-back 3 >> /var/log/harvester_fred.log 2>&1

# 3. SEC EDGAR Insider Trades (Every 15 mins during market hours, Mon-Fri)
*/15 9-16 * * 1-5 cd $WORKDIR && uv run harvester run sec_edgar --days-back 1 >> /var/log/harvester_sec.log 2>&1

# 4. CBOE Options Volume & P/C Ratios (5:00 PM ET, Mon-Fri)
0 17 * * 1-5 cd $WORKDIR && uv run harvester run cboe_options --days-back 1 >> /var/log/harvester_cboe.log 2>&1

# 5. Off-Market Daily Stock Bars Sweep (8:00 PM ET, Mon-Fri)
0 20 * * 1-5 cd $WORKDIR && uv run harvester run alphavantage --dataset daily >> /var/log/harvester_daily.log 2>&1

# 6. Off-Market Settled Options Chains Sweep (9:00 PM ET, Mon-Fri)
0 21 * * 1-5 cd $WORKDIR && uv run harvester run alphavantage --dataset options --days-back 1 >> /var/log/harvester_options.log 2>&1

# 7. Congressional STOCK Act Disclosures (Every 4 hours daily)
0 */4 * * * cd $WORKDIR && uv run harvester run congressional >> /var/log/harvester_congress.log 2>&1

# 8. FINRA Dark Pool Volume (Monday 6:00 AM ET)
0 6 * * 1 cd $WORKDIR && uv run harvester run finra_darkpool --weeks-back 2 >> /var/log/harvester_finra.log 2>&1

# 9. Weekend Fundamentals & Financial Statements Backfill (Saturday 2:00 AM ET)
0 2 * * 6 cd $WORKDIR && uv run harvester run alphavantage --dataset fundamentals >> /var/log/harvester_fund.log 2>&1

# 10. Database Synchronization to Production PostgreSQL (Hourly at minute 45)
45 * * * * cd $WORKDIR && uv run harvester sync-pg >> /var/log/harvester_sync.log 2>&1
```

### Running via Docker Compose
For containerized deployments, use `docker-compose.yml`:
```bash
# Build and launch daemon in detached mode
docker-compose up -d --build

# View real-time logs
docker-compose logs -f harvester

# Check service health
curl -s http://localhost:8080/health | jq .
```

---

## 5. PostgreSQL Synchronization (`harvester sync-pg`)

Data accumulated in local SQLite is synchronized incrementally into PostgreSQL:

```bash
# Synchronize all tables to production PostgreSQL (unrestricted history)
uv run harvester sync-pg

# Synchronize trailing 90 days of time-series data (options_chains_eod, stock_bars_daily, cboe_daily_options)
uv run harvester sync-pg --days-back 90

# Synchronize only settled options chains within the last 90 trading days
uv run harvester sync-pg --table options_chains_eod --days-back 90

# Synchronize a specific table only with custom batch size
uv run harvester sync-pg --table stock_bars_daily --batch-size 500

# Specify custom source and destination PostgreSQL connection URL
uv run harvester sync-pg \
  --sqlite-path greeksview_harvester.db \
  --pg-url postgresql://postgres:password@prod-db:5432/greeksview \
  --days-back 90
```

> **Mock data never syncs**: `sync-pg` refuses any SQLite file a `--mock` run has written to (the file carries a mock stamp in its SQLite header). Harvest real data into a fresh file.

> **Storage Optimization Strategy**: Keep deep historical backfill data (e.g. 800 days) in local SQLite or compressed parquet/gzip archives while selectively synchronizing only recent active windows (e.g. `--days-back 90`) into production PostgreSQL to conserve cloud database storage and keep cluster queries fast.


---

## 6. Operational Troubleshooting & Playbook

### Problem 1: Alpha Vantage HTTP 429 Throttle or Burst Notices
* **Symptom**: Logs show `"Alpha Vantage rate burst detected. Pausing for 2000ms cooldown"`.
* **Action**:
  1. The built-in `AlphaVantagePacer` automatically triggers a 2-second cooldown and absorbs burst warnings. No intervention is required.
  2. If throttling persists, verify that no other worker process or developer is concurrently using the same API key.
  3. Verify settings: `ALPHAVANTAGE_MAX_PER_SECOND` and `ALPHAVANTAGE_RPM` (defaults 1 and 9). The key is shared with the product; never raise them to the licence ceiling (30/second, 1,200/minute).

### Problem 2: Congressional PDF OCR Parse Failures
* **Symptom**: House PDF reports parse errors on scanned handwritten disclosures.
* **Action**:
  1. Check filing format: `uv run harvester house --year <YYYY> --limit 5`.
  2. Scanned image PDFs that lack embedded text are flagged with `ocr_failed` and skipped so structured parsing can proceed unimpeded.

### Problem 3: Database Lock on SQLite (`database is locked`)
* **Symptom**: Simultaneous writes encounter SQLite busy timeout.
* **Action**:
  1. SQLite connections are initialized with WAL mode (`PRAGMA journal_mode=WAL;`) and a 60-second busy timeout.
  2. If a lock persists, check for orphaned worker processes: `ps aux | grep harvester` and terminate stale instances.

### Problem 4: Verifying System Integrity & Test Suite
Before deploying or after modifying schema/workers, run the complete test suite:
```bash
uv run pytest
```
* **Acceptance Criteria**: 100% tests passing, test coverage must satisfy `>= 90.0%`.
