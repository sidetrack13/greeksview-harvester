# GreeksView Harvester Operational Runbook

This runbook details how to operate, configure, schedule, and troubleshoot all background data harvesting workers in the **GreeksView Harvester** repository.

---

## 1. System Architecture & Operating Principles

GreeksView Harvester is the **authoritative single source of truth** for sourcing, normalizing, and persisting all historical, fundamental, macroeconomic, and regulatory alpha datasets for the GreeksView analytics platform.

### Storage & Persistence Model
* **Local Storage (Tier 1)**: Embedded SQLite database (`greeksview_harvester.db`) optimized for high-throughput batch writes and zero-latency local analytical operations.
* **Production Synchronization (Tier 2)**: Bidirectional upsert synchronization engine (`harvester/core/sync.py`) replicating normalized records to production PostgreSQL.
* **Rate Pacing & Quota Governance**: Token-bucket rate pacers enforce strict provider quota ceilings (e.g., 30 requests/second and 1,200 requests/minute for Alpha Vantage) with automated burst detection regex and exponential cooldown backoffs.

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

# 2. Intraday Stock Bars (1-min and 5-min intervals)
uv run harvester run alphavantage --dataset intraday --symbols SPY,QQQ --limit 100

# 3. Settled End-of-Day Options Chains
uv run harvester run alphavantage --dataset options --symbols SPY,QQQ --days-back 30

# 4. Comprehensive Corporate Fundamentals & Earnings Statements
uv run harvester run alphavantage --dataset fundamentals --symbols AAPL,MSFT,NVDA

# 5. Corporate Actions (Cash Dividends & Stock Splits)
uv run harvester run alphavantage --dataset actions --symbols AAPL,TSLA

# 6. Market Reference & Discovery (Listing Status & ETF Profiles)
uv run harvester run alphavantage --dataset reference --symbols SPY,QQQ

# 7. Complete Multi-Dataset Pass (All Data Families for Universe)
uv run harvester run alphavantage --dataset all --symbols SPY

# Offline Sandbox Mode (Uses synthetic mock fixtures without burning API quota)
uv run harvester run alphavantage --dataset all --mock
```

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
# Synchronize all tables to production PostgreSQL
uv run harvester sync-pg

# Synchronize a specific table only
uv run harvester sync-pg --table stock_bars_daily

# Dry-run inspection (logs rows to be transferred without writing to target)
uv run harvester sync-pg --dry-run

# Specify custom source and destination database connection URLs
uv run harvester sync-pg \
  --source-url sqlite:///greeksview_harvester.db \
  --dest-url postgresql://postgres:password@prod-db:5432/greeksview
```

---

## 6. Operational Troubleshooting & Playbook

### Problem 1: Alpha Vantage HTTP 429 Throttle or Burst Notices
* **Symptom**: Logs show `"Alpha Vantage rate burst detected. Pausing for 2000ms cooldown"`.
* **Action**:
  1. The built-in `AlphaVantagePacer` automatically triggers a 2-second cooldown and absorbs burst warnings. No intervention is required.
  2. If throttling persists, verify that no other worker process or developer is concurrently using the same API key.
  3. Verify settings: `ALPHAVANTAGE_MAX_PER_SECOND=30` and `ALPHAVANTAGE_RPM=1200`.

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
