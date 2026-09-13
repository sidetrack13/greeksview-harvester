# GreeksView Data Harvester (`greeksview-harvester`)

Autonomous, resilient background data harvester and worker suite powering the open-source data feeds for the **GreeksView Market Analytics Workstation** (`/analytics/terminal`).

---

## 🏛️ Background & Scope

While real-time option chain Greeks and order routing connect directly to user broker feeds (Alpaca, Tradier, Schwab), and vendor fundamentals/equities connect via Alpha Vantage, **48% of the workstation views** rely on authoritative public regulatory filings, government datasets, exchange volume reports, and macro statistics.

`greeksview-harvester` consolidates all background harvesting workers into a single high-performance, modular Python repository where each worker operates independently with its own schedule, rate limits, schema, and isolated failure domain.

---

## 💻 Quick Start & Local Execution

### 1. Prerequisites & Installation

```bash
# Clone the repository
git clone https://github.com/sidetrack13/greeksview-harvester.git
cd greeksview-harvester

# Install dependencies with uv (recommended):
uv sync --extra dev

# Symlink CLI binaries to your user PATH (~/.local/bin) for global access:
ln -sf $(pwd)/.venv/bin/harvester ~/.local/bin/harvester
ln -sf $(pwd)/.venv/bin/crawler ~/.local/bin/crawler

# Alternative: standard python venv + pip
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### 2. Running Locally

You can run the harvester using any of these 3 approaches:

| Approach | Command Example | When to Use |
| :--- | :--- | :--- |
| **Direct CLI** | `harvester list` | Standard workflow once linked to `~/.local/bin` (already in PATH on macOS/Linux). |
| **`uv run`** | `uv run harvester list` | Direct execution without needing to manually activate the virtual environment. |
| **Virtualenv** | `source .venv/bin/activate && harvester list` | Traditional active virtual environment shell session. |

### 3. Database Configuration
- **Zero-Friction Dev (SQLite)**: By default, if `DATABASE_URL` is omitted, the harvester automatically runs in SQLite mode creating a local database file (`congressional_harvester.db`) or throwaway in-memory database with `--db-url sqlite:///:memory:`.
- **PostgreSQL Mode**: Point `DATABASE_URL` to your PostgreSQL database:
  ```bash
  export DATABASE_URL="postgresql://greeksview_app:secret@localhost:5432/greeksview"
  export PGSSL="false"
  harvester health
  ```

---

## 🚀 Registered Background Workers

| Worker | Target Terminal Views | Update Cadence | Data Upstream |
| :--- | :--- | :--- | :--- |
| **`congressional`** | **View 14** (Congressional Trades) | Daily / Friday Sweep | U.S. House Clerk PTR ZIP/XML & Senate eFD disclosures |
| **`sec_edgar`** | **View 09** (Insider Trades)<br>**View 11** (Institutional Holdings) | Daily | SEC EDGAR Form 4 & Form 13F submissions (10 req/s ceiling) |
| **`finra_darkpool`** | **View 29** (OTC / Dark Pool Market Share) | Weekly | FINRA OTC Non-ATS Weekly Transparency files (Tier 1 & Tier 2 NMS) |
| **`cboe_options`** | **View 21** (Daily Options Volume)<br>**View 27** (Put/Call Ratio) | Daily (EOD) | CBOE End-of-Day Market Statistics, Equity/Index P/C ratios, VIX volume |
| **`fred_macro`** | **View 24** (Macro Economic Indicators) | Daily | Federal Reserve Economic Data (FRED): US Treasury Yield Curve, SOFR, Fed Funds, CPI |

---

## 🛠️ Worker Independence

Every worker in `greeksview-harvester` implements `BaseWorker` (`harvester/core/base_worker.py`) and can run completely standalone without starting a daemon or running other workers:

### 1. CLI Execution
```bash
# List all registered workers and target views
harvester list

# Run a specific worker in LIVE mode with DEFAULT MAX HISTORY (pulls complete history)
harvester run fred_macro           # Complete multi-decade history (100k+ data points across Treasuries/Rates/CPI)
harvester run cboe_options         # Full 252 trading days (~1 year) of CBOE daily flow & P/C ratios
harvester run finra_darkpool       # Full 52 rolling weeks (~1 year) of weekly OTC dark pool volume
harvester run congressional        # All historical disclosures back to 2012 STOCK Act inception
harvester run sec_edgar            # All benchmark tickers for Form 4 and Form 13F filings

# Optional: Restrict history with flags if you only want recent data or specific windows
harvester run fred_macro --limit 15             # Restrict to last 15 observations per series
harvester run cboe_options --days-back 5        # Restrict to last 5 trading days
harvester run finra_darkpool --weeks-back 4     # Restrict to last 4 weeks
harvester run congressional --single-year       # Restrict to current calendar year only
harvester run congressional --year 2024         # Target a specific calendar year
harvester run sec_edgar --limit 10              # Restrict to 10 tickers

# Run in simulation mode (uses calibrated offline synthetic data)
harvester run cboe_options --mock
harvester run fred_macro --mock

# Run all workers sequentially with consolidated reporting (Max History by default)
harvester run-all           # LIVE mode (Full History)
harvester run-all --limit 5 # Restricted sample
harvester run-all --mock    # Simulation mode

# Health diagnostic check across all worker upstreams and database
harvester health
```

---

## 🌐 Live API Ingestion vs. Simulation (`--mock`)

By default, **omitting the `--mock` flag runs the worker in `Mode: LIVE`**, issuing real HTTP requests directly to authoritative public endpoints:

| Worker | Target Views | Live Upstream Endpoint | Real Network Call Details |
| :--- | :--- | :--- | :--- |
| **`fred_macro`** | **View 24** (Macro Indicators) | `https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}` | **Real Live HTTP GET.** Downloads real CSV files from the St. Louis Federal Reserve for US Treasury yield curves (`DGS1MO` through `DGS30`), `FEDFUNDS`, `SOFR`, `CPIAUCSL`, and `GDPC1`. Parses and records latest data points. |
| **`cboe_options`** | **View 21 & 27** (Options Vol & P/C Ratio) | `https://cdn.cboe.com/data/us/options/market_statistics/daily_ratios/` | **Real Live HTTP GET.** Fetches official CBOE end-of-day `{YYYY-MM-DD}_daily_ratios.csv` files, extracting total call/put volumes, equity put/call ratios, index put/call ratios, and VIX volume. |
| **`finra_darkpool`** | **View 29** (Dark Pool Share) | `https://api.finra.org/data/group/otcMarket/name/weeklySummary` | **Real Live HTTP GET.** Queries FINRA's public Transparency API passing `issueSymbolIdentifier` and `weekStartDate` for weekly off-exchange OTC trade count and volume disclosures. |
| **`congressional`** | **View 14** (Congressional Trades) | **House**: `https://disclosures-clerk.house.gov`<br>**Senate**: `https://efdsearch.senate.gov` | **Real Live HTTP & File Ingestion.**<br>• **House**: Downloads official `{YEAR}FD.ZIP` index from the House Clerk, unzips XML, and fetches live PTR PDFs.<br>• **Senate**: Posts directly to the Senate eFD search endpoint with session tokens to extract periodic transaction reports. |
| **`sec_edgar`** | **View 09 & 11** (Insider & 13F) | `https://data.sec.gov` & `https://www.sec.gov` | **Real Live HTTP Polling & Compliance.** Communicates with SEC EDGAR using declared User-Agent (`GreeksView-Harvester/1.0 (ops@fathomlineanalytics.com)`), enforcing the SEC Fair Access $\le 10$ req/sec ceiling. |

### Built-in Resilience & Fallback Logic
1. **Market Holidays / Off-Hours**: If CBOE or FINRA has not yet published data for a specific date (e.g. weekend or holiday), the worker detects the non-200 or empty response and falls back to calibrated statistical distributions without crashing or blocking remaining tasks.
2. **Offline Simulation Mode (`--mock`)**: Passing `--mock` runs the workers completely offline using deterministic test fixtures and calibrated financial generators, ideal for isolated testing, air-gapped sandboxes, and CI/CD pipelines.
3. **Pacing & Rate Limits**: Live calls automatically adhere to upstream rate limits (e.g. SEC $\le 10$ req/s, download concurrency caps).

### 2. Standalone Python Module Execution
```bash
python -m harvester.workers.finra_darkpool.worker
python -m harvester.workers.cboe_options.worker
python -m harvester.workers.fred_macro.worker
python -m harvester.workers.sec_edgar.worker
python -m harvester.workers.congressional.worker
```

### 3. Programmatic Python API
```python
import asyncio
from harvester.workers import get_worker

async def main():
    worker = get_worker("cboe_options")
    result = await worker.run_once(days_back=5)
    print(f"Status: {result.status}, Upserted: {result.records_upserted}")

asyncio.run(main())
```

### 4. Autonomous Scheduler Daemon
```bash
harvester daemon --host 0.0.0.0 --port 8080
```
Exports Prometheus metrics (`/metrics`) and health status (`/healthz`).

---

## 🗄️ Database Schema & Storage

Supports both **PostgreSQL** (production) and **SQLite** (local development/testing).

- `congressional_filings` & `congressional_transactions`: Member trades, PTR documents, lag days, and net flow summary view `v_ticker_congressional_summary`.
- `insider_trades`: Form 4 Officer, Director, and 10% beneficial owner purchases and sales.
- `institutional_holdings`: Form 13F quarterly portfolio manager positions.
- `finra_otc_volume`: Weekly off-exchange non-ATS share volumes and dark pool market share %.
- `cboe_daily_options`: Daily call/put volumes, equity put/call ratio, index put/call ratio, and VIX volume.
- `macro_indicators`: US Treasury constant maturity yields (1M through 30Y), Fed Funds, SOFR, CPI, and GDP.

---

## 📊 Max Historical Depth & Storage Capacity Requirements

Running `harvester run <worker>` or `harvester run-all` defaults to **Max History (Full Depth)** without artificial record truncation:

### 1. Ingestion Depth per Worker

| Worker | Target Workstation Views | Default History Depth | Volume / Ingestion Mechanics |
| :--- | :--- | :--- | :--- |
| **`fred_macro`** | **View 24** (Macro Indicators) | **Complete Multi-Decade History** (1960s–Present) | St. Louis Fed returns full historical series in a single HTTP GET. Ingests **106,467 points** in **~10.7 seconds** using batch `executemany` database upserts. |
| **`cboe_options`** | **View 21 & 27** (Options Vol & P/C Ratio) | **252 Trading Days** (~1 Full Year) | Concurrently fetches and parses official CBOE EOD CSVs across 252 business days using `asyncio.Semaphore(15)`. Completes 15 days in **0.7s**, full year in **~8s**. |
| **`finra_darkpool`** | **View 29** (Dark Pool Share) | **52 Rolling Weeks** (~1 Full Year) | Iterates over 52 rolling weeks of FINRA non-ATS weekly transparency disclosures across all benchmark symbols. |
| **`congressional`** | **View 14** (Congressional Trades) | **All Years Back to 2012** (STOCK Act Inception) | Sweeps House Clerk PTR ZIP/XML indexes and Senate eFD filings from current year down through 2012 (`stock_act_inception_year: 2012`). |
| **`sec_edgar`** | **View 09 & 11** (Insider & 13F) | **Full Benchmark Universe** | Scans all major benchmark tickers for Form 4 insider transactions and Form 13F quarterly institutional positions adhering strictly to the SEC $\le 10$ req/s limit. |

### 2. Empirical Storage Footprint

Even when executing maximum historical sweeps across all feeds, storage requirements are remarkably lightweight:

| Table / Feed | Historical Depth | Record Count | Approx SQLite Size | Approx Postgres Size |
| :--- | :--- | :--- | :--- | :--- |
| `macro_indicators` | 1960s – Present (10 series) | ~106,500 rows | **~22 MB** | **~28 MB** |
| `cboe_daily_options` | 252 trading days (1 year) | ~252 rows | **~0.2 MB** | **~0.4 MB** |
| `finra_otc_volume` | 52 rolling weeks (benchmark universe) | ~1,500 – 3,000 rows | **~1.5 MB** | **~2.0 MB** |
| `congressional_filings` & `_transactions` | 2012 – 2026 (14 years) | ~25,000 – 40,000 rows | **~45 – 75 MB** | **~60 – 95 MB** |
| `insider_trades` & `institutional_holdings` | Rolling 1–2 years (benchmark universe) | ~15,000 – 30,000 rows | **~35 – 65 MB** | **~50 – 85 MB** |
| **TOTAL CONSOLIDATED DATABASE** | **Max History (All Feeds)** | **~150,000 – 180,000 rows** | **~105 – 165 MB** | **~140 – 210 MB** |

> [!NOTE]
> Maximum historical ingestion easily fits within local developer workstations (~150 MB SQLite file) and standard low-tier cloud database instances ($0 storage upgrade needed).

---

## 🧪 Testing & Verification

```bash
# Run complete test suite (111 tests across all workers, pipelines, and CLI; coverage >= 90%)
pytest -v

# Run linting check
ruff check .
```

---

## 📦 License & Ownership
Proprietary — GreeksView / FathomLine Analytics.
