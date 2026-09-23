# GreeksView Data Harvester (`greeksview-harvester`)

Autonomous, resilient background data harvester and worker suite powering the open-source data feeds for the **GreeksView Market Analytics Workstation** (`/analytics/terminal`).

---

## 🏛️ Background & Scope

While real-time option chain Greeks and order routing connect directly to user broker feeds (Alpaca, Tradier, Schwab), and vendor fundamentals/equities connect via Alpha Vantage, **48% of the workstation views** rely on authoritative public regulatory filings, government datasets, exchange volume reports, and macro statistics.

`greeksview-harvester` consolidates all background harvesting workers into a single high-performance, modular Python repository where each worker operates independently with its own schedule, rate limits, schema, and isolated failure domain.

> 📖 **Operational Runbook**: For a full guide on running workers on-demand, cron configurations, daemon operations, and troubleshooting, see the [Operational Runbook](docs/RUNBOOK.md).

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
| **`fred_macro`** | **View 24** (Macro Economic Indicators) | Daily | Federal Reserve Economic Data (FRED): US Treasury Yield Curve, SOFR, Fed Funds, CPI |

### Retired Workers

`cboe_options` and `finra_darkpool` were withdrawn and their code deleted. They are not runnable, are not part of `run-all`, and do not appear in `harvester list` or `harvester health`. Asking for one by name exits with the reason:

| Retired Worker | Why |
| :--- | :--- |
| **`cboe_options`** | Cboe's data terms require Cboe's written consent for commercial use, which GreeksView does not have. The worker also wrote an invented record on any failed fetch, and even on a successful fetch it derived the equity put/call ratio as `total * 0.78` — a guess presented as a measurement. |
| **`finra_darkpool`** | FINRA's data terms permit non-commercial use only, and GreeksView is a paid product. The worker also wrote an invented record on any failed fetch. |

The `cboe_daily_options` and `finra_otc_volume` tables are left in the schema so existing rows stay readable and countable (`harvester stats` labels them retired). Nothing writes them, and `sync-pg` no longer copies them.

---

## 🛠️ Worker Independence

Every worker in `greeksview-harvester` implements `BaseWorker` (`harvester/core/base_worker.py`) and can run completely standalone without starting a daemon or running other workers:

### 1. CLI Execution
```bash
# List all registered workers and target views
harvester list

# Run a specific worker in LIVE mode with DEFAULT MAX HISTORY (pulls complete history)
harvester run fred_macro           # Complete multi-decade history (100k+ data points across Treasuries/Rates/CPI)
harvester run congressional        # All historical disclosures back to 2012 STOCK Act inception
harvester run sec_edgar            # All benchmark tickers for Form 4 and Form 13F filings

# Optional: Restrict history with flags if you only want recent data or specific windows
harvester run fred_macro --limit 15             # Restrict to last 15 observations per series
harvester run congressional --single-year       # Restrict to current calendar year only
harvester run congressional --year 2024         # Target a specific calendar year
harvester run sec_edgar --limit 10              # Restrict to 10 tickers

# Run in simulation mode (uses calibrated offline synthetic data)
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
| **`congressional`** | **View 14** (Congressional Trades) | **House**: `https://disclosures-clerk.house.gov`<br>**Senate**: `https://efdsearch.senate.gov` | **Real Live HTTP & File Ingestion.**<br>• **House**: Downloads official `{YEAR}FD.ZIP` index from the House Clerk, unzips XML, and fetches live PTR PDFs.<br>• **Senate**: Posts directly to the Senate eFD search endpoint with session tokens to extract periodic transaction reports. |
| **`sec_edgar`** | **View 09 & 11** (Insider & 13F) | `https://data.sec.gov` & `https://www.sec.gov` | **Real Live HTTP Polling & Compliance.** Communicates with SEC EDGAR using declared User-Agent (`GreeksView-Harvester/1.0 (ops@fathomlineanalytics.com)`), enforcing the SEC Fair Access $\le 10$ req/sec ceiling. |

### Built-in Resilience & Fallback Logic
1. **Market Holidays / Off-Hours**: A live worker that gets a non-200 or an empty response records nothing for that date and carries on with the remaining tasks. It never substitutes a generated figure for data it could not fetch: outside `--mock`, an absence stays an absence.
2. **Offline Simulation Mode (`--mock`)**: Passing `--mock` runs the workers completely offline using deterministic test fixtures and calibrated financial generators, ideal for isolated testing, air-gapped sandboxes, and CI/CD pipelines. Mock rows are written to `greeksview_harvester.mock.db` unless `--db-url` names another SQLite file; either way the file is stamped as mock-written and `sync-pg` refuses to push it. `--mock` with a PostgreSQL `--db-url` is refused.
3. **Pacing & Rate Limits**: Live calls automatically adhere to upstream rate limits (e.g. SEC $\le 10$ req/s, download concurrency caps).

### 2. Standalone Python Module Execution
```bash
python -m harvester.workers.fred_macro.worker
python -m harvester.workers.sec_edgar.worker
python -m harvester.workers.congressional.worker
```

### 3. Programmatic Python API
```python
import asyncio
from harvester.workers import get_worker


async def main():
    worker = get_worker("fred_macro")
    result = await worker.run_once(limit_points_per_series=5)
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
- `macro_indicators`: US Treasury constant maturity yields (1M through 30Y), Fed Funds, SOFR, CPI, and GDP.

### Synchronizing SQLite to PostgreSQL (`sync-pg`)

Push all locally harvested records directly into your PostgreSQL database with a single CLI command:

```bash
# Sync all tables using DATABASE_URL from .env
harvester sync-pg

# Sync using an explicit PostgreSQL connection string
harvester sync-pg --pg-url "postgresql://user:pass@host:5432/greeksview"

# Sync specific tables with a custom batch size
harvester sync-pg --table congressional_filings --table congressional_transactions --batch-size 2000

# Specify a custom source SQLite database path
harvester sync-pg --sqlite-path ./custom_harvester.db --pg-url "postgresql://..."
```

**Features:**
- **Automatic Schema Initialization**: Automatically verifies and executes the PostgreSQL table schema and views (`v_ticker_congressional_summary`) idempotently prior to insertion.
- **Dependency Ordering**: Syncs parent tables (`congressional_filings`) before child tables (`congressional_transactions`) to preserve foreign key constraints.
- **Idempotent Batch Upserts**: Resolves record collisions via `ON CONFLICT DO NOTHING` or `DO UPDATE`, making synchronization completely safe to re-run.
- **High-Performance Chunking**: Configurable `--batch-size` (default 1,000 rows) using native asyncpg parameter binding and connection pooling.
- **Retired Tables Are Refused**: `cboe_daily_options` and `finra_otc_volume` are no longer synchronised. A default sync skips them, and naming one with `--table` exits with the reason rather than quietly syncing nothing. Rows already stored locally or in PostgreSQL are left untouched.

---

## 📊 Max Historical Depth & Storage Capacity Requirements

Running `harvester run <worker>` or `harvester run-all` defaults to **Max History (Full Depth)** without artificial record truncation:

### 1. Ingestion Depth per Worker

| Worker | Target Workstation Views | Default History Depth | Volume / Ingestion Mechanics |
| :--- | :--- | :--- | :--- |
| **`fred_macro`** | **View 24** (Macro Indicators) | **Complete Multi-Decade History** (1960s–Present) | St. Louis Fed returns full historical series in a single HTTP GET. Ingests **106,467 points** in **~10.7 seconds** using batch `executemany` database upserts. |
| **`congressional`** | **View 14** (Congressional Trades) | **All Years Back to 2012** (STOCK Act Inception) | Sweeps House Clerk PTR ZIP/XML indexes and Senate eFD filings from current year down through 2012 (`stock_act_inception_year: 2012`). |
| **`sec_edgar`** | **View 09 & 11** (Insider & 13F) | **Full Benchmark Universe** | Scans all major benchmark tickers for Form 4 insider transactions and Form 13F quarterly institutional positions adhering strictly to the SEC $\le 10$ req/s limit. |

### 2. Empirical Storage Footprint

Even when executing maximum historical sweeps across all feeds, storage requirements are remarkably lightweight:

| Table / Feed | Historical Depth | Record Count | Approx SQLite Size | Approx Postgres Size |
| :--- | :--- | :--- | :--- | :--- |
| `macro_indicators` | 1960s – Present (10 series) | ~106,500 rows | **~22 MB** | **~28 MB** |
| `congressional_filings` & `_transactions` | 2012 – 2026 (14 years) | ~25,000 – 40,000 rows | **~45 – 75 MB** | **~60 – 95 MB** |
| `insider_trades` & `institutional_holdings` | Rolling 1–2 years (benchmark universe) | ~15,000 – 30,000 rows | **~35 – 65 MB** | **~50 – 85 MB** |
| **TOTAL CONSOLIDATED DATABASE** | **Max History (All Feeds)** | **~150,000 – 180,000 rows** | **~105 – 165 MB** | **~140 – 210 MB** |

> [!NOTE]
> `cboe_daily_options` and `finra_otc_volume` are absent from this table because nothing writes them any more: the `cboe_options` and `finra_darkpool` workers were retired. Any rows still in the two tables are historical.

> [!NOTE]
> Maximum historical ingestion easily fits within local developer workstations (~150 MB SQLite file) and standard low-tier cloud database instances ($0 storage upgrade needed).

---

## 🧪 Testing & Verification

```bash
# Run complete test suite (119 tests across all workers, pipelines, sync engine, and CLI; coverage >= 90%)
pytest -v

# Run linting check
ruff check .
```

---

## 📦 License & Ownership
Proprietary — GreeksView / FathomLine Analytics.
