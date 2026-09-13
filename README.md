# GreeksView Data Harvester (`greeksview-harvester`)

Autonomous, resilient background data harvester and worker suite powering the open-source data feeds for the **GreeksView Market Analytics Workstation** (`/analytics/terminal`).

---

## 🏛️ Background & Scope

While real-time option chain Greeks and order routing connect directly to user broker feeds (Alpaca, Tradier, Schwab), and vendor fundamentals/equities connect via Alpha Vantage, **48% of the workstation views** rely on authoritative public regulatory filings, government datasets, exchange volume reports, and macro statistics.

`greeksview-harvester` consolidates all background harvesting workers into a single high-performance, modular Python repository where each worker operates independently with its own schedule, rate limits, schema, and isolated failure domain.

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

# Run a specific worker independently
harvester run finra_darkpool --limit 10
harvester run sec_edgar --limit 50
harvester run cboe_options --days-back 5
harvester run fred_macro --limit 15
harvester run congressional --year 2024

# Run all workers sequentially with consolidated reporting
harvester run-all

# Health diagnostic check across all worker upstreams and database
harvester health
```

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
- `macro_indicators`: US Treasury constant maturity yields (1M through 30Y), Fed Funds, and SOFR.

---

## 🧪 Testing & Verification

```bash
# Run complete test suite (107 tests across all workers, pipelines, and CLI)
pytest -v

# Run linting check
ruff check .
```

---

## 📦 License & Ownership
Proprietary — GreeksView / FathomLine Analytics.
