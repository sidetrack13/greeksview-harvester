# AI Agent Operating Protocol & Architecture Standard

Welcome, AI Agent. This document is the **mandatory operating protocol** for all autonomous or semi-autonomous AI agents (Antigravity, Claude, Cursor, Codex, etc.) operating on `greeksview-congressional-crawler`.

---

## 1. Mission & Purpose of this Repository

`greeksview-congressional-crawler` is the autonomous ingestion and normalization subsystem for **GreeksView** (Privacy-First Institutional Options Analytics & Trading Desk).

Under the **STOCK Act of 2012** (5 U.S.C. § 13107), members of the U.S. House of Representatives and Senate must disclose securities transactions within 30–45 days. Commercial vendors charge $2,400–$6,000+/year for this data. This repository delivers:
1. **Zero Data Vendor Dependency ($0 Data Cost)**: Ingests directly from official government disclosure portals.
2. **Operational Decoupling**: Completely isolated from the main `greeksview` web/WebSocket application to prevent heavy PDF downloads and XML parsing from competing with real-time UI/options math threads.
3. **Database-Shared Interoperability**: Ingests normalized filings and transactions directly into GreeksView's PostgreSQL database (or local SQLite).

---

## 2. Project Tracking & Knowledge Base

### Jira Project: `GV` (GreeksView)
- **Master Epic**: `GV-95` — `[CRAWLER-APP] Congress Trading Crawler & Alert Dispatch System`
- **GV-96**: `[CRAWLER-01]` Database Schema Design & Migration for Congressional Trading in PostgreSQL (Completed & Merged in GreeksView PR #515)
- **GV-97**: `[CRAWLER-02]` House of Representatives Bulk Ingestion Worker (`YYYYFD.ZIP` + Digital PDF Parser)
- **GV-98**: `[CRAWLER-03]` Senate eFD Automated Playwright Crawler & Disclaimer Handshake Worker
- **GV-99**: `[CRAWLER-04]` Unified CLI, Real-Time Ingestion Orchestrator & Deployment Dockerization
- **Jira Instance**: [https://fathomlineanalytics.atlassian.net](https://fathomlineanalytics.atlassian.net)

### Confluence Cloud Documentation:
- **Architecture Specification**: [Confluence Page #7176193 (Space: SD)](https://fathomlineanalytics.atlassian.net/wiki/spaces/SD/pages/7176193) — *Autonomous Congressional Trading Crawler & Alert Dispatch System*
- **GV-96 Schema Runbook**: [Confluence Page #7602177 (Space: SD)](https://fathomlineanalytics.atlassian.net/wiki/spaces/SD/pages/7602177) — *QA Runbook: Congressional Trading Database Schema & Idempotent Migration Runner*

---

## 3. Mandatory AI Agent Checklist (DOs and DONTs)

### Agent DOs (Strictly Enforced)
- **DO enforce 100% test coverage**: Every PR must satisfy `--cov-fail-under=100`. No uncovered branches or statements permitted.
- **DO guarantee strict idempotency**: All database writes must use `ON CONFLICT DO NOTHING` or `DO UPDATE` against unique constraint `uq_congressional_tx_dedup`. Re-running the crawler multiple times on the same inputs must never duplicate transactions.
- **DO support full local simulation**: The crawler must run offline without external internet or live government servers using mock fixtures (`--mock` / `SIMULATION_MODE=true`).
- **DO use conditional HTTP caching**: Respect government servers with `If-Modified-Since` and `ETag` headers; avoid redundant payload downloads.
- **DO follow asynchronous non-blocking patterns**: Use `asyncio`, `httpx.AsyncClient`, `asyncpg`, and `aiosqlite`.
- **DO publish Confluence QA Runbooks & Transition Jira**: Every implemented story must have an accompanying Confluence QA runbook in Space `SD` under parent `7176193`, and the Jira ticket must transition to `Done`.
- **DO auto-merge to main**: When GitHub Actions CI passes on the PR, squash-merge to `main`.

### Agent DONTs (Strictly Prohibited)
- **DONT hardcode credentials or secrets**: Never commit `.env`, API tokens, or production connection strings.
- **DONT scrape aggressively**: Never flood government endpoints without backoff and concurrency limits (`MAX_CONCURRENT_DOWNLOADS=5`).
- **DONT crash on malformed PDFs**: Congressional disclosures contain messy handwritten notes, skewed scans, and bad formatting. The parser must gracefully handle anomalies and classify status as `error` or `manual_review` instead of raising unhandled exceptions.
- **DONT bypass type checking**: Use Pydantic v2 and Python type hints throughout.

---

## 4. Repository Directory Layout

```
greeksview-congressional-crawler/
├── .agents/                     # AI Agent configuration & specialized skills
│   ├── AGENTS.md                # This document
│   └── skills/                  # Deep-dive skill playbooks
├── .github/workflows/ci.yml     # GitHub Actions CI (Ruff, Mypy, Pytest 100% Coverage)
├── crawler/
│   ├── config.py                # Pydantic Settings with env parsing
│   ├── cli.py                   # Typer/Rich terminal CLI runner
│   ├── core/
│   │   ├── http_client.py       # Resilient HTTPX client with retries & backoff
│   │   ├── db.py                # Async Postgres/SQLite dual-engine abstraction
│   │   └── models.py            # Pydantic models (Filing, Transaction, CrawlReport)
│   ├── house/
│   │   ├── bulk_crawler.py      # House YYYYFD.ZIP downloader & XML parser
│   │   ├── pdf_parser.py        # Vector table extractor & regex symbol parser
│   │   └── pipeline.py          # End-to-end House ingestion coordinator
│   ├── orchestration/
│   │   ├── daemon.py            # Long-running daemon coordinator & signal handler
│   │   ├── metrics.py           # Prometheus metrics collector & HTTP healthcheck server
│   │   └── scheduler.py         # APScheduler cron coordinator (House/Senate/Friday sweeps)
│   ├── senate/
│   │   ├── client.py            # Senate eFD disclaimer handshake & DataTables search
│   │   ├── html_parser.py       # Senate PTR HTML table parser & normalizer
│   │   └── pipeline.py          # End-to-end Senate ingestion coordinator
│   └── simulation/
│       ├── mock_house_server.py # Offline House HTTP mock fixtures server
│       └── mock_senate_server.py# Offline Senate eFD mock server
└── tests/
    ├── conftest.py              # Pytest fixtures, test DBs, sample files
    ├── fixtures/                # 2024FD_sample.xml and synthetic House/Senate PTRs
    └── test_*.py                # 100% unit & integration test coverage
```

---

## 5. Local Development Commands

```bash
# Install dependencies using uv
uv sync --all-extras

# Run full test suite with 100% coverage assertion
uv run pytest

# Run linter and formatting
uv run ruff check .
uv run ruff format --check .

# Run type checker
uv run mypy crawler

# Run House crawler in dry-run simulation mode (offline)
uv run crawler house --year 2024 --mock --limit 10

# Run Senate crawler in dry-run simulation mode (offline)
uv run crawler senate --year 2024 --mock --limit 10

# Run autonomous daemon (one-shot pass or long-running)
uv run crawler daemon --run-once --mock --dry-run
uv run crawler daemon --host 0.0.0.0 --port 8080 --initial-sync

# View database ingestion statistics
uv run crawler stats
```
