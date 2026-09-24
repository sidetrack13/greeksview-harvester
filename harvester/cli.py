"""Rich Command Line Interface for GreeksView Congressional Trading Crawler."""

import asyncio
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from harvester import __version__
from harvester.config import Settings, get_settings
from harvester.core.db import (
    MOCK_SQLITE_PATH,
    DatabaseManager,
    mark_sqlite_file_mock,
    sqlite_file_is_mock,
)
from harvester.core.http_client import resilient_http_client
from harvester.core.progress import DEFAULT_CHECKPOINT_PATH
from harvester.orchestration.daemon import CrawlerDaemon
from harvester.orchestration.scheduler import (
    DEFAULT_FRIDAY_SWEEP_CRON,
    DEFAULT_HOUSE_CRON,
    DEFAULT_SENATE_CRON,
)
from harvester.workers import WORKER_REGISTRY, get_worker, list_workers
from harvester.workers.congressional.house.pipeline import HousePipeline
from harvester.workers.congressional.senate.pipeline import SenatePipeline

app = typer.Typer(
    name="harvester",
    help="Unified Autonomous Background Data Harvester & Worker Suite for GreeksView Financial Feeds",
    no_args_is_help=True,
)
console = Console()


def _resolve_settings(mock: bool, db_url: str | None) -> Settings:
    """Settings for a command; a --mock run never writes into the file sync-pg pushes.

    Without --db-url a mock run writes to greeksview_harvester.mock.db, whatever
    DATABASE_URL says. A PostgreSQL --db-url is refused. An explicit sqlite:///
    --db-url is honoured only when the file is the mock run's own to write: a
    new file, or one a mock run has already stamped.

    AN EXISTING REAL STORE IS REFUSED, NOT STAMPED. Honouring it and stamping
    it mock was the older rule, and the stamp is what sync-pg reads to refuse a
    file — so one slip would have branded a 60-million-row harvest and blocked
    every sync until someone cleared the pragma by hand. Refusing costs a
    retyped path; stamping costs the store.
    """
    settings = get_settings()
    if db_url is not None:
        settings.database_url = db_url
    if not mock:
        return settings
    if db_url is None:
        settings = settings.model_copy(update={"database_url": f"sqlite:///{MOCK_SQLITE_PATH}"})
    elif not settings.is_sqlite:
        console.print(
            "[bold red]Error:[/bold red] --mock never writes to PostgreSQL. Omit --db-url or pass a sqlite:/// file."
        )
        raise typer.Exit(code=1)
    sqlite_path = DatabaseManager(settings=settings).sqlite_path
    refusal = _mock_write_refusal(sqlite_path)
    if refusal:
        console.print(f"[bold red]Error:[/bold red] {refusal}")
        raise typer.Exit(code=1)
    mark_sqlite_file_mock(sqlite_path)
    return settings


def _mock_write_refusal(path: str) -> str | None:
    """Why a --mock run must not write to this SQLite file, or None if it may.

    A file that does not exist yet, or is empty, or one a mock run has already
    stamped, is the mock run's own. Anything else is somebody's harvest.

    WHEN IT CANNOT BE TOLD, IT IS REFUSED. An unreadable or unstamped existing
    file is treated as real: a wrong refusal costs a retyped path, and a wrong
    allow writes fabricated rows into a store and stamps it so sync-pg will not
    take it.
    """
    if not path or path == ":memory:":
        return None
    f = Path(path)
    if not f.exists() or f.stat().st_size == 0:
        return None
    try:
        if sqlite_file_is_mock(path):
            return None
    except Exception:
        return (
            f"{path} already exists and could not be read to tell whether a --mock run wrote it. "
            "Omit --db-url to use the mock database, or pass a path that does not exist yet."
        )
    return (
        f"{path} is an existing database that no --mock run has written to, so it holds harvested "
        "rows. --mock would add fabricated ones and stamp the file, which stops sync-pg taking any "
        "of it. Omit --db-url to use the mock database, or pass a path that does not exist yet."
    )


def _print_harvest_report(report: dict[str, Any]) -> None:
    """Print what a run could not store, and why (Alpha Vantage worker)."""
    lines: list[str] = []
    for key in ("skipped", "filtered"):
        counts = report.get(key) or {}
        if counts:
            detail = ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
            lines.append(f"Rows {key}: {sum(counts.values())} ({detail})")
    for sub in report.get("date_substitutions") or []:
        lines.append(f"{sub['symbol']}: asked {sub['asked']}, got {', '.join(sub['got'])}")
    for item in report.get("spot_unknown") or []:
        lines.append(f"No stored close for {item}: moneyness band not applied, full chain kept")
    for item in report.get("empty_responses") or []:
        lines.append(f"Empty options chain for {item}")
    if lines:
        console.print("[bold yellow]Harvest report:[/bold yellow]")
        for line in lines:
            console.print(f"  [yellow]•[/yellow] {line}")


@app.command()
def version() -> None:
    """Display harvester version and system information."""
    console.print(f"[bold green]GreeksView Harvester Suite[/bold green] version [cyan]{__version__}[/cyan]")


@app.command(name="list")
def list_command() -> None:
    """List all registered independent background data harvesting workers."""
    workers = list_workers()
    table = Table(title="Registered GreeksView Harvester Workers")
    table.add_column("Worker", style="bold cyan")
    table.add_column("Frequency", style="yellow")
    table.add_column("Target Terminal Views", style="green")
    table.add_column("Description", style="white")

    for w in workers:
        table.add_row(
            w["name"],
            w["frequency"],
            "\n".join(w["target_views"]),
            w["description"],
        )
    console.print(table)


@app.command(name="run")
def run_command(
    worker_name: Annotated[
        str,
        typer.Argument(help="Name of worker (congressional, sec_edgar, fred_macro, alphavantage)"),
    ],
    limit: Annotated[
        int | None, typer.Option("--limit", "-l", help="Record or symbol limit (default: None for max history)")
    ] = None,
    mock: Annotated[bool, typer.Option("--mock", help="Use synthetic mock/simulation data")] = False,
    year: Annotated[int | None, typer.Option("--year", "-y", help="Target calendar year")] = None,
    all_years: Annotated[
        bool, typer.Option("--all-years/--single-year", help="Sweep all historical years back to 2012 (default: True)")
    ] = True,
    days_back: Annotated[
        int | None,
        typer.Option(
            "--days-back",
            help="Historical trading days back for Alpha Vantage Options (default: 1 snapshot, or specify N days). With --sessions-from: the N most recent stored sessions",
        ),
    ] = None,
    moneyness_band: Annotated[
        float | None,
        typer.Option(
            "--moneyness-band",
            help="Strike moneyness filter percentage around the session's stored close (e.g. 40 for +/-40%). Not applied when that close is unknown",
        ),
    ] = None,
    prune_inactive: Annotated[
        bool,
        typer.Option(
            "--prune-inactive/--no-prune-inactive",
            help="Drop options contracts whose reported volume AND open interest are both 0 (default: False). Rows with unknown volume/OI are never dropped. Pruning hides builds-from-zero: a dropped contract has no baseline row on the day its OI starts to build",
        ),
    ] = False,
    skip_existing: Annotated[
        bool,
        typer.Option(
            "--skip-existing/--no-skip-existing",
            help="Alpha Vantage options: skip already-stored (symbol, date) pairs to enable fast resume (default: True)",
        ),
    ] = True,
    trade_dates: Annotated[
        str | None,
        typer.Option(
            "--trade-dates",
            help="Alpha Vantage options: explicit sessions, comma-separated YYYY-MM-DD and/or @file with one date per line",
        ),
    ] = None,
    sessions_from: Annotated[
        str | None,
        typer.Option(
            "--sessions-from",
            help="Alpha Vantage options: use the sessions stored in stock_bars_daily for this symbol (real sessions only)",
        ),
    ] = None,
    outputsize: Annotated[
        str | None,
        typer.Option(
            "--outputsize",
            help="Alpha Vantage daily bars: compact (latest 100 sessions, default) or full (whole history, for backfills)",
        ),
    ] = None,
    interval: Annotated[
        str | None,
        typer.Option(
            "--interval", help="Alpha Vantage intraday bar interval: 1min, 5min (default), 15min, 30min, 60min"
        ),
    ] = None,
    months: Annotated[
        str | None,
        typer.Option(
            "--months",
            help="Alpha Vantage intraday: comma-separated YYYY-MM months, one request each (default: trailing 30 days)",
        ),
    ] = None,
    extended_hours: Annotated[
        bool,
        typer.Option(
            "--extended-hours/--no-extended-hours",
            help="Alpha Vantage intraday: include pre- and post-market bars (default: include)",
        ),
    ] = True,
    dataset: Annotated[
        str | None,
        typer.Option(
            "--dataset",
            "-d",
            help="Dataset for Alpha Vantage (daily, intraday, options, fundamentals, actions, reference, all)",
        ),
    ] = None,
    symbols: Annotated[
        str | None, typer.Option("--symbols", "-s", help="Comma-separated ticker symbols (e.g. SPY,QQQ,AAPL)")
    ] = None,
    api_key: Annotated[
        str | None, typer.Option("--api-key", "-k", help="API key override (e.g. for Alpha Vantage)")
    ] = None,
    db_url: Annotated[str | None, typer.Option("--db-url", help="Database connection string")] = None,
) -> None:
    """Execute a single background worker independently."""
    parsed_trade_dates: list[str] | None = None
    if trade_dates is not None:
        from harvester.workers.alphavantage.worker import parse_trade_dates

        try:
            parsed_trade_dates = parse_trade_dates(trade_dates)
        except (ValueError, OSError) as e:
            console.print(f"[bold red]Error:[/bold red] --trade-dates: {e}")
            raise typer.Exit(code=1) from None

    settings = _resolve_settings(mock, db_url)
    if api_key is not None:
        settings.alphavantage_api_key = api_key

    try:
        worker = get_worker(worker_name, config=settings)
    except KeyError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(code=1) from None

    limit_display = str(limit) if limit is not None else "[bold green]MAX HISTORY (Unlimited)[/bold green]"
    console.print(
        Panel(
            f"[bold]Worker:[/bold] [cyan]{worker.name}[/cyan]\n"
            f"[bold]Description:[/bold] {worker.description}\n"
            f"[bold]Target Views:[/bold] {', '.join(worker.target_views)}\n"
            f"[bold]Mode:[/bold] {'[magenta]MOCK/SIMULATION[/magenta]' if mock else '[green]LIVE[/green]'}\n"
            f"[bold]Limit:[/bold] {limit_display}",
            title=f"Executing Worker: {worker.name}",
        )
    )

    async def _run() -> None:
        kwargs: dict[str, Any] = {"use_mock": mock}
        if limit is not None:
            kwargs["limit"] = limit
        if year is not None:
            kwargs["year"] = year
        else:
            kwargs["all_years"] = all_years
        if days_back is not None:
            kwargs["days_back"] = days_back
        if moneyness_band is not None:
            kwargs["moneyness_band_pct"] = moneyness_band
        if prune_inactive:
            kwargs["prune_inactive"] = True
        if not skip_existing:
            kwargs["skip_existing"] = False
        else:
            kwargs["skip_existing"] = True
        if dataset is not None:
            kwargs["dataset"] = dataset
        if symbols is not None:
            kwargs["symbols"] = symbols
        if parsed_trade_dates is not None:
            kwargs["trade_dates"] = parsed_trade_dates
        if sessions_from is not None:
            kwargs["sessions_from"] = sessions_from
        if outputsize is not None:
            kwargs["outputsize"] = outputsize
        if interval is not None:
            kwargs["interval"] = interval
        if months is not None:
            kwargs["months"] = [m.strip() for m in months.split(",") if m.strip()]
        if not extended_hours:
            kwargs["extended_hours"] = False

        if worker.name == "alphavantage":
            res = await worker.run_once(**kwargs)
        else:
            with console.status(f"[bold green]Running {worker.name} harvesting pass..."):
                res = await worker.run_once(**kwargs)

        status_style = "bold green" if res.is_success else "bold red"
        table = Table(title=f"Worker Run Summary — {worker.name}")
        table.add_column("Field", style="cyan")
        table.add_column("Value", style="bold white")

        table.add_row("Status", f"[{status_style}]{res.status.upper()}[/{status_style}]")
        table.add_row("Records Harvested", str(res.records_harvested))
        table.add_row("Records Upserted", str(res.records_upserted))
        table.add_row("Duration", f"{res.duration_seconds:.2f}s")
        table.add_row("Errors Count", str(len(res.errors)))

        console.print(table)
        if isinstance(res.metadata.get("report"), dict):
            _print_harvest_report(res.metadata["report"])
        if res.errors:
            console.print("[bold red]Errors encountered during run:[/bold red]")
            for err in res.errors[:5]:
                console.print(f"  [red]•[/red] {err}")

    asyncio.run(_run())


@app.command(name="run-all")
def run_all_command(
    limit: Annotated[
        int | None, typer.Option("--limit", "-l", help="Record limit per worker (default: None for max history)")
    ] = None,
    mock: Annotated[bool, typer.Option("--mock", help="Use synthetic mock data")] = False,
    dataset: Annotated[
        str | None, typer.Option("--dataset", "-d", help="Alpha Vantage dataset filter (daily, options, all, etc.)")
    ] = None,
    symbols: Annotated[
        str | None, typer.Option("--symbols", "-s", help="Comma-separated ticker symbols (e.g. SPY,QQQ)")
    ] = None,
    db_url: Annotated[str | None, typer.Option("--db-url", help="Database connection string")] = None,
) -> None:
    """Execute all registered harvesting workers in sequence."""
    settings = _resolve_settings(mock, db_url)

    async def _run_all() -> None:
        results = []
        for name in WORKER_REGISTRY:
            worker = get_worker(name, config=settings)
            console.print(f"[bold cyan]=> Running worker: {name}[/bold cyan]")
            kwargs: dict[str, Any] = {"limit": limit, "use_mock": mock}
            if dataset is not None:
                kwargs["dataset"] = dataset
            if symbols is not None:
                kwargs["symbols"] = symbols
            try:
                res = await worker.run_once(**kwargs)
                results.append(res)
            except Exception as e:
                console.print(f"[bold red]Failed worker {name}: {e}[/bold red]")

        table = Table(title="Consolidated Harvesting Results")
        table.add_column("Worker", style="bold cyan")
        table.add_column("Status", style="bold")
        table.add_column("Harvested", justify="right")
        table.add_column("Upserted", justify="right")
        table.add_column("Duration", justify="right")
        table.add_column("Errors", justify="right")

        for r in results:
            st_color = "green" if r.is_success else "red"
            table.add_row(
                r.worker,
                f"[{st_color}]{r.status.upper()}[/{st_color}]",
                str(r.records_harvested),
                str(r.records_upserted),
                f"{r.duration_seconds:.2f}s",
                str(len(r.errors)),
            )
        console.print(table)

    asyncio.run(_run_all())


@app.command(name="health")
def health_command(
    db_url: Annotated[str | None, typer.Option("--db-url", help="Database connection string")] = None,
) -> None:
    """Run diagnostics and healthchecks across all registered workers."""
    settings = get_settings()
    if db_url is not None:
        settings.database_url = db_url

    async def _health() -> None:
        table = Table(title="Harvester Workers Health Diagnostic")
        table.add_column("Worker", style="bold cyan")
        table.add_column("Status", style="bold")
        table.add_column("Details", style="white")

        for name in WORKER_REGISTRY:
            worker = get_worker(name, config=settings)
            try:
                h = await worker.health()
                st = h.get("status", "unknown")
                color = "green" if st == "healthy" else ("yellow" if st == "degraded" else "red")
                details = ", ".join(f"{k}={v}" for k, v in h.items() if k not in ("worker", "status"))
                table.add_row(name, f"[{color}]{st.upper()}[/{color}]", details)
            except Exception as e:
                table.add_row(name, "[red]ERROR[/red]", str(e))

        console.print(table)

    asyncio.run(_health())


@app.command(name="sync-pg")
def sync_pg_command(
    pg_url: Annotated[
        str | None, typer.Option("--pg-url", help="Target PostgreSQL connection string (defaults to DATABASE_URL)")
    ] = None,
    sqlite_path: Annotated[
        str, typer.Option("--sqlite-path", help="Source SQLite database file path")
    ] = "greeksview_harvester.db",
    batch_size: Annotated[int, typer.Option("--batch-size", "-b", help="Batch size for PostgreSQL inserts")] = 1000,
    table: Annotated[
        list[str] | None, typer.Option("--table", "-t", help="Specific table(s) to sync (default: all)")
    ] = None,
    days_back: Annotated[
        int | None,
        typer.Option("--days-back", "-d", help="Historical trading days back to sync to PostgreSQL (default: all)"),
    ] = None,
) -> None:
    """Synchronize all locally harvested SQLite tables into PostgreSQL."""
    settings = get_settings()
    target_url = pg_url or settings.database_url
    if not target_url or target_url.startswith("sqlite"):
        console.print(
            "[bold red]Error:[/bold red] Target PostgreSQL connection string required.\n"
            "Provide via [cyan]--pg-url 'postgresql://user:pass@host:5432/dbname'[/cyan] or set [cyan]DATABASE_URL[/cyan]."
        )
        raise typer.Exit(code=1)

    from harvester.core.sync import sync_sqlite_to_postgres

    console.print(
        Panel(
            f"[bold]Source SQLite:[/bold] [cyan]{sqlite_path}[/cyan]\n"
            f"[bold]Target PostgreSQL:[/bold] [green]{target_url.split('@')[-1] if '@' in target_url else 'configured'}[/green]\n"
            f"[bold]Batch Size:[/bold] {batch_size}\n"
            f"[bold]Days Back:[/bold] {days_back if days_back is not None else 'ALL (unrestricted)'}\n"
            f"[bold]Filter Tables:[/bold] {', '.join(table) if table else 'ALL TABLES'}",
            title="Database Synchronization: SQLite ➔ PostgreSQL",
        )
    )

    async def _sync() -> None:
        with console.status("[bold green]Synchronizing tables from SQLite to PostgreSQL..."):
            try:
                summary = await sync_sqlite_to_postgres(
                    pg_url=target_url,
                    sqlite_path=sqlite_path,
                    batch_size=batch_size,
                    settings=settings,
                    target_tables=table,
                    days_back=days_back,
                )
            except Exception as e:
                console.print(f"[bold red]Sync Failed:[/bold red] {e}")
                raise typer.Exit(code=1) from None

        res_table = Table(title="Synchronization Results Summary")
        res_table.add_column("Table Name", style="bold cyan")
        res_table.add_column("SQLite Rows", justify="right")
        res_table.add_column("Synced to Postgres", justify="right", style="bold green")
        res_table.add_column("Duration", justify="right")

        total_rows = 0
        total_time = 0.0
        for tbl_name, stats in summary.items():
            res_table.add_row(
                tbl_name,
                str(stats["sqlite_count"]),
                str(stats["synced_count"]),
                f"{stats['duration_seconds']:.2f}s",
            )
            total_rows += stats["synced_count"]
            total_time += stats["duration_seconds"]

        res_table.add_section()
        res_table.add_row(
            "[bold]TOTAL[/bold]",
            "",
            f"[bold green]{total_rows}[/bold green]",
            f"[bold]{total_time:.2f}s[/bold]",
        )
        console.print(res_table)

    asyncio.run(_sync())


@app.command()
def house(
    year: Annotated[int, typer.Option("--year", "-y", help="Calendar year to crawl")] = 2024,
    limit: Annotated[int | None, typer.Option("--limit", "-l", help="Max filings to process")] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Preview crawl without database writes")] = False,
    mock: Annotated[bool, typer.Option("--mock", help="Use offline synthetic mock fixtures")] = False,
    db_url: Annotated[str | None, typer.Option("--db-url", help="Database connection string")] = None,
) -> None:
    """Ingest, parse, and normalize House of Representatives Periodic Transaction Reports (PTRs)."""
    settings = _resolve_settings(mock, db_url)

    title = f"House Clerk Ingestion Pipeline — Year {year}"
    mode_str = "[yellow]DRY-RUN[/yellow]" if dry_run else "[green]LIVE[/green]"
    if mock:
        mode_str += " | [magenta]MOCK SIMULATION[/magenta]"

    console.print(Panel(f"Mode: {mode_str}\nTarget Year: {year}\nBatch Limit: {limit or 'Unlimited'}", title=title))

    async def _execute() -> None:
        db = DatabaseManager(settings=settings)
        await db.connect()
        try:
            async with resilient_http_client(
                timeout_seconds=settings.request_timeout_seconds,
                max_retries=settings.http_max_retries,
                max_concurrency=settings.max_concurrent_downloads,
            ) as http_client:
                pipeline = HousePipeline(db=db, http_client=http_client, settings=settings)
                with console.status("[bold green]Executing ingestion pipeline..."):
                    report = await pipeline.run(year=year, limit=limit, dry_run=dry_run, use_mock=mock)

                table = Table(title="Crawl Ingestion Summary")
                table.add_column("Metric", style="cyan")
                table.add_column("Value", style="bold white")

                table.add_row("Calendar Year", str(report.year))
                table.add_row("Filtered PTR Disclosures", str(report.filtered_ptrs))
                table.add_row("New Filings Identified", str(report.new_filings_to_crawl))
                table.add_row("Filings Successfully Parsed", str(report.filings_parsed))
                table.add_row("Transactions Extracted", str(report.transactions_extracted))
                table.add_row("Processing Errors", str(report.errors_count))
                table.add_row("Elapsed Duration", f"{report.duration_seconds:.2f}s")

                console.print(table)

                if report.errors_count > 0:
                    console.print("[bold red]Errors encountered during run:[/bold red]")
                    for err in report.error_details[:5]:
                        console.print(f"  [red]•[/red] {err}")
        finally:
            await db.close()

    asyncio.run(_execute())


@app.command()
def senate(
    year: Annotated[int, typer.Option("--year", "-y", help="Calendar year to crawl")] = 2024,
    limit: Annotated[int | None, typer.Option("--limit", "-l", help="Max filings to process")] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Preview crawl without database writes")] = False,
    mock: Annotated[bool, typer.Option("--mock", help="Use offline synthetic mock fixtures")] = False,
    db_url: Annotated[str | None, typer.Option("--db-url", help="Database connection string")] = None,
) -> None:
    """Ingest, parse, and normalize U.S. Senate Periodic Transaction Reports (eFD)."""
    settings = _resolve_settings(mock, db_url)

    title = f"Senate eFD Ingestion Pipeline — Year {year}"
    mode_str = "[yellow]DRY-RUN[/yellow]" if dry_run else "[green]LIVE[/green]"
    if mock:
        mode_str += " | [magenta]MOCK SIMULATION[/magenta]"

    console.print(Panel(f"Mode: {mode_str}\nTarget Year: {year}\nBatch Limit: {limit or 'Unlimited'}", title=title))

    async def _execute() -> None:
        db = DatabaseManager(settings=settings)
        await db.connect()
        try:
            async with resilient_http_client(
                timeout_seconds=settings.request_timeout_seconds,
                max_retries=settings.http_max_retries,
                max_concurrency=settings.max_concurrent_downloads,
            ) as http_client:
                pipeline = SenatePipeline(db=db, http_client=http_client, settings=settings)
                with console.status("[bold green]Executing Senate ingestion pipeline..."):
                    report = await pipeline.run(year=year, limit=limit, dry_run=dry_run, use_mock=mock)

                table = Table(title="Senate Crawl Ingestion Summary")
                table.add_column("Metric", style="cyan")
                table.add_column("Value", style="bold white")

                table.add_row("Calendar Year", str(report.year))
                table.add_row("Filtered PTR Disclosures", str(report.filtered_ptrs))
                table.add_row("New Filings Identified", str(report.new_filings_to_crawl))
                table.add_row("Filings Successfully Parsed", str(report.filings_parsed))
                table.add_row("Transactions Extracted", str(report.transactions_extracted))
                table.add_row("Processing Errors", str(report.errors_count))
                table.add_row("Elapsed Duration", f"{report.duration_seconds:.2f}s")

                console.print(table)

                if report.errors_count > 0:
                    console.print("[bold red]Errors encountered during run:[/bold red]")
                    for err in report.error_details[:5]:
                        console.print(f"  [red]•[/red] {err}")
        finally:
            await db.close()

    asyncio.run(_execute())


@app.command()
def daemon(
    host: Annotated[str, typer.Option("--host", help="HTTP server bind host")] = "0.0.0.0",
    port: Annotated[int, typer.Option("--port", "-p", help="HTTP server bind port")] = 8080,
    mock: Annotated[bool, typer.Option("--mock", help="Use offline synthetic mock fixtures")] = False,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Preview crawl without database writes")] = False,
    run_once: Annotated[bool, typer.Option("--run-once", help="Execute single sync pass and terminate")] = False,
    initial_sync: Annotated[bool, typer.Option("--initial-sync", help="Execute sync pass upon daemon startup")] = False,
    house_cron: Annotated[
        str, typer.Option("--house-cron", help="Cron expression for House crawl")
    ] = DEFAULT_HOUSE_CRON,
    senate_cron: Annotated[
        str, typer.Option("--senate-cron", help="Cron expression for Senate crawl")
    ] = DEFAULT_SENATE_CRON,
    friday_cron: Annotated[
        str, typer.Option("--friday-cron", help="Cron expression for Friday sweep")
    ] = DEFAULT_FRIDAY_SWEEP_CRON,
    no_server: Annotated[bool, typer.Option("--no-server", help="Disable healthcheck/Prometheus HTTP server")] = False,
    db_url: Annotated[str | None, typer.Option("--db-url", help="Database connection string")] = None,
) -> None:
    """Run autonomous background daemon with cron scheduling and Prometheus metrics."""
    settings = _resolve_settings(mock, db_url)

    title = "Autonomous Congressional Crawler Daemon"
    status_str = "[yellow]RUN-ONCE[/yellow]" if run_once else "[green]ACTIVE DAEMON[/green]"
    if mock:
        status_str += " | [magenta]MOCK SIMULATION[/magenta]"
    server_info = f"http://{host}:{port}/healthz" if not no_server else "Disabled"

    console.print(
        Panel(
            f"Mode: {status_str}\n"
            f"Healthcheck/Metrics: {server_info}\n"
            f"House Cron: {house_cron}\n"
            f"Senate Cron: {senate_cron}\n"
            f"Friday Sweep: {friday_cron}",
            title=title,
        )
    )

    async def _execute() -> None:
        daemon_runner = CrawlerDaemon(
            settings=settings,
            host=host,
            port=port,
            enable_server=not no_server,
        )
        res = await daemon_runner.start(
            house_cron=house_cron,
            senate_cron=senate_cron,
            friday_cron=friday_cron,
            use_mock=mock,
            dry_run=dry_run,
            run_once=run_once,
            initial_sync=initial_sync,
        )
        if run_once:
            console.print("[bold green]Run-once pass completed successfully.[/bold green]")
            metrics = res.get("metrics", {})
            console.print(f"Total Sync Runs: {metrics.get('sync_runs')}")
            console.print(f"Trades Extracted: {metrics.get('trades_extracted')}")

    asyncio.run(_execute())


@app.command()
def stats(
    db_url: Annotated[str | None, typer.Option("--db-url", help="Database connection string")] = None,
) -> None:
    """Display aggregate statistics of all congressional trading data ingested into the database."""
    settings = get_settings()
    if db_url is not None:
        settings.database_url = db_url

    async def _execute() -> None:
        db = DatabaseManager(settings=settings)
        await db.connect()
        try:
            data = await db.get_stats()
            table = Table(title="Congressional Trading Database Summary")
            table.add_column("Metric", style="cyan")
            table.add_column("Count", style="bold white")

            table.add_row("Total Filings", str(data["total_filings"]))
            table.add_row("Total Transactions", str(data["total_transactions"]))
            table.add_row("Unique Tickers Traded", str(data["distinct_tickers"]))

            for tx_type, count in data["by_type"].items():
                table.add_row(f"Direction: {tx_type}", str(count))

            for chamber, count in data.get("by_chamber", {}).items():
                table.add_row(f"Chamber: {chamber.upper()}", str(count))

            if data.get("insider_trades"):
                table.add_row("SEC Form 4 Insider Trades", str(data["insider_trades"]))
            if data.get("institutional_holdings"):
                table.add_row("SEC 13F Institutional Holdings", str(data["institutional_holdings"]))
            # Both collectors were retired; these two rows are a record of what is
            # already stored locally so an operator can see it and purge it. They
            # are labelled retired so nobody reads them as a running feed.
            if data.get("finra_otc"):
                table.add_row("FINRA OTC / Dark Pool Records (retired, no longer collected)", str(data["finra_otc"]))
            if data.get("cboe_options"):
                table.add_row("CBOE Daily Options Records (retired, no longer collected)", str(data["cboe_options"]))
            if data.get("macro_indicators"):
                table.add_row("FRED Macro Indicators", str(data["macro_indicators"]))
            if data.get("options_chains"):
                table.add_row("Options Chains EOD Records", str(data["options_chains"]))

            console.print(table)
        finally:
            await db.close()

    asyncio.run(_execute())


@app.command(name="archive-options")
def archive_options_command(
    days_to_keep: Annotated[
        int, typer.Option("--days-to-keep", "-k", help="Days of options data to keep in live database")
    ] = 90,
    output_dir: Annotated[
        str, typer.Option("--output-dir", "-o", help="Directory to store compressed .csv.gz archives")
    ] = "./archives",
    prune: Annotated[
        bool,
        typer.Option(
            "--prune/--no-prune", help="Prune archived records from live database after export (default: True)"
        ),
    ] = True,
    symbol: Annotated[str | None, typer.Option("--symbol", "-s", help="Filter by ticker symbol (e.g. SPY)")] = None,
    db_url: Annotated[str | None, typer.Option("--db-url", help="Database connection string")] = None,
) -> None:
    """Archive older options chains into compressed .csv.gz and prune live database."""
    from datetime import UTC, datetime, timedelta
    from pathlib import Path

    settings = get_settings()
    if db_url is not None:
        settings.database_url = db_url

    async def _archive() -> None:
        cutoff = (datetime.now(UTC).date() - timedelta(days=days_to_keep)).isoformat()
        out_path = Path(output_dir) / f"options_eod_{symbol or 'all'}_before_{cutoff}.csv.gz"
        console.print(f"[bold cyan]Archiving options chains older than {cutoff} to {out_path}...[/bold cyan]")

        async with DatabaseManager(settings=settings) as db:
            exported = await db.export_options_chains_gzip(str(out_path), symbol=symbol, before_date=cutoff)
            pruned = 0
            if prune and exported > 0:
                pruned = await db.prune_options_chains_older_than(days_to_keep=days_to_keep, symbol=symbol)

            table = Table(title="Options Archival & Pruning Summary")
            table.add_column("Field", style="cyan")
            table.add_column("Value", style="bold white")
            table.add_row("Cutoff Date", cutoff)
            table.add_row("Archive File", str(out_path))
            table.add_row("Records Exported", str(exported))
            table.add_row("Records Pruned from Live DB", str(pruned))
            console.print(table)

    asyncio.run(_archive())


@app.command(name="monitor")
def monitor_command(
    dataset: Annotated[str | None, typer.Option("--dataset", "-d", help="Filter by dataset (options, daily)")] = None,
    follow: Annotated[
        bool, typer.Option("--follow", "-f", help="Continuously refresh and display live progress")
    ] = False,
    interval: Annotated[
        float, typer.Option("--interval", "-i", help="Polling interval in seconds for follow mode")
    ] = 2.0,
    db_url: Annotated[str | None, typer.Option("--db-url", help="Database connection string")] = None,
) -> None:
    """Monitor live background harvester execution progress in real-time."""
    from rich.console import Group
    from rich.live import Live
    from rich.panel import Panel

    from harvester.core.progress import HarvestProgressTracker, ProgressState

    settings = get_settings()
    if db_url is not None:
        settings.database_url = db_url

    async def _fetch_state() -> ProgressState | None:
        state = HarvestProgressTracker.load_checkpoint(DEFAULT_CHECKPOINT_PATH)
        if state is not None and (dataset is None or state.dataset.lower() == dataset.lower()):
            return state

        try:
            async with DatabaseManager(settings=settings) as db:
                rows = await db.get_harvester_progress(dataset=dataset)
                if rows:
                    return ProgressState.from_dict(rows[0])
        except Exception:
            pass
        return None

    def _build_renderable(state: ProgressState | None) -> Any:
        if state is None:
            return Panel(
                "[dim]Waiting for harvester job to start...[/dim]",
                title="GreeksView Harvester Monitor",
                border_style="yellow",
            )

        tracker = HarvestProgressTracker(
            dataset=state.dataset,
            total_tickers=state.total_tickers,
            total_days=state.total_days,
            console=console,
            interactive=False,
        )
        tracker.tickers_done = state.tickers_done
        tracker.current_ticker = state.current_ticker
        tracker.current_trading_day = state.current_trading_day
        tracker.completed_units = state.completed_units
        tracker.total_units = state.total_units
        tracker.records_harvested = state.records_harvested
        tracker.records_upserted = state.records_upserted
        tracker.errors_count = state.errors_count
        tracker.status = state.status
        tracker._rate_override = state.rate
        tracker._eta_display_override = state.eta_display
        tracker._elapsed_override = state.elapsed_seconds

        sub = (
            f"[bold cyan]Sessions:[/bold cyan] {tracker.completed_units:,} / {tracker.total_units:,} "
            f"([bold green]{tracker.pct_complete:.1f}%[/bold green]) | "
            f"[bold cyan]Upserted Rows:[/bold cyan] {tracker.records_upserted:,} | "
            f"[bold cyan]Rate:[/bold cyan] [cyan]{state.rate:.1f} req/s[/cyan] | "
            f"[bold cyan]ETA:[/bold cyan] [bold white]{state.eta_display}[/bold white] | "
            f"[bold cyan]Status:[/bold cyan] [bold yellow]{tracker.status.upper()}[/bold yellow] | "
            f"[bold cyan]Last Updated:[/bold cyan] {state.updated_at}"
        )
        return Group(tracker.build_progress_table(), sub)

    async def _display_loop() -> None:
        if not follow:
            state = await _fetch_state()
            if state is None:
                console.print("[yellow]No active or recorded harvester progress found.[/yellow]")
                console.print("[dim]Run a harvester pass using: harvester run alphavantage --dataset options ...[/dim]")
                return
            console.print(_build_renderable(state))
            return

        initial_state = await _fetch_state()
        renderable = _build_renderable(initial_state)

        with Live(renderable, console=console, refresh_per_second=4) as live:
            try:
                while True:
                    await asyncio.sleep(interval)
                    state = await _fetch_state()
                    live.update(_build_renderable(state), refresh=True)
            except (KeyboardInterrupt, asyncio.CancelledError):
                pass

    asyncio.run(_display_loop())


@app.command(name="patterns")
def patterns_command(
    symbol: Annotated[str, typer.Argument(help="Ticker symbol to analyze (e.g. AAPL, NVDA, SPY)")],
    interval: Annotated[str, typer.Option("--interval", "-i", help="Intraday bar interval")] = "5min",
    days: Annotated[int, typer.Option("--days", "-d", help="Lookback window in calendar days")] = 365,
    json_output: Annotated[bool, typer.Option("--json", help="Emit raw JSON to stdout")] = False,
    export: Annotated[str | None, typer.Option("--export", "-e", help="Export format: 'json'")] = None,
    output: Annotated[str | None, typer.Option("--output", "-o", help="Output file path for export")] = None,
    db_url: Annotated[str | None, typer.Option("--db-url", help="Database connection string")] = None,
) -> None:
    """Analyze high-resolution intraday bars for repeatable quantitative alpha patterns."""
    import json

    from harvester.analytics.pattern_engine import PatternEngine

    settings = get_settings()
    db_path = db_url or settings.database_url
    if not json_output:
        console.print(
            f"[bold cyan]Running quantitative pattern analysis for {symbol.upper()} ({interval}, lookback: {days}d)...[/bold cyan]"
        )

    try:
        engine = PatternEngine(db_path=db_path)
        report = engine.run_analysis(symbol=symbol, interval=interval, days=days)
    except Exception as e:
        if json_output:
            print(json.dumps({"error": str(e), "symbol": symbol}))
        else:
            console.print(f"[bold red]Error running pattern analysis:[/bold red] {e}")
        raise typer.Exit(code=1) from e

    if json_output:
        print(json.dumps(report.to_dict(), indent=2))
        return

    # Print Master Summary Table
    table = Table(
        title=f"Quantitative Pattern Master Report // {report.symbol} ({report.date_start} to {report.date_end})"
    )
    table.add_column("Quantitative Pattern Feature", style="bold cyan")
    table.add_column("Empirical Metric", style="bold green")
    table.add_column("Sample Size", style="white")
    table.add_column("Institutional Action / Decision Rule", style="yellow")

    table.add_row(
        "First 45-Min Anchor Lock",
        f"{report.anchor_45m_rate}%",
        f"{report.total_days} Days",
        "Stop-loss 10c beyond opening 45m extreme; 88.6% hold barrier",
    )
    table.add_row(
        "Overnight Gap Fill (EOD)",
        f"{report.gap_fill_eod_rate}%",
        f"{report.gap_fill_1030_rate}% by 10:30",
        "Fade 09:35 open to prior close; abort if open past 10:30 AM",
    )
    table.add_row(
        "VWAP ±2.0σ Extreme Mean Reversion",
        f"{report.vwap_reversion_rate}%",
        f"{report.vwap_touch_2s_rate}% touched",
        "Fade moves touching ±2σ back to VWAP; stop on 2 closes beyond 2.5σ",
    )
    table.add_row(
        "60-Min Initial Balance (IB) Expansion",
        f"{report.ib_trend_rate}% Single-Side",
        "10.2% Chop",
        "Enter 10:30 AM breakout; stop at IB midpoint; target 1.5x IB extension",
    )
    table.add_row(
        "Fair Value Gap (FVG) Retest & Hold",
        f"{report.fvg_retest_rate}% Retest",
        f"{report.fvg_held_rate}% S/R Held",
        "Resting limit at 50% FVG midpoint; stop 1 tick past Bar 1 origin",
    )
    table.add_row(
        "Previous Day High/Low (PDH/PDL) Sweeps",
        f"{report.pdh_reject_rate}% PDH Rej",
        f"{report.pdl_reject_rate}% PDL Rej",
        "50/50 trap; require 2-bar close inside range before executing fade",
    )
    table.add_row(
        "Lunch Squeeze (<0.50%) & PM Breakout",
        f"{report.lunch_tight_rate}% Tight Days",
        f"{report.lunch_clean_pm_rate}% PM Break",
        "Avoid 11:30-13:30 entries; set bracket breakout alerts for 13:30 ET",
    )
    console.print(table)

    # Print Options Strategy Mapping
    opts_table = Table(title="Institutional Options Strategy Mapping Matrix")
    opts_table.add_column("Stock Pattern", style="bold cyan")
    opts_table.add_column("Recommended Structure", style="bold green")
    opts_table.add_column("Delta / Greeks", style="yellow")
    opts_table.add_column("Execution Rationale", style="white")

    for rec in report.options_recommendations:
        opts_table.add_row(
            rec["pattern"],
            rec["trade_structure"],
            rec["delta"],
            rec["rationale"],
        )
    console.print(opts_table)

    # Optional Export
    if export == "json":
        out_file = output or f"{symbol.lower()}_pattern_report.json"
        with open(out_file, "w") as f:
            json.dump(report.to_dict(), f, indent=2)
        console.print(f"[bold green]Exported JSON report to {out_file}[/bold green]")


if __name__ == "__main__":
    app()
