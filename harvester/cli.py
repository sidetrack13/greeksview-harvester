"""Rich Command Line Interface for GreeksView Congressional Trading Crawler."""

import asyncio
from typing import Annotated, Any

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from harvester import __version__
from harvester.config import get_settings
from harvester.core.db import DatabaseManager
from harvester.core.http_client import resilient_http_client
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
    worker_name: Annotated[str, typer.Argument(help="Name of worker (congressional, sec_edgar, finra_darkpool, cboe_options, fred_macro, alphavantage)")],
    limit: Annotated[int | None, typer.Option("--limit", "-l", help="Record or symbol limit (default: None for max history)")] = None,
    mock: Annotated[bool, typer.Option("--mock", help="Use synthetic mock/simulation data")] = False,
    year: Annotated[int | None, typer.Option("--year", "-y", help="Target calendar year")] = None,
    all_years: Annotated[bool, typer.Option("--all-years/--single-year", help="Sweep all historical years back to 2012 (default: True)")] = True,
    days_back: Annotated[int | None, typer.Option("--days-back", help="Historical trading days back for CBOE (default: 252 for full year)")] = None,
    weeks_back: Annotated[int | None, typer.Option("--weeks-back", help="Historical weeks back for FINRA OTC (default: 52 for full year)")] = None,
    dataset: Annotated[str | None, typer.Option("--dataset", "-d", help="Dataset for Alpha Vantage (daily, intraday, options, fundamentals, actions, reference, all)")] = None,
    symbols: Annotated[str | None, typer.Option("--symbols", "-s", help="Comma-separated ticker symbols (e.g. SPY,QQQ,AAPL)")] = None,
    db_url: Annotated[str | None, typer.Option("--db-url", help="Database connection string")] = None,
) -> None:
    """Execute a single background worker independently."""
    settings = get_settings()
    if db_url is not None:
        settings.database_url = db_url

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
        with console.status(f"[bold green]Running {worker.name} harvesting pass..."):
            kwargs: dict[str, Any] = {"use_mock": mock}
            if limit is not None:
                kwargs["limit"] = limit
            if year is not None:
                kwargs["year"] = year
            else:
                kwargs["all_years"] = all_years
            if days_back is not None:
                kwargs["days_back"] = days_back
            if weeks_back is not None:
                kwargs["weeks_back"] = weeks_back
            if dataset is not None:
                kwargs["dataset"] = dataset
            if symbols is not None:
                kwargs["symbols"] = symbols
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
        if res.errors:
            console.print("[bold red]Errors encountered during run:[/bold red]")
            for err in res.errors[:5]:
                console.print(f"  [red]•[/red] {err}")

    asyncio.run(_run())


@app.command(name="run-all")
def run_all_command(
    limit: Annotated[int | None, typer.Option("--limit", "-l", help="Record limit per worker (default: None for max history)")] = None,
    mock: Annotated[bool, typer.Option("--mock", help="Use synthetic mock data")] = False,
    db_url: Annotated[str | None, typer.Option("--db-url", help="Database connection string")] = None,
) -> None:
    """Execute all registered harvesting workers in sequence."""
    settings = get_settings()
    if db_url is not None:
        settings.database_url = db_url

    async def _run_all() -> None:
        results = []
        for name in WORKER_REGISTRY:
            worker = get_worker(name, config=settings)
            console.print(f"[bold cyan]=> Running worker: {name}[/bold cyan]")
            try:
                res = await worker.run_once(limit=limit, use_mock=mock)
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
    pg_url: Annotated[str | None, typer.Option("--pg-url", help="Target PostgreSQL connection string (defaults to DATABASE_URL)")] = None,
    sqlite_path: Annotated[str, typer.Option("--sqlite-path", help="Source SQLite database file path")] = "greeksview_harvester.db",
    batch_size: Annotated[int, typer.Option("--batch-size", "-b", help="Batch size for PostgreSQL inserts")] = 1000,
    table: Annotated[list[str] | None, typer.Option("--table", "-t", help="Specific table(s) to sync (default: all)")] = None,
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
    settings = get_settings()
    if db_url is not None:
        settings.database_url = db_url

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
    settings = get_settings()
    if db_url is not None:
        settings.database_url = db_url

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
    settings = get_settings()
    if db_url is not None:
        settings.database_url = db_url

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
            if data.get("finra_otc"):
                table.add_row("FINRA OTC / Dark Pool Records", str(data["finra_otc"]))
            if data.get("cboe_options"):
                table.add_row("CBOE Daily Options Records", str(data["cboe_options"]))
            if data.get("macro_indicators"):
                table.add_row("FRED Macro Indicators", str(data["macro_indicators"]))

            console.print(table)
        finally:
            await db.close()

    asyncio.run(_execute())


if __name__ == "__main__":
    app()
