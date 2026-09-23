"""Unit and integration tests for GreeksView Multi-Worker Harvester Suite."""

import sqlite3
from pathlib import Path

import pytest
from typer.testing import CliRunner

from harvester.cli import app
from harvester.config import Settings
from harvester.core.base_worker import WorkerResult
from harvester.workers import (
    RETIRED_WORKERS,
    WORKER_REGISTRY,
    CongressionalWorker,
    FredMacroWorker,
    RetiredWorkerError,
    SecEdgarWorker,
    get_worker,
    list_workers,
)

runner = CliRunner()

RETIRED_NAMES = ("cboe_options", "finra_darkpool")


@pytest.fixture
def mock_settings() -> Settings:
    return Settings(
        database_url="sqlite:///:memory:",
        simulation_mode=True,
        request_timeout_seconds=2.0,
        http_max_retries=1,
    )


def test_worker_registry_coverage() -> None:
    """Verify all 4 remaining core background workers are registered."""
    expected = {"congressional", "sec_edgar", "fred_macro", "alphavantage"}
    assert set(WORKER_REGISTRY.keys()) == expected

    workers = list_workers()
    assert len(workers) == 4
    names = {w["name"] for w in workers}
    assert names == expected


def test_get_worker() -> None:
    """Test get_worker lookup and error handling."""
    w = get_worker("fred_macro")
    assert isinstance(w, FredMacroWorker)
    assert w.name == "fred_macro"

    w_sec = get_worker("sec_edgar")
    assert isinstance(w_sec, SecEdgarWorker)

    with pytest.raises(KeyError):
        get_worker("nonexistent_feed")


def test_retired_workers_are_not_registered() -> None:
    """The retired names are declared retired and are not runnable names."""
    assert set(RETIRED_WORKERS) == set(RETIRED_NAMES)
    # Presence: the live registry is non-empty. Absence: no retired name is in it.
    assert WORKER_REGISTRY
    assert set(RETIRED_WORKERS).isdisjoint(WORKER_REGISTRY)

    listed = {w["name"] for w in list_workers()}
    assert listed  # presence
    assert listed.isdisjoint(RETIRED_NAMES)


@pytest.mark.parametrize("name", RETIRED_NAMES)
def test_get_worker_refuses_a_retired_worker_with_a_reason(name: str) -> None:
    """Asking for a retired worker raises, and the message says why."""
    with pytest.raises(RetiredWorkerError) as exc:
        get_worker(name)
    message = str(exc.value)
    assert name in message
    assert "retired" in message.lower()
    # The licence position and the invented-row behaviour both have to be in the reason.
    assert "invented a record" in message
    assert "Cboe's data terms" in message or "FINRA's data terms" in message
    # A retired name must never fall through to a live worker.
    assert isinstance(exc.value, KeyError)


@pytest.mark.parametrize("name", RETIRED_NAMES)
def test_cli_run_refuses_a_retired_worker(name: str) -> None:
    """`harvester run <retired>` exits non-zero and prints the reason; it never runs a worker."""
    result = runner.invoke(app, ["run", name, "--mock", "--db-url", "sqlite:///:memory:"])
    assert result.exit_code == 1
    assert "retired" in result.stdout.lower()
    assert "Worker Run Summary" not in result.stdout
    assert "SUCCESS" not in result.stdout


@pytest.mark.parametrize("name", RETIRED_NAMES)
def test_retired_worker_modules_are_gone(name: str) -> None:
    """The retired worker packages are deleted, so nothing can import or `python -m` them."""
    package = "cboe_options" if name == "cboe_options" else "finra_darkpool"
    workers_dir = Path(__file__).resolve().parent.parent / "harvester" / "workers"
    assert workers_dir.is_dir()  # presence
    assert not (workers_dir / package).exists()  # absence

    with pytest.raises(ModuleNotFoundError):
        __import__(f"harvester.workers.{package}.worker")


def test_cli_run_rejects_the_retired_weeks_back_flag() -> None:
    """--weeks-back belonged to the retired FINRA worker; it must be rejected, not ignored.

    A flag no remaining worker consumes would otherwise be accepted and silently
    dropped, which reads to the operator as if the window had been applied.
    """
    result = runner.invoke(app, ["run", "fred_macro", "--weeks-back", "4", "--mock", "--db-url", "sqlite:///:memory:"])
    assert result.exit_code != 0
    assert "Worker Run Summary" not in result.stdout
    # Same invocation without the flag is accepted, so the failure is the flag, not the command.
    ok = runner.invoke(app, ["run", "fred_macro", "--limit", "2", "--mock", "--db-url", "sqlite:///:memory:"])
    assert ok.exit_code == 0


def test_cli_run_all_touches_neither_retired_table(tmp_path: Path) -> None:
    """A real `run-all` writes rows for a live worker and none for either retired feed."""
    db_file = tmp_path / "run_all_retired.db"
    result = runner.invoke(app, ["run-all", "--mock", "--db-url", f"sqlite:///{db_file}"])
    assert result.exit_code == 0

    for name in RETIRED_NAMES:
        assert f"Running worker: {name}" not in result.stdout

    conn = sqlite3.connect(str(db_file))
    try:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        # Presence: a live worker actually wrote rows in this same run.
        assert "macro_indicators" in tables
        assert conn.execute("SELECT COUNT(*) FROM macro_indicators").fetchone()[0] > 0
        # Absence: the retired tables still exist in the schema and stayed empty.
        for table in ("cboe_daily_options", "finra_otc_volume"):
            assert table in tables
            assert conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_fred_macro_worker(mock_settings: Settings) -> None:
    """Test FRED Macro indicators & Treasury Yield Curve harvester."""
    worker = FredMacroWorker(settings=mock_settings)
    assert any("24" in v for v in worker.target_views)

    res = await worker.run_once(series_ids=["DGS10", "DGS2", "FEDFUNDS"], limit_points_per_series=5, use_mock=True)
    assert isinstance(res, WorkerResult)
    assert res.is_success
    assert res.records_harvested > 0
    assert res.records_upserted > 0

    h = await worker.health()
    assert h["worker"] == "fred_macro"
    assert h["database_connected"] is True


@pytest.mark.asyncio
async def test_sec_edgar_worker(mock_settings: Settings) -> None:
    """Test SEC EDGAR Form 4 / 13F harvester."""
    worker = SecEdgarWorker(settings=mock_settings)
    assert any("09" in v for v in worker.target_views)

    res = await worker.run_once(form4=True, form13f=True, tickers=["NVDA", "AAPL"], limit=2)
    assert isinstance(res, WorkerResult)
    assert res.is_success
    assert res.records_harvested >= 2


@pytest.mark.asyncio
async def test_congressional_worker_run_once(mock_settings: Settings) -> None:
    """Test Congressional disclosure worker run_once in simulation mode."""
    worker = CongressionalWorker(settings=mock_settings)
    res = await worker.run_once(house=True, senate=True, limit=2, use_mock=True)
    assert isinstance(res, WorkerResult)
    assert res.is_success
    assert res.records_harvested > 0

    # Test individual chamber execution
    res_house = await worker.run_once(house=True, senate=False, limit=1, use_mock=True)
    assert res_house.is_success


@pytest.mark.asyncio
async def test_congressional_worker_health(mock_settings: Settings) -> None:
    """Test Congressional disclosure worker healthcheck."""
    worker = CongressionalWorker(settings=mock_settings)
    h = await worker.health()
    assert h["worker"] == "congressional"
    assert h["database_connected"] is True


def test_cli_list() -> None:
    """Test harvester list command output: live workers listed, retired ones offered to nobody."""
    result = runner.invoke(app, ["list"])
    assert result.exit_code == 0
    assert "fred_macro" in result.stdout
    assert "sec_edgar" in result.stdout
    assert "congressional" in result.stdout
    for name in RETIRED_NAMES:
        assert name not in result.stdout


def test_cli_run_help_offers_only_live_workers() -> None:
    """The `run` argument help is the reader-facing contract; it must not name a retired worker."""
    result = runner.invoke(app, ["run", "--help"])
    assert result.exit_code == 0
    # Rich wraps the help text, so compare with whitespace collapsed.
    help_text = " ".join(result.stdout.split())
    assert "congressional" in help_text
    assert "alphavantage" in help_text
    for name in RETIRED_NAMES:
        assert name not in help_text


def test_cli_run_worker() -> None:
    """Test harvester run <worker> command."""
    # Run FRED worker
    res_fred = runner.invoke(app, ["run", "fred_macro", "--limit", "2", "--mock", "--db-url", "sqlite:///:memory:"])
    assert res_fred.exit_code == 0
    assert "SUCCESS" in res_fred.stdout

    # Run SEC EDGAR worker
    res_sec = runner.invoke(app, ["run", "sec_edgar", "--limit", "2", "--mock", "--db-url", "sqlite:///:memory:"])
    assert res_sec.exit_code == 0


def test_cli_health() -> None:
    """Test harvester health diagnostic command."""
    result = runner.invoke(app, ["health", "--db-url", "sqlite:///:memory:"])
    assert result.exit_code == 0
    assert "sec_edgar" in result.stdout
    assert "fred_macro" in result.stdout
    assert "congressional" in result.stdout
    for name in RETIRED_NAMES:
        assert name not in result.stdout


def test_cli_run_all() -> None:
    """Test harvester run-all command in simulation mode."""
    result = runner.invoke(app, ["run-all", "--mock", "--db-url", "sqlite:///:memory:"])
    assert result.exit_code == 0
    assert "Running worker: congressional" in result.stdout
    assert "Running worker: fred_macro" in result.stdout
