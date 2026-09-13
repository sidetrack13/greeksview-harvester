"""Unit and integration tests for GreeksView Multi-Worker Harvester Suite."""

import pytest
from typer.testing import CliRunner

from harvester.cli import app
from harvester.config import Settings
from harvester.core.base_worker import WorkerResult
from harvester.workers import (
    WORKER_REGISTRY,
    CboeOptionsWorker,
    CongressionalWorker,
    FinraDarkPoolWorker,
    FredMacroWorker,
    SecEdgarWorker,
    get_worker,
    list_workers,
)

runner = CliRunner()


@pytest.fixture
def mock_settings() -> Settings:
    return Settings(
        database_url="sqlite:///:memory:",
        simulation_mode=True,
        request_timeout_seconds=2.0,
        http_max_retries=1,
    )


def test_worker_registry_coverage() -> None:
    """Verify all 5 core background workers are registered."""
    expected = {"congressional", "sec_edgar", "finra_darkpool", "cboe_options", "fred_macro"}
    assert set(WORKER_REGISTRY.keys()) == expected

    workers = list_workers()
    assert len(workers) == 5
    names = {w["name"] for w in workers}
    assert names == expected


def test_get_worker() -> None:
    """Test get_worker lookup and error handling."""
    w = get_worker("finra_darkpool")
    assert isinstance(w, FinraDarkPoolWorker)
    assert w.name == "finra_darkpool"

    w_sec = get_worker("sec_edgar")
    assert isinstance(w_sec, SecEdgarWorker)

    with pytest.raises(KeyError):
        get_worker("nonexistent_feed")


@pytest.mark.asyncio
async def test_finra_darkpool_worker(mock_settings: Settings) -> None:
    """Test FINRA OTC Dark Pool volume harvester in simulation mode."""
    worker = FinraDarkPoolWorker(settings=mock_settings)
    assert "View 29 (OTC / Dark Pool Market Share)" in worker.target_views

    res = await worker.run_once(symbols=["NVDA", "AAPL", "SPY"], limit=3, use_mock=True)
    assert isinstance(res, WorkerResult)
    assert res.is_success
    assert res.records_harvested == 3
    assert res.records_upserted == 3
    assert len(res.errors) == 0

    h = await worker.health()
    assert h["worker"] == "finra_darkpool"
    assert h["database_connected"] is True


@pytest.mark.asyncio
async def test_cboe_options_worker(mock_settings: Settings) -> None:
    """Test CBOE Daily Options Volume & Put/Call Ratio harvester."""
    worker = CboeOptionsWorker(settings=mock_settings)
    assert any("21" in v for v in worker.target_views)
    assert any("27" in v for v in worker.target_views)

    res = await worker.run_once(days_back=3, use_mock=True)
    assert isinstance(res, WorkerResult)
    assert res.is_success
    assert res.records_harvested == 3
    assert res.records_upserted == 3

    h = await worker.health()
    assert h["worker"] == "cboe_options"
    assert h["database_connected"] is True


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
    """Test harvester list command output."""
    result = runner.invoke(app, ["list"])
    assert result.exit_code == 0
    assert "finra_darkpool" in result.stdout
    assert "cboe_options" in result.stdout
    assert "fred_macro" in result.stdout
    assert "sec_edgar" in result.stdout
    assert "congressional" in result.stdout


def test_cli_run_worker() -> None:
    """Test harvester run <worker> command."""
    result = runner.invoke(app, ["run", "finra_darkpool", "--limit", "2", "--mock", "--db-url", "sqlite:///:memory:"])
    assert result.exit_code == 0
    assert "SUCCESS" in result.stdout

    # Run CBOE worker
    res_cboe = runner.invoke(app, ["run", "cboe_options", "--limit", "2", "--mock", "--db-url", "sqlite:///:memory:"])
    assert res_cboe.exit_code == 0

    # Run FRED worker
    res_fred = runner.invoke(app, ["run", "fred_macro", "--limit", "2", "--mock", "--db-url", "sqlite:///:memory:"])
    assert res_fred.exit_code == 0

    # Run SEC EDGAR worker
    res_sec = runner.invoke(app, ["run", "sec_edgar", "--limit", "2", "--mock", "--db-url", "sqlite:///:memory:"])
    assert res_sec.exit_code == 0


def test_cli_health() -> None:
    """Test harvester health diagnostic command."""
    result = runner.invoke(app, ["health", "--db-url", "sqlite:///:memory:"])
    assert result.exit_code == 0
    assert "finra_darkpool" in result.stdout
    assert "cboe_options" in result.stdout
    assert "sec_edgar" in result.stdout
    assert "fred_macro" in result.stdout
    assert "congressional" in result.stdout


def test_cli_run_all() -> None:
    """Test harvester run-all command in simulation mode."""
    result = runner.invoke(app, ["run-all", "--mock", "--db-url", "sqlite:///:memory:"])
    assert result.exit_code == 0
    assert "Running worker: congressional" in result.stdout
    assert "Running worker: fred_macro" in result.stdout
