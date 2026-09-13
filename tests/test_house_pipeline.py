"""Unit and integration tests for the HousePipeline orchestrator."""

from datetime import date
from unittest.mock import AsyncMock, patch

import pytest

from harvester.config import Settings
from harvester.core.db import DatabaseManager
from harvester.core.http_client import ResilientHttpClient
from harvester.workers.congressional.house.pipeline import HousePipeline


@pytest.mark.asyncio
async def test_pipeline_mock_execution(test_db: DatabaseManager, test_http_client: ResilientHttpClient) -> None:
    pipeline = HousePipeline(db=test_db, http_client=test_http_client)
    report = await pipeline.run(year=2024, use_mock=True)

    assert report.year == 2024
    assert report.filtered_ptrs == 2
    assert report.new_filings_to_crawl == 2
    assert report.filings_parsed == 2
    assert report.transactions_extracted >= 2
    assert report.errors_count == 0

    # Re-running pipeline immediately sees existing records and crawls 0 new filings
    report_rerun = await pipeline.run(year=2024, use_mock=True)
    assert report_rerun.filtered_ptrs == 2
    assert report_rerun.new_filings_to_crawl == 0
    assert report_rerun.filings_parsed == 0


@pytest.mark.asyncio
async def test_pipeline_dry_run(test_db: DatabaseManager, test_http_client: ResilientHttpClient) -> None:
    # Fresh in-memory DB
    settings = Settings(database_url="sqlite:///:memory:", simulation_mode=True)
    db = DatabaseManager(settings=settings, sqlite_path=":memory:")
    await db.connect()

    pipeline = HousePipeline(db=db, http_client=test_http_client, settings=settings)
    report = await pipeline.run(year=2024, dry_run=True, use_mock=True)

    assert report.filings_parsed == 2
    assert report.transactions_extracted >= 2

    # Verify nothing was written to DB in dry-run
    stats = await db.get_stats()
    assert stats["total_filings"] == 0
    assert stats["total_transactions"] == 0
    await db.close()


@pytest.mark.asyncio
async def test_pipeline_limit_applied(test_db: DatabaseManager, test_http_client: ResilientHttpClient) -> None:
    pipeline = HousePipeline(db=test_db, http_client=test_http_client)
    report = await pipeline.run(year=2024, limit=1, use_mock=True)

    assert report.new_filings_to_crawl == 1
    assert report.filings_parsed == 1


@pytest.mark.asyncio
async def test_pipeline_no_ptrs_found(test_db: DatabaseManager, test_http_client: ResilientHttpClient) -> None:
    pipeline = HousePipeline(db=test_db, http_client=test_http_client)
    with patch.object(pipeline.bulk_crawler, "get_ptrs", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = []
        report = await pipeline.run(year=2024, use_mock=False)
        assert report.filtered_ptrs == 0
        assert report.filings_parsed == 0


@pytest.mark.asyncio
async def test_pipeline_error_handling(test_db: DatabaseManager, test_http_client: ResilientHttpClient) -> None:
    pipeline = HousePipeline(db=test_db, http_client=test_http_client)
    # Simulate PDF parse failure
    with patch.object(pipeline.pdf_parser, "parse_pdf", side_effect=RuntimeError("Corrupt PDF")):
        report = await pipeline.run(year=2024, use_mock=True)
        assert report.errors_count == 2
        assert len(report.error_details) == 2

    # Simulate DB write error
    with patch.object(test_db, "upsert_filing", side_effect=RuntimeError("DB Disk Full")):
        report_db_err = await pipeline.run(year=2024, use_mock=True)
        assert report_db_err.errors_count == 2


@pytest.mark.asyncio
async def test_pipeline_live_http_pdf_fetch(test_db: DatabaseManager) -> None:
    from unittest.mock import MagicMock

    from harvester.core.models import HouseIndexRecord
    from harvester.workers.congressional.simulation.mock_house_server import MockHouseServer

    settings = Settings(database_url="sqlite:///:memory:", simulation_mode=False)
    client = ResilientHttpClient()
    pipeline = HousePipeline(db=test_db, http_client=client, settings=settings)

    mock_pdf = MockHouseServer.generate_mock_ptr_pdf()
    mock_resp = MagicMock()
    mock_resp.content = mock_pdf

    with patch.object(pipeline.bulk_crawler, "get_ptrs", new_callable=AsyncMock) as mock_get_ptrs:
        mock_rec = HouseIndexRecord(
            last_name="Pelosi",
            first_name="Nancy",
            filing_type="P",
            doc_id="20024999",
            year=2024,
            filing_date=date(2024, 1, 15),
        )
        mock_get_ptrs.return_value = [mock_rec]
        with patch.object(client, "get", new_callable=AsyncMock) as mock_http_get:
            mock_http_get.return_value = mock_resp
            report = await pipeline.run(year=2024, use_mock=False)
            assert report.filings_parsed == 1
            assert report.transactions_extracted >= 1
    await client.close()


@pytest.mark.asyncio
async def test_pipeline_filing_without_transactions(
    test_db: DatabaseManager, test_http_client: ResilientHttpClient
) -> None:
    from harvester.core.models import CongressionalFiling

    pipeline = HousePipeline(db=test_db, http_client=test_http_client)
    filing = CongressionalFiling(
        filing_id="FILING_HOUSE_20024888",
        chamber="house",
        member_name="Jane EmptyFiler",
        filing_year=2024,
        filing_date=date(2024, 1, 15),
        sha256_hash="emptyhash",
    )
    with patch.object(pipeline.pdf_parser, "parse_pdf", return_value=(filing, [])):
        report = await pipeline.run(year=2024, limit=1, use_mock=True)
        assert report.filings_parsed == 1
        assert report.transactions_extracted == 0
