"""Unit and integration tests for SenatePipeline coordinator."""

from datetime import date
from unittest.mock import AsyncMock, patch

import pytest

from harvester.config import Settings
from harvester.core.db import DatabaseManager
from harvester.core.http_client import ResilientHttpClient
from harvester.core.models import CongressionalFiling, SenateReportRecord
from harvester.workers.congressional.senate.pipeline import SenatePipeline
from harvester.workers.congressional.simulation.mock_house_server import MockHouseServer
from harvester.workers.congressional.simulation.mock_senate_server import MockSenateServer


@pytest.mark.asyncio
async def test_senate_pipeline_mock_execution(test_db: DatabaseManager, test_http_client: ResilientHttpClient) -> None:
    pipeline = SenatePipeline(db=test_db, http_client=test_http_client)
    report = await pipeline.run(
        year=2024,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 12, 31),
        use_mock=True,
    )

    assert report.year == 2024
    assert report.chamber == "senate"
    assert report.filtered_ptrs == 2
    assert report.new_filings_to_crawl == 2
    assert report.filings_parsed == 2
    assert report.transactions_extracted >= 2
    assert report.errors_count == 0

    # Immediate rerun skips already ingested filings
    rerun_report = await pipeline.run(year=2024, use_mock=True)
    assert rerun_report.filtered_ptrs == 2
    assert rerun_report.new_filings_to_crawl == 0
    assert rerun_report.filings_parsed == 0


@pytest.mark.asyncio
async def test_senate_pipeline_dry_run(test_http_client: ResilientHttpClient) -> None:
    settings = Settings(database_url="sqlite:///:memory:", simulation_mode=True)
    db = DatabaseManager(settings=settings, sqlite_path=":memory:")
    await db.connect()

    pipeline = SenatePipeline(db=db, http_client=test_http_client, settings=settings)
    report = await pipeline.run(year=2024, dry_run=True, use_mock=True)

    assert report.filings_parsed == 2
    assert report.transactions_extracted >= 2

    # Verify no DB records were written in dry-run mode
    stats = await db.get_stats()
    assert stats["total_filings"] == 0
    assert stats["total_transactions"] == 0
    await db.close()


@pytest.mark.asyncio
async def test_senate_pipeline_limit(test_db: DatabaseManager, test_http_client: ResilientHttpClient) -> None:
    pipeline = SenatePipeline(db=test_db, http_client=test_http_client)
    report = await pipeline.run(year=2024, limit=1, use_mock=True)

    assert report.new_filings_to_crawl == 1
    assert report.filings_parsed == 1


@pytest.mark.asyncio
async def test_senate_pipeline_no_ptrs(test_db: DatabaseManager, test_http_client: ResilientHttpClient) -> None:
    pipeline = SenatePipeline(db=test_db, http_client=test_http_client)
    with patch.object(pipeline.client, "search_reports", new_callable=AsyncMock) as mock_search:
        mock_search.return_value = (0, [])
        report = await pipeline.run(year=2024, use_mock=False)
        assert report.filtered_ptrs == 0
        assert report.filings_parsed == 0


@pytest.mark.asyncio
async def test_senate_pipeline_live_and_paper_pdf(
    test_db: DatabaseManager, test_http_client: ResilientHttpClient
) -> None:
    pipeline = SenatePipeline(db=test_db, http_client=test_http_client)

    rec_html = SenateReportRecord(
        first_name="Mark",
        last_name="Warner",
        office="Senator (VA)",
        report_title="Periodic Transaction Report",
        report_url="https://efdsearch.senate.gov/search/view/ptr/d7a9b0c1/",
        received_date=date(2024, 2, 15),
        doc_id="d7a9b0c1",
    )
    rec_paper = SenateReportRecord(
        first_name="Tommy",
        last_name="Tuberville",
        office="Senator (AL)",
        report_title="Periodic Transaction Report",
        report_url="https://efdsearch.senate.gov/search/view/paper/88889999/",
        received_date=date(2024, 2, 10),
        doc_id="88889999",
    )

    mock_html = MockSenateServer.generate_mock_ptr_html("Mark Warner")
    mock_pdf = MockHouseServer.generate_mock_ptr_pdf("Tommy Tuberville")

    with patch.object(pipeline.client, "search_reports", new_callable=AsyncMock) as mock_search:
        mock_search.return_value = (2, [rec_html, rec_paper])
        with patch.object(pipeline.client, "fetch_report", new_callable=AsyncMock) as mock_fetch_html:
            mock_fetch_html.return_value = mock_html
            with patch.object(pipeline.client, "fetch_paper_pdf", new_callable=AsyncMock) as mock_fetch_pdf:
                mock_fetch_pdf.return_value = mock_pdf
                report = await pipeline.run(year=2024, use_mock=False)
                assert report.filtered_ptrs == 2
                assert report.filings_parsed == 2
                assert report.transactions_extracted >= 2


@pytest.mark.asyncio
async def test_senate_pipeline_paper_pdf_mock_branch(
    test_db: DatabaseManager, test_http_client: ResilientHttpClient
) -> None:
    pipeline = SenatePipeline(db=test_db, http_client=test_http_client)
    mock_data = {
        "data": [
            [
                "Paper",
                "Senator",
                "Senator (WY)",
                '<a href="/search/view/paper/12345/" target="_blank">Periodic Transaction Report</a>',
                "01/15/2024",
            ]
        ]
    }
    with patch(
        "harvester.workers.congressional.simulation.mock_senate_server.MockSenateServer.generate_mock_datatables_response",
        return_value=mock_data,
    ):
        report = await pipeline.run(year=2024, use_mock=True)
        assert report.filings_parsed == 1
        assert report.transactions_extracted >= 1


@pytest.mark.asyncio
async def test_senate_pipeline_error_handling(test_db: DatabaseManager, test_http_client: ResilientHttpClient) -> None:
    pipeline = SenatePipeline(db=test_db, http_client=test_http_client)

    # Simulate parse exception
    with patch.object(pipeline.html_parser, "parse_html", side_effect=RuntimeError("HTML parse crashed")):
        report = await pipeline.run(year=2024, use_mock=True)
        assert report.errors_count == 2
        assert len(report.error_details) == 2

    # Simulate DB write error
    with patch.object(test_db, "upsert_filing", side_effect=RuntimeError("Disk full")):
        report_db_err = await pipeline.run(year=2024, use_mock=True)
        assert report_db_err.errors_count == 2


@pytest.mark.asyncio
async def test_senate_pipeline_empty_transactions(
    test_db: DatabaseManager, test_http_client: ResilientHttpClient
) -> None:
    pipeline = SenatePipeline(db=test_db, http_client=test_http_client)
    filing = CongressionalFiling(
        filing_id="FILING_SENATE_999",
        chamber="senate",
        member_name="Empty Senator",
        filing_year=2024,
        filing_date=date(2024, 1, 15),
        sha256_hash="empty_hash",
    )
    with patch.object(pipeline.html_parser, "parse_html", return_value=(filing, [])):
        report = await pipeline.run(year=2024, limit=1, use_mock=True)
        assert report.filings_parsed == 1
        assert report.transactions_extracted == 0
