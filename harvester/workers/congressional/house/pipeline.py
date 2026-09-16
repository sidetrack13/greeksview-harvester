"""End-to-end House of Representatives disclosure ingestion and persistence pipeline."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING

from harvester.config import Settings, get_settings
from harvester.core.db import DatabaseManager
from harvester.core.http_client import ResilientHttpClient
from harvester.core.models import (
    CongressionalFiling,
    CongressionalTransaction,
    CrawlReport,
    HouseIndexRecord,
)
from harvester.workers.congressional.house.bulk_crawler import HouseBulkCrawler
from harvester.workers.congressional.house.pdf_parser import HousePTRParser

if TYPE_CHECKING:
    from harvester.workers.congressional.simulation.mock_house_server import MockHouseServer

logger = logging.getLogger(__name__)


class HousePipeline:
    """Orchestrator for House bulk archive ingestion, parsing, and database persistence."""

    def __init__(
        self,
        db: DatabaseManager,
        http_client: ResilientHttpClient,
        settings: Settings | None = None,
        mock_server: MockHouseServer | None = None,
    ) -> None:
        self.db = db
        self.http_client = http_client
        self.settings = settings or get_settings()
        self.bulk_crawler = HouseBulkCrawler(http_client, self.settings)
        self.pdf_parser = HousePTRParser()
        self.mock_server = mock_server

    def build_pdf_url(self, year: int, doc_id: str) -> str:
        """Construct official direct PDF URL for a given filing DocID."""
        return f"{self.settings.house_pdf_base_url.rstrip('/')}/{year}/{doc_id}.pdf"

    async def run(
        self,
        year: int | None = None,
        limit: int | None = None,
        dry_run: bool = False,
        use_mock: bool = False,
    ) -> CrawlReport:
        """Execute the full House ingestion pipeline for the specified calendar year."""
        start_time = time.perf_counter()
        target_year = year or self.settings.default_year
        report = CrawlReport(year=target_year)

        logger.info(
            "Starting House ingestion pipeline for year %s (dry_run=%s, mock=%s, limit=%s)",
            target_year,
            dry_run,
            use_mock,
            limit,
        )

        # 1. Fetch and parse index
        if use_mock or self.settings.simulation_mode:
            from harvester.workers.congressional.simulation.mock_house_server import MockHouseServer

            mock_zip = MockHouseServer.generate_mock_zip(target_year)
            all_records = self.bulk_crawler.parse_xml_index(mock_zip, target_year)
            ptrs = [r for r in all_records if r.is_ptr]
        else:
            ptrs = await self.bulk_crawler.get_ptrs(target_year)

        report.filtered_ptrs = len(ptrs)
        if not ptrs:
            report.duration_seconds = time.perf_counter() - start_time
            logger.info("No House PTR filings found for year %s", target_year)
            return report

        # 2. Database differential: check existing filings to avoid redundant downloads
        candidate_filing_ids = [f"FILING_HOUSE_{r.doc_id}" for r in ptrs]
        existing_ids = await self.db.get_existing_filing_ids(candidate_filing_ids)
        new_records = [r for r in ptrs if f"FILING_HOUSE_{r.doc_id}" not in existing_ids]

        if limit is not None and limit > 0:
            new_records = new_records[:limit]

        report.new_filings_to_crawl = len(new_records)
        logger.info(
            "Total PTRs: %s, Already Ingested: %s, New to process: %s",
            len(ptrs),
            len(existing_ids),
            len(new_records),
        )

        if not new_records:
            report.duration_seconds = time.perf_counter() - start_time
            return report

        # 3. Process candidate filings with concurrency control
        semaphore = asyncio.Semaphore(self.settings.max_concurrent_downloads)

        async def process_record(
            rec: HouseIndexRecord,
        ) -> tuple[CongressionalFiling, list[CongressionalTransaction]] | None:
            async with semaphore:
                pdf_url = self.build_pdf_url(rec.year, rec.doc_id)
                try:
                    if use_mock or self.settings.simulation_mode:
                        from harvester.workers.congressional.simulation.mock_house_server import MockHouseServer

                        pdf_bytes = MockHouseServer.generate_mock_ptr_pdf(rec.member_name)
                    else:
                        resp = await self.http_client.get(pdf_url, use_cache=True)
                        pdf_bytes = resp.content

                    filing, txs = self.pdf_parser.parse_pdf(pdf_bytes, rec, doc_url=pdf_url)
                    return filing, txs
                except Exception as exc:
                    err_msg = f"Failed processing DocID {rec.doc_id} ({rec.member_name}): {exc}"
                    logger.error(err_msg)
                    report.errors_count += 1
                    report.error_details.append(err_msg)
                    return None

        tasks = [process_record(r) for r in new_records]
        results = await asyncio.gather(*tasks)

        # 4. Persistence (unless dry_run)
        for res in results:
            if res is None:
                continue
            filing, txs = res
            report.filings_parsed += 1
            report.transactions_extracted += len(txs)

            if not dry_run:
                try:
                    await self.db.upsert_filing(filing)
                    if txs:
                        await self.db.insert_transactions(txs)
                except Exception as exc:
                    err_msg = f"Database write error on {filing.filing_id}: {exc}"
                    logger.error(err_msg)
                    report.errors_count += 1
                    report.error_details.append(err_msg)

        report.duration_seconds = time.perf_counter() - start_time
        logger.info(
            "Pipeline completed in %.2fs: %s filings parsed, %s transactions extracted, %s errors",
            report.duration_seconds,
            report.filings_parsed,
            report.transactions_extracted,
            report.errors_count,
        )
        return report
