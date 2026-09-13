"""End-to-end ingestion orchestrator for U.S. Senate Financial Disclosures (eFD)."""

import asyncio
import logging
import time
from datetime import date

from harvester.config import Settings, get_settings
from harvester.core.db import DatabaseManager
from harvester.core.http_client import ResilientHttpClient
from harvester.core.models import (
    CongressionalFiling,
    CongressionalTransaction,
    CrawlReport,
    HouseIndexRecord,
    SenateReportRecord,
)
from harvester.workers.congressional.house.pdf_parser import HousePTRParser
from harvester.workers.congressional.senate.client import SenateEfdClient
from harvester.workers.congressional.senate.html_parser import SenateHtmlParser
from harvester.workers.congressional.simulation.mock_senate_server import MockSenateServer

logger = logging.getLogger(__name__)


class SenatePipeline:
    """Coordinates search, fetch, parse, and persistent storage of Senate PTR disclosures."""

    def __init__(
        self,
        db: DatabaseManager,
        http_client: ResilientHttpClient,
        settings: Settings | None = None,
    ) -> None:
        self.db = db
        self.http_client = http_client
        self.settings = settings or get_settings()
        self.client = SenateEfdClient(http_client=http_client, settings=self.settings)
        self.html_parser = SenateHtmlParser()
        self.pdf_parser = HousePTRParser()

    async def run(
        self,
        year: int | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int | None = None,
        dry_run: bool = False,
        use_mock: bool = False,
    ) -> CrawlReport:
        """Execute Senate PTR disclosure ingestion pipeline."""
        start_time = time.perf_counter()
        target_year = year or self.settings.default_year
        if not start_date:
            start_date = date(target_year, 1, 1)
        if not end_date:
            end_date = date(target_year, 12, 31)

        report = CrawlReport(year=target_year, chamber="senate")
        logger.info(
            "Starting Senate eFD crawl | Year: %s (%s to %s) | Mock: %s | Dry-run: %s",
            target_year,
            start_date,
            end_date,
            use_mock,
            dry_run,
        )

        # 1. Discover PTR reports
        if use_mock or self.settings.simulation_mode:
            mock_data = MockSenateServer.generate_mock_datatables_response()
            raw_rows = mock_data.get("data", [])
            ptrs: list[SenateReportRecord] = []
            for r in raw_rows:
                rec = self.client.parse_datatables_row(r)
                if rec is not None and rec.is_ptr:
                    ptrs.append(rec)
            report.total_index_filings = len(raw_rows)
        else:
            total_count, ptrs = await self.client.search_reports(
                start_date=start_date,
                end_date=end_date,
                limit=self.settings.crawler_batch_size,
            )
            report.total_index_filings = total_count

        report.filtered_ptrs = len(ptrs)
        if not ptrs:
            report.duration_seconds = time.perf_counter() - start_time
            logger.info("No Senate PTR filings discovered for period %s - %s", start_date, end_date)
            return report

        # 2. Database differential
        candidate_filing_ids = [f"FILING_SENATE_{r.doc_id}" for r in ptrs]
        existing_ids = await self.db.get_existing_filing_ids(candidate_filing_ids)
        new_records = [r for r in ptrs if f"FILING_SENATE_{r.doc_id}" not in existing_ids]

        if limit is not None and limit > 0:
            new_records = new_records[:limit]

        report.new_filings_to_crawl = len(new_records)
        logger.info(
            "Total Senate PTRs: %s, Already Ingested: %s, New to process: %s",
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
            rec: SenateReportRecord,
        ) -> tuple[CongressionalFiling, list[CongressionalTransaction]] | None:
            async with semaphore:
                try:
                    if rec.is_paper:
                        # Paper scan PDF fallback
                        if use_mock or self.settings.simulation_mode:
                            from harvester.workers.congressional.simulation.mock_house_server import MockHouseServer

                            pdf_bytes = MockHouseServer.generate_mock_ptr_pdf(rec.member_name)
                        else:
                            pdf_bytes = await self.client.fetch_paper_pdf(rec.report_url)

                        house_dummy_rec = HouseIndexRecord(
                            last_name=rec.last_name,
                            first_name=rec.first_name,
                            filing_type="P",
                            state_dst=rec.state,
                            year=rec.received_date.year,
                            filing_date=rec.received_date,
                            doc_id=rec.doc_id,
                        )
                        filing, txs = self.pdf_parser.parse_pdf(pdf_bytes, house_dummy_rec, doc_url=rec.report_url)
                        # Re-brand chamber to senate
                        filing.chamber = "senate"
                        filing.filing_id = f"FILING_SENATE_{rec.doc_id}"
                        for t in txs:
                            t.chamber = "senate"
                            t.filing_id = filing.filing_id
                        return filing, txs
                    else:
                        # Digital HTML report
                        if use_mock or self.settings.simulation_mode:
                            html_content = MockSenateServer.generate_mock_ptr_html(rec.member_name)
                        else:
                            html_content = await self.client.fetch_report(rec.report_url)

                        return self.html_parser.parse_html(html_content, rec)
                except Exception as exc:
                    err_msg = f"Failed processing Senate DocID {rec.doc_id} ({rec.member_name}): {exc}"
                    logger.error(err_msg)
                    report.errors_count += 1
                    report.error_details.append(err_msg)
                    return None

        tasks = [process_record(r) for r in new_records]
        results = await asyncio.gather(*tasks)

        # 4. Persistence
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
            "Senate crawl finished in %.2fs. Filings parsed: %s, Transactions: %s, Errors: %s",
            report.duration_seconds,
            report.filings_parsed,
            report.transactions_extracted,
            report.errors_count,
        )
        return report
