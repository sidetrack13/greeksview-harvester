"""APScheduler-based background cron coordinator for House & Senate disclosure synchronization."""

import logging
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from harvester.config import Settings
from harvester.core.db import DatabaseManager
from harvester.core.http_client import ResilientHttpClient
from harvester.orchestration.metrics import MetricsCollector
from harvester.workers.congressional.house.pipeline import HousePipeline
from harvester.workers.congressional.senate.pipeline import SenatePipeline

logger = logging.getLogger(__name__)

DEFAULT_HOUSE_CRON = "0 */4 * * *"
DEFAULT_SENATE_CRON = "0 * * * *"
DEFAULT_FRIDAY_SWEEP_CRON = "*/15 12-18 * * 4"


class CrawlerScheduler:
    """Coordinates cron jobs for periodic House and Senate trading ingestion."""

    def __init__(
        self,
        db: DatabaseManager,
        http_client: ResilientHttpClient,
        settings: Settings | None = None,
        collector: MetricsCollector | None = None,
        scheduler: AsyncIOScheduler | None = None,
    ) -> None:
        self.db = db
        self.http_client = http_client
        self.settings = settings or Settings()
        self.collector = collector or MetricsCollector()
        self.scheduler = scheduler or AsyncIOScheduler()
        self.house_pipeline = HousePipeline(db=self.db, http_client=self.http_client, settings=self.settings)
        self.senate_pipeline = SenatePipeline(db=self.db, http_client=self.http_client, settings=self.settings)
        self._running = False

    @property
    def is_running(self) -> bool:
        """Return True if the scheduler is active."""
        return self._running

    def setup_default_jobs(
        self,
        house_cron: str = DEFAULT_HOUSE_CRON,
        senate_cron: str = DEFAULT_SENATE_CRON,
        friday_cron: str = DEFAULT_FRIDAY_SWEEP_CRON,
        use_mock: bool = False,
        dry_run: bool = False,
    ) -> None:
        """Register the standard periodic crawl schedules."""
        # House sync
        self.scheduler.add_job(
            self.run_house_sync,
            CronTrigger.from_crontab(house_cron),
            id="house_sync",
            name="House PTR Periodic Sync",
            replace_existing=True,
            kwargs={"use_mock": use_mock, "dry_run": dry_run},
        )
        logger.info("Registered House crawl job with schedule: %s", house_cron)

        # Senate sync
        self.scheduler.add_job(
            self.run_senate_sync,
            CronTrigger.from_crontab(senate_cron),
            id="senate_sync",
            name="Senate eFD Hourly Sync",
            replace_existing=True,
            kwargs={"use_mock": use_mock, "dry_run": dry_run},
        )
        logger.info("Registered Senate crawl job with schedule: %s", senate_cron)

        # Friday afternoon sweep (both chambers)
        self.scheduler.add_job(
            self.run_all_sync,
            CronTrigger.from_crontab(friday_cron),
            id="friday_sweep",
            name="Friday Afternoon Rapid Disclosure Sweep",
            replace_existing=True,
            kwargs={"use_mock": use_mock, "dry_run": dry_run},
        )
        logger.info("Registered Friday sweep job with schedule: %s", friday_cron)

    async def run_house_sync(
        self,
        year: int | None = None,
        limit: int | None = None,
        dry_run: bool = False,
        use_mock: bool = False,
    ) -> dict[str, Any]:
        """Execute House crawl pipeline and update metrics."""
        logger.info("Executing scheduled House sync...")
        try:
            report = await self.house_pipeline.run(year=year, limit=limit, dry_run=dry_run, use_mock=use_mock)
            self.collector.record_sync(
                chamber="house",
                filings_count=report.filings_parsed,
                trades_count=report.transactions_extracted,
                errors=report.errors_count,
            )
            return {
                "chamber": "house",
                "status": "success",
                "filings_parsed": report.filings_parsed,
                "transactions_extracted": report.transactions_extracted,
                "errors_count": report.errors_count,
            }
        except Exception as exc:
            logger.error("Error during scheduled House sync: %s", exc)
            self.collector.record_sync(chamber="house", errors=1)
            return {"chamber": "house", "status": "error", "error": str(exc)}

    async def run_senate_sync(
        self,
        year: int | None = None,
        limit: int | None = None,
        dry_run: bool = False,
        use_mock: bool = False,
    ) -> dict[str, Any]:
        """Execute Senate crawl pipeline and update metrics."""
        logger.info("Executing scheduled Senate sync...")
        try:
            report = await self.senate_pipeline.run(year=year, limit=limit, dry_run=dry_run, use_mock=use_mock)
            self.collector.record_sync(
                chamber="senate",
                filings_count=report.filings_parsed,
                trades_count=report.transactions_extracted,
                errors=report.errors_count,
            )
            return {
                "chamber": "senate",
                "status": "success",
                "filings_parsed": report.filings_parsed,
                "transactions_extracted": report.transactions_extracted,
                "errors_count": report.errors_count,
            }
        except Exception as exc:
            logger.error("Error during scheduled Senate sync: %s", exc)
            self.collector.record_sync(chamber="senate", errors=1)
            return {"chamber": "senate", "status": "error", "error": str(exc)}

    async def run_all_sync(
        self,
        year: int | None = None,
        limit: int | None = None,
        dry_run: bool = False,
        use_mock: bool = False,
    ) -> dict[str, Any]:
        """Execute synchronization across both House and Senate."""
        house_res = await self.run_house_sync(year=year, limit=limit, dry_run=dry_run, use_mock=use_mock)
        senate_res = await self.run_senate_sync(year=year, limit=limit, dry_run=dry_run, use_mock=use_mock)
        return {
            "house": house_res,
            "senate": senate_res,
        }

    def get_jobs_info(self) -> list[dict[str, Any]]:
        """Return list of registered scheduler jobs and their next fire times."""
        jobs = []
        for j in self.scheduler.get_jobs():
            next_time = getattr(j, "next_run_time", None)
            next_run = next_time.isoformat() if next_time is not None else None
            jobs.append(
                {
                    "id": j.id,
                    "name": j.name,
                    "next_run_time": next_run,
                }
            )
        return jobs

    def start(self) -> None:
        """Start scheduler event loop."""
        if not self._running:
            self.scheduler.start()
            self._running = True
            logger.info("CrawlerScheduler started successfully with %s active jobs", len(self.scheduler.get_jobs()))

    def stop(self) -> None:
        """Gracefully stop scheduler."""
        if self._running:
            self.scheduler.shutdown(wait=False)
            self._running = False
            logger.info("CrawlerScheduler stopped")
