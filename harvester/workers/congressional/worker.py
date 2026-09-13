"""
GreeksView Data Harvester — Congressional Disclosures Worker
============================================================
Autonomous background worker for U.S. House & Senate financial disclosures.
Inherits BaseWorker for independent execution and centralized orchestration.
"""

import asyncio
import logging
import time
from datetime import UTC, datetime
from typing import Any

from harvester.config import Settings, get_settings
from harvester.core.base_worker import BaseWorker, WorkerResult
from harvester.core.db import DatabaseManager
from harvester.core.http_client import resilient_http_client
from harvester.workers.congressional.house.pipeline import HousePipeline
from harvester.workers.congressional.senate.client import SenateEfdClient
from harvester.workers.congressional.senate.pipeline import SenatePipeline

logger = logging.getLogger("harvester.workers.congressional")


class CongressionalWorker(BaseWorker):
    """Background worker ingesting House PTRs and Senate eFD filings."""

    name: str = "congressional"
    description: str = "U.S. Congressional (House & Senate) Periodic Transaction Disclosures Harvester"
    frequency: str = "daily"
    target_views: list[str] = ["View 14 (Congressional Trades)"]

    def __init__(self, settings: Settings | None = None, config: Any | None = None):
        super().__init__(settings or config or get_settings())
        self.settings: Settings = self.config

    async def run_once(
        self,
        house: bool = True,
        senate: bool = True,
        year: int | None = None,
        all_years: bool = True,
        limit: int | None = None,
        **kwargs,
    ) -> WorkerResult:
        """Run single ingestion pass for House and/or Senate disclosures."""
        start_time = time.time()
        errors: list[str] = []
        harvested = 0
        upserted = 0

        current_year = datetime.now(UTC).year
        effective_all_years = kwargs.get("all_years", all_years)
        if year is not None:
            target_years = [year]
        elif effective_all_years:
            target_years = list(range(current_year, self.settings.stock_act_inception_year - 1, -1))
        else:
            target_years = [current_year]

        logger.info(
            "Starting Congressional harvest (Years=%s, House=%s, Senate=%s, Limit=%s)",
            target_years,
            house,
            senate,
            limit if limit is not None else "UNLIMITED",
        )

        use_mock = kwargs.get("use_mock", False) or self.settings.simulation_mode

        try:
            async with DatabaseManager(self.settings) as db, resilient_http_client(
                timeout_seconds=self.settings.request_timeout_seconds,
                max_retries=self.settings.http_max_retries,
                max_concurrency=self.settings.max_concurrent_downloads,
            ) as http_client:
                senate_client = SenateEfdClient(http_client=http_client, settings=self.settings) if senate else None
                for target_year in target_years:
                    if house:
                        try:
                            hp = HousePipeline(db=db, http_client=http_client, settings=self.settings)
                            h_report = await hp.run(year=target_year, limit=limit, use_mock=use_mock)
                            harvested += h_report.filings_parsed
                            upserted += h_report.transactions_extracted
                        except Exception as e:
                            logger.error("House ingestion failed for %d: %s", target_year, e)
                            errors.append(f"House ({target_year}): {str(e)}")

                    if senate:
                        try:
                            sp = SenatePipeline(db=db, http_client=http_client, settings=self.settings, client=senate_client)
                            s_report = await sp.run(year=target_year, limit=limit, use_mock=use_mock)
                            harvested += s_report.filings_parsed
                            upserted += s_report.transactions_extracted
                        except Exception as e:
                            logger.error("Senate ingestion failed for %d: %s", target_year, e)
                            errors.append(f"Senate ({target_year}): {str(e)}")

                    # Small polite delay between years to avoid upstream rate limiting
                    await asyncio.sleep(0.5)

        except Exception as e:
            logger.error("Database or network setup failed: %s", e)
            errors.append(f"Infrastructure: {str(e)}")

        duration = round(time.time() - start_time, 2)
        status = "success" if not errors else ("partial" if upserted > 0 else "failed")

        return WorkerResult(
            worker=self.name,
            status=status,
            records_harvested=harvested,
            records_upserted=upserted,
            duration_seconds=duration,
            errors=errors,
            metadata={"years": target_years, "house": house, "senate": senate},
        )

    async def health(self) -> dict[str, Any]:
        """Verify database connectivity and target endpoint availability."""
        db_ok = False
        try:
            async with DatabaseManager(self.settings) as db:
                await db.initialize_tables()
                db_ok = True
        except Exception as e:
            logger.warning("Congressional worker healthcheck DB failed: %s", e)

        return {
            "worker": self.name,
            "database_connected": db_ok,
            "status": "healthy" if db_ok else "unhealthy",
        }


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)
    worker = CongressionalWorker()
    res = asyncio.run(worker.run_once(house=True, senate=True, limit=5))
    print(res.model_dump_json(indent=2))
    sys.exit(0 if res.is_success else 1)
