"""Autonomous containerized crawler daemon coordinator with signal handling and lifecycle management."""

import asyncio
import logging
import signal
from typing import Any

from harvester.config import Settings
from harvester.core.db import DatabaseManager
from harvester.core.http_client import ResilientHttpClient
from harvester.orchestration.metrics import HealthcheckServer, MetricsCollector
from harvester.orchestration.scheduler import (
    DEFAULT_FRIDAY_SWEEP_CRON,
    DEFAULT_HOUSE_CRON,
    DEFAULT_SENATE_CRON,
    CrawlerScheduler,
)

logger = logging.getLogger(__name__)


class CrawlerDaemon:
    """Long-running daemon managing HTTP healthchecks, APScheduler, and signal shutdown."""

    def __init__(
        self,
        settings: Settings | None = None,
        db: DatabaseManager | None = None,
        http_client: ResilientHttpClient | None = None,
        collector: MetricsCollector | None = None,
        host: str = "0.0.0.0",
        port: int = 8080,
        enable_server: bool = True,
    ) -> None:
        self.settings = settings or Settings()
        self.db = db or DatabaseManager(settings=self.settings)
        self.http_client = http_client or ResilientHttpClient(
            timeout_seconds=self.settings.request_timeout_seconds,
            max_retries=self.settings.http_max_retries,
            max_concurrency=self.settings.max_concurrent_downloads,
        )
        self.collector = collector or MetricsCollector()
        self.host = host
        self.port = port
        self.enable_server = enable_server

        self.scheduler = CrawlerScheduler(
            db=self.db,
            http_client=self.http_client,
            settings=self.settings,
            collector=self.collector,
        )

        async def check_db() -> bool:
            try:
                stats = await self.db.get_stats()
                return stats is not None
            except Exception:
                return False

        self.health_server = HealthcheckServer(
            host=self.host,
            port=self.port,
            collector=self.collector,
            db_check_cb=check_db,
        )
        self.shutdown_event = asyncio.Event()

    def handle_signal(self, sig: int, frame: Any = None) -> None:
        """Signal handler to initiate graceful daemon termination."""
        sig_name = signal.Signals(sig).name if hasattr(signal, "Signals") else str(sig)
        logger.info("Received termination signal %s. Initiating graceful shutdown...", sig_name)
        self.shutdown_event.set()

    async def start(
        self,
        house_cron: str = DEFAULT_HOUSE_CRON,
        senate_cron: str = DEFAULT_SENATE_CRON,
        friday_cron: str = DEFAULT_FRIDAY_SWEEP_CRON,
        use_mock: bool = False,
        dry_run: bool = False,
        run_once: bool = False,
        initial_sync: bool = False,
    ) -> dict[str, Any]:
        """Start the daemon lifecycle."""
        logger.info("Starting CrawlerDaemon...")
        await self.db.connect()

        # 1. Start HTTP health/metrics server
        if self.enable_server:
            try:
                await self.health_server.start()
            except Exception as exc:
                logger.warning("Failed to start healthcheck server on %s:%s: %s", self.host, self.port, exc)

        # 2. Setup periodic schedules
        self.scheduler.setup_default_jobs(
            house_cron=house_cron,
            senate_cron=senate_cron,
            friday_cron=friday_cron,
            use_mock=use_mock,
            dry_run=dry_run,
        )
        self.scheduler.start()

        # 3. Optional initial sync sweep
        initial_res = {}
        if initial_sync or run_once:
            logger.info("Executing initial synchronization pass...")
            initial_res = await self.scheduler.run_all_sync(use_mock=use_mock, dry_run=dry_run)

        # 4. If run_once, shutdown immediately and return summary
        if run_once:
            logger.info("Run-once completed. Shutting down daemon...")
            await self.stop()
            return {
                "mode": "run-once",
                "initial_sync": initial_res,
                "metrics": self.collector.to_dict(),
            }

        # 5. Wait for shutdown signal
        logger.info("Daemon running. Awaiting termination signal or jobs...")
        await self.shutdown_event.wait()

        # 6. Graceful cleanup
        await self.stop()
        return {
            "mode": "daemon",
            "metrics": self.collector.to_dict(),
        }

    async def stop(self) -> None:
        """Gracefully release all resources, background jobs, and DB connections."""
        logger.info("Stopping CrawlerDaemon resources...")
        self.scheduler.stop()
        if self.enable_server:
            await self.health_server.stop()
        await self.http_client.close()
        await self.db.close()
        logger.info("CrawlerDaemon stopped gracefully")
