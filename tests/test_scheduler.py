"""Unit and integration tests for CrawlerScheduler."""

from unittest.mock import patch

import pytest

from harvester.core.db import DatabaseManager
from harvester.core.http_client import ResilientHttpClient
from harvester.orchestration.metrics import MetricsCollector
from harvester.orchestration.scheduler import CrawlerScheduler


@pytest.mark.asyncio
async def test_scheduler_setup_and_info(test_db: DatabaseManager, test_http_client: ResilientHttpClient) -> None:
    collector = MetricsCollector()
    scheduler = CrawlerScheduler(
        db=test_db,
        http_client=test_http_client,
        collector=collector,
    )
    scheduler.setup_default_jobs(use_mock=True, dry_run=True)

    jobs = scheduler.get_jobs_info()
    job_ids = {j["id"] for j in jobs}
    assert "house_sync" in job_ids
    assert "senate_sync" in job_ids
    assert "friday_sweep" in job_ids

    assert scheduler.is_running is False
    scheduler.start()
    assert scheduler.is_running is True
    # Repeat start is idempotent
    scheduler.start()
    assert scheduler.is_running is True

    scheduler.stop()
    assert scheduler.is_running is False
    # Repeat stop is idempotent
    scheduler.stop()
    assert scheduler.is_running is False


@pytest.mark.asyncio
async def test_scheduler_run_sync_methods(test_db: DatabaseManager, test_http_client: ResilientHttpClient) -> None:
    collector = MetricsCollector()
    scheduler = CrawlerScheduler(
        db=test_db,
        http_client=test_http_client,
        collector=collector,
    )

    # 1. House sync success
    h_res = await scheduler.run_house_sync(year=2024, limit=2, dry_run=True, use_mock=True)
    assert h_res["status"] == "success"
    assert h_res["chamber"] == "house"
    assert collector.sync_runs["house"] == 1

    # 2. Senate sync success
    s_res = await scheduler.run_senate_sync(year=2024, limit=2, dry_run=True, use_mock=True)
    assert s_res["status"] == "success"
    assert s_res["chamber"] == "senate"
    assert collector.sync_runs["senate"] == 1

    # 3. All sync
    all_res = await scheduler.run_all_sync(year=2024, limit=1, dry_run=True, use_mock=True)
    assert all_res["house"]["status"] == "success"
    assert all_res["senate"]["status"] == "success"
    assert collector.sync_runs["house"] == 2
    assert collector.sync_runs["senate"] == 2


@pytest.mark.asyncio
async def test_scheduler_error_handling(test_db: DatabaseManager, test_http_client: ResilientHttpClient) -> None:
    collector = MetricsCollector()
    scheduler = CrawlerScheduler(
        db=test_db,
        http_client=test_http_client,
        collector=collector,
    )

    # House error
    with patch.object(scheduler.house_pipeline, "run", side_effect=RuntimeError("House network failure")):
        h_res = await scheduler.run_house_sync(year=2024)
        assert h_res["status"] == "error"
        assert "House network failure" in h_res["error"]

    # Senate error
    with patch.object(scheduler.senate_pipeline, "run", side_effect=RuntimeError("Senate eFD handshake fail")):
        s_res = await scheduler.run_senate_sync(year=2024)
        assert s_res["status"] == "error"
        assert "Senate eFD handshake fail" in s_res["error"]

    assert collector.errors_count == 2
