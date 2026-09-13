"""Unit and integration tests for CrawlerDaemon."""

import asyncio
import signal
import socket
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from harvester.config import Settings
from harvester.core.db import DatabaseManager
from harvester.core.http_client import ResilientHttpClient
from harvester.orchestration.daemon import CrawlerDaemon


def get_free_port() -> int:
    """Find an available port for test server binding."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.mark.asyncio
async def test_daemon_run_once() -> None:
    settings = Settings(database_url="sqlite:///:memory:", simulation_mode=True)
    db = DatabaseManager(settings=settings, sqlite_path=":memory:")
    http_client = ResilientHttpClient()
    port = get_free_port()

    daemon = CrawlerDaemon(
        settings=settings,
        db=db,
        http_client=http_client,
        host="127.0.0.1",
        port=port,
        enable_server=True,
    )

    res = await daemon.start(
        use_mock=True,
        dry_run=True,
        run_once=True,
        initial_sync=True,
    )

    assert res["mode"] == "run-once"
    assert "initial_sync" in res
    assert res["metrics"]["sync_runs"]["house"] >= 1
    assert res["metrics"]["sync_runs"]["senate"] >= 1


@pytest.mark.asyncio
async def test_daemon_signal_and_shutdown() -> None:
    settings = Settings(database_url="sqlite:///:memory:", simulation_mode=True)
    db = DatabaseManager(settings=settings, sqlite_path=":memory:")
    http_client = ResilientHttpClient()
    port = get_free_port()

    daemon = CrawlerDaemon(
        settings=settings,
        db=db,
        http_client=http_client,
        host="127.0.0.1",
        port=port,
        enable_server=True,
    )

    # Launch daemon in background task
    task = asyncio.create_task(
        daemon.start(
            use_mock=True,
            dry_run=True,
            run_once=False,
            initial_sync=False,
        )
    )

    # Wait for daemon and server to be ready
    await asyncio.sleep(0.2)

    # Verify health server responds
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"http://127.0.0.1:{port}/healthz")
        assert resp.status_code == 200
        assert resp.json()["status"] == "healthy"

    # Send SIGINT signal
    daemon.handle_signal(signal.SIGINT)

    # Wait for task completion
    res = await task
    assert res["mode"] == "daemon"


@pytest.mark.asyncio
async def test_daemon_server_bind_failure_handled() -> None:
    settings = Settings(database_url="sqlite:///:memory:", simulation_mode=True)
    db = DatabaseManager(settings=settings, sqlite_path=":memory:")
    http_client = ResilientHttpClient()
    port = get_free_port()

    # Occupy the port with a temporary socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", port))
    sock.listen(1)

    try:
        daemon = CrawlerDaemon(
            settings=settings,
            db=db,
            http_client=http_client,
            host="127.0.0.1",
            port=port,
            enable_server=True,
        )

        res = await daemon.start(
            use_mock=True,
            dry_run=True,
            run_once=True,
            initial_sync=False,
        )
        assert res["mode"] == "run-once"
    finally:
        sock.close()


@pytest.mark.asyncio
async def test_daemon_check_db_callback_healthy_and_failure() -> None:
    daemon = CrawlerDaemon()
    check_cb = daemon.health_server.db_check_cb
    assert check_cb is not None

    # Healthy
    with patch.object(daemon.db, "get_stats", new_callable=AsyncMock, return_value={"total": 1}):
        assert await check_cb() is True

    # Failure (covers lines 58-59 in daemon.py)
    with patch.object(daemon.db, "get_stats", new_callable=AsyncMock, side_effect=RuntimeError("db crash")):
        assert await check_cb() is False
