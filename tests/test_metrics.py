"""Unit tests for MetricsCollector and HealthcheckServer."""

import asyncio
import socket

import httpx
import pytest

from harvester.orchestration.metrics import HealthcheckServer, MetricsCollector


def get_free_port() -> int:
    """Find an available port for test server binding."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def test_metrics_collector_basic() -> None:
    collector = MetricsCollector()
    assert collector.uptime_seconds >= 0.0

    # Record syncs
    collector.record_sync(chamber="house", filings_count=5, trades_count=12, errors=0)
    collector.record_sync(chamber="senate", filings_count=3, trades_count=8, errors=1)
    collector.record_sync(chamber="custom_ch", filings_count=1, trades_count=2, errors=0)

    assert collector.sync_runs["house"] == 1
    assert collector.sync_runs["senate"] == 1
    assert collector.sync_runs["custom_ch"] == 1
    assert collector.trades_extracted["house"] == 12
    assert collector.trades_extracted["senate"] == 8
    assert collector.trades_extracted["custom_ch"] == 2
    assert collector.filings_parsed["house"] == 5
    assert collector.filings_parsed["senate"] == 3
    assert collector.filings_parsed["custom_ch"] == 1
    assert collector.errors_count == 1

    # Format to prometheus
    prom_text = collector.generate_prometheus_metrics()
    assert "crawler_uptime_seconds" in prom_text
    assert 'crawler_sync_runs_total{chamber="house"} 1' in prom_text
    assert 'crawler_trades_extracted_total{chamber="senate"} 8' in prom_text
    assert "crawler_errors_total 1" in prom_text

    # Dict serialization
    d = collector.to_dict()
    assert d["errors_count"] == 1
    assert d["sync_runs"]["house"] == 1


@pytest.mark.asyncio
async def test_healthcheck_server_endpoints() -> None:
    port = get_free_port()
    collector = MetricsCollector()
    collector.record_sync(chamber="house", filings_count=2, trades_count=4)

    async def mock_db_healthy() -> bool:
        return True

    server = HealthcheckServer(
        host="127.0.0.1",
        port=port,
        collector=collector,
        db_check_cb=mock_db_healthy,
    )
    await server.start()

    async with httpx.AsyncClient() as client:
        # 1. /healthz (healthy)
        resp_h = await client.get(f"http://127.0.0.1:{port}/healthz")
        assert resp_h.status_code == 200
        data_h = resp_h.json()
        assert data_h["status"] == "healthy"
        assert data_h["db_connected"] is True

        # 2. /metrics
        resp_m = await client.get(f"http://127.0.0.1:{port}/metrics")
        assert resp_m.status_code == 200
        assert "crawler_sync_runs_total" in resp_m.text

        # 3. /status
        resp_s = await client.get(f"http://127.0.0.1:{port}/status")
        assert resp_s.status_code == 200
        data_s = resp_s.json()
        assert data_s["status"] == "running"
        assert data_s["metrics"]["sync_runs"]["house"] == 1

        # 4. 404 Not Found
        resp_404 = await client.get(f"http://127.0.0.1:{port}/unknown-path")
        assert resp_404.status_code == 404

        # 5. 405 Method Not Allowed (POST)
        resp_405 = await client.post(f"http://127.0.0.1:{port}/healthz")
        assert resp_405.status_code == 405

    await server.stop()


@pytest.mark.asyncio
async def test_healthcheck_server_degraded_db() -> None:
    port = get_free_port()

    async def mock_db_failing() -> bool:
        raise RuntimeError("DB unreachable")

    server = HealthcheckServer(
        host="127.0.0.1",
        port=port,
        db_check_cb=mock_db_failing,
    )
    await server.start()

    async with httpx.AsyncClient() as client:
        resp = await client.get(f"http://127.0.0.1:{port}/healthz")
        assert resp.status_code == 503
        data = resp.json()
        assert data["status"] == "degraded"
        assert data["db_connected"] is False

    await server.stop()


@pytest.mark.asyncio
async def test_healthcheck_server_malformed_and_empty_requests() -> None:
    port = get_free_port()
    server = HealthcheckServer(host="127.0.0.1", port=port)
    await server.start()

    # Send raw empty TCP connection
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.close()
    await writer.wait_closed()

    # Send malformed request line
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(b"BADREQUEST\r\n\r\n")
    await writer.drain()
    resp_raw = await reader.read(1024)
    assert b"400 Bad Request" in resp_raw
    writer.close()
    # Request healthz with db_check_cb=None (covers branch 152->158)
    async with httpx.AsyncClient() as client:
        resp_no_cb = await client.get(f"http://127.0.0.1:{port}/healthz")
        assert resp_no_cb.status_code == 200
        assert resp_no_cb.json()["db_connected"] is True

    await server.stop()


@pytest.mark.asyncio
async def test_healthcheck_server_client_exception_and_writer_close_error() -> None:
    from unittest.mock import AsyncMock, MagicMock

    server = HealthcheckServer()
    mock_reader = AsyncMock()
    mock_reader.readline.side_effect = RuntimeError("network broken")
    mock_writer = AsyncMock()
    mock_writer.close = MagicMock()
    mock_writer.wait_closed.side_effect = RuntimeError("socket close broken")

    # Gracefully catch both reader exception and writer close exception
    await server._handle_client(mock_reader, mock_writer)
