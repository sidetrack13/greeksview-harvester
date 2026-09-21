"""Shared pytest fixtures and test environment setup."""

import ipaddress
import socket
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio

import harvester.config
from harvester.config import Settings
from harvester.core.db import DatabaseManager
from harvester.core.http_client import ResilientHttpClient
from harvester.workers.congressional.simulation.mock_house_server import MockHouseServer

FIXTURES_DIR = Path(__file__).parent / "fixtures"

_REAL_CONNECT = socket.socket.connect
_REAL_CONNECT_EX = socket.socket.connect_ex
_REAL_GETADDRINFO = socket.getaddrinfo


class OutboundNetworkBlockedError(OSError):
    """A test tried to reach a host other than loopback."""


def _is_loopback_host(host: Any) -> bool:
    if host in ("localhost", "", None):
        return True
    if isinstance(host, bytes):
        host = host.decode()
    try:
        return ipaddress.ip_address(str(host).split("%", 1)[0]).is_loopback
    except ValueError:
        return False


def _guard_address(address: Any) -> None:
    if isinstance(address, (str, bytes)):  # AF_UNIX path
        return
    if isinstance(address, tuple) and address and _is_loopback_host(address[0]):
        return
    raise OutboundNetworkBlockedError(f"Outbound network is blocked in tests: {address!r}")


def _guarded_connect(self: socket.socket, address: Any) -> None:
    _guard_address(address)
    _REAL_CONNECT(self, address)


def _guarded_connect_ex(self: socket.socket, address: Any) -> int:
    _guard_address(address)
    return _REAL_CONNECT_EX(self, address)


def _guarded_getaddrinfo(host: Any, *args: Any, **kwargs: Any) -> Any:
    if not _is_loopback_host(host):
        raise OutboundNetworkBlockedError(f"DNS lookup is blocked in tests: {host!r}")
    return _REAL_GETADDRINFO(host, *args, **kwargs)


@pytest.fixture(autouse=True)
def _no_outbound_network_and_no_real_credentials(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """No test may reach a vendor, read a real key, or pick up a real database URL.

    respx-mocked requests never open a socket, and loopback stays open for the
    daemon's local health server.
    """
    monkeypatch.setattr(socket.socket, "connect", _guarded_connect)
    monkeypatch.setattr(socket.socket, "connect_ex", _guarded_connect_ex)
    monkeypatch.setattr(socket, "getaddrinfo", _guarded_getaddrinfo)
    # Environment variables outrank .env files in pydantic-settings; pin them empty.
    monkeypatch.setenv("ALPHAVANTAGE_API_KEY", "")
    monkeypatch.setenv("DATABASE_URL", "")
    monkeypatch.setattr(harvester.config, "_SIBLING_GV_ENV", tmp_path / "no-sibling.env")


@pytest.fixture
def test_settings() -> Settings:
    """Return test configuration using in-memory SQLite and simulation mode."""
    return Settings(
        database_url="sqlite:///:memory:",
        simulation_mode=True,
        request_timeout_seconds=5.0,
        http_max_retries=2,
        max_concurrent_downloads=2,
    )


@pytest_asyncio.fixture
async def test_db(test_settings: Settings) -> DatabaseManager:
    """Provide an initialized in-memory SQLite database manager."""
    db = DatabaseManager(settings=test_settings, sqlite_path=":memory:")
    await db.connect()
    yield db
    await db.close()


@pytest_asyncio.fixture
async def test_http_client() -> ResilientHttpClient:
    """Provide an initialized async HTTP client."""
    client = ResilientHttpClient(timeout_seconds=5.0, max_retries=2)
    yield client
    await client.close()


@pytest.fixture
def sample_xml_bytes() -> bytes:
    """Load sample 2024FD_sample.xml fixture bytes."""
    xml_path = FIXTURES_DIR / "2024FD_sample.xml"
    if xml_path.exists():
        return xml_path.read_bytes()
    return MockHouseServer.generate_mock_xml()


@pytest.fixture
def sample_pdf_bytes() -> bytes:
    """Load sample House PTR PDF fixture bytes."""
    pdf_path = FIXTURES_DIR / "sample_house_ptr.pdf"
    if pdf_path.exists():
        return pdf_path.read_bytes()
    return MockHouseServer.generate_mock_ptr_pdf("Nancy Pelosi")
