"""Shared pytest fixtures and test environment setup."""

from pathlib import Path

import pytest
import pytest_asyncio

from harvester.config import Settings
from harvester.core.db import DatabaseManager
from harvester.core.http_client import ResilientHttpClient
from harvester.workers.congressional.simulation.mock_house_server import MockHouseServer

FIXTURES_DIR = Path(__file__).parent / "fixtures"


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
