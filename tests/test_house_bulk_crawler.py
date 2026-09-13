"""Unit tests for HouseBulkCrawler archive downloader and XML parser."""

import io
import zipfile
from datetime import date

import pytest
import respx

from harvester.core.http_client import ResilientHttpClient
from harvester.workers.congressional.house.bulk_crawler import HouseBulkCrawler
from harvester.workers.congressional.simulation.mock_house_server import MockHouseServer


@pytest.mark.asyncio
async def test_build_archive_url_and_fetch() -> None:
    client = ResilientHttpClient()
    crawler = HouseBulkCrawler(http_client=client)
    url = crawler.build_archive_url(2024)
    assert "2024FD.ZIP" in url

    mock_zip = MockHouseServer.generate_mock_zip(2024)
    with respx.mock() as respx_mock:
        respx_mock.get(url).respond(200, content=mock_zip)
        data = await crawler.fetch_archive(2024)
        assert data == mock_zip

    # Test 304 Not Modified
    with respx.mock() as respx_mock:
        respx_mock.get(url).respond(304)
        data_304 = await crawler.fetch_archive(2024)
        assert data_304 is None

    await client.close()


def test_parse_xml_index_fallback_xml_name() -> None:
    client = ResilientHttpClient()
    crawler = HouseBulkCrawler(http_client=client)

    # Zip with non-standard XML filename
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("different_name.xml", MockHouseServer.generate_mock_xml())

    records = crawler.parse_xml_index(buf.getvalue(), 2024)
    assert len(records) >= 2


def test_parse_xml_index_edge_cases() -> None:
    client = ResilientHttpClient()
    crawler = HouseBulkCrawler(http_client=client)

    # Empty bytes
    assert crawler.parse_xml_index(b"", 2024) == []

    # Corrupt zip
    assert crawler.parse_xml_index(b"not a valid zip", 2024) == []

    # Zip without XML
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("some_text.txt", "hello")
    assert crawler.parse_xml_index(buf.getvalue(), 2024) == []


def test_parse_xml_content_date_formats_and_fallbacks() -> None:
    client = ResilientHttpClient()
    crawler = HouseBulkCrawler(http_client=client)

    xml = """
    <FinancialDisclosure>
      <Member>
        <Last>One</Last>
        <First>Member</First>
        <FilingType>P</FilingType>
        <DocID>1001</DocID>
        <FilingDate>2024-01-15</FilingDate>
        <Year>2024</Year>
      </Member>
      <Member>
        <Last>Two</Last>
        <First>Member</First>
        <FilingType>P</FilingType>
        <DocID>1002</DocID>
        <FilingDate>01-20-2024</FilingDate>
        <Year>invalid_year</Year>
      </Member>
    </FinancialDisclosure>
    """
    records = crawler.parse_xml_content(xml, fallback_year=2024)
    assert len(records) == 2
    assert records[0].filing_date == date(2024, 1, 15)
    assert records[0].year == 2024
    assert records[1].filing_date == date(2024, 1, 20)
    assert records[1].year == 2024


def test_parse_xml_content_malformed() -> None:
    client = ResilientHttpClient()
    crawler = HouseBulkCrawler(http_client=client)

    # Empty content
    assert crawler.parse_xml_content("", 2024) == []

    # Malformed XML
    assert crawler.parse_xml_content("<broken>xml", 2024) == []

    # Missing required tags or invalid dates
    xml_with_missing = """
    <FinancialDisclosure>
      <Member>
        <First>NoLast</First>
      </Member>
      <Member>
        <Last>Smith</Last>
        <FilingType>P</FilingType>
        <DocID>123</DocID>
        <FilingDate>invalid-date</FilingDate>
      </Member>
      <Member>
        <Last>NoDateTag</Last>
        <FilingType>P</FilingType>
        <DocID>124</DocID>
      </Member>
      <Member>
        <Last>EmptyDateTag</Last>
        <FilingType>P</FilingType>
        <DocID>125</DocID>
        <FilingDate></FilingDate>
      </Member>
    </FinancialDisclosure>
    """
    records = crawler.parse_xml_content(xml_with_missing, 2024)
    assert len(records) == 0


@pytest.mark.asyncio
async def test_get_ptrs_filtering() -> None:
    client = ResilientHttpClient()
    crawler = HouseBulkCrawler(http_client=client)
    mock_zip = MockHouseServer.generate_mock_zip(2024)

    with respx.mock() as respx_mock:
        respx_mock.get(crawler.build_archive_url(2024)).respond(200, content=mock_zip)
        ptrs = await crawler.get_ptrs(2024)
        assert len(ptrs) == 2
        assert {p.last_name for p in ptrs} == {"Pelosi", "McCaul"}

    # If 304, returns empty list
    with respx.mock() as respx_mock:
        respx_mock.get(crawler.build_archive_url(2024)).respond(304)
        ptrs_304 = await crawler.get_ptrs(2024)
        assert ptrs_304 == []

    await client.close()
