"""Unit tests for Senate eFD client disclaimer handshake and DataTables search."""

from datetime import date
from unittest.mock import AsyncMock, patch

import pytest
import respx

from harvester.config import Settings
from harvester.core.http_client import ResilientHttpClient
from harvester.workers.congressional.senate.client import SenateEfdClient
from harvester.workers.congressional.simulation.mock_senate_server import MockSenateServer


@pytest.mark.asyncio
async def test_senate_client_handshake_flow() -> None:
    client = ResilientHttpClient()
    settings = Settings(
        senate_base_url="https://efdsearch.senate.gov",
        senate_home_url="https://efdsearch.senate.gov/search/home/",
        senate_search_url="https://efdsearch.senate.gov/search/",
        senate_data_endpoint="https://efdsearch.senate.gov/search/report/data/",
    )
    senate_client = SenateEfdClient(http_client=client, settings=settings)

    mock_html = MockSenateServer.generate_mock_disclaimer_page("test_token_999")

    with respx.mock() as respx_mock:
        # 1. GET search/home page
        respx_mock.get("https://efdsearch.senate.gov/search/home/").respond(
            200, text=mock_html, headers={"Set-Cookie": "csrftoken=test_token_999;"}
        )
        # 2. POST agreement
        respx_mock.post("https://efdsearch.senate.gov/search/home/").respond(
            200, text="Agreement accepted", headers={"Set-Cookie": "csrftoken=fresh_token_888; sessionid=sess123;"}
        )

        token = await senate_client.ensure_handshake()
        assert token in ("test_token_999", "fresh_token_888")
        assert senate_client._handshake_completed is True

        # Second call returns cached token without network request
        token_cached = await senate_client.ensure_handshake()
        assert token_cached == token

    await client.close()


@pytest.mark.asyncio
async def test_senate_client_handshake_fallback_cookie_and_dummy() -> None:
    client = ResilientHttpClient()
    settings = Settings(
        senate_base_url="https://efdsearch.senate.gov",
        senate_home_url="https://efdsearch.senate.gov/search/home/",
        senate_search_url="https://efdsearch.senate.gov/search/",
        senate_data_endpoint="https://efdsearch.senate.gov/search/report/data/",
    )
    senate_client = SenateEfdClient(http_client=client, settings=settings)

    # HTML without CSRF input
    html_without_csrf = "<html><body>No input here</body></html>"

    with respx.mock() as respx_mock:
        respx_mock.get("https://efdsearch.senate.gov/search/home/").respond(200, text=html_without_csrf)
        respx_mock.post("https://efdsearch.senate.gov/search/home/").respond(
            200, text="OK", headers={"Set-Cookie": "sessionid=sess123;"}
        )

        token = await senate_client.ensure_handshake()
        assert token == "dummy_csrf_token"

    await client.close()


@pytest.mark.asyncio
async def test_senate_client_handshake_fallback_cookie() -> None:
    client = ResilientHttpClient()
    http_c = await client.get_client()
    http_c.cookies.set("csrftoken", "cookie_csrf_val")
    settings = Settings(
        senate_base_url="https://efdsearch.senate.gov",
        senate_home_url="https://efdsearch.senate.gov/search/home/",
        senate_search_url="https://efdsearch.senate.gov/search/",
        senate_data_endpoint="https://efdsearch.senate.gov/search/report/data/",
    )
    senate_client = SenateEfdClient(http_client=client, settings=settings)

    # Empty value in input
    html_with_empty_input = "<html><body><input name='csrfmiddlewaretoken' value=''></body></html>"

    with respx.mock() as respx_mock:
        respx_mock.get("https://efdsearch.senate.gov/search/home/").respond(200, text=html_with_empty_input)
        respx_mock.post("https://efdsearch.senate.gov/search/home/").respond(
            200, text="OK", headers={"Set-Cookie": "sessionid=sess123;"}
        )

        token = await senate_client.ensure_handshake()
        assert token == "cookie_csrf_val"

    await client.close()


def test_parse_datatables_row() -> None:
    client = ResilientHttpClient()
    senate_client = SenateEfdClient(http_client=client)

    # Valid PTR row
    valid_row = [
        "Mark",
        "Warner",
        "Senator (VA)",
        '<a href="/search/view/ptr/d7a9b0c1-1111-2222-3333-444455556666/" target="_blank">Periodic Transaction Report</a>',
        "02/15/2024 10:00:00",
    ]
    rec = senate_client.parse_datatables_row(valid_row)
    assert rec is not None
    assert rec.member_name == "Mark Warner"
    assert rec.state == "VA"
    assert rec.is_ptr is True
    assert rec.is_paper is False
    assert rec.doc_id == "d7a9b0c1-1111-2222-3333-444455556666"
    assert rec.received_date == date(2024, 2, 15)

    # Short row
    assert senate_client.parse_datatables_row(["short", "row"]) is None

    # Row without link
    no_link_row = ["John", "Doe", "Office", "No link here", "01/01/2024"]
    assert senate_client.parse_datatables_row(no_link_row) is None

    # Row with fallback date
    bad_date_row = [
        "Jane",
        "Smith",
        "Office",
        '<a href="/search/view/paper/998877/">Paper Report</a>',
        "unparseable-date",
    ]
    rec_bad_date = senate_client.parse_datatables_row(bad_date_row)
    assert rec_bad_date is not None
    assert rec_bad_date.received_date == date.today()
    assert rec_bad_date.is_paper is True
    assert rec_bad_date.doc_id == "998877"


@pytest.mark.asyncio
async def test_search_reports() -> None:
    client = ResilientHttpClient()
    settings = Settings(
        senate_base_url="https://efdsearch.senate.gov",
        senate_search_url="https://efdsearch.senate.gov/search/",
        senate_data_endpoint="https://efdsearch.senate.gov/search/report/data/",
    )
    senate_client = SenateEfdClient(http_client=client, settings=settings)

    mock_resp = MockSenateServer.generate_mock_datatables_response()

    with patch.object(senate_client, "ensure_handshake", new_callable=AsyncMock) as mock_hs:
        mock_hs.return_value = "token123"
        with respx.mock() as respx_mock:
            respx_mock.post("https://efdsearch.senate.gov/search/report/data/").respond(200, json=mock_resp)

            total, records = await senate_client.search_reports(
                start_date=date(2024, 1, 1),
                end_date=date(2024, 12, 31),
                limit=10,
            )
            assert total == 3
            # Third record is Annual Report, filtered out by is_ptr
            assert len(records) == 2
            assert records[0].member_name == "Mark Warner"
            assert records[1].member_name == "Tommy Tuberville"

            # Test without start_date and without end_date
            total_no_dates, _ = await senate_client.search_reports(
                start_date=None,
                end_date=None,
                limit=5,
            )
            assert total_no_dates == 3

            # Test with start_date only
            total_start_only, _ = await senate_client.search_reports(
                start_date=date(2024, 1, 1),
                end_date=None,
            )
            assert total_start_only == 3

            # Test with end_date only
            total_end_only, _ = await senate_client.search_reports(
                start_date=None,
                end_date=date(2024, 12, 31),
            )
            assert total_end_only == 3

    await client.close()


@pytest.mark.asyncio
async def test_fetch_report_and_paper() -> None:
    client = ResilientHttpClient()
    senate_client = SenateEfdClient(http_client=client)

    with patch.object(senate_client, "ensure_handshake", new_callable=AsyncMock) as mock_hs:
        mock_hs.return_value = "token123"
        with respx.mock() as respx_mock:
            respx_mock.get("https://example.com/ptr-report").respond(200, text="<html>PTR Report</html>")
            respx_mock.get("https://example.com/paper.pdf").respond(200, content=b"%PDF-paper")

            html_text = await senate_client.fetch_report("https://example.com/ptr-report")
            assert "<html>PTR Report</html>" in html_text

            pdf_bytes = await senate_client.fetch_paper_pdf("https://example.com/paper.pdf")
            assert pdf_bytes == b"%PDF-paper"

    await client.close()
