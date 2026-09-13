"""Senate eFD client handling disclaimer handshake and DataTables search."""

import logging
import re
from datetime import date, datetime
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from harvester.config import Settings, get_settings
from harvester.core.http_client import ResilientHttpClient
from harvester.core.models import SenateReportRecord

logger = logging.getLogger(__name__)

DOC_ID_REGEX = re.compile(r"/(?:ptr|paper)/([a-zA-Z0-9_-]+)/?", re.IGNORECASE)


class SenateEfdClient:
    """Asynchronous client for Senate Financial Disclosures portal (efdsearch.senate.gov)."""

    def __init__(
        self,
        http_client: ResilientHttpClient,
        settings: Settings | None = None,
    ) -> None:
        self.http_client = http_client
        self.settings = settings or get_settings()
        self.csrf_token: str | None = None
        self._handshake_completed: bool = False

    async def ensure_handshake(self) -> str:
        """Perform statutory disclaimer agreement handshake to establish session cookies."""
        if self._handshake_completed and self.csrf_token:
            return self.csrf_token

        search_url = self.settings.senate_search_url
        logger.info("Initiating Senate eFD disclaimer handshake at %s", search_url)

        # 1. GET search page to extract CSRF token
        get_resp = await self.http_client.get(search_url, use_cache=False)
        soup = BeautifulSoup(get_resp.text, "html.parser")
        csrf_input = soup.find("input", {"name": "csrfmiddlewaretoken"})

        token = ""
        if csrf_input is not None:
            raw_val = csrf_input.get("value")
            if isinstance(raw_val, str) and raw_val:
                token = raw_val

        # Check cookies if not in form input
        if not token:
            client = await self.http_client.get_client()
            cookie_val = client.cookies.get("csrftoken")
            if cookie_val:
                token = str(cookie_val)

        if not token:
            token = "dummy_csrf_token"

        # 2. POST agreement form with prohibition_agreement=1
        post_data = {
            "prohibition_agreement": "1",
            "csrfmiddlewaretoken": token,
        }
        headers = {
            "Referer": search_url,
            "Origin": self.settings.senate_base_url,
        }
        post_resp = await self.http_client.post(search_url, data=post_data, extra_headers=headers)
        post_resp.raise_for_status()

        # Update csrf token from response cookies if refreshed
        client = await self.http_client.get_client()
        fresh_token = client.cookies.get("csrftoken", "")
        self.csrf_token = fresh_token or token
        self._handshake_completed = True
        logger.info("Senate eFD disclaimer handshake established successfully")
        return self.csrf_token

    def parse_datatables_row(self, row: list[Any]) -> SenateReportRecord | None:
        """Parse a single DataTables 5-element array into a SenateReportRecord."""
        if len(row) < 5:
            return None

        first_name = str(row[0]).strip()
        last_name = str(row[1]).strip()
        office = str(row[2]).strip()
        link_html = str(row[3]).strip()
        date_str = str(row[4]).strip()

        # Extract URL and title from HTML anchor
        soup = BeautifulSoup(link_html, "html.parser")
        anchor = soup.find("a")
        if not anchor or not anchor.get("href"):
            return None

        href = str(anchor["href"])
        full_url = urljoin(self.settings.senate_base_url, href)
        report_title = anchor.get_text().strip()

        # Extract DocID / UUID from URL
        doc_id_match = DOC_ID_REGEX.search(href)
        doc_id = doc_id_match.group(1) if doc_id_match else href.strip("/").split("/")[-1]

        # Parse date
        received_date = None
        for fmt in ("%m/%d/%Y", "%m/%d/%Y %H:%M:%S", "%Y-%m-%d"):
            try:
                clean_date_str = date_str.split(" ")[0] if " " in date_str else date_str
                received_date = datetime.strptime(clean_date_str, fmt).date()
                break
            except ValueError:
                continue

        if received_date is None:
            received_date = date.today()

        return SenateReportRecord(
            first_name=first_name,
            last_name=last_name,
            office=office,
            report_title=report_title,
            report_url=full_url,
            received_date=received_date,
            doc_id=doc_id,
        )

    async def search_reports(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
        offset: int = 0,
        limit: int = 100,
        draw: int = 1,
    ) -> tuple[int, list[SenateReportRecord]]:
        """Query Senate eFD internal DataTables endpoint for PTR reports."""
        token = await self.ensure_handshake()
        endpoint = self.settings.senate_data_endpoint

        payload: dict[str, str] = {
            "draw": str(draw),
            "start": str(offset),
            "length": str(limit),
            "report_types": "[11]",  # 11 = Periodic Transaction Report
            "csrfmiddlewaretoken": token,
        }
        if start_date:
            payload["submitted_start_date"] = f"{start_date.strftime('%m/%d/%Y')} 00:00:00"
        if end_date:
            payload["submitted_end_date"] = f"{end_date.strftime('%m/%d/%Y')} 23:59:59"

        headers = {
            "Referer": self.settings.senate_search_url,
            "X-CSRFToken": token,
            "X-Requested-With": "XMLHttpRequest",
        }

        resp = await self.http_client.post(endpoint, data=payload, extra_headers=headers)
        data = resp.json()

        total_records = int(data.get("recordsFiltered") or data.get("recordsTotal") or 0)
        raw_rows = data.get("data", [])

        records: list[SenateReportRecord] = []
        for r in raw_rows:
            parsed = self.parse_datatables_row(r)
            if parsed is not None and parsed.is_ptr:
                records.append(parsed)

        return total_records, records

    async def fetch_report(self, report_url: str) -> str:
        """Fetch raw HTML string of a Senate PTR disclosure."""
        await self.ensure_handshake()
        resp = await self.http_client.get(report_url, use_cache=True)
        return resp.text

    async def fetch_paper_pdf(self, paper_url: str) -> bytes:
        """Fetch binary PDF of a scanned paper Senate disclosure."""
        await self.ensure_handshake()
        resp = await self.http_client.get(paper_url, use_cache=True)
        return resp.content
