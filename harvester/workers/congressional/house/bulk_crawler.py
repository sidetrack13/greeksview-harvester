"""House of Representatives Bulk Ingestion Worker for annual index archives (YYYYFD.ZIP)."""

import io
import logging
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime

from harvester.config import Settings, get_settings
from harvester.core.http_client import ResilientHttpClient
from harvester.core.models import HouseIndexRecord

logger = logging.getLogger(__name__)


class HouseBulkCrawler:
    """Downloader and XML parser for U.S. House of Representatives annual bulk archives."""

    def __init__(self, http_client: ResilientHttpClient, settings: Settings | None = None) -> None:
        self.http_client = http_client
        self.settings = settings or get_settings()

    def build_archive_url(self, year: int) -> str:
        """Construct official House Clerk bulk archive URL for a given year."""
        return f"{self.settings.house_index_base_url.rstrip('/')}/{year}FD.ZIP"

    async def fetch_archive(self, year: int) -> bytes | None:
        """Download the annual {Year}FD.ZIP archive; return raw bytes or None if 304."""
        url = self.build_archive_url(year)
        logger.info("Fetching House bulk archive for year %s from %s", year, url)
        response = await self.http_client.get(url, use_cache=True)
        if response.status_code == 304:
            logger.info("House archive for %s unchanged (HTTP 304)", year)
            return None
        return response.content

    def parse_xml_index(self, zip_bytes: bytes, year: int) -> list[HouseIndexRecord]:
        """Extract and parse YYYYFD.xml from the in-memory ZIP archive."""
        if not zip_bytes:
            return []

        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                target_xml = f"{year}FD.xml"
                xml_filename = None
                for name in zf.namelist():
                    if name.lower() == target_xml.lower():
                        xml_filename = name
                        break

                if not xml_filename:
                    # Fallback: look for any .xml file
                    for name in zf.namelist():
                        if name.lower().endswith(".xml"):
                            xml_filename = name
                            break

                if not xml_filename:
                    logger.warning("No XML file found in %sFD.ZIP", year)
                    return []

                with zf.open(xml_filename) as xml_file:
                    xml_content = xml_file.read()
        except zipfile.BadZipFile as exc:
            logger.error("Corrupt or invalid ZIP archive for year %s: %s", year, exc)
            return []

        return self.parse_xml_content(xml_content, year)

    def parse_xml_content(self, xml_content: bytes | str, fallback_year: int) -> list[HouseIndexRecord]:
        """Parse House disclosure XML content into structured HouseIndexRecord objects."""
        records: list[HouseIndexRecord] = []
        if not xml_content:
            return records

        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError as exc:
            logger.error("Failed to parse House XML index: %s", exc)
            return records

        # XML structure typically contains <Member> nodes
        for member in root.findall(".//Member"):
            last_elem = member.find("Last")
            first_elem = member.find("First")
            filing_type_elem = member.find("FilingType")
            state_dst_elem = member.find("StateDst")
            year_elem = member.find("Year")
            filing_date_elem = member.find("FilingDate")
            doc_id_elem = member.find("DocID")

            if (
                last_elem is None
                or last_elem.text is None
                or filing_type_elem is None
                or filing_type_elem.text is None
                or doc_id_elem is None
                or doc_id_elem.text is None
            ):
                continue

            last_name = last_elem.text.strip()
            first_name = (first_elem.text or "").strip() if first_elem is not None else ""
            filing_type = filing_type_elem.text.strip()
            state_dst = (state_dst_elem.text or "").strip() if state_dst_elem is not None else ""
            doc_id = doc_id_elem.text.strip()

            record_year = fallback_year
            if year_elem is not None and year_elem.text and year_elem.text.strip().isdigit():
                record_year = int(year_elem.text.strip())

            filing_date = None
            if filing_date_elem is not None and filing_date_elem.text:
                raw_date = filing_date_elem.text.strip()
                for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
                    try:
                        filing_date = datetime.strptime(raw_date, fmt).date()
                        break
                    except ValueError:
                        continue

            if filing_date is None:
                continue

            records.append(
                HouseIndexRecord(
                    prefix="",
                    last_name=last_name,
                    first_name=first_name,
                    filing_type=filing_type,
                    state_dst=state_dst,
                    year=record_year,
                    filing_date=filing_date,
                    doc_id=doc_id,
                )
            )

        return records

    async def get_ptrs(self, year: int) -> list[HouseIndexRecord]:
        """Fetch index, parse records, and filter strictly for Periodic Transaction Reports (FilingType == 'P')."""
        archive_bytes = await self.fetch_archive(year)
        if archive_bytes is None:
            return []
        all_records = self.parse_xml_index(archive_bytes, year)
        ptrs = [r for r in all_records if r.is_ptr]
        logger.info("Parsed %s total filings from %s index, found %s PTRs", len(all_records), year, len(ptrs))
        return ptrs
