"""Offline simulation mock data generator for House Clerk disclosure ingestion."""

import io
import xml.etree.ElementTree as ET
import zipfile


class MockHouseServer:
    """Provides synthetic House ZIP indexes and digital PTR PDFs for offline testing."""

    @staticmethod
    def generate_mock_xml(records: list[dict[str, str]] | None = None) -> bytes:
        """Generate valid House Clerk disclosure XML string."""
        root = ET.Element("FinancialDisclosure")
        if records is None:
            records = [
                {
                    "Last": "Pelosi",
                    "First": "Nancy",
                    "FilingType": "P",
                    "StateDst": "CA11",
                    "Year": "2024",
                    "FilingDate": "01/15/2024",
                    "DocID": "20024101",
                },
                {
                    "Last": "McCaul",
                    "First": "Michael",
                    "FilingType": "P",
                    "StateDst": "TX10",
                    "Year": "2024",
                    "FilingDate": "02/10/2024",
                    "DocID": "20024102",
                },
                {
                    "Last": "Smith",
                    "First": "John",
                    "FilingType": "A",  # Annual report, should be filtered out
                    "StateDst": "NY01",
                    "Year": "2024",
                    "FilingDate": "05/15/2024",
                    "DocID": "20024103",
                },
            ]

        for r in records:
            member = ET.SubElement(root, "Member")
            for tag, val in r.items():
                child = ET.SubElement(member, tag)
                child.text = val

        return bytes(ET.tostring(root, encoding="utf-8"))

    @classmethod
    def generate_mock_zip(cls, year: int, xml_content: bytes | None = None) -> bytes:
        """Pack synthetic XML into an in-memory ZIP archive matching YYYYFD.ZIP structure."""
        if xml_content is None:
            xml_content = cls.generate_mock_xml()

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(f"{year}FD.xml", xml_content)
        return bytes(buf.getvalue())

    @staticmethod
    def generate_mock_ptr_pdf(
        member_name: str = "Nancy Pelosi",
        transactions: list[dict[str, str]] | None = None,
    ) -> bytes:
        """Generate a valid, vectorized digital PTR PDF with a clean transaction table."""
        if transactions is None:
            transactions = [
                {
                    "asset": "NVIDIA Corporation - Common Stock (NVDA) [ST]",
                    "type": "P",
                    "date": "01/10/2024",
                    "notif_date": "01/15/2024",
                    "amount": "$1,000,001 - $5,000,000",
                    "owner": "[SP]",
                },
                {
                    "asset": "Apple Inc. (AAPL) [ST]",
                    "type": "S",
                    "date": "01/08/2024",
                    "notif_date": "01/15/2024",
                    "amount": "$250,001 - $500,000",
                    "owner": "",
                },
            ]

        try:
            from reportlab.lib.pagesizes import letter
            from reportlab.pdfgen import canvas
        except ImportError as exc:
            raise ImportError(
                "reportlab is required for synthetic PDF generation in simulation mode. "
                "Install it via `pip install reportlab` or `uv sync --extra dev`."
            ) from exc

        buf = io.BytesIO()
        c = canvas.Canvas(buf, pagesize=letter)
        c.drawString(100, 750, "PERIODIC TRANSACTION REPORT")
        c.drawString(100, 730, f"Filer Name: {member_name}")
        c.drawString(100, 715, "Status: Member of the U.S. House of Representatives")
        c.drawString(100, 700, "Transactions:")

        y = 670
        # Header
        c.drawString(50, y, "Asset")
        c.drawString(280, y, "Type")
        c.drawString(330, y, "Date")
        c.drawString(400, y, "Amount")
        c.drawString(520, y, "Owner")
        y -= 20

        for tx in transactions:
            asset_text = tx["asset"]
            if tx.get("owner"):
                asset_text += f" {tx['owner']}"
            c.drawString(50, y, asset_text[:40])
            c.drawString(280, y, tx["type"])
            c.drawString(330, y, tx["date"])
            c.drawString(400, y, tx["amount"])
            c.drawString(520, y, tx.get("owner", ""))
            y -= 25

        c.save()
        return bytes(buf.getvalue())
