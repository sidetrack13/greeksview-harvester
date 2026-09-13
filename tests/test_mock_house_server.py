"""Unit tests for offline MockHouseServer data generation."""

import zipfile
from io import BytesIO

from harvester.workers.congressional.simulation.mock_house_server import MockHouseServer


def test_generate_mock_xml_custom_records() -> None:
    custom = [{"Last": "CustomLast", "First": "CustomFirst", "FilingType": "P", "DocID": "9999"}]
    data = MockHouseServer.generate_mock_xml(records=custom)
    assert b"CustomLast" in data
    assert b"9999" in data


def test_generate_mock_zip_custom_content() -> None:
    zip_bytes = MockHouseServer.generate_mock_zip(2024, xml_content=b"<CustomXML>data</CustomXML>")
    with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
        assert "2024FD.xml" in zf.namelist()
        assert zf.read("2024FD.xml") == b"<CustomXML>data</CustomXML>"


def test_generate_mock_ptr_pdf_custom_transactions() -> None:
    custom_txs = [
        {
            "asset": "Microsoft Corporation (MSFT) [ST]",
            "type": "P",
            "date": "03/01/2024",
            "notif_date": "03/05/2024",
            "amount": "$50,001 - $100,000",
            "owner": "[JT]",
        }
    ]
    pdf_bytes = MockHouseServer.generate_mock_ptr_pdf(member_name="Custom Member", transactions=custom_txs)
    assert pdf_bytes.startswith(b"%PDF")
    assert len(pdf_bytes) > 500
