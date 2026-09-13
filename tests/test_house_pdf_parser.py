"""Unit tests for House PTR PDF parser and transaction normalizer."""

from datetime import date
from pathlib import Path

import pytest

from harvester.core.models import (
    FilingStatus,
    HouseIndexRecord,
    OwnerType,
    TransactionType,
)
from harvester.workers.congressional.house.pdf_parser import HousePTRParser


def test_parser_utility_methods() -> None:
    parser = HousePTRParser()

    # compute_sha256
    sha = parser.compute_sha256(b"test data")
    assert len(sha) == 64

    # normalize_ticker
    assert parser.normalize_ticker("NVIDIA Corporation (NVDA) [ST]") == "NVDA"
    assert parser.normalize_ticker("Apple Inc. (AAPL)") == "AAPL"
    assert parser.normalize_ticker("Ticker: MSFT") == "MSFT"
    assert parser.normalize_ticker("Spouse position [SP]") is None
    assert parser.normalize_ticker("") is None

    # parse_transaction_type
    assert parser.parse_transaction_type("P") == TransactionType.BUY
    assert parser.parse_transaction_type("Purchase") == TransactionType.BUY
    assert parser.parse_transaction_type("S") == TransactionType.SALE_FULL
    assert parser.parse_transaction_type("Sale (Full)") == TransactionType.SALE_FULL
    assert parser.parse_transaction_type("S (partial)") == TransactionType.SALE_PARTIAL
    assert parser.parse_transaction_type("Exchange") == TransactionType.EXCHANGE
    assert parser.parse_transaction_type("unknown") is None

    # parse_date
    assert parser.parse_date("01/15/2024") == date(2024, 1, 15)
    assert parser.parse_date("2024/01/15") == date(2024, 1, 15)
    assert parser.parse_date("bad-date") is None
    assert parser.parse_date("") is None

    # parse_transaction_type empty
    assert parser.parse_transaction_type("") is None

    # normalize_ticker excluded keywords
    assert parser.normalize_ticker("Holding (SP)") is None
    assert parser.normalize_ticker("SP [ST]") is None
    assert parser.normalize_ticker("AAPL [ST]") == "AAPL"

    # parse_amount
    b_str, b_min, b_max = parser.parse_amount("$1,001 - $15,000")
    assert b_min == 1001.0
    assert b_max == 15000.0

    b_str, b_min, b_max = parser.parse_amount("$25,000 to $45,000")
    assert b_min == 25000.0
    assert b_max == 45000.0

    b_str, b_min, b_max = parser.parse_amount("Over $50,000,000")
    assert b_min == 50000001.0
    assert b_max is None

    b_str, b_min, b_max = parser.parse_amount("$50,000,000+")
    assert b_min == 50000001.0
    assert b_max is None

    b_str, b_min, b_max = parser.parse_amount("unspecified")
    assert b_min == 1001.0

    # parse_owner
    assert parser.parse_owner("[SP]") == OwnerType.SPOUSE
    assert parser.parse_owner("[DC]") == OwnerType.DEPENDENT
    assert parser.parse_owner("[JT]") == OwnerType.JOINT
    assert parser.parse_owner("") == OwnerType.SELF
    assert parser.parse_owner("OtherUnknown") == OwnerType.SELF


def test_extract_transaction_from_row() -> None:
    parser = HousePTRParser()
    rec = HouseIndexRecord(
        prefix="Hon.",
        last_name="Pelosi",
        first_name="Nancy",
        filing_type="P",
        state_dst="CA11",
        year=2024,
        filing_date=date(2024, 1, 15),
        doc_id="20024101",
    )
    filing_id = "FILING_HOUSE_20024101"

    # Row with < 3 cells
    assert parser._extract_transaction_from_row(["AAPL", "P"], rec, filing_id) is None

    # Row without ticker
    assert parser._extract_transaction_from_row(["no ticker here", "01/10/2024", "1000"], rec, filing_id) is None

    # Row with ticker, [SP], Exchange, and date
    row_sp = ["NVIDIA Corp (NVDA) [ST]", "Exchange", "01/10/2024", "$1,000,001 - $5,000,000", "[SP]"]
    tx_sp = parser._extract_transaction_from_row(row_sp, rec, filing_id)
    assert tx_sp is not None
    assert tx_sp.ticker == "NVDA"
    assert tx_sp.owner == OwnerType.SPOUSE
    assert tx_sp.transaction_type == TransactionType.EXCHANGE
    assert tx_sp.transaction_date == date(2024, 1, 10)
    assert tx_sp.amount_min == 1000001.0

    # Row with [DC], Sale, no date (defaults to filing_date), no amount (defaults)
    row_dc = ["Apple Inc (AAPL)", "Sale (Full)", "[DC]"]
    tx_dc = parser._extract_transaction_from_row(row_dc, rec, filing_id)
    assert tx_dc is not None
    assert tx_dc.ticker == "AAPL"
    assert tx_dc.owner == OwnerType.DEPENDENT
    assert tx_dc.transaction_type == TransactionType.SALE_FULL
    assert tx_dc.transaction_date == date(2024, 1, 15)

    # Row with [JT], no explicit type (defaults to BUY)
    row_jt = ["Amazon (AMZN) [ST]", "[JT]", "$15,001 - $50,000"]
    tx_jt = parser._extract_transaction_from_row(row_jt, rec, filing_id)
    assert tx_jt is not None
    assert tx_jt.ticker == "AMZN"
    assert tx_jt.owner == OwnerType.JOINT
    assert tx_jt.transaction_type == TransactionType.BUY

    # Row without dollar amount (defaults)
    row_no_dollar = ["Tesla Inc (TSLA)", "Buy", "01/10/2024", "no dollar"]
    tx_no_dollar = parser._extract_transaction_from_row(row_no_dollar, rec, filing_id)
    assert tx_no_dollar is not None
    assert tx_no_dollar.ticker == "TSLA"
    assert tx_no_dollar.amount_min == 1001.0


def test_extract_transactions_from_text() -> None:
    parser = HousePTRParser()
    rec = HouseIndexRecord(
        prefix="Hon.",
        last_name="Pelosi",
        first_name="Nancy",
        filing_type="P",
        state_dst="CA11",
        year=2024,
        filing_date=date(2024, 1, 15),
        doc_id="20024101",
    )
    filing_id = "FILING_HOUSE_20024101"

    text_chunks = [
        "NVIDIA Corporation (NVDA) [ST] [SP] Purchase 01/10/2024 $1,000,001 - $5,000,000",
        "Apple Inc (AAPL) [DC] Sale 01/08/2024 $250,001 - $500,000",
        "Microsoft Corp (MSFT) [JT] Purchase 01/05/2024 $100,001 - $250,000",
        "Non-trading informational text line with no ticker",
    ]
    txs = parser._extract_transactions_from_text(text_chunks, rec, filing_id)
    assert len(txs) == 3
    assert txs[0].ticker == "NVDA"
    assert txs[0].owner == OwnerType.SPOUSE
    assert txs[1].ticker == "AAPL"
    assert txs[1].owner == OwnerType.DEPENDENT
    assert txs[2].ticker == "MSFT"
    assert txs[2].owner == OwnerType.JOINT


def test_parse_pdf_with_tables() -> None:
    from unittest.mock import MagicMock, patch

    parser = HousePTRParser()
    rec = HouseIndexRecord(
        prefix="Hon.",
        last_name="Pelosi",
        first_name="Nancy",
        filing_type="P",
        state_dst="CA11",
        year=2024,
        filing_date=date(2024, 1, 15),
        doc_id="20024101",
    )

    mock_pdf = MagicMock()
    mock_page_empty_text = MagicMock()
    mock_page_empty_text.extract_text.return_value = ""  # tests empty text line 171->175
    mock_page_empty_text.extract_tables.return_value = [
        [],  # empty table branch
        [["single row header only"]],  # len < 2 branch
        [
            ["Asset", "Type", "Date", "Amount", "Owner"],
            ["Alphabet Inc (GOOGL) [ST]", "Purchase", "01/10/2024", "$50,001 - $100,000", "[SP]"],
        ],
    ]
    mock_pdf.pages = [mock_page_empty_text]
    mock_pdf.__enter__.return_value = mock_pdf
    mock_pdf.__exit__.return_value = None

    with patch("pdfplumber.open", return_value=mock_pdf):
        filing, txs = parser.parse_pdf(b"dummy pdf content", rec)
        assert filing.status == FilingStatus.PARSED
        assert len(txs) == 1
        assert txs[0].ticker == "GOOGL"
        assert txs[0].owner == OwnerType.SPOUSE


def test_parse_valid_pdf_fixture(sample_pdf_bytes: bytes) -> None:
    parser = HousePTRParser()
    rec = HouseIndexRecord(
        prefix="Hon.",
        last_name="Pelosi",
        first_name="Nancy",
        filing_type="P",
        state_dst="CA11",
        year=2024,
        filing_date=date(2024, 1, 15),
        doc_id="20024101",
    )
    filing, txs = parser.parse_pdf(sample_pdf_bytes, rec, doc_url="https://example.com/ptr.pdf")

    assert filing.filing_id == "FILING_HOUSE_20024101"
    assert filing.status == FilingStatus.PARSED
    assert len(txs) >= 1
    # Check that NVDA was extracted
    tickers = {tx.ticker for tx in txs}
    assert "NVDA" in tickers or "AAPL" in tickers


def test_parse_multi_trade_pdf_fixture() -> None:
    multi_pdf = Path("tests/fixtures/sample_house_ptr_multi.pdf")
    if not multi_pdf.exists():
        pytest.skip("Multi-trade fixture not available")

    parser = HousePTRParser()
    rec = HouseIndexRecord(
        prefix="Hon.",
        last_name="McCaul",
        first_name="Michael",
        filing_type="P",
        state_dst="TX10",
        year=2024,
        filing_date=date(2024, 1, 15),
        doc_id="20024102",
    )
    filing, txs = parser.parse_pdf(multi_pdf.read_bytes(), rec)
    assert filing.status == FilingStatus.PARSED
    assert len(txs) >= 3


def test_parse_corrupt_pdf_bytes() -> None:
    parser = HousePTRParser()
    rec = HouseIndexRecord(
        last_name="Test",
        first_name="User",
        filing_type="P",
        state_dst="TX01",
        year=2024,
        filing_date=date(2024, 1, 15),
        doc_id="999",
    )
    filing, txs = parser.parse_pdf(b"not a valid pdf", rec)
    assert filing.status == FilingStatus.ERROR
    assert len(txs) == 0
