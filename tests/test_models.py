"""Unit tests for domain models and data validation."""

from datetime import date

import pytest

from harvester.core.models import (
    CongressionalFiling,
    CongressionalTransaction,
    CrawlReport,
    FilingStatus,
    HouseIndexRecord,
    OwnerType,
    SenateReportRecord,
    TransactionType,
)


def test_house_index_record_properties() -> None:
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
    assert rec.member_name == "Nancy Pelosi"
    assert rec.state == "CA"
    assert rec.district == "11"
    assert rec.is_ptr is True


def test_house_index_record_edge_cases() -> None:
    rec = HouseIndexRecord(
        last_name="SingleName",
        first_name="",
        filing_type="A",
        state_dst="1",
        year=2024,
        filing_date=date(2024, 1, 15),
        doc_id="123",
    )
    assert rec.member_name == "SingleName"
    assert rec.state == ""
    assert rec.district == "1"
    assert rec.is_ptr is False


def test_congressional_filing_validation() -> None:
    filing = CongressionalFiling(
        filing_id="FILING_HOUSE_123",
        chamber="HOUSE",
        member_name="Nancy Pelosi",
        filing_year=2024,
        filing_date=date(2024, 1, 15),
        sha256_hash="abc123hash",
    )
    assert filing.chamber == "house"
    assert filing.status == FilingStatus.PENDING

    with pytest.raises(ValueError, match="Chamber must be 'house' or 'senate'"):
        CongressionalFiling(
            filing_id="FILING_INVALID",
            chamber="parliament",
            member_name="Test",
            filing_year=2024,
            filing_date=date(2024, 1, 15),
            sha256_hash="hash",
        )


def test_congressional_transaction_validation() -> None:
    tx = CongressionalTransaction(
        filing_id="FILING_HOUSE_123",
        member_name="Nancy Pelosi",
        ticker="nvda",
        asset_description="NVIDIA Common Stock",
        transaction_type=TransactionType.BUY,
        amount_bracket="$1,001 - $15,000",
        amount_min=1001.0,
        amount_max=15000.0,
        transaction_date=date(2024, 1, 10),
        filing_date=date(2024, 1, 15),
        owner=OwnerType.SELF,
    )
    assert tx.ticker == "NVDA"
    assert tx.disclosure_lag_days == 5

    # Explicit disclosure_lag_days validator mode before
    tx_explicit = CongressionalTransaction(
        filing_id="FILING_HOUSE_123",
        member_name="Nancy Pelosi",
        ticker="AAPL",
        asset_description="Apple Stock",
        transaction_type=TransactionType.SALE_FULL,
        amount_bracket="$1,001 - $15,000",
        amount_min=1001.0,
        transaction_date=date(2024, 1, 10),
        filing_date=date(2024, 1, 15),
        disclosure_lag_days=10,
    )
    assert tx_explicit.disclosure_lag_days == 10

    # Invalid ticker validation
    with pytest.raises(ValueError, match="Invalid equity ticker"):
        CongressionalTransaction(
            filing_id="FILING_123",
            member_name="Test",
            ticker="TOOLONGTICKERNAME",
            asset_description="Desc",
            transaction_type=TransactionType.BUY,
            amount_bracket="bracket",
            amount_min=1.0,
            transaction_date=date(2024, 1, 1),
            filing_date=date(2024, 1, 5),
        )


def test_crawl_report_defaults() -> None:
    report = CrawlReport(year=2024)
    assert report.year == 2024
    assert report.total_index_filings == 0
    assert report.error_details == []


def test_senate_report_record_properties() -> None:
    rec = SenateReportRecord(
        first_name="Mark",
        last_name="Warner",
        office="Senator (VA)",
        report_title="Periodic Transaction Report",
        report_url="https://efdsearch.senate.gov/search/view/ptr/123/",
        received_date=date(2024, 2, 1),
        doc_id="123",
    )
    assert rec.member_name == "Mark Warner"
    assert rec.state == "VA"
    assert rec.is_ptr is True
    assert rec.is_paper is False


def test_senate_report_record_edge_cases() -> None:
    # Only last name and office without state
    rec1 = SenateReportRecord(
        first_name="",
        last_name="Tuberville",
        office="Senator",
        report_title="Annual Report",
        report_url="https://efdsearch.senate.gov/search/view/paper/456/",
        received_date=date(2024, 2, 1),
        doc_id="456",
    )
    assert rec1.member_name == "Tuberville"
    assert rec1.state == ""
    assert rec1.is_ptr is False
    assert rec1.is_paper is True

    # Only first name
    rec2 = SenateReportRecord(
        first_name="Cheri",
        last_name="",
        office="Candidate",
        report_title="PTR Report",
        report_url="https://efdsearch.senate.gov/reports/123.pdf",
        received_date=date(2024, 2, 1),
        doc_id="789",
    )
    assert rec2.member_name == "Cheri"
    assert rec2.state == ""
    assert rec2.is_ptr is True
    assert rec2.is_paper is True
