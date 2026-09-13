"""Unit tests for Senate HTML disclosure parser and transaction normalizer."""

from datetime import date

from harvester.core.models import FilingStatus, OwnerType, SenateReportRecord, TransactionType
from harvester.workers.congressional.senate.html_parser import SenateHtmlParser
from harvester.workers.congressional.simulation.mock_senate_server import MockSenateServer


def test_senate_parser_utility_methods() -> None:
    parser = SenateHtmlParser()

    # compute_sha256
    assert len(parser.compute_sha256("test html")) == 64
    assert len(parser.compute_sha256(b"test bytes")) == 64

    # normalize_ticker
    assert parser.normalize_ticker("MSFT") == "MSFT"
    assert parser.normalize_ticker("--", asset_name="Tesla Inc. (TSLA)") == "TSLA"
    assert parser.normalize_ticker("N/A", asset_name="Alphabet Inc Class A (GOOGL) [ST]") == "GOOGL"
    assert parser.normalize_ticker("SP") is None
    assert parser.normalize_ticker("UNKNOWN") is None
    assert parser.normalize_ticker("", asset_name="Holding (SP)") is None
    assert parser.normalize_ticker("", asset_name="No symbol description") is None

    # parse_transaction_type
    assert parser.parse_transaction_type("Purchase") == TransactionType.BUY
    assert parser.parse_transaction_type("Buy") == TransactionType.BUY
    assert parser.parse_transaction_type("Sale (Full)") == TransactionType.SALE_FULL
    assert parser.parse_transaction_type("Sale (Partial)") == TransactionType.SALE_PARTIAL
    assert parser.parse_transaction_type("Exchange") == TransactionType.EXCHANGE
    assert parser.parse_transaction_type("Unknown") is None
    assert parser.parse_transaction_type("") is None

    # parse_date
    assert parser.parse_date("01/10/2024") == date(2024, 1, 10)
    assert parser.parse_date("2024-01-10") == date(2024, 1, 10)
    assert parser.parse_date("invalid-date") is None
    assert parser.parse_date("") is None

    # parse_amount
    b_str, b_min, b_max = parser.parse_amount("$1,001 - $15,000")
    assert b_min == 1001.0
    assert b_max == 15000.0

    b_str, b_min, b_max = parser.parse_amount("$20,000 - $40,000")
    assert b_min == 20000.0
    assert b_max == 40000.0

    b_str, b_min, b_max = parser.parse_amount("Over $50,000,000")
    assert b_min == 50000001.0
    assert b_max is None

    b_str, b_min, b_max = parser.parse_amount("$50,000,000+")
    assert b_min == 50000001.0
    assert b_max is None

    b_str, b_min, b_max = parser.parse_amount("unspecified")
    assert b_min == 1001.0

    # parse_owner
    assert parser.parse_owner("Spouse") == OwnerType.SPOUSE
    assert parser.parse_owner("Child") == OwnerType.DEPENDENT
    assert parser.parse_owner("Dependent") == OwnerType.DEPENDENT
    assert parser.parse_owner("Joint") == OwnerType.JOINT
    assert parser.parse_owner("Self") == OwnerType.SELF
    assert parser.parse_owner("") == OwnerType.SELF
    assert parser.parse_owner("other") == OwnerType.SELF


def test_parse_valid_html_report() -> None:
    parser = SenateHtmlParser()
    rec = SenateReportRecord(
        first_name="Mark",
        last_name="Warner",
        office="Senator (VA)",
        report_title="Periodic Transaction Report",
        report_url="https://efdsearch.senate.gov/search/view/ptr/d7a9b0c1-1111-2222-3333-444455556666/",
        received_date=date(2024, 2, 15),
        doc_id="d7a9b0c1-1111-2222-3333-444455556666",
    )

    html_content = MockSenateServer.generate_mock_ptr_html("Mark Warner")
    filing, txs = parser.parse_html(html_content, rec)

    assert filing.filing_id == "FILING_SENATE_d7a9b0c1-1111-2222-3333-444455556666"
    assert filing.status == FilingStatus.PARSED
    assert filing.chamber == "senate"
    assert len(txs) == 2

    # Verify first transaction (MSFT purchase)
    tx1 = txs[0]
    assert tx1.ticker == "MSFT"
    assert tx1.transaction_type == TransactionType.BUY
    assert tx1.owner == OwnerType.SPOUSE
    assert tx1.amount_min == 1000001.0
    assert tx1.comment == "Holding in blind trust"
    assert tx1.state == "VA"

    # Verify second transaction (AAPL sale)
    tx2 = txs[1]
    assert tx2.ticker == "AAPL"
    assert tx2.transaction_type == TransactionType.SALE_FULL
    assert tx2.owner == OwnerType.SELF
    assert tx2.amount_min == 250001.0


def test_parse_html_edge_cases() -> None:
    parser = SenateHtmlParser()
    rec = SenateReportRecord(
        first_name="Tommy",
        last_name="Tuberville",
        office="Senator (AL)",
        report_title="Periodic Transaction Report",
        report_url="https://example.com/ptr/1",
        received_date=date(2024, 2, 10),
        doc_id="1",
    )

    # 1. Empty HTML
    empty_filing, empty_txs = parser.parse_html("", rec)
    assert empty_filing.status == FilingStatus.ERROR
    assert len(empty_txs) == 0

    # 2. HTML without table
    no_table_html = "<html><body>No tables here</body></html>"
    no_table_filing, no_table_txs = parser.parse_html(no_table_html, rec)
    assert no_table_filing.status == FilingStatus.PARSED
    assert len(no_table_txs) == 0

    # 3. Table with row < 6 cells or missing date
    table_short_html = """
    <html><body>
    <table>
        <tr><th>Col1</th><th>Col2</th></tr>
        <tr><td>Only</td><td>Two</td></tr>
    </table>
    </body></html>
    """
    filing_short, txs_short = parser.parse_html(table_short_html, rec)
    assert len(txs_short) == 0


def test_extract_transaction_from_cells() -> None:
    parser = SenateHtmlParser()
    rec = SenateReportRecord(
        first_name="Test",
        last_name="Senator",
        office="Senator (TX)",
        report_title="Periodic Transaction Report",
        report_url="https://example.com/ptr/2",
        received_date=date(2024, 2, 10),
        doc_id="2",
    )
    filing_id = "FILING_SENATE_2"

    # No date in cells
    assert (
        parser._extract_transaction_from_cells(
            ["1", "not-a-date", "MSFT", "Purchase", "$1,001 - $15,000", "notes"], rec, filing_id
        )
        is None
    )

    # Date present but < 3 remaining cells
    assert parser._extract_transaction_from_cells(["1", "01/10/2024", "short"], rec, filing_id) is None

    # Date present but no ticker found
    assert (
        parser._extract_transaction_from_cells(
            ["1", "01/10/2024", "Self", "--", "No ticker asset", "Other", "Purchase", "$1,001 - $15,000"],
            rec,
            filing_id,
        )
        is None
    )

    # Ticker found in asset name, default BUY type, default amount
    cells_asset_ticker = [
        "1",
        "01/10/2024",
        "Joint",
        "--",
        "Amazon.com Inc (AMZN) [ST]",
        "Stock",
        "no explicit type",
        "no dollar",
    ]
    tx = parser._extract_transaction_from_cells(cells_asset_ticker, rec, filing_id)
    assert tx is not None
    assert tx.ticker == "AMZN"
    assert tx.owner == OwnerType.JOINT
    assert tx.transaction_type == TransactionType.BUY
    assert tx.amount_min == 1001.0

    # Ticker pattern search branch (match without parens, e.g. Ticker: AMD [ST])
    assert parser.normalize_ticker("", asset_name="Advanced Micro Devices Ticker: AMD [ST]") == "AMD"

    # Table without rows
    empty_table_html = "<html><body><table></table></body></html>"
    f_empty, tx_empty = parser.parse_html(empty_table_html, rec)
    assert len(tx_empty) == 0

    # Row where all remaining cells are shorter or start with $ (branch 274->278)
    cells_no_long_desc = [
        "1",
        "01/10/2024",
        "GOOGL",
        "P",
        "$1,001 - $15,000",
    ]
    tx_no_desc = parser._extract_transaction_from_cells(cells_no_long_desc, rec, filing_id)
    assert tx_no_desc is not None
    assert tx_no_desc.ticker == "GOOGL"
    assert tx_no_desc.asset_description == "Equity (GOOGL)"
