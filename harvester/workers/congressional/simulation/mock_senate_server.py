"""Offline simulation mock data generator for Senate eFD disclosure ingestion."""

from typing import Any


class MockSenateServer:
    """Provides synthetic Senate disclaimer pages, DataTables responses, and HTML PTRs."""

    @staticmethod
    def generate_mock_disclaimer_page(csrf_token: str = "mock_csrf_token_456") -> str:
        """Generate synthetic Senate eFD statutory agreement HTML with CSRF token."""
        return f"""<!DOCTYPE html>
<html>
<head><title>Senate eFD Search Disclaimer</title></head>
<body>
<div class="container">
    <h1>Electronic Financial Disclosure (eFD) Search</h1>
    <form method="post" action="/search/">
        <input type="hidden" name="csrfmiddlewaretoken" value="{csrf_token}">
        <input type="checkbox" name="prohibition_agreement" value="1" id="id_prohibition_agreement">
        <label for="id_prohibition_agreement">I agree to the statutory requirements</label>
        <button type="submit">Submit</button>
    </form>
</div>
</body>
</html>
"""

    @staticmethod
    def generate_mock_datatables_response(
        records: list[dict[str, str]] | None = None,
        draw: int = 1,
    ) -> dict[str, Any]:
        """Generate synthetic DataTables JSON payload for Senate search endpoint."""
        if records is None:
            records = [
                {
                    "first": "Mark",
                    "last": "Warner",
                    "office": "Senator (VA)",
                    "url": "/search/view/ptr/d7a9b0c1-1111-2222-3333-444455556666/",
                    "title": "Periodic Transaction Report",
                    "date": "02/15/2024",
                },
                {
                    "first": "Tommy",
                    "last": "Tuberville",
                    "office": "Senator (AL)",
                    "url": "/search/view/ptr/e8b0c1d2-7777-8888-9999-000011112222/",
                    "title": "Periodic Transaction Report",
                    "date": "02/10/2024",
                },
                {
                    "first": "John",
                    "last": "Doe",
                    "office": "Candidate",
                    "url": "/search/view/annual/999999/",
                    "title": "Annual Report",  # Not a PTR, should be filtered out
                    "date": "05/15/2024",
                },
            ]

        data = []
        for r in records:
            link_html = f'<a href="{r["url"]}" target="_blank">{r["title"]}</a>'
            data.append(
                [
                    r["first"],
                    r["last"],
                    r["office"],
                    link_html,
                    r["date"],
                ]
            )

        return {
            "draw": draw,
            "recordsTotal": len(records),
            "recordsFiltered": len(records),
            "data": data,
        }

    @staticmethod
    def generate_mock_ptr_html(
        member_name: str = "Mark Warner",
        transactions: list[dict[str, str]] | None = None,
    ) -> str:
        """Generate realistic Senate PTR disclosure HTML document with transaction tables."""
        if transactions is None:
            transactions = [
                {
                    "date": "01/10/2024",
                    "owner": "Spouse",
                    "ticker": "MSFT",
                    "asset": "Microsoft Corporation - Common Stock",
                    "type": "Purchase",
                    "amount": "$1,000,001 - $5,000,000",
                    "comment": "Holding in blind trust",
                },
                {
                    "date": "01/08/2024",
                    "owner": "Self",
                    "ticker": "AAPL",
                    "asset": "Apple Inc.",
                    "type": "Sale (Full)",
                    "amount": "$250,001 - $500,000",
                    "comment": "",
                },
            ]

        rows_html = ""
        for idx, tx in enumerate(transactions, start=1):
            rows_html += f"""
            <tr>
                <td>{idx}</td>
                <td>{tx["date"]}</td>
                <td>{tx["owner"]}</td>
                <td><a href="https://finance.yahoo.com/quote/{tx["ticker"]}">{tx["ticker"]}</a></td>
                <td>{tx["asset"]}</td>
                <td>Stock</td>
                <td>{tx["type"]}</td>
                <td>{tx["amount"]}</td>
                <td>{tx.get("comment", "")}</td>
            </tr>
            """

        return f"""<!DOCTYPE html>
<html>
<head><title>Senate Periodic Transaction Report - {member_name}</title></head>
<body>
<div class="container">
    <h1 class="page-title">Periodic Transaction Report</h1>
    <h2>Filer: The Honorable {member_name}</h2>
    <div class="table-responsive">
        <table class="table table-striped">
            <thead>
                <tr>
                    <th>#</th>
                    <th>Transaction Date</th>
                    <th>Owner</th>
                    <th>Ticker</th>
                    <th>Asset Name</th>
                    <th>Asset Type</th>
                    <th>Type</th>
                    <th>Amount</th>
                    <th>Comment</th>
                </tr>
            </thead>
            <tbody>
                {rows_html}
            </tbody>
        </table>
    </div>
</div>
</body>
</html>
"""
