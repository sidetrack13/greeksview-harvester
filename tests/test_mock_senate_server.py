"""Unit tests for offline MockSenateServer generator methods."""

from harvester.workers.congressional.simulation.mock_senate_server import MockSenateServer


def test_generate_mock_disclaimer_page() -> None:
    html_default = MockSenateServer.generate_mock_disclaimer_page()
    assert "mock_csrf_token_456" in html_default
    assert "prohibition_agreement" in html_default

    html_custom = MockSenateServer.generate_mock_disclaimer_page("custom_token_abc")
    assert "custom_token_abc" in html_custom


def test_generate_mock_datatables_response_custom() -> None:
    custom_records = [
        {
            "first": "CustomFirst",
            "last": "CustomLast",
            "office": "Senator (CA)",
            "url": "/search/view/ptr/custom-123/",
            "title": "Periodic Transaction Report",
            "date": "03/01/2024",
        }
    ]
    resp = MockSenateServer.generate_mock_datatables_response(records=custom_records, draw=2)
    assert resp["draw"] == 2
    assert resp["recordsTotal"] == 1
    assert "CustomFirst" in resp["data"][0][0]
    assert "CustomLast" in resp["data"][0][1]


def test_generate_mock_ptr_html_custom() -> None:
    custom_txs = [
        {
            "date": "03/10/2024",
            "owner": "Child",
            "ticker": "GOOGL",
            "asset": "Alphabet Inc.",
            "type": "Exchange",
            "amount": "$50,001 - $100,000",
            "comment": "Custom note",
        }
    ]
    html = MockSenateServer.generate_mock_ptr_html(member_name="Custom Senator", transactions=custom_txs)
    assert "Custom Senator" in html
    assert "GOOGL" in html
    assert "Exchange" in html
    assert "$50,001 - $100,000" in html
    assert "Custom note" in html
