"""HTML disclosure parser for U.S. Senate Periodic Transaction Reports (eFD)."""

import hashlib
import logging
import re
from datetime import date, datetime

from bs4 import BeautifulSoup

from harvester.core.models import (
    CongressionalFiling,
    CongressionalTransaction,
    FilingStatus,
    OwnerType,
    SenateReportRecord,
    TransactionType,
)

logger = logging.getLogger(__name__)

# Standard STOCK Act amount brackets
AMOUNT_BRACKETS: dict[str, tuple[float, float | None]] = {
    "$1,001 - $15,000": (1001.0, 15000.0),
    "$15,001 - $50,000": (15001.0, 50000.0),
    "$50,001 - $100,000": (50001.0, 100000.0),
    "$100,001 - $250,000": (100001.0, 250000.0),
    "$250,001 - $500,000": (250001.0, 500000.0),
    "$500,001 - $1,000,000": (500001.0, 1000000.0),
    "$1,000,001 - $5,000,000": (1000001.0, 5000000.0),
    "$5,000,001 - $25,000,000": (5000001.0, 25000000.0),
    "$25,000,001 - $50,000,000": (25000001.0, 50000000.0),
    "Over $50,000,000": (50000001.0, None),
    "$50,000,001+": (50000001.0, None),
}

NON_TICKERS = {
    "SP",
    "DC",
    "JT",
    "ST",
    "OP",
    "OT",
    "USA",
    "INC",
    "LLC",
    "CORP",
    "SELF",
    "SPOUSE",
    "JOINT",
    "CHILD",
    "OTHER",
    "STOCK",
    "BOND",
    "FUNDS",
    "TRUST",
    "TOTAL",
    "SHARE",
    "TRADE",
    "CHECK",
    "ESTATE",
    "NOTE",
    "NOTES",
    "DEBT",
    "BANK",
    "MONEY",
    "REAL",
    "LAND",
    "MUNI",
    "STATE",
    "GOVT",
    "BUY",
    "SALE",
    "PURCH",
    "SOLD",
    "EXCH",
    "N/A",
    "NONE",
    "UNKNOWN",
    "--",
}

TICKER_PATTERN = re.compile(r"\b(?:Ticker:\s*)?\(?([A-Z]{1,5})\)?(?:\s*\[(ST|OP|CS|OT)\])?\b")
CUSTOM_AMOUNT_PATTERN = re.compile(r"\$([0-9,]+)\s*(?:-|to)\s*\$([0-9,]+)", re.IGNORECASE)


class SenateHtmlParser:
    """Extracts and normalizes transactions from Senate PTR HTML documents."""

    @staticmethod
    def compute_sha256(data: str | bytes) -> str:
        """Compute SHA-256 hash of HTML report payload."""
        if isinstance(data, str):
            data = data.encode("utf-8")
        return hashlib.sha256(data).hexdigest()

    @staticmethod
    def normalize_ticker(raw_ticker: str, asset_name: str = "") -> str | None:
        """Extract clean uppercase equity ticker symbol."""
        clean = raw_ticker.strip().upper()
        if clean and clean not in NON_TICKERS and clean.isalpha() and len(clean) <= 5:
            return clean

        # Search within asset name if ticker cell was empty or placeholder
        if asset_name:
            paren_match = re.search(r"\(([A-Z]{1,5})\)", asset_name)
            if paren_match:
                candidate = paren_match.group(1).upper()
                if candidate not in NON_TICKERS:
                    return candidate

            match = TICKER_PATTERN.search(asset_name)
            if match:
                candidate = match.group(1).upper()
                if candidate not in NON_TICKERS:
                    return candidate

        return None

    @staticmethod
    def parse_transaction_type(raw_type: str) -> TransactionType | None:
        """Parse transaction type into standard enum."""
        if not raw_type:
            return None
        clean = raw_type.strip().lower()
        if "partial" in clean or clean in ("s (partial)", "sp", "sale (partial)"):
            return TransactionType.SALE_PARTIAL
        if (
            clean in ("s", "sale", "sale (full)", "full sale", "s (full)")
            or clean.startswith("sale (full)")
            or clean.startswith("sale")
        ):
            return TransactionType.SALE_FULL
        if clean in ("p", "purchase", "buy") or clean.startswith("purchase") or clean.startswith("buy"):
            return TransactionType.BUY
        if clean in ("e", "exchange") or clean.startswith("exchange"):
            return TransactionType.EXCHANGE
        return None

    @staticmethod
    def parse_date(date_str: str) -> date | None:
        """Parse date string supporting standard formats."""
        if not date_str:
            return None
        clean = date_str.strip().replace("-", "/")
        for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y/%m/%d"):
            try:
                return datetime.strptime(clean, fmt).date()
            except ValueError:
                continue
        return None

    @staticmethod
    def parse_amount(raw_amount: str) -> tuple[str, float, float | None]:
        """Quantize amount string into (bracket, min_val, max_val)."""
        clean = raw_amount.strip()
        for bracket, (b_min, b_max) in AMOUNT_BRACKETS.items():
            if bracket.lower() in clean.lower():
                return bracket, b_min, b_max

        match = CUSTOM_AMOUNT_PATTERN.search(clean)
        if match:
            min_val = float(match.group(1).replace(",", ""))
            max_val = float(match.group(2).replace(",", ""))
            return f"${int(min_val):,} - ${int(max_val):,}", min_val, max_val

        if "50,000,000" in clean and ("over" in clean.lower() or "+" in clean):
            return "Over $50,000,000", 50000001.0, None

        return "$1,001 - $15,000", 1001.0, 15000.0

    @staticmethod
    def parse_owner(raw_owner: str) -> OwnerType:
        """Parse beneficial owner code."""
        if not raw_owner:
            return OwnerType.SELF
        clean = raw_owner.strip().lower()
        if "sp" in clean or "spouse" in clean:
            return OwnerType.SPOUSE
        if "dc" in clean or "child" in clean or "dependent" in clean:
            return OwnerType.DEPENDENT
        if "jt" in clean or "joint" in clean:
            return OwnerType.JOINT
        return OwnerType.SELF

    def parse_html(
        self,
        html_content: str,
        record: SenateReportRecord,
    ) -> tuple[CongressionalFiling, list[CongressionalTransaction]]:
        """Parse Senate PTR HTML report into CongressionalFiling and list of transactions."""
        sha256 = self.compute_sha256(html_content)
        filing_id = f"FILING_SENATE_{record.doc_id}"

        filing = CongressionalFiling(
            filing_id=filing_id,
            chamber="senate",
            member_name=record.member_name,
            member_id=None,
            filing_year=record.received_date.year,
            filing_date=record.received_date,
            doc_url=record.report_url,
            raw_text=html_content[:10000] if html_content else None,
            sha256_hash=sha256,
            status=FilingStatus.PARSED,
        )

        transactions: list[CongressionalTransaction] = []
        soup = BeautifulSoup(html_content, "html.parser")

        # Find transaction tables
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            if not rows:
                continue

            for tr in rows:
                cells = [td.get_text().strip() for td in tr.find_all(["td", "th"])]
                if len(cells) < 6:
                    continue

                tx = self._extract_transaction_from_cells(cells, record, filing_id)
                if tx is not None:
                    transactions.append(tx)

        if not transactions:
            # Check if document has valid text or is empty/corrupted
            text = soup.get_text().strip()
            if not text:
                filing.status = FilingStatus.ERROR

        return filing, transactions

    def _extract_transaction_from_cells(
        self,
        cells: list[str],
        record: SenateReportRecord,
        filing_id: str,
    ) -> CongressionalTransaction | None:
        """Extract a CongressionalTransaction from a Senate HTML table row."""
        # Standard columns:
        # [# or empty, Transaction Date, Owner, Ticker, Asset Name, Asset Type, Type, Amount, Comment]
        # Or variable column layouts
        # Find cell with date
        tx_date = None
        date_idx = -1
        for idx, c in enumerate(cells):
            d = self.parse_date(c)
            if d is not None:
                tx_date = d
                date_idx = idx
                break

        if tx_date is None:
            return None

        # Remaining cells after date
        remaining = cells[date_idx + 1 :]
        if len(remaining) < 3:
            return None

        # Look for owner in remaining cells or default SELF
        owner = OwnerType.SELF
        for c in remaining:
            parsed_owner = self.parse_owner(c)
            if parsed_owner != OwnerType.SELF:
                owner = parsed_owner
                break

        # Look for ticker and asset name
        ticker = None
        asset_desc = ""
        for c in remaining:
            t = self.normalize_ticker(c)
            if t is not None:
                ticker = t
                # If there's a subsequent or preceding cell with longer text, treat as asset desc
                for other_c in remaining:
                    if len(other_c) > len(c) and not other_c.startswith("$"):
                        asset_desc = other_c
                        break
                break

        if not ticker:
            # Try finding ticker inside descriptions
            for c in remaining:
                t = self.normalize_ticker("", asset_name=c)
                if t is not None:
                    ticker = t
                    asset_desc = c
                    break

        if not ticker:
            return None

        # Look for transaction type
        tx_type = None
        for c in remaining:
            tt = self.parse_transaction_type(c)
            if tt is not None:
                tx_type = tt
                break

        if tx_type is None:
            tx_type = TransactionType.BUY

        # Look for amount
        amount_bracket = "$1,001 - $15,000"
        amount_min = 1001.0
        amount_max: float | None = 15000.0
        for c in remaining:
            if "$" in c:
                amount_bracket, amount_min, amount_max = self.parse_amount(c)
                break

        # Look for optional comment (last cell if not amount)
        comment = None
        if len(remaining) >= 5 and remaining[-1] and not remaining[-1].startswith("$"):
            comment = remaining[-1]

        return CongressionalTransaction(
            filing_id=filing_id,
            member_name=record.member_name,
            chamber="senate",
            party=None,
            state=record.state or None,
            district=None,
            ticker=ticker,
            asset_description=asset_desc or f"Equity ({ticker})",
            asset_type="stock",
            transaction_type=tx_type,
            amount_bracket=amount_bracket,
            amount_min=amount_min,
            amount_max=amount_max,
            transaction_date=tx_date,
            filing_date=record.received_date,
            owner=owner,
            comment=comment,
        )
