"""Digital PDF parser and transaction normalizer for House Periodic Transaction Reports (PTRs)."""

import hashlib
import io
import logging
import re
from datetime import date, datetime

import pdfplumber

from harvester.core.models import (
    CongressionalFiling,
    CongressionalTransaction,
    FilingStatus,
    HouseIndexRecord,
    OwnerType,
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

# Regex to capture stock tickers, e.g. "(AMZN) [ST]", "(NVDA)", "Ticker: AAPL"
TICKER_PATTERN = re.compile(r"\b(?:Ticker:\s*)?\(?([A-Z]{1,5})\)?(?:\s*\[(ST|OP|CS|OT)\])?\b")

# Regex to extract amount range numbers if not matching standard bracket strings
CUSTOM_AMOUNT_PATTERN = re.compile(r"\$([0-9,]+)\s*(?:-|to)\s*\$([0-9,]+)", re.IGNORECASE)


class HousePTRParser:
    """Extracts, validates, and normalizes transactions from House digital PTR PDFs."""

    def __init__(self) -> None:
        pass

    @staticmethod
    def compute_sha256(data: bytes) -> str:
        """Compute SHA-256 hash of PDF payload for integrity and deduplication."""
        return hashlib.sha256(data).hexdigest()

    @staticmethod
    def normalize_ticker(raw_text: str) -> str | None:
        """Extract clean equity ticker from asset description text."""
        if not raw_text:
            return None

        # Look for explicit parenthetical symbol e.g. "Apple Inc. (AAPL) [ST]"
        paren_match = re.search(r"\(([A-Z]{1,5})\)", raw_text)
        if paren_match:
            candidate = paren_match.group(1).upper()
            if candidate not in ("SP", "DC", "JT", "ST", "OP", "FD", "PTR", "USA", "INC"):
                return candidate

        # Look for [ST] or standard ticker pattern
        match = TICKER_PATTERN.search(raw_text)
        if match:
            candidate = match.group(1).upper()
            if candidate not in ("SP", "DC", "JT", "ST", "OP", "FD", "PTR", "USA", "INC"):
                return candidate

        return None

    @staticmethod
    def parse_transaction_type(raw_type: str) -> TransactionType | None:
        """Map raw transaction code or text to normalized TransactionType."""
        if not raw_type:
            return None
        clean = raw_type.strip().lower()
        if "partial" in clean or clean in ("s (partial)", "sp"):
            return TransactionType.SALE_PARTIAL
        if clean.startswith("s") or "sale" in clean:
            return TransactionType.SALE_FULL
        if clean.startswith("p") or "purchase" in clean or "buy" in clean:
            return TransactionType.BUY
        if clean.startswith("e") or "exchange" in clean:
            return TransactionType.EXCHANGE
        return None

    @staticmethod
    def parse_date(date_str: str) -> date | None:
        """Parse date string supporting multiple standard date formats."""
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
        """Quantize amount string into (amount_bracket, amount_min, amount_max)."""
        clean = raw_amount.strip()
        for bracket, (b_min, b_max) in AMOUNT_BRACKETS.items():
            if bracket.lower() in clean.lower():
                return bracket, b_min, b_max

        # Fallback to regex numeric parsing
        match = CUSTOM_AMOUNT_PATTERN.search(clean)
        if match:
            min_val = float(match.group(1).replace(",", ""))
            max_val = float(match.group(2).replace(",", ""))
            return f"${int(min_val):,} - ${int(max_val):,}", min_val, max_val

        if "50,000,000" in clean and ("over" in clean.lower() or "+" in clean):
            return "Over $50,000,000", 50000001.0, None

        # Conservative fallback
        return "$1,001 - $15,000", 1001.0, 15000.0

    @staticmethod
    def parse_owner(raw_owner: str) -> OwnerType:
        """Parse beneficial owner code (SP -> spouse, DC -> dependent, JT -> joint)."""
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

    def parse_pdf(
        self,
        pdf_bytes: bytes,
        record: HouseIndexRecord,
        doc_url: str | None = None,
    ) -> tuple[CongressionalFiling, list[CongressionalTransaction]]:
        """Parse in-memory PDF bytes into a CongressionalFiling and list of transactions."""
        sha256 = self.compute_sha256(pdf_bytes)
        filing_id = f"FILING_HOUSE_{record.doc_id}"

        filing = CongressionalFiling(
            filing_id=filing_id,
            chamber="house",
            member_name=record.member_name,
            member_id=None,
            filing_year=record.year,
            filing_date=record.filing_date,
            doc_url=doc_url,
            raw_text=None,
            sha256_hash=sha256,
            status=FilingStatus.PARSED,
        )

        transactions: list[CongressionalTransaction] = []
        raw_text_chunks: list[str] = []

        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    text = page.extract_text() or ""
                    if text:
                        raw_text_chunks.append(text)

                    # Extract tabular structure
                    tables = page.extract_tables()
                    for table in tables:
                        if not table or len(table) < 2:
                            continue

                        # Inspect rows
                        for row in table:
                            row_clean = [str(c).strip() if c is not None else "" for c in row]
                            # Look for transaction row with ticker and date
                            parsed_tx = self._extract_transaction_from_row(row_clean, record, filing_id)
                            if parsed_tx is not None:
                                transactions.append(parsed_tx)
        except Exception as exc:
            logger.warning("pdfplumber encountered issue on %s: %s; trying pypdf fallback", record.doc_id, exc)

        # Fallback text-based extraction if table geometry did not yield transactions
        if not transactions:
            transactions = self._extract_transactions_from_text(raw_text_chunks, record, filing_id)

        if not transactions and not raw_text_chunks:
            filing.status = FilingStatus.ERROR

        filing.raw_text = "\n".join(raw_text_chunks)[:10000] if raw_text_chunks else None
        return filing, transactions

    def _extract_transaction_from_row(
        self,
        row: list[str],
        record: HouseIndexRecord,
        filing_id: str,
    ) -> CongressionalTransaction | None:
        """Extract a CongressionalTransaction from a row of tabular PDF cells."""
        # Row must have at least 3 cells (Asset, Type, Date or Amount)
        if len(row) < 3:
            return None

        # Look for asset description containing ticker
        ticker = None
        asset_desc = ""
        owner = OwnerType.SELF
        for cell in row:
            t = self.normalize_ticker(cell)
            if t:
                ticker = t
                asset_desc = cell
                break

        if not ticker:
            return None

        # Check for owner notation [SP], [DC], [JT]
        full_row_str = " ".join(row)
        for code, ot in (("[SP]", OwnerType.SPOUSE), ("[DC]", OwnerType.DEPENDENT), ("[JT]", OwnerType.JOINT)):
            if code in full_row_str:
                owner = ot
                break

        # Extract transaction type
        tx_type = None
        for cell in row:
            parsed_type = self.parse_transaction_type(cell)
            if parsed_type is not None:
                tx_type = parsed_type
                break

        if tx_type is None:
            tx_type = TransactionType.BUY

        # Extract transaction date
        tx_date = None
        for cell in row:
            d = self.parse_date(cell)
            if d is not None and d != record.filing_date:
                tx_date = d
                break

        if tx_date is None:
            tx_date = record.filing_date

        # Extract amount
        amount_bracket: str = ",001 - ,000"
        amount_min: float = 1001.0
        amount_max: float | None = 15000.0
        for cell in row:
            if "$" in cell:
                amount_bracket, amount_min, amount_max = self.parse_amount(cell)
                break

        return CongressionalTransaction(
            filing_id=filing_id,
            member_name=record.member_name,
            chamber="house",
            party=None,
            state=record.state or None,
            district=record.district or None,
            ticker=ticker,
            asset_description=asset_desc or f"Equity ({ticker})",
            asset_type="stock",
            transaction_type=tx_type,
            amount_bracket=amount_bracket,
            amount_min=amount_min,
            amount_max=amount_max,
            transaction_date=tx_date,
            filing_date=record.filing_date,
            owner=owner,
            comment=None,
        )

    def _extract_transactions_from_text(
        self,
        text_chunks: list[str],
        record: HouseIndexRecord,
        filing_id: str,
    ) -> list[CongressionalTransaction]:
        """Fallback line-by-line regex parser for unstructured PDF text."""
        transactions: list[CongressionalTransaction] = []
        combined = "\n".join(text_chunks)
        lines = combined.splitlines()

        for line in lines:
            ticker = self.normalize_ticker(line)
            if not ticker:
                continue

            tx_type = self.parse_transaction_type(line) or TransactionType.BUY
            tx_date = self.parse_date(line) or record.filing_date
            amount_bracket, amount_min, amount_max = self.parse_amount(line)

            owner = OwnerType.SELF
            if "[SP]" in line or "Spouse" in line:
                owner = OwnerType.SPOUSE
            elif "[DC]" in line or "Child" in line:
                owner = OwnerType.DEPENDENT
            elif "[JT]" in line or "Joint" in line:
                owner = OwnerType.JOINT

            transactions.append(
                CongressionalTransaction(
                    filing_id=filing_id,
                    member_name=record.member_name,
                    chamber="house",
                    party=None,
                    state=record.state or None,
                    district=record.district or None,
                    ticker=ticker,
                    asset_description=line.strip() or f"Equity ({ticker})",
                    asset_type="stock",
                    transaction_type=tx_type,
                    amount_bracket=amount_bracket,
                    amount_min=amount_min,
                    amount_max=amount_max,
                    transaction_date=tx_date,
                    filing_date=record.filing_date,
                    owner=owner,
                    comment=None,
                )
            )

        return transactions
