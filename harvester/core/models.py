"""Core domain and validation models for Congressional Trading Crawler."""

from datetime import date
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator


class TransactionType(StrEnum):
    """Normalized direction of disclosed trade."""

    BUY = "BUY"
    SALE_FULL = "SALE_FULL"
    SALE_PARTIAL = "SALE_PARTIAL"
    EXCHANGE = "EXCHANGE"


class OwnerType(StrEnum):
    """Beneficial owner of the security."""

    SELF = "self"
    SPOUSE = "spouse"
    DEPENDENT = "dependent"
    JOINT = "joint"


class FilingStatus(StrEnum):
    """Processing lifecycle state of a filing."""

    PENDING = "pending"
    PARSED = "parsed"
    ERROR = "error"
    MANUAL_REVIEW = "manual_review"


class HouseIndexRecord(BaseModel):
    """Single disclosure entry extracted from House YYYYFD.xml index."""

    model_config = ConfigDict(frozen=True)

    prefix: str = ""
    last_name: str
    first_name: str
    filing_type: str
    state_dst: str = ""
    year: int
    filing_date: date
    doc_id: str

    @property
    def member_name(self) -> str:
        """Return formatted clean full member name."""
        first = self.first_name.strip()
        last = self.last_name.strip()
        if first and last:
            return f"{first} {last}"
        return last or first

    @property
    def state(self) -> str:
        """Extract two-letter state code from StateDst (e.g. CA12 -> CA)."""
        clean = self.state_dst.strip().upper()
        if len(clean) >= 2 and clean[:2].isalpha():
            return clean[:2]
        return ""

    @property
    def district(self) -> str:
        """Extract district number/code from StateDst (e.g. CA12 -> 12)."""
        clean = self.state_dst.strip()
        if len(clean) > 2 and clean[:2].isalpha():
            return clean[2:]
        return clean

    @property
    def is_ptr(self) -> bool:
        """Return True if this filing is a Periodic Transaction Report."""
        return self.filing_type.strip().upper() == "P"


class SenateReportRecord(BaseModel):
    """Single disclosure entry discovered from Senate eFD DataTables search."""

    model_config = ConfigDict(frozen=True)

    first_name: str
    last_name: str
    office: str = ""
    report_title: str
    report_url: str
    received_date: date
    doc_id: str

    @property
    def member_name(self) -> str:
        """Return formatted clean full member name."""
        first = self.first_name.strip()
        last = self.last_name.strip()
        if first and last:
            return f"{first} {last}"
        return last or first

    @property
    def state(self) -> str:
        """Extract two-letter state abbreviation from office (e.g. 'Senator (AL)' -> 'AL')."""
        import re

        match = re.search(r"\(([A-Z]{2})\)", self.office)
        if match:
            return match.group(1)
        return ""

    @property
    def is_ptr(self) -> bool:
        """Return True if this filing is a Periodic Transaction Report."""
        clean = self.report_title.lower()
        return "periodic" in clean or "ptr" in clean or "/ptr/" in self.report_url.lower()

    @property
    def is_paper(self) -> bool:
        """Return True if this is a legacy scanned paper report."""
        return "/paper/" in self.report_url.lower() or self.report_url.lower().endswith(".pdf")


class CongressionalFiling(BaseModel):
    """Canonical database record representing a raw disclosure filing."""

    model_config = ConfigDict(from_attributes=True)

    filing_id: str
    chamber: str = "house"
    member_name: str
    member_id: str | None = None
    filing_year: int
    filing_date: date
    doc_url: str | None = None
    raw_text: str | None = None
    sha256_hash: str
    status: FilingStatus = FilingStatus.PENDING

    @field_validator("chamber")
    @classmethod
    def validate_chamber(cls, v: str) -> str:
        clean = v.strip().lower()
        if clean not in ("house", "senate"):
            raise ValueError("Chamber must be 'house' or 'senate'")
        return clean


class CongressionalTransaction(BaseModel):
    """Individual disclosed transaction linked to a filing."""

    model_config = ConfigDict(from_attributes=True)

    filing_id: str
    member_name: str
    chamber: str = "house"
    party: str | None = None
    state: str | None = None
    district: str | None = None
    ticker: str
    asset_description: str
    asset_type: str = "stock"
    transaction_type: TransactionType
    amount_bracket: str
    amount_min: float
    amount_max: float | None = None
    transaction_date: date
    filing_date: date
    disclosure_lag_days: int | None = None
    owner: OwnerType = OwnerType.SELF
    comment: str | None = None

    @field_validator("ticker")
    @classmethod
    def validate_ticker(cls, v: str) -> str:
        clean = v.strip().upper()
        if not clean or not clean.isalpha() or len(clean) > 10:
            raise ValueError(f"Invalid equity ticker: '{v}'")
        return clean

    @field_validator("disclosure_lag_days", mode="before")
    @classmethod
    def compute_lag(cls, v: int | None) -> int | None:
        return v

    def model_post_init(self, __context: object) -> None:
        """Automatically calculate disclosure_lag_days if not set."""
        if self.disclosure_lag_days is None:
            self.disclosure_lag_days = (self.filing_date - self.transaction_date).days


class CrawlReport(BaseModel):
    """Aggregated metrics and diagnostics for a crawl execution run."""

    year: int
    chamber: str = "house"
    total_index_filings: int = 0
    filtered_ptrs: int = 0
    new_filings_to_crawl: int = 0
    filings_parsed: int = 0
    transactions_extracted: int = 0
    errors_count: int = 0
    duration_seconds: float = 0.0
    error_details: list[str] = Field(default_factory=list)


# Alpha Vantage Data Models
class StockBarDaily(BaseModel):
    """Daily adjusted stock bar model matching stock_bars_daily table."""

    symbol: str
    trade_date: str
    open: float
    high: float
    low: float
    close: float
    adjusted_close: float
    volume: int
    dividend_amount: float = 0.0
    split_coefficient: float = 1.0


class StockBarIntraday(BaseModel):
    """Intraday stock bar model matching stock_bars_intraday table."""

    symbol: str
    bar_timestamp: str
    interval: str
    open: float
    high: float
    low: float
    close: float
    volume: int


class OptionChainEOD(BaseModel):
    """End-of-day options contract record matching options_chains_eod table."""

    contract_id: str
    symbol: str
    trade_date: str
    expiration: str
    strike: float
    option_type: str
    last_price: float | None = None
    mark_price: float | None = None
    bid: float | None = None
    ask: float | None = None
    volume: int = 0
    open_interest: int = 0
    implied_volatility: float | None = None
    delta: float | None = None
    gamma: float | None = None
    theta: float | None = None
    vega: float | None = None
    rho: float | None = None


class CompanyFundamental(BaseModel):
    """Corporate fundamental report record matching company_fundamentals table."""

    symbol: str
    fiscal_date_ending: str
    report_type: str
    period_type: str = "annual"
    data_json: str


class CorporateDividend(BaseModel):
    """Corporate dividend distribution record matching corporate_dividends table."""

    symbol: str
    ex_dividend_date: str
    declaration_date: str | None = None
    record_date: str | None = None
    payment_date: str | None = None
    amount: float


class CorporateSplit(BaseModel):
    """Corporate stock split record matching corporate_splits table."""

    symbol: str
    effective_date: str
    split_factor: float


class ETFProfile(BaseModel):
    """ETF overview and constituent weightings matching etf_profiles table."""

    symbol: str
    net_assets: float | None = None
    portfolio_turnover: float | None = None
    dividend_yield: float | None = None
    expense_ratio: float | None = None
    holdings_json: str = "[]"
    sectors_json: str = "[]"


class ListingStatusRecord(BaseModel):
    """Exchange listing status record matching listing_status table."""

    symbol: str
    name: str | None = None
    exchange: str | None = None
    asset_type: str | None = None
    ipo_date: str | None = None
    delisting_date: str | None = None
    status: str = "Active"

