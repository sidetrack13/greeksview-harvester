"""Configuration settings for the Congressional Trading Crawler."""

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

_HARVESTER_DIR = Path(__file__).resolve().parent.parent
_SIBLING_GV_ENV = _HARVESTER_DIR.parent / "greeksview" / ".env"


def _load_sibling_alphavantage_key() -> str:
    """Read ALPHAVANTAGE_API_KEY from sibling greeksview .env if present and not otherwise configured."""
    if _SIBLING_GV_ENV.is_file():
        try:
            for line in _SIBLING_GV_ENV.read_text().splitlines():
                line = line.strip()
                if line.startswith("ALPHAVANTAGE_API_KEY=") and not line.startswith("#"):
                    return line.split("=", 1)[1].strip().strip("'\"")
        except Exception:
            pass
    return ""


class Settings(BaseSettings):
    """Application settings with environment variable overrides."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Database
    database_url: str = ""
    database_schema: str = "gv"
    pgssl: str = "false"

    # House Clerk Endpoints
    house_index_base_url: str = "https://disclosures-clerk.house.gov/public_disc/financial-pdfs"
    house_pdf_base_url: str = "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs"

    # Senate eFD Endpoints
    senate_base_url: str = "https://efdsearch.senate.gov"
    senate_home_url: str = "https://efdsearch.senate.gov/search/home/"
    senate_search_url: str = "https://efdsearch.senate.gov/search/"
    senate_data_endpoint: str = "https://efdsearch.senate.gov/search/report/data/"

    # Runtime & Historical Depth Settings
    default_year: int = 2024
    stock_act_inception_year: int = 2012
    crawler_batch_size: int = 50
    max_concurrent_downloads: int = 10
    request_timeout_seconds: float = 30.0
    http_max_retries: int = 3

    # Alpha Vantage Settings
    alphavantage_api_key: str = Field(default_factory=_load_sibling_alphavantage_key)
    alphavantage_base_url: str = "https://www.alphavantage.co/query"
    # Pacing. The Alpha Vantage key is SHARED with the GreeksView product, whose
    # worst case already spends most of the key's per-minute and per-second
    # budget. These defaults are deliberately tiny; the owner
    # must set ALPHAVANTAGE_RPM / ALPHAVANTAGE_MAX_PER_SECOND so that the harvester
    # plus the product's worst case stays inside the key's budget. Never raise them
    # to the licence ceiling. The pacer enforces both limits (pacer.py).
    alphavantage_max_per_second: int = 1
    alphavantage_rpm: int = 9
    alphavantage_cooldown_ms: int = 2000

    # Simulation & Testing
    simulation_mode: bool = False
    mock_fixtures_dir: Path = Path("tests/fixtures")

    @property
    def is_sqlite(self) -> bool:
        """Return True if using SQLite or if database_url is unset."""
        if not self.database_url:
            return True
        return self.database_url.startswith("sqlite")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached application settings."""
    return Settings()
