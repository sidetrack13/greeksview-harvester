"""Configuration settings for the Congressional Trading Crawler."""

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings with environment variable overrides."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Database
    database_url: str = ""
    database_schema: str = "public"
    pgssl: str = "false"

    # House Clerk Endpoints
    house_index_base_url: str = "https://disclosures-clerk.house.gov/public_disc/financial-pdfs"
    house_pdf_base_url: str = "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs"

    # Senate eFD Endpoints
    senate_base_url: str = "https://efdsearch.senate.gov"
    senate_search_url: str = "https://efdsearch.senate.gov/search/"
    senate_data_endpoint: str = "https://efdsearch.senate.gov/search/report/data/"

    # Runtime & Historical Depth Settings
    default_year: int = 2024
    stock_act_inception_year: int = 2012
    cboe_default_days_back: int = 252  # 1 full trading year of daily P/C flow
    finra_default_weeks_back: int = 52  # 1 full year of weekly off-exchange volume
    crawler_batch_size: int = 50
    max_concurrent_downloads: int = 10
    request_timeout_seconds: float = 30.0
    http_max_retries: int = 3

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
