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

    # Runtime Settings
    default_year: int = 2024
    crawler_batch_size: int = 50
    max_concurrent_downloads: int = 5
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
