"""Unit tests for configuration management."""

from harvester.config import Settings, get_settings


def test_settings_defaults() -> None:
    settings = Settings(_env_file=None, database_url="")
    assert settings.default_year == 2024
    assert settings.crawler_batch_size == 50
    assert settings.is_sqlite is True


def test_settings_sqlite_explicit() -> None:
    settings = Settings(database_url="sqlite:///data/test.db")
    assert settings.is_sqlite is True


def test_settings_postgres_url() -> None:
    settings = Settings(database_url="postgresql://user:pass@localhost:5432/testdb")
    assert settings.is_sqlite is False


def test_get_settings_cached() -> None:
    s1 = get_settings()
    s2 = get_settings()
    assert s1 is s2
