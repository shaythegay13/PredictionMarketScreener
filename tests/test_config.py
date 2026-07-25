"""Unit tests for configuration settings."""

from src.config import Settings


def test_default_settings():
    settings = Settings()
    assert settings.database_url.startswith("postgresql+asyncpg://")
    assert settings.poll_interval_seconds == 300
    assert settings.kalshi_rate_limit_rps == 2.0
    assert settings.polymarket_rate_limit_rps == 5.0
    assert settings.log_level == "INFO"


def test_custom_env_override(monkeypatch):
    monkeypatch.setenv("POLL_INTERVAL_SECONDS", "60")
    monkeypatch.setenv("KALSHI_RATE_LIMIT_RPS", "5.0")
    settings = Settings()
    assert settings.poll_interval_seconds == 60
    assert settings.kalshi_rate_limit_rps == 5.0
