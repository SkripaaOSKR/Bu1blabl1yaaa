"""Application configuration management."""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"


class Settings(BaseSettings):
    """Environment-driven settings for the platform."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "Personal AI News Intelligence"
    log_level: str = "INFO"
    environment: str = "prod"

    api_host: str = "0.0.0.0"
    api_port: int = 8000

    telegram_api_id: int = 0
    telegram_api_hash: str = ""
    telegram_session_name: str = "aggregator"
    telegram_publish_channel: str = ""
    telegram_bot_token: str = ""

    telegram_sources: str = ""
    admin_user_ids: str = ""

    batch_size: int = 500
    embedding_batch_size: int = 128
    dedupe_similarity_threshold: float = 0.80
    dedupe_window_days: int = 14
    merge_max_chars: int = 1800

    sqlite_path: Path = Field(default=DATA_DIR / "aggregator.db")
    faiss_index_path: Path = Field(default=DATA_DIR / "faiss.index")
    analytics_path: Path = Field(default=DATA_DIR / "analytics.json")
    embedding_model_name: str = "sentence-transformers/all-MiniLM-L6-v2"

    postgres_dsn: str = "postgresql+asyncpg://news:news@postgres:5432/news"

    api_secret_key: str = "change-me"
    miniapp_base_url: str = "https://example.com/miniapp"

    @property
    def sources(self) -> list[str]:
        return [item.strip() for item in self.telegram_sources.split(",") if item.strip()]

    @property
    def admin_ids(self) -> set[int]:
        return {int(item.strip()) for item in self.admin_user_ids.split(",") if item.strip()}


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached settings instance."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return Settings()
