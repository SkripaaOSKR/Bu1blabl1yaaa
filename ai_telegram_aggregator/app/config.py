"""Application configuration management."""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import List

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"


class Settings(BaseSettings):
    """Environment-driven settings for the aggregator."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "AI Telegram Semantic Aggregator"
    log_level: str = "INFO"

    telegram_api_id: int = 0
    telegram_api_hash: str = ""
    telegram_session_name: str = "aggregator"
    telegram_sources: str = ""
    telegram_publish_channel: str = ""
    telegram_bot_token: str = ""

    batch_size: int = 200
    embedding_batch_size: int = 64
    dedupe_similarity_threshold: float = 0.80
    dedupe_window_days: int = 7
    merge_max_chars: int = 1000

    sqlite_path: Path = Field(default=DATA_DIR / "aggregator.db")
    faiss_index_path: Path = Field(default=DATA_DIR / "faiss.index")
    analytics_path: Path = Field(default=DATA_DIR / "analytics.json")

    embedding_model_name: str = "sentence-transformers/all-MiniLM-L6-v2"
    translation_model_name: str = "Helsinki-NLP/opus-mt-mul-ru"
    translation_cache_size: int = 1024

    spam_min_words: int = 15
    spam_max_links: int = 2
    spam_repeat_threshold: int = 10

    @property
    def sources(self) -> List[str]:
        return [item.strip() for item in self.telegram_sources.split(",") if item.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached settings instance."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return Settings()
