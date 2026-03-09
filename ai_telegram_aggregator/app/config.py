"""Application configuration management."""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from urllib.parse import urlparse

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
    telegram_string_session: str = ""
    telegram_publish_channel: str = ""
    telegram_bot_token: str = ""

    # --- НОВЫЕ НАСТРОЙКИ ДЛЯ ИИ И КАРАНТИНА ---
    groq_api_key: str = ""
    telegram_spam_topic_id: int | None = None
    # ------------------------------------------
    
    # --- ОБЛАЧНЫЙ ЧЕРНЫЙ ЯЩИК (SENTRY) ---
    sentry_dsn: str | None = None
    # ------------------------------------------

    # --- ФАЗА 2: ОЧЕРЕДИ ЗАДАЧ (REDIS) ---
    # По умолчанию подключаемся к контейнеру 'redis' из docker-compose
    redis_url: str = "redis://redis:6379/0"
    # ------------------------------------------

    telegram_sources: str = ""
    admin_user_ids: str = ""

    batch_size: int = 500
    embedding_batch_size: int = 128
    dedupe_similarity_threshold: float = 0.80
    dedupe_window_days: int = 14
    merge_max_chars: int = 1800

    spam_min_words: int = 4
    spam_max_links: int = 3
    spam_repeat_threshold: int = 3

    faiss_index_path: Path = Field(default=DATA_DIR / "faiss.index")
    analytics_path: Path = Field(default=DATA_DIR / "analytics.json")
    embedding_model_name: str = "sentence-transformers/all-MiniLM-L6-v2"

    postgres_dsn: str = "postgresql+asyncpg://news:news@postgres:5432/news"

    miniapp_base_url: str = "http://localhost:8000/miniapp"

    @property
    def sources(self) -> list[str]:
        return[item.strip() for item in self.telegram_sources.split(",") if item.strip()]

    @property
    def admin_ids(self) -> set[int]:
        return {int(item.strip()) for item in self.admin_user_ids.split(",") if item.strip()}

    @property
    def miniapp_origin(self) -> str:
        parsed = urlparse(self.miniapp_base_url)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"
        return self.miniapp_base_url


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached settings instance."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return Settings()