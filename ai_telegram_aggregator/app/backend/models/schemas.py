from __future__ import annotations

from datetime import datetime
from pydantic import BaseModel, Field, ConfigDict

# --- ИСТОЧНИКИ (SOURCES) ---

class SourceCreate(BaseModel):
    """Схема для добавления нового канала-источника."""
    channel: str
    title: str | None = None
    priority: int = 100
    category: str | None = None
    language: str | None = None
    topic_id: int | None = None     # ID ветки (темы) в супергруппе

class SourceUpdate(BaseModel):
    """Схема для частичного обновления настроек канала."""
    is_active: bool | None = None
    priority: int | None = None
    category: str | None = None
    language: str | None = None
    topic_id: int | None = None     # Позволяет изменить ветку (куда постить новости)
    title: str | None = None

class SourceOut(BaseModel):
    """Схема для отображения полной информации о канале в админке."""
    model_config = ConfigDict(from_attributes=True)

    id: int
    channel: str
    is_active: bool
    priority: int
    category: str | None
    language: str | None
    topic_id: int | None            # Чтобы админка видела привязанную ветку
    added_at: datetime
    total_messages: int
    published_messages: int
    duplicate_count: int
    spam_count: int

# --- ТЕГИ (TAGS) ---

class TagToggle(BaseModel):
    """Схема для управления состоянием тега (разрешить/заблокировать)."""
    name: str
    is_allowed: bool | None = None
    is_blocked: bool | None = None

class TagMerge(BaseModel):
    """Схема для объединения двух тегов в один."""
    from_name: str
    to_name: str

# --- СООБЩЕНИЯ (MESSAGES) ---

class MessageOut(BaseModel):
    """Схема для отображения постов в базе данных (Mini App)."""
    model_config = ConfigDict(from_attributes=True)

    id: int
    source_id: int
    text: str                       # Чистый текст
    fmt_text: str | None            # Текст с HTML разметкой
    merged_text: str | None         # Результат работы ИИ / Слияния
    created_at: datetime
    processed_at: datetime | None   # Может быть None, если воркер еще не обработал
    language: str | None
    is_duplicate: bool
    is_published: bool
    similarity_score: float | None
    media_group_id: str | None      # Для альбомов
    media_type: str | None          # photo, video, document, voice
    original_message_id: int | None # ID сообщения в Telegram

# --- ЗАПРОСЫ (REQUESTS) ---

class SearchRequest(BaseModel):
    """Схема для выполнения семантического поиска через FAISS."""
    query: str
    limit: int = Field(default=20, le=100)
    source_id: int | None = None
    tag: str | None = None

class ProcessingRunRequest(BaseModel):
    """Схема для запуска воркера вручную на X часов назад."""
    hours: int | None = None

# --- НАСТРОЙКИ (SETTINGS) ---

class SettingsOut(BaseModel):
    """Схема для отправки настроек на фронтенд (Mini App)."""
    model_config = ConfigDict(from_attributes=True)

    dedupe_threshold: float
    merge_enabled: bool
    batch_size: int
    dedupe_window_days: int
    max_merge_chars: int
    ai_prompt: str | None           # Добавлено для фронтенда

class SettingsUpdate(BaseModel):
    """Схема для обновления глобальных настроек системы."""
    dedupe_threshold: float | None = None
    merge_enabled: bool | None = None
    batch_size: int | None = None
    dedupe_window_days: int | None = None
    max_merge_chars: int | None = None
    ai_prompt: str | None = None    # Добавлено для фронтенда
    
class SpamKeywordCreate(BaseModel):
    word: str

class SpamKeywordUpdate(BaseModel):
    is_active: bool

class SpamKeywordOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    word: str
    is_active: bool