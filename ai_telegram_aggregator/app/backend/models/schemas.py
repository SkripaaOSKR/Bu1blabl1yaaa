from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class SourceCreate(BaseModel):
    channel: str
    priority: int = 100
    category: str | None = None
    language: str | None = None


class SourceUpdate(BaseModel):
    is_active: bool | None = None
    priority: int | None = None
    category: str | None = None
    language: str | None = None


class SourceOut(BaseModel):
    id: int
    channel: str
    is_active: bool
    priority: int
    category: str | None
    language: str | None
    total_messages: int
    published_messages: int
    duplicate_count: int
    spam_count: int


class TagToggle(BaseModel):
    name: str
    is_allowed: bool | None = None
    is_blocked: bool | None = None


class TagMerge(BaseModel):
    from_name: str
    to_name: str


class MessageOut(BaseModel):
    id: int
    source_id: int
    text: str
    merged_text: str | None
    created_at: datetime
    processed_at: datetime
    language: str | None
    is_duplicate: bool
    is_published: bool
    similarity_score: float | None


class SearchRequest(BaseModel):
    query: str
    limit: int = Field(default=20, le=100)
    source_id: int | None = None
    tag: str | None = None


class ProcessingRunRequest(BaseModel):
    hours: int | None = None


class SettingsUpdate(BaseModel):
    dedupe_threshold: float | None = None
    merge_enabled: bool | None = None
    batch_size: int | None = None
    dedupe_window_days: int | None = None
    max_merge_chars: int | None = None
