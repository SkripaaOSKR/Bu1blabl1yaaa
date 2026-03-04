from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS sources (
    id BIGSERIAL PRIMARY KEY,
    channel TEXT UNIQUE NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    priority INTEGER NOT NULL DEFAULT 100,
    category TEXT,
    language TEXT,
    added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_scan_at TIMESTAMPTZ,
    total_messages BIGINT NOT NULL DEFAULT 0,
    published_messages BIGINT NOT NULL DEFAULT 0,
    duplicate_count BIGINT NOT NULL DEFAULT 0,
    spam_count BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS embeddings (
    id BIGSERIAL PRIMARY KEY,
    vector BYTEA NOT NULL
);

CREATE TABLE IF NOT EXISTS messages (
    id BIGSERIAL PRIMARY KEY,
    source_id BIGINT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    text TEXT NOT NULL,
    merged_text TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    embedding_id BIGINT REFERENCES embeddings(id) ON DELETE SET NULL,
    language TEXT,
    is_duplicate BOOLEAN NOT NULL DEFAULT FALSE,
    is_published BOOLEAN NOT NULL DEFAULT FALSE,
    similarity_score DOUBLE PRECISION,
    media_group_id TEXT
);

CREATE TABLE IF NOT EXISTS tags (
    id BIGSERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    is_allowed BOOLEAN NOT NULL DEFAULT TRUE,
    is_blocked BOOLEAN NOT NULL DEFAULT FALSE,
    usage_count BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS message_tags (
    message_id BIGINT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    tag_id BIGINT NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (message_id, tag_id)
);

CREATE TABLE IF NOT EXISTS processing_state (
    id SMALLINT PRIMARY KEY DEFAULT 1,
    last_processed_timestamp TIMESTAMPTZ,
    last_run_status TEXT,
    last_run_duration DOUBLE PRECISION,
    last_run_count BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS settings (
    id SMALLINT PRIMARY KEY DEFAULT 1,
    dedupe_threshold DOUBLE PRECISION NOT NULL DEFAULT 0.8,
    merge_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    batch_size INTEGER NOT NULL DEFAULT 500,
    dedupe_window_days INTEGER NOT NULL DEFAULT 14,
    max_merge_chars INTEGER NOT NULL DEFAULT 1800,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS audit_log (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    action TEXT NOT NULL,
    payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sources_active_priority ON sources(is_active, priority);
CREATE INDEX IF NOT EXISTS idx_messages_source_created ON messages(source_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_created ON messages(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_duplicate ON messages(is_duplicate);
CREATE INDEX IF NOT EXISTS idx_messages_published ON messages(is_published);
CREATE INDEX IF NOT EXISTS idx_tags_name ON tags(name);
CREATE INDEX IF NOT EXISTS idx_message_tags_tag ON message_tags(tag_id);
CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_log(created_at DESC);
"""


async def init_postgres_schema(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        for statement in [s.strip() for s in SCHEMA_SQL.split(";") if s.strip()]:
            await conn.execute(text(statement))
        await conn.execute(
            text(
                """
                INSERT INTO processing_state(id) VALUES (1)
                ON CONFLICT (id) DO NOTHING;
                """
            )
        )
        await conn.execute(
            text(
                """
                INSERT INTO settings(id) VALUES (1)
                ON CONFLICT (id) DO NOTHING;
                """
            )
        )
