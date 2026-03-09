from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

# Полная SQL-схема со всеми необходимыми полями
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS sources (
    id BIGSERIAL PRIMARY KEY,
    channel TEXT UNIQUE NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    priority INTEGER NOT NULL DEFAULT 100,
    category TEXT,
    language TEXT,
    topic_id BIGINT,                -- ID ветки (темы) в супергруппе
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
    text TEXT NOT NULL,             -- Чистый текст без форматирования
    fmt_text TEXT,                  -- Красивый текст с HTML-разметкой
    merged_text TEXT,               -- Текст после слияния нескольких постов
    text_hash TEXT,                 -- Быстрый хэш текста (Уровень 1 дедупликации)
    created_at TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    embedding_id BIGINT REFERENCES embeddings(id) ON DELETE SET NULL,
    language TEXT,
    is_duplicate BOOLEAN NOT NULL DEFAULT FALSE,
    is_published BOOLEAN NOT NULL DEFAULT FALSE,
    similarity_score DOUBLE PRECISION,
    media_group_id TEXT,            -- ID группы медиа (для альбомов)
    media_type TEXT,                -- Тип медиа: photo, video, document
    original_message_id BIGINT,     -- ID оригинального сообщения в Telegram
    published_message_id BIGINT     -- ID опубликованного сообщения в нашем канале (для Reply)
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
    ai_prompt TEXT,                 -- ПРОМПТ ДЛЯ ИИ
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS audit_log (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    action TEXT NOT NULL,
    payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS spam_keywords (
    id SERIAL PRIMARY KEY,
    word TEXT UNIQUE NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Индексы для ускорения работы системы
CREATE INDEX IF NOT EXISTS idx_sources_active_priority ON sources(is_active, priority);
CREATE INDEX IF NOT EXISTS idx_messages_source_created ON messages(source_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_created ON messages(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_duplicate ON messages(is_duplicate);

-- Partial index для быстрых запросов только опубликованных сообщений
CREATE INDEX IF NOT EXISTS idx_messages_published_true ON messages(id) WHERE is_published = TRUE;

CREATE INDEX IF NOT EXISTS idx_tags_name ON tags(name);
CREATE INDEX IF NOT EXISTS idx_message_tags_tag ON message_tags(tag_id);
CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_log(created_at DESC);

-- Индекс для молниеносного поиска точных дубликатов по хэшу
CREATE INDEX IF NOT EXISTS idx_messages_text_hash ON messages(text_hash);

-- Индексы для внешних ключей и частых выборок
CREATE INDEX IF NOT EXISTS idx_messages_embedding_id ON messages(embedding_id);
CREATE INDEX IF NOT EXISTS idx_messages_original_message_id ON messages(original_message_id);
"""


async def init_postgres_schema(engine: AsyncEngine) -> None:
    """Инициализация БД и автоматическая накатка миграций."""
    async with engine.begin() as conn:
        # 1. Создаем базовые таблицы, если их нет
        for statement in [s.strip() for s in SCHEMA_SQL.split(";") if s.strip()]:
            if statement: # Защита от пустых строк после сплита
                await conn.execute(text(statement))
        
        # 2. АВТО-МИГРАЦИЯ: Добавляем новые колонки в существующие таблицы
        await conn.execute(text("ALTER TABLE messages ADD COLUMN IF NOT EXISTS fmt_text TEXT;"))
        await conn.execute(text("ALTER TABLE messages ADD COLUMN IF NOT EXISTS media_type TEXT;"))
        await conn.execute(text("ALTER TABLE messages ADD COLUMN IF NOT EXISTS original_message_id BIGINT;"))
        await conn.execute(text("ALTER TABLE messages ADD COLUMN IF NOT EXISTS published_message_id BIGINT;"))
        await conn.execute(text("ALTER TABLE messages ADD COLUMN IF NOT EXISTS is_confirmed_spam BOOLEAN NOT NULL DEFAULT FALSE;"))
        await conn.execute(text("ALTER TABLE messages ADD COLUMN IF NOT EXISTS text_hash TEXT;"))
        
        # 3. АВТО-МИГРАЦИЯ: Добавляем topic_id в таблицу sources для работы с ветками
        await conn.execute(text("ALTER TABLE sources ADD COLUMN IF NOT EXISTS topic_id BIGINT;"))
        
        # 4. АВТО-МИГРАЦИЯ: Добавляем ai_prompt в settings
        await conn.execute(text("ALTER TABLE settings ADD COLUMN IF NOT EXISTS ai_prompt TEXT;"))
        
        await conn.execute(text("CREATE TABLE IF NOT EXISTS spam_keywords (id SERIAL PRIMARY KEY, word TEXT UNIQUE NOT NULL, is_active BOOLEAN NOT NULL DEFAULT TRUE, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW());"))
        
        # Создаем новые индексы (для уже существующих баз данных)
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_messages_text_hash ON messages(text_hash);"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_messages_embedding_id ON messages(embedding_id);"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_messages_original_message_id ON messages(original_message_id);"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_messages_published_true ON messages(id) WHERE is_published = TRUE;"))

        # 5. Инициализация системных строк, если они отсутствуют
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