"""Async DB initialization and helpers."""
from __future__ import annotations

import aiosqlite

SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    text TEXT NOT NULL,
    merged_text TEXT,
    created_at TEXT NOT NULL,
    embedding_id INTEGER,
    tags TEXT,
    language TEXT,
    sources TEXT,
    media_group_id TEXT
);

CREATE TABLE IF NOT EXISTS embeddings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    vector BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS media_hashes (
    message_id INTEGER NOT NULL,
    hash TEXT NOT NULL,
    FOREIGN KEY(message_id) REFERENCES messages(id)
);

CREATE TABLE IF NOT EXISTS faiss_mappings (
    embedding_id INTEGER PRIMARY KEY,
    faiss_id INTEGER NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    FOREIGN KEY(embedding_id) REFERENCES embeddings(id)
);

CREATE TABLE IF NOT EXISTS state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_messages_created_at ON messages(created_at);
CREATE INDEX IF NOT EXISTS idx_faiss_mappings_created_at ON faiss_mappings(created_at);
"""


async def init_db(db_path: str) -> None:
    """Create database tables if missing."""
    async with aiosqlite.connect(db_path) as db:
        await db.executescript(SCHEMA)
        await db.commit()
