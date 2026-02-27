"""Data access layer for persisted messages and embeddings."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional

import aiosqlite


@dataclass(slots=True)
class StoredMessage:
    id: int
    text: str
    merged_text: str | None
    created_at: str
    embedding_id: int | None
    tags: str | None
    language: str | None
    sources: str | None
    media_group_id: str | None


class Repository:
    """Repository for application persistence."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    async def save_embedding(self, vector: bytes) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("INSERT INTO embeddings(vector) VALUES (?)", (vector,))
            await db.commit()
            return cur.lastrowid

    async def save_message(
        self,
        text: str,
        created_at: datetime,
        embedding_id: Optional[int],
        tags: list[str],
        language: str,
        sources: list[str],
        media_group_id: Optional[str],
        merged_text: Optional[str] = None,
    ) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                """
                INSERT INTO messages(text, merged_text, created_at, embedding_id, tags, language, sources, media_group_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    text,
                    merged_text,
                    created_at.isoformat(),
                    embedding_id,
                    json.dumps(tags, ensure_ascii=False),
                    language,
                    json.dumps(sources, ensure_ascii=False),
                    media_group_id,
                ),
            )
            await db.commit()
            return cur.lastrowid

    async def get_recent_embeddings(self, window_days: int) -> List[tuple[int, str, int]]:
        start = (datetime.now(timezone.utc) - timedelta(days=window_days)).isoformat()
        query = """
            SELECT m.id, e.vector, m.embedding_id
            FROM messages m
            JOIN embeddings e ON m.embedding_id = e.id
            WHERE m.created_at >= ?
        """
        async with aiosqlite.connect(self.db_path) as db:
            rows = await db.execute_fetchall(query, (start,))
        return [(row[0], row[1], row[2]) for row in rows]

    async def message_stats(self) -> dict[str, int]:
        async with aiosqlite.connect(self.db_path) as db:
            cur_total = await db.execute("SELECT COUNT(*) FROM messages")
            total = await cur_total.fetchone()
            cur_merged = await db.execute("SELECT COUNT(*) FROM messages WHERE merged_text IS NOT NULL")
            merged = await cur_merged.fetchone()
        return {"stored_messages": total[0], "merged_messages": merged[0]}
