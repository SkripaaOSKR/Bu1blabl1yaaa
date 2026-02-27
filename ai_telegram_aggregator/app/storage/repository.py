"""Data access layer for persisted messages and embeddings."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import aiosqlite


class Repository:
    """Repository for application persistence."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    async def save_embedding(self, vector: bytes) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("INSERT INTO embeddings(vector) VALUES (?)", (vector,))
            await db.commit()
            return int(cur.lastrowid)

    async def save_faiss_mapping(self, embedding_id: int, faiss_id: int, created_at: datetime) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT OR REPLACE INTO faiss_mappings(embedding_id, faiss_id, created_at)
                VALUES (?, ?, ?)
                """,
                (embedding_id, faiss_id, created_at.isoformat()),
            )
            await db.commit()

    async def get_all_embeddings(self) -> List[tuple[int, bytes]]:
        async with aiosqlite.connect(self.db_path) as db:
            rows = await db.execute_fetchall("SELECT id, vector FROM embeddings ORDER BY id")
        return [(int(row[0]), row[1]) for row in rows]

    async def get_recent_embeddings(self, window_days: int) -> List[tuple[int, bytes, int]]:
        start = (datetime.now(timezone.utc) - timedelta(days=window_days)).isoformat()
        query = """
            SELECT m.id, e.vector, m.embedding_id
            FROM messages m
            JOIN embeddings e ON m.embedding_id = e.id
            WHERE m.created_at >= ?
            ORDER BY m.id
        """
        async with aiosqlite.connect(self.db_path) as db:
            rows = await db.execute_fetchall(query, (start,))
        return [(int(row[0]), row[1], int(row[2])) for row in rows]

    async def get_faiss_mapping_count(self) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT COUNT(*) FROM faiss_mappings")
            row = await cur.fetchone()
        return int(row[0])

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
            return int(cur.lastrowid)

    async def message_stats(self) -> dict[str, int]:
        async with aiosqlite.connect(self.db_path) as db:
            cur_total = await db.execute("SELECT COUNT(*) FROM messages")
            total = await cur_total.fetchone()
            cur_merged = await db.execute("SELECT COUNT(*) FROM messages WHERE merged_text IS NOT NULL")
            merged = await cur_merged.fetchone()
        return {"stored_messages": int(total[0]), "merged_messages": int(merged[0])}
