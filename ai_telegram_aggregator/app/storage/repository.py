"""Data access layer for persisted messages and embeddings."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import aiosqlite


class Repository:
    """Repository for application persistence."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._db: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        """Open a persistent SQLite connection if not yet connected."""
        if self._db is not None:
            return
        db = await aiosqlite.connect(self.db_path)
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")
        await db.execute("PRAGMA temp_store=MEMORY")
        self._db = db

    async def close(self) -> None:
        """Close persistent SQLite connection."""
        if self._db is None:
            return
        await self._db.close()
        self._db = None

    async def _get_db(self) -> aiosqlite.Connection:
        if self._db is None:
            await self.connect()
        assert self._db is not None
        return self._db

    async def get_state(self, key: str) -> str | None:
        db = await self._get_db()
        cur = await db.execute("SELECT value FROM state WHERE key = ?", (key,))
        row = await cur.fetchone()
        return None if row is None else str(row[0])

    async def set_state(self, key: str, value: str) -> None:
        db = await self._get_db()
        await db.execute(
            """
            INSERT INTO state(key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )
        await db.commit()

    async def save_embedding(self, vector: bytes) -> int:
        db = await self._get_db()
        cur = await db.execute("INSERT INTO embeddings(vector) VALUES (?)", (vector,))
        await db.commit()
        return int(cur.lastrowid)

    async def save_faiss_mapping(self, embedding_id: int, faiss_id: int, created_at: datetime) -> None:
        db = await self._get_db()
        await db.execute(
            """
            INSERT OR REPLACE INTO faiss_mappings(embedding_id, faiss_id, created_at)
            VALUES (?, ?, ?)
            """,
            (embedding_id, embedding_id if faiss_id is None else faiss_id, created_at.isoformat()),
        )
        await db.commit()

    async def save_processed_message(
        self,
        *,
        vector: bytes,
        faiss_id: int | None,
        text: str,
        merged_text: str | None,
        created_at: datetime,
        tags: list[str],
        language: str,
        sources: list[str],
        media_group_id: str | None,
    ) -> int:
        """Atomically persist embedding, FAISS mapping, and message row."""
        db = await self._get_db()
        try:
            await db.execute("BEGIN")

            cur_embedding = await db.execute("INSERT INTO embeddings(vector) VALUES (?)", (vector,))
            embedding_id = int(cur_embedding.lastrowid)

            await db.execute(
                """
                INSERT OR REPLACE INTO faiss_mappings(embedding_id, faiss_id, created_at)
                VALUES (?, ?, ?)
                """,
                (embedding_id, embedding_id if faiss_id is None else faiss_id, created_at.isoformat()),
            )

            await db.execute(
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
            return embedding_id
        except Exception:
            await db.rollback()
            raise

    async def get_all_embeddings(self) -> list[tuple[int, bytes]]:
        db = await self._get_db()
        rows = await db.execute_fetchall("SELECT id, vector FROM embeddings ORDER BY id")
        return [(int(row[0]), row[1]) for row in rows]

    async def get_recent_embeddings(self, window_days: int) -> list[tuple[int, bytes, int]]:
        start = (datetime.now(timezone.utc) - timedelta(days=window_days)).isoformat()
        query = """
            SELECT m.id, e.vector, m.embedding_id
            FROM messages m
            JOIN embeddings e ON m.embedding_id = e.id
            WHERE m.created_at >= ?
            ORDER BY m.id
        """
        db = await self._get_db()
        rows = await db.execute_fetchall(query, (start,))
        return [(int(row[0]), row[1], int(row[2])) for row in rows]

    async def get_faiss_mapping_count(self) -> int:
        db = await self._get_db()
        cur = await db.execute("SELECT COUNT(*) FROM faiss_mappings")
        row = await cur.fetchone()
        assert row is not None
        return int(row[0])

    async def save_message(
        self,
        text: str,
        created_at: datetime,
        embedding_id: int | None,
        tags: list[str],
        language: str,
        sources: list[str],
        media_group_id: str | None,
        merged_text: str | None = None,
    ) -> int:
        db = await self._get_db()
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
        db = await self._get_db()
        cur_total = await db.execute("SELECT COUNT(*) FROM messages")
        total = await cur_total.fetchone()
        cur_merged = await db.execute("SELECT COUNT(*) FROM messages WHERE merged_text IS NOT NULL")
        merged = await cur_merged.fetchone()
        assert total is not None
        assert merged is not None
        return {"stored_messages": int(total[0]), "merged_messages": int(merged[0])}
