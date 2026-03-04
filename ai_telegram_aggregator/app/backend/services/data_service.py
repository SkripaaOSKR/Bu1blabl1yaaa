from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


class DataService:
    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    async def log_action(self, user_id: int, action: str, payload: dict) -> None:
        await self.db.execute(
            text("INSERT INTO audit_log(user_id, action, payload) VALUES (:u, :a, CAST(:p AS JSONB))"),
            {"u": user_id, "a": action, "p": str(payload).replace("'", '"')},
        )
        await self.db.commit()

    async def list_sources(self) -> list[dict]:
        rows = await self.db.execute(text("SELECT * FROM sources ORDER BY priority, id"))
        return [dict(row._mapping) for row in rows.fetchall()]

    async def add_source(self, channel: str, priority: int, category: str | None, language: str | None) -> dict:
        row = await self.db.execute(
            text(
                """
                INSERT INTO sources(channel, priority, category, language)
                VALUES (:c, :p, :cat, :lang)
                ON CONFLICT(channel) DO UPDATE SET priority=EXCLUDED.priority
                RETURNING *
                """
            ),
            {"c": channel, "p": priority, "cat": category, "lang": language},
        )
        await self.db.commit()
        return dict(row.fetchone()._mapping)

    async def update_source(self, source_id: int, values: dict) -> None:
        allowed_fields = {"is_active", "priority", "category", "language"}
        values = {k: v for k, v in values.items() if k in allowed_fields}
        if not values:
            return
        set_parts = [f"{k}=:{k}" for k in values.keys()]
        values["id"] = source_id
        await self.db.execute(text(f"UPDATE sources SET {', '.join(set_parts)} WHERE id=:id"), values)
        await self.db.commit()

    async def remove_source(self, source_id: int) -> None:
        await self.db.execute(text("DELETE FROM sources WHERE id=:id"), {"id": source_id})
        await self.db.commit()

    async def upsert_tag(self, name: str, allowed: bool | None = None, blocked: bool | None = None) -> None:
        await self.db.execute(
            text(
                """
                INSERT INTO tags(name, is_allowed, is_blocked) VALUES (:n, COALESCE(:a, TRUE), COALESCE(:b, FALSE))
                ON CONFLICT(name) DO UPDATE SET
                  is_allowed = COALESCE(:a, tags.is_allowed),
                  is_blocked = COALESCE(:b, tags.is_blocked)
                """
            ),
            {"n": name, "a": allowed, "b": blocked},
        )
        await self.db.commit()

    async def list_tags(self) -> list[dict]:
        rows = await self.db.execute(text("SELECT * FROM tags ORDER BY usage_count DESC, name"))
        return [dict(row._mapping) for row in rows.fetchall()]

    async def merge_tags(self, from_name: str, to_name: str) -> None:
        await self.db.execute(text("INSERT INTO tags(name) VALUES (:n) ON CONFLICT(name) DO NOTHING"), {"n": to_name})
        await self.db.execute(
            text(
                """
                INSERT INTO message_tags(message_id, tag_id)
                SELECT mt.message_id, t_to.id
                FROM message_tags mt
                JOIN tags t_from ON mt.tag_id=t_from.id
                JOIN tags t_to ON t_to.name=:to_name
                WHERE t_from.name=:from_name
                ON CONFLICT DO NOTHING
                """
            ),
            {"to_name": to_name, "from_name": from_name},
        )
        await self.db.execute(
            text(
                "DELETE FROM message_tags WHERE tag_id IN (SELECT id FROM tags WHERE name=:from_name)"
            ),
            {"from_name": from_name},
        )
        await self.db.execute(text("DELETE FROM tags WHERE name=:from_name"), {"from_name": from_name})
        await self.db.commit()

    async def list_messages(self, source_id: int | None, tag: str | None, date_from: datetime | None, date_to: datetime | None, limit: int = 200) -> list[dict]:
        query = """
        SELECT m.* FROM messages m
        LEFT JOIN message_tags mt ON mt.message_id = m.id
        LEFT JOIN tags t ON t.id = mt.tag_id
        WHERE (:source_id IS NULL OR m.source_id=:source_id)
          AND (:tag IS NULL OR t.name=:tag)
          AND (:date_from IS NULL OR m.created_at >= :date_from)
          AND (:date_to IS NULL OR m.created_at <= :date_to)
        ORDER BY m.created_at DESC
        LIMIT :limit
        """
        rows = await self.db.execute(text(query), {"source_id": source_id, "tag": tag, "date_from": date_from, "date_to": date_to, "limit": limit})
        return [dict(row._mapping) for row in rows.fetchall()]

    async def delete_message(self, message_id: int) -> None:
        await self.db.execute(text("DELETE FROM messages WHERE id=:id"), {"id": message_id})
        await self.db.commit()

    async def mark_published(self, message_id: int) -> None:
        await self.db.execute(text("UPDATE messages SET is_published=TRUE WHERE id=:id"), {"id": message_id})
        await self.db.commit()

    async def get_processing_state(self) -> dict:
        row = await self.db.execute(text("SELECT * FROM processing_state WHERE id=1"))
        found = row.fetchone()
        return dict(found._mapping) if found else {}

    async def set_processing_state(self, status: str, duration: float, count: int, timestamp: datetime | None) -> None:
        await self.db.execute(
            text(
                """
                UPDATE processing_state
                SET last_run_status=:s,
                    last_run_duration=:d,
                    last_run_count=:c,
                    last_processed_timestamp=COALESCE(:ts, last_processed_timestamp),
                    updated_at=:updated
                WHERE id=1
                """
            ),
            {"s": status, "d": duration, "c": count, "ts": timestamp, "updated": datetime.now(timezone.utc)},
        )
        await self.db.commit()

    async def get_settings(self) -> dict:
        row = await self.db.execute(text("SELECT * FROM settings WHERE id=1"))
        found = row.fetchone()
        return dict(found._mapping) if found else {}

    async def update_settings(self, values: dict) -> None:
        allowed_fields = {"dedupe_threshold", "merge_enabled", "batch_size", "dedupe_window_days", "max_merge_chars"}
        values = {k: v for k, v in values.items() if k in allowed_fields}
        if not values:
            return
        set_parts = [f"{k}=:{k}" for k in values.keys()]
        values["updated_at"] = datetime.now(timezone.utc)
        sql = f"UPDATE settings SET {', '.join(set_parts)}, updated_at=:updated_at WHERE id=1"
        await self.db.execute(text(sql), values)
        await self.db.commit()

    async def analytics_daily(self) -> list[dict]:
        rows = await self.db.execute(
            text(
                """
                SELECT date_trunc('day', created_at) AS day,
                       count(*) AS total,
                       sum(CASE WHEN is_duplicate THEN 1 ELSE 0 END) AS duplicates,
                       sum(CASE WHEN is_published THEN 1 ELSE 0 END) AS published
                FROM messages
                GROUP BY 1
                ORDER BY 1 DESC
                LIMIT 30
                """
            )
        )
        return [dict(row._mapping) for row in rows.fetchall()]
