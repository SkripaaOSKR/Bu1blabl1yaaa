from __future__ import annotations

import json
from typing import Any
from sqlalchemy import text
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import AsyncSession


class DataService:
    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    async def log_action(self, user_id: int, action: str, payload: dict) -> None:
        """Логирование действий администратора в таблицу audit_log."""
        await self.db.execute(
            text("INSERT INTO audit_log(user_id, action, payload) VALUES (:u, :a, CAST(:p AS JSONB))"),
            {"u": user_id, "a": action, "p": json.dumps(payload)},
        )
        await self.db.commit()

    async def list_sources(self) -> list[dict]:
        """Получение списка всех каналов-источников со всей статистикой."""
        rows = await self.db.execute(text("SELECT * FROM sources ORDER BY priority, id"))
        return [dict(row._mapping) for row in rows.fetchall()]

    async def add_source(self, channel: str, priority: int, category: str | None, language: str | None, topic_id: int | None = None, title: str | None = None) -> dict:
        """
        Добавление нового источника. 
        Если канал уже существует, обновляет его приоритет, ветку (topic_id) и название (title).
        """
        row = await self.db.execute(
            text(
                """
                INSERT INTO sources(channel, priority, category, language, topic_id, title)
                VALUES (:c, :p, :cat, :lang, :tid, :title)
                ON CONFLICT(channel) DO UPDATE SET 
                    priority=EXCLUDED.priority,
                    topic_id=COALESCE(EXCLUDED.topic_id, sources.topic_id),
                    title=COALESCE(EXCLUDED.title, sources.title)
                RETURNING *
                """
            ),
            {"c": channel, "p": priority, "cat": category, "lang": language, "tid": topic_id, "title": title},
        )
        await self.db.commit()
        return dict(row.fetchone()._mapping)

    async def update_source(self, source_id: int, values: dict) -> None:
        """Частичное обновление параметров источника (включая ветку)."""
        allowed_fields = {"is_active", "priority", "category", "language", "topic_id"}
        values = {k: v for k, v in values.items() if k in allowed_fields}
        if not values:
            return
        set_parts = [f"{k}=:{k}" for k in values.keys()]
        values["id"] = source_id
        await self.db.execute(text(f"UPDATE sources SET {', '.join(set_parts)} WHERE id=:id"), values)
        await self.db.commit()

    async def remove_source(self, source_id: int) -> None:
        """Удаление источника из системы (все сообщения удалятся каскадно)."""
        await self.db.execute(text("DELETE FROM sources WHERE id=:id"), {"id": source_id})
        await self.db.commit()

    async def upsert_tag(self, name: str, allowed: bool | None = None, blocked: bool | None = None) -> None:
        """Создание или обновление статуса тега."""
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
        """Список всех тегов с количеством их использований."""
        rows = await self.db.execute(text("SELECT * FROM tags ORDER BY usage_count DESC, name"))
        return [dict(row._mapping) for row in rows.fetchall()]

    async def merge_tags(self, from_name: str, to_name: str) -> None:
        """Перенос всех сообщений с одного тега на другой и удаление старого."""
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
            text("DELETE FROM message_tags WHERE tag_id IN (SELECT id FROM tags WHERE name=:from_name)"),
            {"from_name": from_name},
        )
        await self.db.execute(text("DELETE FROM tags WHERE name=:from_name"), {"from_name": from_name})
        await self.db.commit()

    async def list_messages(self, source_id: int | None, tag: str | None, date_from: datetime | None, date_to: datetime | None, limit: int = 200) -> list[dict]:
        """
        Получение списка сообщений. 
        ВАЖНО: используется CAST для стабильной работы с NULL параметрами в asyncpg.
        """
        query = """
        SELECT m.* FROM messages m
        LEFT JOIN message_tags mt ON mt.message_id = m.id
        LEFT JOIN tags t ON t.id = mt.tag_id
        WHERE (CAST(:source_id AS BIGINT) IS NULL OR m.source_id=CAST(:source_id AS BIGINT))
          AND (CAST(:tag AS TEXT) IS NULL OR t.name=CAST(:tag AS TEXT))
          AND (CAST(:date_from AS TIMESTAMPTZ) IS NULL OR m.created_at >= CAST(:date_from AS TIMESTAMPTZ))
          AND (CAST(:date_to AS TIMESTAMPTZ) IS NULL OR m.created_at <= CAST(:date_to AS TIMESTAMPTZ))
        ORDER BY m.created_at DESC
        LIMIT :limit
        """
        rows = await self.db.execute(
            text(query), 
            {"source_id": source_id, "tag": tag, "date_from": date_from, "date_to": date_to, "limit": limit}
        )
        return [dict(row._mapping) for row in rows.fetchall()]

    async def delete_message(self, message_id: int) -> None:
        """Удаление конкретного сообщения."""
        await self.db.execute(text("DELETE FROM messages WHERE id=:id"), {"id": message_id})
        await self.db.commit()

    async def mark_published(self, message_id: int) -> None:
        """Ручная пометка сообщения как опубликованного."""
        await self.db.execute(text("UPDATE messages SET is_published=TRUE WHERE id=:id"), {"id": message_id})
        await self.db.commit()

    async def get_processing_state(self) -> dict:
        """Состояние последней работы воркера."""
        row = await self.db.execute(text("SELECT * FROM processing_state WHERE id=1"))
        found = row.fetchone()
        return dict(found._mapping) if found else {}

    async def set_processing_state(self, status: str, duration: float, count: int, timestamp: datetime | None) -> None:
        """Обновление глобального чекпоинта и статуса запуска."""
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
        """Загрузка глобальных настроек из БД."""
        row = await self.db.execute(text("SELECT * FROM settings WHERE id=1"))
        found = row.fetchone()
        return dict(found._mapping) if found else {}

    async def update_settings(self, values: dict) -> None:
        """Обновление настроек дедупликации и слияния."""
        allowed_fields = {"dedupe_threshold", "merge_enabled", "batch_size", "dedupe_window_days", "max_merge_chars"}
        values = {k: v for k, v in values.items() if k in allowed_fields}
        if not values:
            return
        set_parts = [f"{k}=:{k}" for k in values.keys()]
        values["updated_at"] = datetime.now(timezone.utc)
        sql = f"UPDATE settings SET {', '.join(set_parts)}, updated_at=:updated_at WHERE id=1"
        await self.db.execute(text(sql), values)
        await self.db.commit()
    async def list_spam_keywords(self) -> list[dict]:
        """Получить список всех стоп-слов."""
        rows = await self.db.execute(text("SELECT * FROM spam_keywords ORDER BY word"))
        return [dict(row._mapping) for row in rows.fetchall()]

    async def add_spam_keyword(self, word: str) -> dict:
        """Добавить новое стоп-слово."""
        row = await self.db.execute(
            text("INSERT INTO spam_keywords(word) VALUES (:w) ON CONFLICT (word) DO UPDATE SET is_active=TRUE RETURNING *"),
            {"w": word.lower().strip()}
        )
        await self.db.commit()
        return dict(row.fetchone()._mapping)

    async def toggle_spam_keyword(self, keyword_id: int, is_active: bool) -> None:
        """Включить или выключить стоп-слово."""
        await self.db.execute(
            text("UPDATE spam_keywords SET is_active=:a WHERE id=:id"),
            {"a": is_active, "id": keyword_id}
        )
        await self.db.commit()

    async def remove_spam_keyword(self, keyword_id: int) -> None:
        """Удалить слово из фильтра совсем."""
        await self.db.execute(text("DELETE FROM spam_keywords WHERE id=:id"), {"id": keyword_id})
        await self.db.commit()

    async def get_active_spam_keywords(self) -> set[str]:
        """Получить только активные слова (для воркера)."""
        rows = await self.db.execute(text("SELECT word FROM spam_keywords WHERE is_active=TRUE"))
        return {row.word for row in rows.fetchall()}
        
    async def mark_confirmed_spam(self, message_id: int) -> None:
        """Помечает сообщение как эталонный спам для обучения фильтра."""
        await self.db.execute(
            text("UPDATE messages SET is_confirmed_spam = TRUE, is_published = FALSE WHERE id = :id"),
            {"id": message_id}
        )
        # Увеличиваем счетчик спама у источника
        await self.db.execute(
            text("""
                UPDATE sources SET spam_count = spam_count + 1 
                WHERE id = (SELECT source_id FROM messages WHERE id = :id)
            """),
            {"id": message_id}
        )
        await self.db.commit()

    async def analytics_daily(self) -> list[dict]:
        """Агрегированная статистика по дням для дашборда."""
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