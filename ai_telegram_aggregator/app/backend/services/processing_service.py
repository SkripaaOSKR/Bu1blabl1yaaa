from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import numpy as np
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.collector.telegram_client import CollectedMessage, TelegramCollector
from app.config import get_settings
from app.processing.duplicate_engine import DuplicateEngine
from app.processing.merge_engine import MergeEngine
from app.processing.preprocessor import Preprocessor
from app.processing.spam_filter import SpamFilter
from app.processing.tagger import TagGenerator
from app.semantic.embedding_model import EmbeddingModel
from app.semantic.faiss_index import FaissStore

logger = logging.getLogger(__name__)


class ProcessingService:
    def __init__(self, db: AsyncSession) -> None:
        self.db = db
        self.settings = get_settings()
        self.preprocessor = Preprocessor()
        self.spam_filter = SpamFilter(
            min_words=self.settings.spam_min_words,
            max_links=self.settings.spam_max_links,
            repeat_threshold=self.settings.spam_repeat_threshold,
        )
        self.embedding_model = EmbeddingModel(self.settings.embedding_model_name, self.settings.embedding_batch_size)
        self.faiss = FaissStore(self.settings.faiss_index_path)
        self.dedupe = DuplicateEngine(self.settings.dedupe_similarity_threshold, self.faiss)
        self.merger = MergeEngine(self.settings.merge_max_chars)
        self.tagger = TagGenerator()
        self.cancelled = False

    async def _load_source_map(self) -> dict[str, int]:
        rows = await self.db.execute(text("SELECT id, channel FROM sources WHERE is_active=TRUE ORDER BY priority,id"))
        return {str(r.channel): int(r.id) for r in rows.fetchall()}

    async def _get_checkpoint(self, hours: int | None) -> datetime:
        row = await self.db.execute(text("SELECT last_processed_timestamp FROM processing_state WHERE id=1"))
        found = row.fetchone()
        if found and found.last_processed_timestamp:
            return found.last_processed_timestamp.astimezone(timezone.utc)
        return datetime.now(timezone.utc) - timedelta(hours=hours or 24)

    async def _save_processed(self, source_id: int, msg: CollectedMessage, cleaned: str, language: str, vector: np.ndarray, is_dup: bool, score: float, tags: list[str]) -> tuple[int, bool]:
        try:
            await self.db.execute(text("BEGIN"))
            emb_row = await self.db.execute(
                text("INSERT INTO embeddings(vector) VALUES (:v) RETURNING id"),
                {"v": vector.tobytes()},
            )
            emb_id = int(emb_row.fetchone().id)
            await self.db.execute(
                text(
                    """
                    INSERT INTO messages(source_id,text,merged_text,created_at,processed_at,embedding_id,language,is_duplicate,is_published,similarity_score,media_group_id)
                    VALUES (:sid,:text,NULL,:created,:processed,:eid,:lang,:dup,FALSE,:score,:mg)
                    RETURNING id
                    """
                ),
                {
                    "sid": source_id,
                    "text": msg.text,
                    "created": msg.created_at,
                    "processed": datetime.now(timezone.utc),
                    "eid": emb_id,
                    "lang": language,
                    "dup": is_dup,
                    "score": score,
                    "mg": msg.media_group_id,
                },
            )
            await self.db.commit()
        except Exception:
            await self.db.rollback()
            raise

        try:
            self.faiss.add_with_ids(vector.reshape(1, -1), np.asarray([emb_id], dtype=np.int64))
        except Exception:
            logger.exception("Failed to update FAISS for embedding id=%s", emb_id)
        for tag in tags:
            await self.db.execute(text("INSERT INTO tags(name) VALUES (:n) ON CONFLICT(name) DO NOTHING"), {"n": tag})
            await self.db.execute(text("UPDATE tags SET usage_count=usage_count+1 WHERE name=:n"), {"n": tag})
            await self.db.execute(
                text(
                    """
                    INSERT INTO message_tags(message_id, tag_id)
                    SELECT m.id, t.id FROM messages m, tags t
                    WHERE m.embedding_id=:eid AND t.name=:n
                    ON CONFLICT DO NOTHING
                    """
                ),
                {"eid": emb_id, "n": tag},
            )
        await self.db.execute(text("UPDATE sources SET total_messages=total_messages+1, duplicate_count=duplicate_count + :dup, last_scan_at=NOW() WHERE id=:id"), {"id": source_id, "dup": 1 if is_dup else 0})
        await self.db.commit()
        return emb_id, is_dup

    async def _process_batch(self, batch: list[tuple[int, CollectedMessage]]) -> tuple[dict[str, list[str]], dict[str, list[int]], datetime | None, int]:
        grouped: dict[str, list[str]] = defaultdict(list)
        grouped_message_ids: dict[str, list[int]] = defaultdict(list)
        newest: datetime | None = None
        processed_count = 0
        prepared: list[tuple[int, CollectedMessage, str, str]] = []

        for source_id, msg in batch:
            if newest is None or msg.created_at > newest:
                newest = msg.created_at
            processed = self.preprocessor.run(msg.text)
            if not processed.cleaned or self.spam_filter.is_spam(processed.cleaned):
                await self.db.execute(text("UPDATE sources SET spam_count=spam_count+1 WHERE id=:id"), {"id": source_id})
                continue
            prepared.append((source_id, msg, processed.cleaned, processed.language))
        await self.db.commit()

        if not prepared:
            return grouped, grouped_message_ids, newest, processed_count

        vectors = await asyncio.to_thread(self.embedding_model.encode, [x[2] for x in prepared])
        for idx, (source_id, msg, cleaned, language) in enumerate(prepared):
            if self.cancelled:
                break
            vector = vectors[idx]
            is_dup, score = self.dedupe.find_duplicates(vector)
            tags = self.tagger.generate(cleaned)
            emb_id, _ = await self._save_processed(source_id, msg, cleaned, language, vector, is_dup, score, tags)
            key = cleaned if not is_dup else f"dup:{cleaned[:120]}"
            grouped[key].append(cleaned)
            grouped_message_ids[key].append(emb_id)
            processed_count += 1

        return grouped, grouped_message_ids, newest, processed_count

    async def run_batch(self, hours: int | None = None) -> dict[str, int | str]:
        started = time.perf_counter()
        status = "success"
        processed_total = 0
        newest: datetime | None = None
        source_map = await self._load_source_map()
        since = await self._get_checkpoint(hours)

        groups: dict[str, list[str]] = defaultdict(list)
        grouped_ids: dict[str, list[int]] = defaultdict(list)

        try:
            async with TelegramCollector(
                self.settings.telegram_api_id,
                self.settings.telegram_api_hash,
                self.settings.telegram_session_name,
            ) as collector:
                current_batch: list[tuple[int, CollectedMessage]] = []
                async for msg in collector.iter_messages(list(source_map.keys()), hours or 24, since_timestamp=since):
                    if self.cancelled:
                        status = "cancelled"
                        break
                    sid = source_map.get(msg.source)
                    if sid is None:
                        continue
                    current_batch.append((sid, msg))
                    if len(current_batch) >= self.settings.batch_size:
                        g, gid, n, cnt = await self._process_batch(current_batch)
                        processed_total += cnt
                        if n and (newest is None or n > newest):
                            newest = n
                        for k, v in g.items():
                            groups[k].extend(v)
                        for k, v in gid.items():
                            grouped_ids[k].extend(v)
                        current_batch = []

                if current_batch and not self.cancelled:
                    g, gid, n, cnt = await self._process_batch(current_batch)
                    processed_total += cnt
                    if n and (newest is None or n > newest):
                        newest = n
                    for k, v in g.items():
                        groups[k].extend(v)
                    for k, v in gid.items():
                        grouped_ids[k].extend(v)

                if not self.cancelled:
                    for key, texts in groups.items():
                        published_text = self.merger.merge(texts, ["source"] * len(texts)) if len(texts) > 1 else texts[0]
                        await collector.publish(self.settings.telegram_publish_channel, published_text)
                        for emb_id in grouped_ids[key]:
                            await self.db.execute(text("UPDATE messages SET is_published=TRUE WHERE embedding_id=:id"), {"id": emb_id})
                    await self.db.commit()
        except Exception:
            status = "failed"
            logger.exception("Processing failed")
            raise
        finally:
            try:
                self.faiss.persist()
            except Exception:
                logger.exception("FAISS persist failed")

        duration = time.perf_counter() - started
        checkpoint = newest.astimezone(timezone.utc) if (newest and status == "success") else None
        await self.db.execute(
            text(
                """
                UPDATE processing_state
                SET last_run_status=:status,
                    last_run_duration=:duration,
                    last_run_count=:count,
                    last_processed_timestamp=COALESCE(:checkpoint,last_processed_timestamp),
                    updated_at=NOW()
                WHERE id=1
                """
            ),
            {"status": status, "duration": duration, "count": processed_total, "checkpoint": checkpoint},
        )
        await self.db.commit()

        return {
            "status": status,
            "processed": processed_total,
            "duration_sec": round(duration, 3),
            "checkpoint": checkpoint.isoformat() if checkpoint else "unchanged",
        }

    def cancel(self) -> None:
        self.cancelled = True
