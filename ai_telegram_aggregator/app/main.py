"""Application entrypoint and async pipeline orchestration."""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from app.cli import select_hours
from app.collector.telegram_client import CollectedMessage, TelegramCollector
from app.config import get_settings
from app.processing.duplicate_engine import DuplicateEngine
from app.processing.merge_engine import MergeEngine
from app.processing.preprocessor import Preprocessor
from app.processing.spam_filter import SpamFilter
from app.processing.tagger import TagGenerator
from app.semantic.embedding_model import EmbeddingModel
from app.semantic.faiss_index import FaissStore
from app.storage.database import init_db
from app.storage.repository import Repository
from app.utils.logger import setup_logger

logger = logging.getLogger(__name__)
CHECKPOINT_KEY = "last_processed_timestamp"


class AggregatorService:
    """Coordinates full collection and semantic processing pipeline."""

    def __init__(self) -> None:
        self.settings = get_settings()
        setup_logger(self.settings.log_level)
        self.repo = Repository(str(self.settings.sqlite_path))
        self.preprocessor = Preprocessor()
        self.spam_filter = SpamFilter(
            min_words=self.settings.spam_min_words,
            max_links=self.settings.spam_max_links,
            repeat_threshold=self.settings.spam_repeat_threshold,
        )
        self.embedding_model = EmbeddingModel(
            self.settings.embedding_model_name,
            batch_size=self.settings.embedding_batch_size,
        )
        self.faiss = FaissStore(self.settings.faiss_index_path)
        self.dedupe = DuplicateEngine(self.settings.dedupe_similarity_threshold, self.faiss)
        self.merger = MergeEngine(self.settings.merge_max_chars)
        self.tagger = TagGenerator()
        self.analytics = defaultdict(int)

    async def initialize(self) -> None:
        await init_db(str(self.settings.sqlite_path))
        await self.repo.connect()
        await self._sync_faiss_index()

    async def close(self) -> None:
        await self.repo.close()

    def _parse_checkpoint(self, value: str) -> datetime | None:
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            logger.warning("Invalid checkpoint value '%s'; fallback to --hours mode", value)
            return None

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    async def _sync_faiss_index(self) -> None:
        """Validate and rebuild FAISS when DB/index state diverges."""
        mapping_count = await self.repo.get_faiss_mapping_count()
        embeddings = await self.repo.get_all_embeddings()
        if self.faiss.ntotal == mapping_count == len(embeddings):
            return

        logger.warning(
            "FAISS desync detected (index=%s mapping=%s embeddings=%s). Rebuilding index.",
            self.faiss.ntotal,
            mapping_count,
            len(embeddings),
        )
        self.faiss = FaissStore(self.settings.faiss_index_path)
        self.faiss.index.reset()
        if embeddings:
            ids = np.asarray([row[0] for row in embeddings], dtype=np.int64)
            vectors = np.vstack([np.frombuffer(row[1], dtype=np.float32) for row in embeddings])
            self.faiss.add_with_ids(vectors, ids)
        self.faiss.persist()
        self.dedupe.set_faiss_store(self.faiss)

    async def _encode_safe(self, texts: list[str]) -> dict[int, np.ndarray]:
        if not texts:
            return {}

        try:
            vectors = await asyncio.to_thread(self.embedding_model.encode, texts)
            return {idx: vectors[idx] for idx in range(len(texts))}
        except Exception:
            logger.exception("Batch embedding failed; retrying item-by-item")

        encoded: dict[int, np.ndarray] = {}
        for idx, text in enumerate(texts):
            try:
                vector = await asyncio.to_thread(self.embedding_model.encode, [text])
                encoded[idx] = vector[0]
            except Exception:
                logger.exception("Embedding failed for message index=%s; message skipped", idx)
        return encoded

    async def _process_batch(
        self,
        batch: list[CollectedMessage],
        groups: dict[str, list[str]],
        source_map: dict[str, list[str]],
    ) -> None:
        valid_messages: list[tuple[CollectedMessage, str, str]] = []
        for msg in batch:
            self.analytics["processed"] += 1
            processed = self.preprocessor.run(msg.text)
            if not processed.cleaned or self.spam_filter.is_spam(processed.cleaned):
                self.analytics["spam_removed"] += 1
                continue
            valid_messages.append((msg, processed.language, processed.cleaned))

        if not valid_messages:
            return

        cleaned_texts = [item[2] for item in valid_messages]
        encoded_map = await self._encode_safe(cleaned_texts)

        for idx, (msg, language, _) in enumerate(valid_messages):
            vector = encoded_map.get(idx)
            if vector is None:
                continue

            cleaned = cleaned_texts[idx]
            is_dup, _ = self.dedupe.find_duplicates(vector)
            if is_dup:
                self.analytics["duplicates"] += 1

            key = cleaned if not is_dup else "dup:" + cleaned[:120]
            groups[key].append(cleaned)
            source_map[key].append(msg.source)

            tags = self.tagger.generate(cleaned)
            emb_id = await self.repo.save_processed_message(
                vector=vector.tobytes(),
                faiss_id=None,
                text=msg.text,
                merged_text=None,
                created_at=msg.created_at.astimezone(timezone.utc),
                tags=tags,
                language=language,
                sources=[msg.source],
                media_group_id=msg.media_group_id,
            )
            try:
                self.faiss.add_with_ids(vector.reshape(1, -1), np.asarray([emb_id], dtype=np.int64))
            except Exception:
                logger.exception("Failed to append vector to FAISS for embedding_id=%s", emb_id)

    async def run(self, hours: int) -> dict[str, int]:
        groups: dict[str, list[str]] = defaultdict(list)
        source_map: dict[str, list[str]] = defaultdict(list)
        batch: list[CollectedMessage] = []
        newest_seen: datetime | None = None

        since_timestamp: datetime | None = None
        checkpoint_value = await self.repo.get_state(CHECKPOINT_KEY)
        if checkpoint_value is not None:
            since_timestamp = self._parse_checkpoint(checkpoint_value)
        if since_timestamp is not None:
            logger.info("Using checkpoint mode from %s", since_timestamp.isoformat())
        else:
            logger.info("Using hours mode for last %s hours", hours)

        async with TelegramCollector(
            self.settings.telegram_api_id,
            self.settings.telegram_api_hash,
            self.settings.telegram_session_name,
        ) as collector:
            async for msg in collector.iter_messages(self.settings.sources, hours, since_timestamp=since_timestamp):
                if newest_seen is None or msg.created_at > newest_seen:
                    newest_seen = msg.created_at
                batch.append(msg)
                if len(batch) >= self.settings.batch_size:
                    await self._process_batch(batch, groups, source_map)
                    batch = []

            if batch:
                await self._process_batch(batch, groups, source_map)

            for key, texts in groups.items():
                if len(texts) > 1:
                    merged = self.merger.merge(texts, source_map[key])
                    await collector.publish(self.settings.telegram_publish_channel, merged)
                    self.analytics["merged"] += 1
                    self.analytics["published"] += 1
                else:
                    await collector.publish(self.settings.telegram_publish_channel, texts[0])
                    self.analytics["published"] += 1

        try:
            self.faiss.persist()
        except Exception:
            logger.exception("Failed to persist FAISS index")
        await self._persist_analytics()

        if newest_seen is not None:
            checkpoint = newest_seen.astimezone(timezone.utc).isoformat()
            await self.repo.set_state(CHECKPOINT_KEY, checkpoint)
            logger.info("Updated checkpoint %s=%s", CHECKPOINT_KEY, checkpoint)
        else:
            logger.info("No new messages processed; checkpoint not changed")

        return dict(self.analytics)

    async def _persist_analytics(self) -> None:
        payload = {
            "processed": self.analytics["processed"],
            "duplicates": self.analytics["duplicates"],
            "merged": self.analytics["merged"],
            "spam_removed": self.analytics["spam_removed"],
            "published": self.analytics["published"],
        }

        def _write_json(path: Path, data: dict[str, int]) -> None:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", encoding="utf-8") as fp:
                json.dump(data, fp, ensure_ascii=False, indent=2)

        await asyncio.to_thread(_write_json, self.settings.analytics_path, payload)


async def _main() -> None:
    parser = argparse.ArgumentParser(description="AI Telegram Semantic Aggregator")
    parser.add_argument("--hours", type=int, default=None, help="Analyze last N hours (1-24)")
    args = parser.parse_args()

    hours = args.hours if args.hours else select_hours()
    service = AggregatorService()
    try:
        await service.initialize()
        stats = await service.run(hours)
        logger.info("Run completed: %s", stats)
    finally:
        await service.close()


if __name__ == "__main__":
    asyncio.run(_main())
