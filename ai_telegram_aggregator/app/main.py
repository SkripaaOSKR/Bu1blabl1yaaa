"""Application entrypoint and async pipeline orchestration."""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time
from collections import defaultdict
from datetime import timezone

import numpy as np

from app.cli import select_hours
from app.collector.telegram_client import CollectedMessage, TelegramCollector
from app.config import get_settings
from app.processing.duplicate_engine import DuplicateEngine
from app.processing.merge_engine import MergeEngine
from app.processing.preprocessor import Preprocessor
from app.processing.spam_filter import SpamFilter
from app.processing.tagger import TagGenerator
from app.processing.translator import MarianTranslator
from app.semantic.embedding_model import EmbeddingModel
from app.semantic.faiss_index import FaissStore
from app.storage.database import init_db
from app.storage.repository import Repository
from app.utils.logger import setup_logger

logger = logging.getLogger(__name__)


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
        self.embedding_model = EmbeddingModel(self.settings.embedding_model_name)
        self.dedupe = DuplicateEngine(self.settings.dedupe_similarity_threshold)
        self.merger = MergeEngine(self.settings.merge_max_chars)
        self.tagger = TagGenerator()
        self.translator = MarianTranslator(self.settings.translation_model_name)
        self.faiss = FaissStore(self.settings.faiss_index_path)
        self.analytics = defaultdict(int)

    async def initialize(self) -> None:
        await init_db(str(self.settings.sqlite_path))
        await self._ensure_faiss_consistency()

    async def _ensure_faiss_consistency(self) -> None:
        """Keep FAISS vector positions synchronized with embedding ids in DB."""
        mapping_count = await self.repo.get_faiss_mapping_count()
        embeddings = await self.repo.get_all_embeddings()

        if self.faiss.size == len(embeddings) == mapping_count:
            return

        logger.warning(
            "FAISS/SQLite mismatch detected (faiss=%s, embeddings=%s, mapping=%s). Rebuilding index.",
            self.faiss.size,
            len(embeddings),
            mapping_count,
        )
        self.faiss.reset()
        await self.repo.clear_faiss_mapping()

        if not embeddings:
            self.faiss.persist()
            return

        vectors = np.vstack([np.frombuffer(vector, dtype=np.float32) for _, vector in embeddings])
        start_pos = self.faiss.add(vectors)
        for offset, (embedding_id, _) in enumerate(embeddings):
            await self.repo.save_faiss_mapping(embedding_id, start_pos + offset)
        self.faiss.persist()

    async def run(self, hours: int) -> dict[str, int]:
        groups: dict[str, list[str]] = defaultdict(list)
        source_map: dict[str, list[str]] = defaultdict(list)

        async with TelegramCollector(
            self.settings.telegram_api_id,
            self.settings.telegram_api_hash,
            self.settings.telegram_session_name,
        ) as collector:
            batch: list[CollectedMessage] = []
            async for msg in collector.iter_messages(self.settings.sources, hours):
                batch.append(msg)
                if len(batch) >= self.settings.batch_size:
                    await self._process_batch(batch, groups, source_map)
                    batch.clear()

            if batch:
                await self._process_batch(batch, groups, source_map)

            for key, texts in groups.items():
                if len(texts) > 1:
                    merged = self.merger.merge(texts, source_map[key])
                    await collector.publish(self.settings.telegram_publish_channel, merged)
                    self.analytics["merged"] += 1
                else:
                    await collector.publish(self.settings.telegram_publish_channel, texts[0])
                self.analytics["published"] += 1

        self.faiss.persist()
        await self._persist_analytics()
        return dict(self.analytics)

    async def _process_batch(
        self,
        batch: list[CollectedMessage],
        groups: dict[str, list[str]],
        source_map: dict[str, list[str]],
    ) -> None:
        started = time.perf_counter()
        recent_rows = await self.repo.get_recent_embeddings(self.settings.dedupe_window_days)
        history: list[tuple[str, np.ndarray]] = []
        # text is unknown for old vectors; keep empty string for jaccard branch compatibility
        for _, vector in recent_rows:
            history.append(("", np.frombuffer(vector, dtype=np.float32)))

        prepared: list[tuple[CollectedMessage, str, str]] = []
        for msg in batch:
            self.analytics["processed"] += 1
            processed = self.preprocessor.run(msg.text)
            if not processed.cleaned or self.spam_filter.is_spam(processed.cleaned):
                self.analytics["spam_removed"] += 1
                continue
            translated = await self.translator.translate_to_ru(processed.cleaned, processed.language)
            prepared.append((msg, processed.language, translated))

        if not prepared:
            return

        texts = [item[2] for item in prepared]
        vectors = self.embedding_model.encode(texts, batch_size=self.settings.embedding_batch_size)

        for idx, (msg, language, translated) in enumerate(prepared):
            vector = vectors[idx]
            is_dup, _ = self.dedupe.is_duplicate(translated, vector, history)
            if is_dup:
                self.analytics["duplicates"] += 1

            group_key = translated if not is_dup else f"dup:{translated[:120]}"
            groups[group_key].append(translated)
            source_map[group_key].append(msg.source)

            emb_id = await self.repo.save_embedding(vector.tobytes())
            position = self.faiss.add(np.expand_dims(vector, axis=0))
            await self.repo.save_faiss_mapping(emb_id, position)
            history.append((translated, vector))

            tags = self.tagger.generate(translated)
            await self.repo.save_message(
                text=msg.text,
                merged_text=None,
                created_at=msg.created_at.astimezone(timezone.utc),
                embedding_id=emb_id,
                tags=tags,
                language=language,
                sources=[msg.source],
                media_group_id=msg.media_group_id,
            )

        elapsed = time.perf_counter() - started
        logger.info(
            "Batch processed: size=%s valid=%s elapsed=%.2fs memory_hint=index_vectors:%s",
            len(batch),
            len(prepared),
            elapsed,
            self.faiss.size,
        )

    async def _persist_analytics(self) -> None:
        self.settings.analytics_path.parent.mkdir(parents=True, exist_ok=True)
        with self.settings.analytics_path.open("w", encoding="utf-8") as fp:
            json.dump(
                {
                    "processed": self.analytics["processed"],
                    "duplicates": self.analytics["duplicates"],
                    "merged": self.analytics["merged"],
                    "spam_removed": self.analytics["spam_removed"],
                    "published": self.analytics["published"],
                },
                fp,
                ensure_ascii=False,
                indent=2,
            )


async def _main() -> None:
    parser = argparse.ArgumentParser(description="AI Telegram Semantic Aggregator")
    parser.add_argument("--hours", type=int, default=None, help="Analyze last N hours (1-24)")
    args = parser.parse_args()

    hours = args.hours if args.hours else select_hours()
    service = AggregatorService()
    await service.initialize()
    stats = await service.run(hours)
    logger.info("Run completed: %s", stats)


if __name__ == "__main__":
    asyncio.run(_main())
