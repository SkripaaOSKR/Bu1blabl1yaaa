"""Application entrypoint and async pipeline orchestration."""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
from collections import defaultdict
from datetime import datetime, timezone

import numpy as np

from app.cli import select_hours
from app.collector.telegram_client import TelegramCollector
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

    async def run(self, hours: int) -> dict[str, int]:
        groups: dict[str, list[str]] = defaultdict(list)
        source_map: dict[str, list[str]] = defaultdict(list)

        async with TelegramCollector(
            self.settings.telegram_api_id,
            self.settings.telegram_api_hash,
            self.settings.telegram_session_name,
        ) as collector:
            async for msg in collector.iter_messages(self.settings.sources, hours):
                self.analytics["processed"] += 1
                processed = self.preprocessor.run(msg.text)
                if not processed.cleaned or self.spam_filter.is_spam(processed.cleaned):
                    self.analytics["spam_removed"] += 1
                    continue

                translated = await self.translator.translate_to_ru(processed.cleaned, processed.language)
                vector = self.embedding_model.encode([translated])[0]

                recent_rows = await self.repo.get_recent_embeddings(self.settings.dedupe_window_days)
                history = (
                    np.vstack([np.frombuffer(row[1], dtype=np.float32) for row in recent_rows])
                    if recent_rows
                    else np.empty((0, vector.shape[0]), dtype=np.float32)
                )
                is_dup, _ = self.dedupe.find_duplicates(vector, history)
                if is_dup:
                    self.analytics["duplicates"] += 1

                key = translated if not is_dup else "dup:" + translated[:120]
                groups[key].append(translated)
                source_map[key].append(msg.source)

                emb_id = await self.repo.save_embedding(vector.tobytes())
                tags = self.tagger.generate(translated)
                await self.repo.save_message(
                    text=msg.text,
                    merged_text=None,
                    created_at=msg.created_at.astimezone(timezone.utc),
                    embedding_id=emb_id,
                    tags=tags,
                    language=processed.language,
                    sources=[msg.source],
                    media_group_id=msg.media_group_id,
                )

            for key, texts in groups.items():
                if len(texts) > 1:
                    merged = self.merger.merge(texts, source_map[key])
                    await collector.publish(self.settings.telegram_publish_channel, merged)
                    self.analytics["merged"] += 1
                    self.analytics["published"] += 1
                else:
                    await collector.publish(self.settings.telegram_publish_channel, texts[0])
                    self.analytics["published"] += 1

        self.faiss.persist()
        await self._persist_analytics()
        return dict(self.analytics)

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
