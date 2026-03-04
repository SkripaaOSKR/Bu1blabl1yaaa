from __future__ import annotations

import logging
import re
from collections import Counter, deque
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

import numpy as np
from langdetect import detect
from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ProcessedText:
    cleaned: str
    language: str


class Preprocessor:
    """Cleans raw telegram text and detects language."""

    _whitespace = re.compile(r"\s+")

    def run(self, text: str) -> ProcessedText:
        cleaned = self._whitespace.sub(" ", text).strip()
        language = "unknown"
        if cleaned:
            try:
                language = detect(cleaned)
            except Exception:
                language = "unknown"
        return ProcessedText(cleaned=cleaned, language=language)


class SpamFilter:
    """Rule-based spam detector with bounded repeat-memory."""

    ad_keywords = {"buy", "sale", "discount", "promo", "реклама", "скидка", "подписывайтесь"}
    link_pattern = re.compile(r"https?://")
    entity_pattern = re.compile(r"\b([A-ZА-Я][\w-]+|\d+)\b")

    def __init__(self, min_words: int, max_links: int, repeat_threshold: int, memory_limit: int = 10_000) -> None:
        self.min_words = min_words
        self.max_links = max_links
        self.repeat_threshold = repeat_threshold
        self.memory_limit = memory_limit
        self.recent_text_counter: Counter[str] = Counter()
        self._recent_order: deque[str] = deque(maxlen=memory_limit)

    def _remember_text(self, text: str) -> int:
        if len(self._recent_order) >= self.memory_limit:
            oldest = self._recent_order.popleft()
            self.recent_text_counter[oldest] -= 1
            if self.recent_text_counter[oldest] <= 0:
                del self.recent_text_counter[oldest]

        self._recent_order.append(text)
        self.recent_text_counter[text] += 1
        return self.recent_text_counter[text]

    def is_spam(self, text: str) -> bool:
        words = text.split()
        links = len(self.link_pattern.findall(text))
        word_set = {w.lower().strip(".,!?:;()") for w in words}

        repeated = self._remember_text(text) > self.repeat_threshold
        has_ad_keywords = any(k in word_set for k in self.ad_keywords)
        has_entities = bool(self.entity_pattern.search(text))

        return (
            len(words) < self.min_words
            or links > self.max_links
            or has_ad_keywords
            or repeated
            or not has_entities
        )


class MergeEngine:
    """Merge similar texts while preserving conflicting facts."""

    def __init__(self, max_chars: int = 1000) -> None:
        self.max_chars = max_chars

    @staticmethod
    def _unique_paragraphs(text: str) -> list[str]:
        seen: set[str] = set()
        result: list[str] = []
        for paragraph in [p.strip() for p in text.split("\n") if p.strip()]:
            key = paragraph.lower()
            if key not in seen:
                seen.add(key)
                result.append(paragraph)
        return result

    def merge(self, texts: list[str], sources: list[str]) -> str:
        if not texts:
            return ""

        ordered = sorted(texts, key=len, reverse=True)
        merged_parts: list[str] = []
        merged_seen: set[str] = set()
        for text in ordered:
            for paragraph in self._unique_paragraphs(text):
                key = paragraph.lower()
                if key not in merged_seen:
                    merged_seen.add(key)
                    merged_parts.append(paragraph)

        source_line = "Источники: " + ", ".join(sorted(set(sources)))
        body_limit = max(0, self.max_chars - len(source_line) - 2)
        body = "\n\n".join(merged_parts)
        merged_body = body[:body_limit].rstrip()
        return f"{merged_body}\n\n{source_line}"[: self.max_chars]


class TagGenerator:
    stop_words = {
        "the", "and", "for", "that", "with", "this", "from", "have", "will", "into", "about",
        "это", "как", "для", "что", "или", "при", "после", "перед", "который", "также",
    }
    token_pattern = re.compile(r"[a-zA-Zа-яА-Я0-9]{4,}")

    def generate(self, text: str, min_tags: int = 3, max_tags: int = 7) -> list[str]:
        tokens = [t.lower() for t in self.token_pattern.findall(text)]
        filtered = [token for token in tokens if token not in self.stop_words]
        if not filtered:
            return ["#news"]
        counts = Counter(filtered)
        tags = [f"#{token}" for token, _ in counts.most_common(max_tags)]
        return tags[: max(min_tags, min(max_tags, len(tags)))]


class EmbeddingModel:
    def __init__(self, model_name: str, batch_size: int = 64) -> None:
        self.model = SentenceTransformer(model_name)
        self.batch_size = batch_size

    def encode(self, texts: Sequence[str]) -> np.ndarray:
        vectors = self.model.encode(
            texts,
            convert_to_numpy=True,
            normalize_embeddings=True,
            batch_size=self.batch_size,
            show_progress_bar=False,
        )
        return np.asarray(vectors, dtype=np.float32)


class FaissSearchPort(Protocol):
    ntotal: int

    def search(self, vectors: np.ndarray, k: int = 5) -> tuple[np.ndarray, np.ndarray]:
        ...


class DuplicateEngine:
    def __init__(self, threshold: float, faiss_store: FaissSearchPort) -> None:
        self.threshold = threshold
        self.faiss = faiss_store

    def find_duplicates(self, candidate: np.ndarray) -> tuple[bool, float]:
        if self.faiss.ntotal <= 0:
            return False, 0.0
        try:
            query = np.ascontiguousarray(candidate.reshape(1, -1), dtype=np.float32)
            scores, _ = self.faiss.search(query, k=5)
            best = float(np.max(scores[0])) if scores.size else 0.0
        except Exception:
            logger.exception("FAISS similarity search failed; duplicate detection disabled for item")
            return False, 0.0
        return best >= self.threshold, best
