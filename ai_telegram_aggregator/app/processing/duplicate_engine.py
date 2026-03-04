"""Duplicate detection based on FAISS similarity search."""
from __future__ import annotations

import logging
from typing import Protocol

import numpy as np

logger = logging.getLogger(__name__)


class FaissSearchPort(Protocol):
    ntotal: int

    def search(self, vectors: np.ndarray, k: int = 5) -> tuple[np.ndarray, np.ndarray]:
        ...


class DuplicateEngine:
    """Computes FAISS inner-product similarity and flags duplicates over a threshold."""

    def __init__(self, threshold: float, faiss_store: FaissSearchPort) -> None:
        self.threshold = threshold
        self.faiss = faiss_store

    def set_faiss_store(self, faiss_store: FaissSearchPort) -> None:
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
