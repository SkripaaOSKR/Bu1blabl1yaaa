"""Duplicate detection based on cosine similarity and 7-day window."""
from __future__ import annotations

import numpy as np


class DuplicateEngine:
    """Computes similarity and flags duplicates over a threshold."""

    def __init__(self, threshold: float) -> None:
        self.threshold = threshold

    def find_duplicates(self, candidate: np.ndarray, history: np.ndarray) -> tuple[bool, float]:
        if history.size == 0:
            return False, 0.0
        sims = history @ candidate.T
        best = float(np.max(sims))
        return best >= self.threshold, best
