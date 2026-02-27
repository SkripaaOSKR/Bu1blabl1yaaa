"""Duplicate detection based on hybrid semantic similarity."""
from __future__ import annotations

import numpy as np


class DuplicateEngine:
    """Combines cosine, length normalization and token overlap."""

    def __init__(self, threshold: float) -> None:
        self.threshold = threshold

    @staticmethod
    def _jaccard(a: str, b: str) -> float:
        sa, sb = set(a.lower().split()), set(b.lower().split())
        if not sa or not sb:
            return 0.0
        return len(sa & sb) / len(sa | sb)

    def is_duplicate(self, text: str, candidate: np.ndarray, history: list[tuple[str, np.ndarray]]) -> tuple[bool, float]:
        if not history:
            return False, 0.0

        scores: list[float] = []
        candidate_len = max(1, len(text))
        for hist_text, hist_vec in history:
            cosine = float(np.dot(hist_vec, candidate))
            length_ratio = min(candidate_len, len(hist_text)) / max(candidate_len, len(hist_text))
            jaccard = self._jaccard(text, hist_text)
            score = (0.75 * cosine) + (0.15 * length_ratio) + (0.10 * jaccard)
            scores.append(score)

        best = max(scores)
        return best >= self.threshold, best
