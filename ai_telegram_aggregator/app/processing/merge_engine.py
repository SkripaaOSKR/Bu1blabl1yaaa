"""Merging logic for semantically similar messages."""
from __future__ import annotations


class MergeEngine:
    """Merge similar texts while preserving conflicting facts."""

    def __init__(self, max_chars: int = 1000) -> None:
        self.max_chars = max_chars

    def merge(self, texts: list[str], sources: list[str]) -> str:
        if not texts:
            return ""
        ordered = sorted(texts, key=len, reverse=True)
        base = ordered[0]
        for text in ordered[1:]:
            if text not in base:
                base += f"\n\n{text}"
        merged = base[: self.max_chars]
        source_line = "\n\nИсточники: " + ", ".join(sorted(set(sources)))
        return (merged + source_line)[: self.max_chars]
