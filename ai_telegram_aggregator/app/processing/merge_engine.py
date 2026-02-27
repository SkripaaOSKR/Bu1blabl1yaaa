"""Merging logic for semantically similar messages."""
from __future__ import annotations


class MergeEngine:
    """Merge similar texts while preserving conflicting facts."""

    def __init__(self, max_chars: int = 1000) -> None:
        self.max_chars = max_chars

    def merge(self, texts: list[str], sources: list[str]) -> str:
        if not texts:
            return ""

        seen: set[str] = set()
        chunks: list[str] = []
        for text in sorted(texts, key=len, reverse=True):
            for paragraph in [p.strip() for p in text.split("\n") if p.strip()]:
                if paragraph not in seen:
                    seen.add(paragraph)
                    chunks.append(paragraph)

        source_line = "Источники: " + ", ".join(sorted(set(sources)))
        budget = max(0, self.max_chars - len(source_line) - 2)

        body_parts: list[str] = []
        size = 0
        for chunk in chunks:
            add_len = len(chunk) + (2 if body_parts else 0)
            if size + add_len > budget:
                break
            body_parts.append(chunk)
            size += add_len

        body = "\n\n".join(body_parts)
        return f"{body}\n\n{source_line}"[: self.max_chars]
