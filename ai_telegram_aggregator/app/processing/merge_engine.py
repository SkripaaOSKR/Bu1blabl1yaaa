"""Merging logic for semantically similar messages."""
from __future__ import annotations


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
