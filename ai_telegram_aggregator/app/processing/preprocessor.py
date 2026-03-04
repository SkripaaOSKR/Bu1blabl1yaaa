"""Text normalization and language detection."""
from __future__ import annotations

import re
from dataclasses import dataclass

from langdetect import detect


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
