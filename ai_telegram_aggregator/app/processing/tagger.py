"""Keyword-based dynamic tag generation."""
from __future__ import annotations

import re
from collections import Counter


class TagGenerator:
    """Generate 3-7 tags from frequent non-trivial tokens."""

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
