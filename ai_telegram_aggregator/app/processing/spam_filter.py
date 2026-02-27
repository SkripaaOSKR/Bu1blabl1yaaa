"""Spam suppression heuristics."""
from __future__ import annotations

import re
from collections import Counter


class SpamFilter:
    """Rule-based spam detector."""

    ad_keywords = {"buy", "sale", "discount", "promo", "реклама", "скидка", "подписывайтесь"}
    link_pattern = re.compile(r"https?://")
    entity_pattern = re.compile(r"\b([A-ZА-Я][\w-]+|\d+)\b")

    def __init__(self, min_words: int, max_links: int, repeat_threshold: int) -> None:
        self.min_words = min_words
        self.max_links = max_links
        self.repeat_threshold = repeat_threshold
        self.recent_text_counter: Counter[str] = Counter()

    def is_spam(self, text: str) -> bool:
        words = text.split()
        links = len(self.link_pattern.findall(text))
        word_set = {w.lower().strip(".,!?:;()") for w in words}

        self.recent_text_counter[text] += 1
        repeated = self.recent_text_counter[text] > self.repeat_threshold

        has_ad_keywords = any(k in word_set for k in self.ad_keywords)
        has_entities = bool(self.entity_pattern.search(text))

        return (
            len(words) < self.min_words
            or links > self.max_links
            or has_ad_keywords
            or repeated
            or not has_entities
        )
