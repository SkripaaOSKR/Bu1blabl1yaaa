"""Spam suppression heuristics."""
from __future__ import annotations

import re
from collections import Counter, deque


class SpamFilter:
    """Rule-based spam detector with lightweight frequency scoring."""

    ad_keywords = {"buy", "sale", "discount", "promo", "реклама", "скидка", "подписывайтесь"}
    link_pattern = re.compile(r"https?://")
    entity_pattern = re.compile(r"\b([A-ZА-Я][\w-]+|\d+)\b")

    def __init__(self, min_words: int, max_links: int, repeat_threshold: int, window_size: int = 500) -> None:
        self.min_words = min_words
        self.max_links = max_links
        self.repeat_threshold = repeat_threshold
        self.window_size = window_size
        self.recent_text_counter: Counter[str] = Counter()
        self.recent_tokens: Counter[str] = Counter()
        self.window: deque[list[str]] = deque()

    def _update_frequency_window(self, words: list[str]) -> None:
        self.window.append(words)
        self.recent_tokens.update(words)
        if len(self.window) > self.window_size:
            removed = self.window.popleft()
            self.recent_tokens.subtract(removed)

    def is_spam(self, text: str) -> bool:
        words = text.split()
        normalized_words = [w.lower().strip(".,!?:;()") for w in words if w.strip()]
        links = len(self.link_pattern.findall(text))
        link_density = links / max(1, len(words))

        self.recent_text_counter[text] += 1
        repeated = self.recent_text_counter[text] > self.repeat_threshold

        has_ad_keywords = any(k in set(normalized_words) for k in self.ad_keywords)
        has_entities = bool(self.entity_pattern.search(text))

        self._update_frequency_window(normalized_words)
        common_tokens = sum(1 for token in normalized_words if self.recent_tokens[token] > 50)
        frequency_noise = common_tokens > max(5, len(normalized_words) // 2)

        return (
            len(words) < self.min_words
            or links > self.max_links
            or link_density > 0.2
            or has_ad_keywords
            or repeated
            or not has_entities
            or frequency_noise
        )
