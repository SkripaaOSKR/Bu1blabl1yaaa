"""Pluggable translation service."""
from __future__ import annotations

from abc import ABC, abstractmethod
from collections import OrderedDict

from transformers import MarianMTModel, MarianTokenizer


class TranslatorPort(ABC):
    """Translation abstraction for future API-based translator replacement."""

    @abstractmethod
    async def translate_to_ru(self, text: str, language: str) -> str:
        raise NotImplementedError


class MarianTranslator(TranslatorPort):
    """Offline MarianMT translator with lazy-load and LRU cache."""

    def __init__(self, model_name: str, cache_size: int = 2048) -> None:
        self.model_name = model_name
        self.cache_size = cache_size
        self._tokenizer: MarianTokenizer | None = None
        self._model: MarianMTModel | None = None
        self._cache: OrderedDict[str, str] = OrderedDict()

    def _ensure_model(self) -> None:
        if self._tokenizer is None or self._model is None:
            self._tokenizer = MarianTokenizer.from_pretrained(self.model_name)
            self._model = MarianMTModel.from_pretrained(self.model_name)

    def _cache_get(self, key: str) -> str | None:
        value = self._cache.get(key)
        if value is not None:
            self._cache.move_to_end(key)
        return value

    def _cache_set(self, key: str, value: str) -> None:
        self._cache[key] = value
        self._cache.move_to_end(key)
        if len(self._cache) > self.cache_size:
            self._cache.popitem(last=False)

    async def translate_to_ru(self, text: str, language: str) -> str:
        if language in {"ru", "unknown"}:
            return text

        cache_key = f"{language}:{text}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        self._ensure_model()
        tokens = self._tokenizer([text], return_tensors="pt", padding=True)
        generated = self._model.generate(**tokens)
        translated = self._tokenizer.decode(generated[0], skip_special_tokens=True)
        self._cache_set(cache_key, translated)
        return translated
