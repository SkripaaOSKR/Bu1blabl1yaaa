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
    """Offline MarianMT translator with lazy model load and LRU cache."""

    def __init__(self, model_name: str, cache_size: int = 1024) -> None:
        self.model_name = model_name
        self.cache_size = cache_size
        self._tokenizer: MarianTokenizer | None = None
        self._model: MarianMTModel | None = None
        self._cache: OrderedDict[tuple[str, str], str] = OrderedDict()

    def _ensure_model(self) -> None:
        if self._tokenizer is None or self._model is None:
            self._tokenizer = MarianTokenizer.from_pretrained(self.model_name)
            self._model = MarianMTModel.from_pretrained(self.model_name)

    async def translate_to_ru(self, text: str, language: str) -> str:
        if language in {"ru", "unknown"}:
            return text

        key = (language, text)
        if key in self._cache:
            value = self._cache.pop(key)
            self._cache[key] = value
            return value

        self._ensure_model()
        tokens = self._tokenizer([text], return_tensors="pt", padding=True)
        generated = self._model.generate(**tokens)
        translated = self._tokenizer.decode(generated[0], skip_special_tokens=True)

        self._cache[key] = translated
        if len(self._cache) > self.cache_size:
            self._cache.popitem(last=False)

        return translated
