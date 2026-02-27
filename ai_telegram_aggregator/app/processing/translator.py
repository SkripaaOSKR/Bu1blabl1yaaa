"""Pluggable translation service."""
from __future__ import annotations

from abc import ABC, abstractmethod

from transformers import MarianMTModel, MarianTokenizer


class TranslatorPort(ABC):
    """Translation abstraction for future API-based translator replacement."""

    @abstractmethod
    async def translate_to_ru(self, text: str, language: str) -> str:
        raise NotImplementedError


class MarianTranslator(TranslatorPort):
    """Offline MarianMT translator."""

    def __init__(self, model_name: str) -> None:
        self.tokenizer = MarianTokenizer.from_pretrained(model_name)
        self.model = MarianMTModel.from_pretrained(model_name)

    async def translate_to_ru(self, text: str, language: str) -> str:
        if language in {"ru", "unknown"}:
            return text
        tokens = self.tokenizer([text], return_tensors="pt", padding=True)
        generated = self.model.generate(**tokens)
        return self.tokenizer.decode(generated[0], skip_special_tokens=True)
