from __future__ import annotations

import logging
import re
import hashlib
from collections import Counter, deque
from dataclasses import dataclass
from typing import Protocol

import numpy as np
from langdetect import detect
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ProcessedText:
    """Результат предварительной обработки текста."""
    cleaned: str
    language: str


class Preprocessor:
    """Очистка текста от лишних пробелов и определение языка."""
    _whitespace = re.compile(r"\s+")

    def run(self, text: str) -> ProcessedText:
        # Убираем двойные пробелы, переносы и лишние символы по краям
        cleaned = self._whitespace.sub(" ", text).strip()
        language = "unknown"
        
        # Защита: langdetect часто ошибается или падает на слишком коротких текстах
        if len(cleaned) >= 20:
            try:
                # Определяем язык (нужно для ИИ и фильтров)
                language = detect(cleaned)
            except Exception:
                language = "unknown"
                
        return ProcessedText(cleaned=cleaned, language=language)


class SpamFilter:
    """Динамический фильтр спама."""
    link_pattern = re.compile(r"https?://")
    invite_link_pattern = re.compile(r"t\.me/(\+?|joinchat/)") 

    def __init__(self, active_keywords: set[str], min_words: int, max_links: int, repeat_threshold: int):
        self.active_keywords = active_keywords
        self.min_words = min_words
        self.max_links = max_links
        self.repeat_threshold = repeat_threshold
        
        # ОПТИМИЗАЦИЯ: Предкомпилируем все стоп-слова один раз при создании фильтра
        self._compiled_keywords = [
            re.compile(rf"\b{re.escape(word)}\b", re.IGNORECASE) 
            for word in self.active_keywords
        ]

    def is_spam(self, text: str) -> bool:
        text_lower = text.lower()
        words = text_lower.split()
        
        # 1. Базовые правила (длина и ссылки)
        if len(words) < self.min_words:
            return True
        if len(self.link_pattern.findall(text)) > self.max_links:
            return True
        if self.invite_link_pattern.search(text_lower):
            return True

        # 2. ГИБКАЯ ПРОВЕРКА ПО ТВОИМ СЛОВАМ (УМНАЯ И БЫСТРАЯ)
        for pattern in self._compiled_keywords:
            if pattern.search(text_lower):
                return True

        return False


class MergeEngine:
    """Слияние нескольких похожих текстов в один связный черновик."""

    def __init__(self, max_chars: int = 1800) -> None:
        self.max_chars = max_chars

    @staticmethod
    def _unique_paragraphs(text: str) -> list[str]:
        """Разбивает текст на абзацы и удаляет дубликаты внутри одного текста."""
        seen: set[str] = set()
        result: list[str] = []
        for paragraph in [p.strip() for p in text.split("\n") if p.strip()]:
            key = paragraph.lower()
            if key not in seen:
                seen.add(key)
                result.append(paragraph)
        return result

    def merge(self, texts: list[str], sources: list[str]) -> str:
        """Объединяет список текстов, убирая повторы."""
        if not texts:
            return ""

        # Сортируем тексты по длине (самые полные — в начало)
        ordered = sorted(texts, key=len, reverse=True)
        merged_parts: list[str] = []
        merged_seen: set[str] = set()

        for text in ordered:
            for paragraph in self._unique_paragraphs(text):
                key = paragraph.lower()
                # Если такой абзац уже был в другом тексте — пропускаем
                if key not in merged_seen:
                    merged_seen.add(key)
                    merged_parts.append(paragraph)

        # Формируем итоговый текст
        body = "\n\n".join(merged_parts)
        
        # Добавляем список источников (для черновика ИИ)
        source_line = "Источники: " + ", ".join(sorted(set(sources)))
        
        # Обрезаем, если текст слишком длинный
        result = f"{body}\n\n{source_line}"
        return result[: self.max_chars]


class TagGenerator:
    """Генератор хэштегов (Используется как запасной, если ИИ недоступен)."""
    
    # Слова, которые не могут быть тегами
    stop_words = {
        "the", "and", "for", "that", "with", "this", "from", "have", "will", "into", "about",
        "это", "как", "для", "что", "или", "при", "после", "перед", "который", "также",
        "было", "есть", "будет", "свои", "своих", "только", "очень", "через", "между"
    }
    # Ищем слова от 4 символов (буквы и цифры)
    token_pattern = re.compile(r"[a-zA-Zа-яА-Я0-9]{4,}")

    def generate(self, text: str, min_tags: int = 3, max_tags: int = 6) -> list[str]:
        tokens = [t.lower() for t in self.token_pattern.findall(text)]
        # Фильтруем мусор и исключаем числа (чтобы не было тегов вроде #2024)
        filtered = [token for token in tokens if token not in self.stop_words and not token.isdigit()]
        
        if not filtered:
            return ["#news"]
            
        # Считаем частоту слов
        counts = Counter(filtered)
        # Берем самые популярные
        tags = [f"#{token}" for token, _ in counts.most_common(max_tags)]
        
        return tags[: max(min_tags, min(max_tags, len(tags)))]


class FaissSearchPort(Protocol):
    """Интерфейс для работы с FAISS (чтобы не импортировать весь класс)."""
    ntotal: int
    def search(self, vectors: np.ndarray, k: int = 5) -> tuple[np.ndarray, np.ndarray]:
        ...


class DuplicateEngine:
    """Двигатель поиска дубликатов и блокировки подтвержденного спама."""
    
    def __init__(self, threshold: float, faiss_store: FaissSearchPort) -> None:
        self.threshold = threshold
        self.faiss = faiss_store

    def get_text_hash(self, text: str) -> str:
        """Создает быстрый blake2b-хэш текста для Уровня 1 (100% совпадение)."""
        return hashlib.blake2b(text.strip().encode('utf-8')).hexdigest()

    async def check_status(self, candidate_vector: np.ndarray, db: AsyncSession) -> tuple[bool, bool, float]:
        """
        Уровень 2: Проверяет вектор сообщения в FAISS.
        Возвращает: (is_duplicate, is_confirmed_spam, score)
        """
        if self.faiss.ntotal <= 0:
            return False, False, 0.0
            
        try:
            query = np.ascontiguousarray(candidate_vector.reshape(1, -1), dtype=np.float32)
            scores, ids = self.faiss.search(query, k=1) # Берем самый похожий
            
            best_score = float(scores[0][0]) if scores.size else 0.0
            best_id = int(ids[0][0]) if ids.size else -1
            
            if best_score < self.threshold:
                return False, False, best_score

            # Если сходство высокое, проверяем в базе — не спам ли это случайно?
            res = await db.execute(
                text("SELECT is_confirmed_spam FROM messages WHERE embedding_id = :eid"),
                {"eid": best_id}
            )
            row = res.fetchone()
            is_spam = row.is_confirmed_spam if row else False
            
            return True, is_spam, best_score
            
        except Exception as e:
            logger.error(f"FAISS check failed: {e}")
            return False, False, 0.0