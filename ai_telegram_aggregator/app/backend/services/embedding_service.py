from __future__ import annotations

import threading
import logging
from collections.abc import Sequence
import numpy as np
from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)

class EmbeddingService:
    """
    Потокобезопасный синглтон для нейросети.
    Превращает текст в векторы (эмбеддинги).
    """
    _instance: EmbeddingService | None = None
    _instance_lock = threading.Lock()

    def __init__(self, model_name: str, batch_size: int) -> None:
        # Защита от повторной инициализации внутри синглтона
        if hasattr(self, '_initialized'): return
        
        self.model_name = model_name
        self.batch_size = batch_size
        self._model: SentenceTransformer | None = None
        self._model_lock = threading.Lock()
        self._initialized = True

    @classmethod
    def get_instance(cls, model_name: str, batch_size: int) -> EmbeddingService:
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = cls(model_name=model_name, batch_size=batch_size)
        else:
            # Позволяем обновлять размер батча на лету
            cls._instance.batch_size = batch_size
        return cls._instance

    def _ensure_model(self) -> SentenceTransformer:
        """Загружает модель только тогда, когда она реально понадобилась."""
        if self._model is None:
            with self._model_lock:
                if self._model is None:
                    logger.info(f"Loading transformer model: {self.model_name}")
                    # device='cpu' принудительно, так как в Docker обычно нет GPU
                    self._model = SentenceTransformer(self.model_name, device='cpu')
        return self._model

    def encode(self, texts: Sequence[str]) -> np.ndarray:
        """Кодирует тексты в векторы. Возвращает массив float32."""
        if not texts:
            return np.empty((0, 384), dtype=np.float32)
            
        model = self._ensure_model()
        vectors = model.encode(
            texts,
            convert_to_numpy=True,
            normalize_embeddings=True, # Важно для точного поиска дублей
            batch_size=self.batch_size,
            show_progress_bar=False
        )
        return np.asarray(vectors, dtype=np.float32)