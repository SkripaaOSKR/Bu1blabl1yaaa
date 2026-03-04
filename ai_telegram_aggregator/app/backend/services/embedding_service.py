from __future__ import annotations

import threading
from collections.abc import Sequence

import numpy as np
from sentence_transformers import SentenceTransformer


class EmbeddingService:
    """Thread-safe singleton wrapper around sentence-transformers."""

    _instance: EmbeddingService | None = None
    _instance_lock = threading.Lock()

    def __init__(self, model_name: str, batch_size: int) -> None:
        self.model_name = model_name
        self.batch_size = batch_size
        self._model: SentenceTransformer | None = None
        self._model_lock = threading.Lock()

    @classmethod
    def get_instance(cls, model_name: str, batch_size: int) -> EmbeddingService:
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = cls(model_name=model_name, batch_size=batch_size)
        else:
            cls._instance.batch_size = batch_size
        return cls._instance

    def _ensure_model(self) -> SentenceTransformer:
        if self._model is None:
            with self._model_lock:
                if self._model is None:
                    self._model = SentenceTransformer(self.model_name)
        assert self._model is not None
        return self._model

    def encode(self, texts: Sequence[str]) -> np.ndarray:
        model = self._ensure_model()
        vectors = model.encode(
            texts,
            convert_to_numpy=True,
            normalize_embeddings=True,
            batch_size=self.batch_size,
            show_progress_bar=False,
        )
        return np.asarray(vectors, dtype=np.float32)
