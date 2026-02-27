"""Sentence embedding wrapper."""
from __future__ import annotations

from typing import Iterable

import numpy as np
from sentence_transformers import SentenceTransformer


class EmbeddingModel:
    """Thin wrapper over sentence-transformers model."""

    def __init__(self, model_name: str) -> None:
        self.model = SentenceTransformer(model_name)

    def encode(self, texts: Iterable[str]) -> np.ndarray:
        vectors = self.model.encode(list(texts), convert_to_numpy=True, normalize_embeddings=True)
        return vectors.astype("float32")
