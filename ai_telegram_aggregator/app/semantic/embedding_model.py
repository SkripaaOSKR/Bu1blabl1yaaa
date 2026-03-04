"""Sentence embedding wrapper."""
from __future__ import annotations

from collections.abc import Sequence

import numpy as np
from sentence_transformers import SentenceTransformer


class EmbeddingModel:
    """Thin wrapper over sentence-transformers model."""

    def __init__(self, model_name: str, batch_size: int = 64) -> None:
        self.model = SentenceTransformer(model_name)
        self.batch_size = batch_size

    def encode(self, texts: Sequence[str]) -> np.ndarray:
        vectors = self.model.encode(
            texts,
            convert_to_numpy=True,
            normalize_embeddings=True,
            batch_size=self.batch_size,
            show_progress_bar=False,
        )
        return np.asarray(vectors, dtype=np.float32)
