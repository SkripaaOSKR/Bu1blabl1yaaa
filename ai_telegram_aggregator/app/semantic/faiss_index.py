"""Persistent FAISS index utilities."""
from __future__ import annotations

from pathlib import Path

import faiss
import numpy as np


class FaissStore:
    """Manages loading, saving, and querying FAISS index."""

    def __init__(self, index_path: Path, dim: int = 384) -> None:
        self.index_path = index_path
        self.dim = dim
        self.index = self._load_or_create()

    def _load_or_create(self) -> faiss.Index:
        if self.index_path.exists():
            return faiss.read_index(str(self.index_path))
        return faiss.IndexFlatIP(self.dim)

    def add(self, vectors: np.ndarray) -> None:
        self.index.add(vectors)

    def search(self, vectors: np.ndarray, k: int = 5) -> tuple[np.ndarray, np.ndarray]:
        return self.index.search(vectors, k)

    def persist(self) -> None:
        self.index_path.parent.mkdir(parents=True, exist_ok=True)
        faiss.write_index(self.index, str(self.index_path))
