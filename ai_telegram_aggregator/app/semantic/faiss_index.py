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

    @property
    def size(self) -> int:
        return int(self.index.ntotal)

    def reset(self) -> None:
        self.index = faiss.IndexFlatIP(self.dim)

    def add(self, vectors: np.ndarray) -> int:
        start_pos = self.size
        self.index.add(vectors)
        return start_pos

    def search(self, vectors: np.ndarray, k: int = 5) -> tuple[np.ndarray, np.ndarray]:
        if self.size == 0:
            return np.empty((len(vectors), 0)), np.empty((len(vectors), 0), dtype=int)
        top_k = min(k, self.size)
        return self.index.search(vectors, top_k)

    def persist(self) -> None:
        self.index_path.parent.mkdir(parents=True, exist_ok=True)
        faiss.write_index(self.index, str(self.index_path))
