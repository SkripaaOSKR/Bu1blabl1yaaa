"""Persistent FAISS index utilities."""
from __future__ import annotations

from pathlib import Path

import faiss
import numpy as np


class FaissStore:
    """Manages loading, saving, and querying FAISS index with explicit IDs."""

    def __init__(self, index_path: Path, dim: int = 384) -> None:
        self.index_path = index_path
        self.dim = dim
        self.index = self._load_or_create()

    def _new_index(self) -> faiss.Index:
        base = faiss.IndexFlatIP(self.dim)
        return faiss.IndexIDMap2(base)

    def _load_or_create(self) -> faiss.Index:
        if self.index_path.exists():
            loaded = faiss.read_index(str(self.index_path))
            if isinstance(loaded, faiss.IndexIDMap2):
                return loaded
            idmap = self._new_index()
            if loaded.ntotal > 0:
                vecs = np.vstack([loaded.reconstruct(i) for i in range(loaded.ntotal)]).astype("float32")
                ids = np.arange(1, loaded.ntotal + 1, dtype=np.int64)
                idmap.add_with_ids(vecs, ids)
            return idmap
        return self._new_index()

    @property
    def ntotal(self) -> int:
        return int(self.index.ntotal)

    def add_with_ids(self, vectors: np.ndarray, ids: np.ndarray) -> None:
        self.index.add_with_ids(vectors.astype("float32"), ids.astype("int64"))

    def search(self, vectors: np.ndarray, k: int = 5) -> tuple[np.ndarray, np.ndarray]:
        return self.index.search(vectors.astype("float32"), k)

    def persist(self) -> None:
        self.index_path.parent.mkdir(parents=True, exist_ok=True)
        faiss.write_index(self.index, str(self.index_path))
