from __future__ import annotations

import logging
from pathlib import Path

import faiss
import numpy as np
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class FaissStore:
    """Manages loading, validating, rebuilding, and persisting FAISS index."""

    def __init__(self, index_path: Path, dim: int = 384) -> None:
        self.index_path = index_path
        self.dim = dim
        self.index = self._load_or_create()

    def _new_index(self) -> faiss.IndexIDMap2:
        base = faiss.IndexFlatIP(self.dim)
        return faiss.IndexIDMap2(base)

    def _ensure_idmap2(self, loaded: faiss.Index) -> faiss.IndexIDMap2:
        if isinstance(loaded, faiss.IndexIDMap2):
            return loaded

        idmap = self._new_index()
        if loaded.ntotal > 0:
            vecs = np.vstack([loaded.reconstruct(i) for i in range(loaded.ntotal)]).astype("float32")
            ids = np.arange(1, loaded.ntotal + 1, dtype=np.int64)
            idmap.add_with_ids(vecs, ids)
        return idmap

    def _validate_dimension(self, index: faiss.IndexIDMap2) -> None:
        idx_dim = getattr(index, "d", None)
        if idx_dim is None:
            base = index.index if hasattr(index, "index") else None
            idx_dim = getattr(base, "d", None)
        if idx_dim is not None and int(idx_dim) != self.dim:
            raise ValueError(f"FAISS index dim mismatch: expected={self.dim} actual={idx_dim}")

    def _load_or_create(self) -> faiss.IndexIDMap2:
        if not self.index_path.exists():
            return self._new_index()

        try:
            loaded = faiss.read_index(str(self.index_path))
            idmap = self._ensure_idmap2(loaded)
            self._validate_dimension(idmap)
            return idmap
        except Exception:
            logger.exception("Failed to load FAISS index. Recreating a fresh IDMap2 index")
            return self._new_index()

    @property
    def ntotal(self) -> int:
        return int(self.index.ntotal)

    def add_with_ids(self, vectors: np.ndarray, ids: np.ndarray) -> None:
        if vectors.size == 0 or ids.size == 0:
            return
        vectors_f32 = np.ascontiguousarray(vectors, dtype=np.float32)
        ids_i64 = np.ascontiguousarray(ids, dtype=np.int64)
        if vectors_f32.shape[1] != self.dim:
            raise ValueError(f"Vector dimension mismatch: expected={self.dim} actual={vectors_f32.shape[1]}")
        self.index.add_with_ids(vectors_f32, ids_i64)

    def search(self, vectors: np.ndarray, k: int = 5) -> tuple[np.ndarray, np.ndarray]:
        if self.ntotal == 0:
            return np.zeros((vectors.shape[0], k), dtype=np.float32), -np.ones((vectors.shape[0], k), dtype=np.int64)
        query = np.ascontiguousarray(vectors, dtype=np.float32)
        return self.index.search(query, k)

    def persist(self) -> None:
        self.index_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.index_path.with_suffix(self.index_path.suffix + ".tmp")
        faiss.write_index(self.index, str(tmp_path))
        tmp_path.replace(self.index_path)

    async def rebuild_from_db(self, db: AsyncSession) -> None:
        rows = await db.execute(text("SELECT id, vector FROM embeddings ORDER BY id"))
        data = rows.fetchall()
        self.index = self._new_index()
        if data:
            ids = np.asarray([int(row.id) for row in data], dtype=np.int64)
            vectors = np.vstack([np.frombuffer(row.vector, dtype=np.float32) for row in data]).astype(np.float32)
            if vectors.shape[1] != self.dim:
                raise ValueError(f"Embedding dimension mismatch from DB: expected={self.dim} actual={vectors.shape[1]}")
            self.add_with_ids(vectors, ids)
        self.persist()
