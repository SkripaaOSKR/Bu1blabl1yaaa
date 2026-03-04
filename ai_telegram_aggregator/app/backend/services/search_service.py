from __future__ import annotations

import numpy as np
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.backend.services.faiss_store import FaissStore
from app.backend.services.nlp import EmbeddingModel


class SearchService:
    def __init__(self, db: AsyncSession, model_name: str, faiss_path) -> None:
        self.db = db
        self.embedder = EmbeddingModel(model_name)
        self.faiss = FaissStore(faiss_path)

    async def semantic_search(self, query: str, limit: int, source_id: int | None, tag: str | None) -> list[dict]:
        vector = self.embedder.encode([query])[0]
        scores, ids = self.faiss.search(np.ascontiguousarray(vector.reshape(1, -1), dtype=np.float32), k=max(limit, 5))
        emb_ids = [int(i) for i in ids[0] if int(i) > 0]
        if not emb_ids:
            return []

        rows = await self.db.execute(
            text(
                """
                SELECT m.*, e.id as embedding_id
                FROM messages m
                JOIN embeddings e ON e.id=m.embedding_id
                LEFT JOIN message_tags mt ON mt.message_id=m.id
                LEFT JOIN tags t ON t.id=mt.tag_id
                WHERE m.embedding_id = ANY(:ids)
                  AND (:source_id IS NULL OR m.source_id=:source_id)
                  AND (:tag IS NULL OR t.name=:tag)
                LIMIT :limit
                """
            ),
            {"ids": emb_ids, "source_id": source_id, "tag": tag, "limit": limit},
        )
        score_map = {int(eid): float(score) for eid, score in zip(ids[0], scores[0], strict=False) if int(eid) > 0}
        result = []
        for row in rows.fetchall():
            item = dict(row._mapping)
            item["similarity"] = score_map.get(int(item["embedding_id"]), 0.0)
            result.append(item)

        result.sort(key=lambda x: x["similarity"], reverse=True)
        return result[:limit]
