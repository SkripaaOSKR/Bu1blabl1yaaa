from __future__ import annotations

import logging
import threading
from pathlib import Path

import faiss
import numpy as np
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

class FaissStore:
    """
    Управление векторной базой FAISS.
    Использует IndexIDMap2 для связи векторов с ID сообщений из БД.
    """
    _instance = None
    _lock = threading.Lock()

    @classmethod
    def get_instance(cls, index_path: Path, dim: int = 384) -> FaissStore:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls(index_path, dim)
        return cls._instance

    def __init__(self, index_path: Path, dim: int = 384) -> None:
        if hasattr(self, '_initialized'): return
        
        self.index_path = index_path
        self.dim = dim
        # Загружаем или создаем новый
        self.index = self._load_or_create()
        self._initialized = True

    def _new_index(self) -> faiss.IndexIDMap2:
        """Создает новый пустой индекс с поддержкой ID."""
        # IndexFlatIP — поиск по косинусному сходству
        base_index = faiss.IndexFlatIP(self.dim)
        # Оборачиваем в IDMap2, чтобы можно было добавлять свои ID (из базы данных)
        return faiss.IndexIDMap2(base_index)

    def _load_or_create(self) -> faiss.IndexIDMap2:
        """Загружает индекс с диска и проверяет его тип."""
        if not self.index_path.exists():
            logger.info("FAISS index file not found. Creating fresh index.")
            return self._new_index()

        try:
            logger.info(f"Loading FAISS index from {self.index_path}")
            loaded = faiss.read_index(str(self.index_path))
            
            # ПРОВЕРКА ТИПА (ТО, ЧТО БЫЛО СЛОМАНО):
            # Если индекс загрузился без поддержки ID, принудительно оборачиваем его
            if not isinstance(loaded, faiss.IndexIDMap2):
                logger.warning("Loaded index is not an IDMap. Wrapping it now.")
                new_idmap = self._new_index()
                if loaded.ntotal > 0:
                    # Если в нем были данные, переносим их (редкий случай)
                    # Но обычно лучше просто пересобрать из БД
                    logger.error("Index type mismatch with data. Rebuild recommended.")
                return new_idmap
                
            return loaded
        except Exception as e:
            logger.error(f"Failed to load FAISS index: {e}. Creating fresh one.")
            return self._new_index()

    @property
    def ntotal(self) -> int:
        """Возвращает общее количество векторов в памяти."""
        return int(self.index.ntotal)

    def add_with_ids(self, vectors: np.ndarray, ids: np.ndarray) -> None:
        """Добавляет векторы в индекс, привязывая их к ID из PostgreSQL."""
        if vectors.size == 0 or ids.size == 0:
            return
            
        vectors_f32 = np.ascontiguousarray(vectors, dtype=np.float32)
        ids_i64 = np.ascontiguousarray(ids, dtype=np.int64)
        
        if vectors_f32.shape[1] != self.dim:
            raise ValueError(f"Vector dimension mismatch: {vectors_f32.shape[1]} != {self.dim}")
            
        with self._lock: # <--- ДОБАВЛЕНО
            self.index.add_with_ids(vectors_f32, ids_i64)

    def search(self, vectors: np.ndarray, k: int = 5) -> tuple[np.ndarray, np.ndarray]:
        """Ищет K самых похожих сообщений."""
        if self.ntotal == 0:
            # Если база пуста, возвращаем пустые результаты
            return (np.zeros((vectors.shape[0], k), dtype=np.float32), 
                    -np.ones((vectors.shape[0], k), dtype=np.int64))
            
        query = np.ascontiguousarray(vectors, dtype=np.float32)
        return self.index.search(query, k)

    def persist(self) -> None:
        """Сохраняет индекс из оперативной памяти на жесткий диск."""
        try:
            self.index_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = self.index_path.with_suffix(".tmp")
            
            with self._lock: # <--- ДОБАВЛЕНО
                faiss.write_index(self.index, str(tmp_path))
                
            tmp_path.replace(self.index_path)
            logger.info(f"FAISS index saved. Total vectors: {self.ntotal}")
        except Exception as e:
            logger.error(f"Failed to persist FAISS index: {e}")

    async def rebuild_from_db(self, db: AsyncSession) -> None:
        """Полная очистка и пересборка индекса из данных в PostgreSQL (Батчинг)."""
        logger.info("Rebuilding FAISS index from database...")
        
        with self._lock:
            self.index = self._new_index()
        
        # Загружаем эмбеддинги порциями по 10 000, чтобы не уронить сервер по памяти
        batch_size = 10000
        offset = 0
        
        while True:
            rows = await db.execute(
                text("SELECT id, vector FROM embeddings ORDER BY id LIMIT :limit OFFSET :offset"),
                {"limit": batch_size, "offset": offset}
            )
            data = rows.fetchall()
            
            if not data:
                break
                
            ids = np.asarray([int(row.id) for row in data], dtype=np.int64)
            vectors = np.vstack([np.frombuffer(row.vector, dtype=np.float32) for row in data]).astype(np.float32)
            
            if vectors.shape[1] != self.dim:
                logger.error(f"Database vectors dimension mismatch at offset {offset}!")
                break
                
            self.add_with_ids(vectors, ids)
            offset += batch_size
            logger.info(f"Loaded {offset} vectors into FAISS...")
            
        self.persist()