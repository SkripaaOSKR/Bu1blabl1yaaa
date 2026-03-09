from __future__ import annotations

import logging
from typing import List
from app.broker import broker
from app.backend.db.session import SessionLocal
from app.backend.services.processing_service import ProcessingService
from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

@broker.task
async def process_news_task(message_ids: List[int]) -> bool:
    """
    Фоновая задача для обработки и публикации новости.
    Принимает список ID сообщений (из одной группы/альбома),
    выполняет ИИ-анализ и отправляет в Telegram.
    """
    if not message_ids:
        return False

    logger.info(f"🚀 Starting background task for messages: {message_ids}")
    
    # Открываем отдельную сессию БД для этой задачи
    async with SessionLocal() as db:
        try:
            # Инициализируем сервис обработки
            service = ProcessingService(db)
            await service.ensure_initialized()
            
            # 1. Загружаем данные сообщений из БД по их ID
            # Мы передаем ID, чтобы не пересылать тяжелые объекты через Redis
            result = await service.process_specific_messages(message_ids)
            
            if result:
                logger.info(f"✅ Task completed successfully for {message_ids}")
                return True
            else:
                logger.warning(f"⚠️ Task finished without publication for {message_ids}")
                return False

        except Exception as e:
            logger.error(f"❌ Error in process_news_task: {e}", exc_info=True)
            return False

@broker.task
async def sync_faiss_task() -> None:
    """
    Задача для периодической синхронизации FAISS индекса с базой данных.
    """
    async with SessionLocal() as db:
        try:
            service = ProcessingService(db)
            await service.sync_faiss_index()
            logger.info("📡 FAISS index synchronized in background.")
        except Exception as e:
            logger.error(f"❌ FAISS sync task failed: {e}")