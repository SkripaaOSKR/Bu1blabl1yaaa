from __future__ import annotations

import asyncio
import logging
import sys
import sentry_sdk
import redis.asyncio as aioredis

from app.backend.db.session import SessionLocal, engine
from app.backend.db.schema import init_postgres_schema
from app.backend.services.processing_service import ProcessingService
from app.config import get_settings
from app.utils.logger import setup_logger

settings = get_settings()

# --- ИНИЦИАЛИЗАЦИЯ ЧЕРНОГО ЯЩИКА ---
# 1. Локальные логи (запись в файл logs/blackbox.log)
setup_logger(settings.log_level)
logger = logging.getLogger("worker")

# 2. Облачные логи (Sentry)
if settings.sentry_dsn:
    sentry_sdk.init(
        dsn=settings.sentry_dsn,
        environment=settings.environment,
        traces_sample_rate=1.0,  # Записывать 100% ошибок
    )
    logger.info("Sentry (Cloud Black Box) is initialized and active in Worker.")
# -----------------------------------

async def run_collector_loop(interval_seconds: int = 600) -> None:
    """Бесконечный цикл СБОРЩИКА с защитой от переполнения Redis."""
    logger.info("🛠 Checking database schema...")
    try:
        await init_postgres_schema(engine)
        logger.info("✅ Database schema initialized.")
    except Exception as e:
        logger.error(f"❌ Failed to init database: {e}")

    logger.info(f"🚀 Collector started. Interval: {interval_seconds}s")
    redis_client = aioredis.from_url(settings.redis_url)

    while True:
        try:
            # ЗАЩИТА: Проверяем, не задыхается ли ИИ (Redis)
            try:
                # TaskIQ хранит задачи по умолчанию в этом ключе
                queue_len = await redis_client.llen("taskiq:queue:default")
                if queue_len > 500:
                    logger.warning(f"⚠️ Redis queue is full ({queue_len} tasks). Collector is pausing for 3 minutes...")
                    await asyncio.sleep(180)
                    continue
            except Exception as e:
                logger.warning(f"Could not check Redis queue length: {e}")

            async with SessionLocal() as db:
                service = ProcessingService(db)
                logger.info("🔄 Starting news collection cycle...")
                result = await service.run_batch(hours=None)
                logger.info(f"✅ Collection cycle completed: {result}")
                
        except Exception:
            logger.exception("❌ CRITICAL ERROR in collector loop")

        await asyncio.sleep(interval_seconds)

async def main() -> None:
    """Точка входа для Сборщика (Collector)."""
    logger.info("⚡ Starting standalone Collector process...")
    try:
        await run_collector_loop()
    except asyncio.CancelledError:
        logger.info("🛑 Collector tasks cancelled.")
    except KeyboardInterrupt:
        logger.info("🛑 Collector stopped by user.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass