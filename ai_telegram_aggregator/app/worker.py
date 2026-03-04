from __future__ import annotations

import asyncio
import logging

from app.backend.db.session import SessionLocal
from app.backend.services.processing_service import ProcessingService
from app.config import get_settings

logger = logging.getLogger(__name__)


async def run_worker_loop(interval_seconds: int = 60) -> None:
    settings = get_settings()
    logger.info("Starting worker loop with interval=%s seconds", interval_seconds)

    while True:
        try:
            async with SessionLocal() as db:
                service = ProcessingService(db)
                result = await service.run_batch(hours=None)
                logger.info("Worker batch completed: %s", result)
        except Exception:
            logger.exception("Worker batch failed")

        await asyncio.sleep(interval_seconds)


def main() -> None:
    logging.basicConfig(level=getattr(logging, get_settings().log_level.upper(), logging.INFO))
    asyncio.run(run_worker_loop())


if __name__ == "__main__":
    main()
