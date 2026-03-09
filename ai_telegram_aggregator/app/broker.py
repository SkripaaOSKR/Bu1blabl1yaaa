from __future__ import annotations

import logging
from taskiq_redis import ListQueueBroker, RedisAsyncResultBackend
from app.config import get_settings

# Настраиваем логгер для брокера
logger = logging.getLogger(__name__)
settings = get_settings()

result_backend = RedisAsyncResultBackend(
    redis_url=settings.redis_url,
    keep_results=True,          # Хранить результат выполнения
    result_ttl=86400,           # Срок жизни результата — 24 часа
)

broker = ListQueueBroker(
    url=settings.redis_url,
).with_result_backend(result_backend)