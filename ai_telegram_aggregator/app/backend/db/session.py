from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from app.config import get_settings

settings = get_settings()

# Создаем асинхронный движок
engine = create_async_engine(
    settings.postgres_dsn,
    future=True,
    # pool_pre_ping=True проверяет соединение перед каждым запросом. 
    # Если база перезагрузилась, бот не упадет, а просто переподключится.
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
    # Тайм-аут для предотвращения зависших транзакций
    pool_recycle=3600
)

# Настройка фабрики сессий
SessionLocal = async_sessionmaker(
    bind=engine,
    expire_on_commit=False,
    class_=AsyncSession
)

async def get_db():
    """Асинхронный генератор сессий для FastAPI."""
    async with SessionLocal() as session:
        try:
            yield session
        finally:
            # Гарантируем закрытие сессии в любом случае
            await session.close()