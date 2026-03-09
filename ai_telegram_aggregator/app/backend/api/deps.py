from __future__ import annotations

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.backend.db.session import get_db
from app.backend.security.auth import telegram_auth_guard


def get_admin_user_id(user_id: int = Depends(telegram_auth_guard)) -> int:
    """
    Зависимость для защиты эндпоинтов.
    Проверяет X-Telegram-Auth заголовок и возвращает ID админа.
    Если проверка не пройдена, FastAPI автоматически вернет 401/403 ошибку.
    """
    return user_id


async def get_session(session: AsyncSession = Depends(get_db)) -> AsyncSession:
    """
    Зависимость для получения асинхронной сессии базы данных.
    FastAPI сам создаст сессию в начале запроса и закроет её в конце.
    """
    return session