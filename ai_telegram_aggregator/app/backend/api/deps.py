from __future__ import annotations

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.backend.db.session import get_db
from app.backend.security.auth import admin_guard


def get_admin_user_id(user_id: int = Depends(admin_guard)) -> int:
    return user_id


def get_session(session: AsyncSession = Depends(get_db)) -> AsyncSession:
    return session
