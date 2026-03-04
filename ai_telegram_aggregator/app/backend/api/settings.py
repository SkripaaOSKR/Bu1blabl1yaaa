from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.backend.api.deps import get_admin_user_id, get_session
from app.backend.models.schemas import SettingsUpdate
from app.backend.services.data_service import DataService

router = APIRouter(prefix="/api/settings", tags=["settings"])


@router.get("")
async def get_settings(db: AsyncSession = Depends(get_session), user_id: int = Depends(get_admin_user_id)) -> dict:
    svc = DataService(db)
    result = await svc.get_settings()
    await svc.log_action(user_id, "settings.get", {})
    return result


@router.patch("")
async def update_settings(payload: SettingsUpdate, db: AsyncSession = Depends(get_session), user_id: int = Depends(get_admin_user_id)) -> dict:
    svc = DataService(db)
    values = {k: v for k, v in payload.model_dump().items() if v is not None}
    await svc.update_settings(values)
    await svc.log_action(user_id, "settings.update", values)
    return {"ok": True}
