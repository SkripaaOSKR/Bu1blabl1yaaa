from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.backend.api.deps import get_admin_user_id, get_session
from app.backend.models.schemas import TagMerge, TagToggle
from app.backend.services.data_service import DataService

router = APIRouter(prefix="/api/tags", tags=["tags"])


@router.get("")
async def list_tags(db: AsyncSession = Depends(get_session), user_id: int = Depends(get_admin_user_id)) -> list[dict]:
    svc = DataService(db)
    await svc.log_action(user_id, "tags.list", {})
    return await svc.list_tags()


@router.post("/toggle")
async def toggle_tag(payload: TagToggle, db: AsyncSession = Depends(get_session), user_id: int = Depends(get_admin_user_id)) -> dict:
    svc = DataService(db)
    await svc.upsert_tag(payload.name, payload.is_allowed, payload.is_blocked)
    await svc.log_action(user_id, "tags.toggle", payload.model_dump())
    return {"ok": True}


@router.post("/merge")
async def merge_tag(payload: TagMerge, db: AsyncSession = Depends(get_session), user_id: int = Depends(get_admin_user_id)) -> dict:
    svc = DataService(db)
    await svc.merge_tags(payload.from_name, payload.to_name)
    await svc.log_action(user_id, "tags.merge", payload.model_dump())
    return {"ok": True}
