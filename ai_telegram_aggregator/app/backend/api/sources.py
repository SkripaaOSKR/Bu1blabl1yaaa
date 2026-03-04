from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.backend.api.deps import get_admin_user_id, get_session
from app.backend.models.schemas import SourceCreate, SourceUpdate
from app.backend.services.data_service import DataService

router = APIRouter(prefix="/api/sources", tags=["sources"])


@router.get("")
async def list_sources(db: AsyncSession = Depends(get_session), user_id: int = Depends(get_admin_user_id)) -> list[dict]:
    svc = DataService(db)
    rows = await svc.list_sources()
    await svc.log_action(user_id, "sources.list", {})
    return rows


@router.post("")
async def add_source(payload: SourceCreate, db: AsyncSession = Depends(get_session), user_id: int = Depends(get_admin_user_id)) -> dict:
    svc = DataService(db)
    row = await svc.add_source(payload.channel, payload.priority, payload.category, payload.language)
    await svc.log_action(user_id, "sources.add", payload.model_dump())
    return row


@router.patch("/{source_id}")
async def update_source(source_id: int, payload: SourceUpdate, db: AsyncSession = Depends(get_session), user_id: int = Depends(get_admin_user_id)) -> dict:
    svc = DataService(db)
    await svc.update_source(source_id, {k: v for k, v in payload.model_dump().items() if v is not None})
    await svc.log_action(user_id, "sources.update", {"source_id": source_id, **payload.model_dump(exclude_none=True)})
    return {"ok": True}


@router.delete("/{source_id}")
async def remove_source(source_id: int, db: AsyncSession = Depends(get_session), user_id: int = Depends(get_admin_user_id)) -> dict:
    svc = DataService(db)
    await svc.remove_source(source_id)
    await svc.log_action(user_id, "sources.remove", {"source_id": source_id})
    return {"ok": True}
