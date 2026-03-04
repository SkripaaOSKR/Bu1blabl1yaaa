from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.backend.api.deps import get_admin_user_id, get_session
from app.backend.services.data_service import DataService

router = APIRouter(prefix="/api/messages", tags=["messages"])


@router.get("")
async def list_messages(
    source_id: int | None = None,
    tag: str | None = None,
    date_from: datetime | None = Query(default=None),
    date_to: datetime | None = Query(default=None),
    limit: int = 200,
    db: AsyncSession = Depends(get_session),
    user_id: int = Depends(get_admin_user_id),
) -> list[dict]:
    svc = DataService(db)
    await svc.log_action(user_id, "messages.list", {"source_id": source_id, "tag": tag})
    return await svc.list_messages(source_id, tag, date_from, date_to, limit)


@router.delete("/{message_id}")
async def delete_message(message_id: int, db: AsyncSession = Depends(get_session), user_id: int = Depends(get_admin_user_id)) -> dict:
    svc = DataService(db)
    await svc.delete_message(message_id)
    await svc.log_action(user_id, "messages.delete", {"message_id": message_id})
    return {"ok": True}


@router.post("/{message_id}/republish")
async def republish_message(message_id: int, db: AsyncSession = Depends(get_session), user_id: int = Depends(get_admin_user_id)) -> dict:
    svc = DataService(db)
    await svc.mark_published(message_id)
    await svc.log_action(user_id, "messages.republish", {"message_id": message_id})
    return {"ok": True}
