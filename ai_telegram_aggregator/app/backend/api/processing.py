from __future__ import annotations

import asyncio

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.backend.api.deps import get_admin_user_id, get_session
from app.backend.models.schemas import ProcessingRunRequest
from app.backend.services.data_service import DataService
from app.backend.services.processing_service import ProcessingService

router = APIRouter(prefix="/api/processing", tags=["processing"])
_running_task: asyncio.Task | None = None
_running_service: ProcessingService | None = None


@router.post("/run")
async def run_batch(payload: ProcessingRunRequest, db: AsyncSession = Depends(get_session), user_id: int = Depends(get_admin_user_id)) -> dict:
    global _running_task, _running_service
    if _running_task and not _running_task.done():
        raise HTTPException(status_code=409, detail="Run already active")

    service = ProcessingService(db)
    _running_service = service
    _running_task = asyncio.create_task(service.run_batch(payload.hours))
    await DataService(db).log_action(user_id, "processing.run", payload.model_dump())
    return {"ok": True, "status": "started"}


@router.post("/cancel")
async def cancel_run(db: AsyncSession = Depends(get_session), user_id: int = Depends(get_admin_user_id)) -> dict:
    if _running_service is not None:
        _running_service.cancel()
    await DataService(db).log_action(user_id, "processing.cancel", {})
    return {"ok": True}


@router.get("/status")
async def status(db: AsyncSession = Depends(get_session), user_id: int = Depends(get_admin_user_id)) -> dict:
    state = await DataService(db).get_processing_state()
    running = _running_task is not None and not _running_task.done()
    await DataService(db).log_action(user_id, "processing.status", {})
    return {"running": running, "state": state}
