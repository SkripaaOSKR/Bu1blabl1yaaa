from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.backend.api.deps import get_admin_user_id, get_session
from app.backend.models.schemas import SearchRequest
from app.backend.services.data_service import DataService
from app.backend.services.search_service import SearchService
from app.config import get_settings

router = APIRouter(prefix="/api/search", tags=["search"])


@router.post("")
async def semantic_search(payload: SearchRequest, db: AsyncSession = Depends(get_session), user_id: int = Depends(get_admin_user_id)) -> list[dict]:
    settings = get_settings()
    svc = SearchService(db, settings.embedding_model_name, settings.faiss_index_path)
    data = await svc.semantic_search(payload.query, payload.limit, payload.source_id, payload.tag)
    await DataService(db).log_action(user_id, "search.semantic", payload.model_dump())
    return data
