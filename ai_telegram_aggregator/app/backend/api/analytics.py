from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.backend.api.deps import get_admin_user_id, get_session
from app.backend.services.data_service import DataService

router = APIRouter(prefix="/api/analytics", tags=["analytics"])


@router.get("/daily")
async def daily_stats(db: AsyncSession = Depends(get_session), user_id: int = Depends(get_admin_user_id)) -> dict:
    svc = DataService(db)
    daily = await svc.analytics_daily()
    source_rows = await db.execute(
        text(
            """
            SELECT channel, total_messages, published_messages, duplicate_count
            FROM sources ORDER BY total_messages DESC LIMIT 10
            """
        )
    )
    top_sources = [dict(r._mapping) for r in source_rows.fetchall()]
    tag_rows = await db.execute(text("SELECT name, usage_count FROM tags ORDER BY usage_count DESC LIMIT 20"))
    top_tags = [dict(r._mapping) for r in tag_rows.fetchall()]
    await svc.log_action(user_id, "analytics.daily", {})
    return {"daily": daily, "top_sources": top_sources, "top_tags": top_tags}
