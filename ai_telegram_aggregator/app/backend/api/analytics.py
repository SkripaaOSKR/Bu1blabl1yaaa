from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.backend.api.deps import get_admin_user_id, get_session
from app.backend.services.data_service import DataService

router = APIRouter(prefix="/api/analytics", tags=["analytics"])


@router.get("/daily")
async def daily_stats(
    db: AsyncSession = Depends(get_session), 
    user_id: int = Depends(get_admin_user_id)
) -> dict:
    """
    Сбор сводной статистики для главного экрана админки.
    
    Возвращает:
    - daily: статистика по дням (total, duplicates, published).
    - top_sources: список самых активных каналов.
    - top_tags: список самых популярных хэштегов.
    """
    svc = DataService(db)
    
    # 1. Получаем статистику по дням за последние 30 дней
    daily = await svc.analytics_daily()
    
    # 2. Получаем топ-10 источников по количеству сообщений
    source_query = text("""
        SELECT 
            id, 
            channel, 
            total_messages, 
            published_messages, 
            duplicate_count,
            spam_count
        FROM sources 
        ORDER BY total_messages DESC 
        LIMIT 10
    """)
    source_rows = await db.execute(source_query)
    top_sources = [dict(r._mapping) for r in source_rows.fetchall()]
    
    # 3. Получаем топ-20 тегов по частоте использования
    tag_query = text("""
        SELECT 
            name, 
            usage_count 
        FROM tags 
        ORDER BY usage_count DESC 
        LIMIT 20
    """)
    tag_rows = await db.execute(tag_query)
    top_tags = [dict(r._mapping) for r in tag_rows.fetchall()]
    
    # 4. Логируем действие администратора
    await svc.log_action(user_id, "analytics.daily_view", {})
    
    return {
        "daily": daily, 
        "top_sources": top_sources, 
        "top_tags": top_tags
    }