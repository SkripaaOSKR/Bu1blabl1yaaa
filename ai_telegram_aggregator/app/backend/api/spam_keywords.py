from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.backend.api.deps import get_admin_user_id, get_session
from app.backend.models.schemas import SpamKeywordCreate, SpamKeywordUpdate
from app.backend.services.data_service import DataService

router = APIRouter(prefix="/api/spam_keywords", tags=["spam_keywords"])


@router.get("")
async def list_keywords(
    db: AsyncSession = Depends(get_session), 
    user_id: int = Depends(get_admin_user_id)
) -> list[dict]:
    """Получение списка всех стоп-слов для фильтра спама."""
    svc = DataService(db)
    # Логируем просмотр фильтров
    await svc.log_action(user_id, "spam_keywords.list", {})
    return await svc.list_spam_keywords()


@router.post("")
async def add_keyword(
    payload: SpamKeywordCreate, 
    db: AsyncSession = Depends(get_session), 
    user_id: int = Depends(get_admin_user_id)
) -> dict:
    """Добавление нового слова в систему фильтрации."""
    svc = DataService(db)
    
    # Добавляем слово через сервис
    row = await svc.add_spam_keyword(payload.word)
    
    # Логируем добавление
    await svc.log_action(user_id, "spam_keywords.add", payload.model_dump())
    
    return row


@router.patch("/{keyword_id}/toggle")
async def toggle_keyword(
    keyword_id: int, 
    db: AsyncSession = Depends(get_session), 
    user_id: int = Depends(get_admin_user_id)
) -> dict:
    """
    Переключение статуса слова (активно/выключено).
    Находит текущий статус и меняет его на противоположный.
    """
    svc = DataService(db)
    
    # 1. Сначала найдем текущее состояние слова
    from sqlalchemy import text
    res = await db.execute(
        text("SELECT is_active FROM spam_keywords WHERE id = :id"),
        {"id": keyword_id}
    )
    current = res.fetchone()
    
    if not current:
        raise HTTPException(status_code=404, detail="Слово не найдено в базе")
    
    # 2. Меняем статус на противоположный
    new_status = not current.is_active
    await svc.toggle_spam_keyword(keyword_id, new_status)
    
    # Логируем
    await svc.log_action(user_id, "spam_keywords.toggle", {"id": keyword_id, "new_status": new_status})
    
    return {"ok": True, "is_active": new_status}


@router.delete("/{keyword_id}")
async def remove_keyword(
    keyword_id: int, 
    db: AsyncSession = Depends(get_session), 
    user_id: int = Depends(get_admin_user_id)
) -> dict:
    """Полное удаление слова из базы фильтров."""
    svc = DataService(db)
    
    await svc.remove_spam_keyword(keyword_id)
    await svc.log_action(user_id, "spam_keywords.remove", {"id": keyword_id})
    
    return {"ok": True}