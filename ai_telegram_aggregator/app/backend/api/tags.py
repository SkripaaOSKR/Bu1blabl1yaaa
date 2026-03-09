from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.backend.api.deps import get_admin_user_id, get_session
from app.backend.models.schemas import TagMerge, TagToggle
from app.backend.services.data_service import DataService

router = APIRouter(prefix="/api/tags", tags=["tags"])


@router.get("")
async def list_tags(
    db: AsyncSession = Depends(get_session), 
    user_id: int = Depends(get_admin_user_id)
) -> list[dict]:
    """
    Получение списка всех тегов из базы данных.
    Используется в админке для управления фильтрами.
    """
    svc = DataService(db)
    # Логируем просмотр списка тегов
    await svc.log_action(user_id, "tags.list", {})
    # Возвращает список тегов, отсортированный по частоте использования
    return await svc.list_tags()


@router.post("/toggle")
async def toggle_tag(
    payload: TagToggle, 
    db: AsyncSession = Depends(get_session), 
    user_id: int = Depends(get_admin_user_id)
) -> dict:
    """
    Включение или блокировка тега.
    Если тег заблокирован (is_blocked=True), он перестает 
    добавляться в новые сообщения в Telegram.
    """
    svc = DataService(db)
    
    # Вызываем метод upsert, который создаст тег или обновит его статус
    await svc.upsert_tag(
        name=payload.name, 
        allowed=payload.is_allowed, 
        blocked=payload.is_blocked
    )
    
    # Записываем изменение в аудит
    await svc.log_action(user_id, "tags.toggle", payload.model_dump())
    
    return {"ok": True}


@router.post("/merge")
async def merge_tag(
    payload: TagMerge, 
    db: AsyncSession = Depends(get_session), 
    user_id: int = Depends(get_admin_user_id)
) -> dict:
    """
    Слияние двух тегов в один.
    Все сообщения, помеченные тегом 'from_name', 
    теперь будут помечены тегом 'to_name'.
    """
    svc = DataService(db)
    
    # Выполняем перенос связей в базе данных
    await svc.merge_tags(
        from_name=payload.from_name, 
        to_name=payload.to_name
    )
    
    # Логируем слияние
    await svc.log_action(user_id, "tags.merge", payload.model_dump())
    
    return {"ok": True}