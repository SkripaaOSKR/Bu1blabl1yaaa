from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.backend.api.deps import get_admin_user_id, get_session
from app.backend.models.schemas import SettingsOut, SettingsUpdate
from app.backend.services.data_service import DataService

router = APIRouter(prefix="/api/settings", tags=["settings"])


@router.get("", response_model=SettingsOut)
async def get_settings(
    db: AsyncSession = Depends(get_session), 
    user_id: int = Depends(get_admin_user_id)
) -> dict:
    """
    Получение текущих глобальных настроек системы.
    Используется в админке на вкладке 'Settings'.
    """
    svc = DataService(db)
    # Загружаем настройки из таблицы settings (строка с ID=1)
    result = await svc.get_settings()
    
    # Логируем факт просмотра настроек
    await svc.log_action(user_id, "settings.get", {})
    
    return result


@router.patch("", response_model=SettingsOut)
async def update_settings(
    payload: SettingsUpdate, 
    db: AsyncSession = Depends(get_session), 
    user_id: int = Depends(get_admin_user_id)
) -> dict:
    """
    Обновление глобальных настроек.
    Позволяет менять:
    - dedupe_threshold (порог схожести 0.0 - 1.0)
    - batch_size (кол-во сообщений за раз 1 - 1000)
    - dedupe_window_days (глубина поиска дублей)
    - ai_prompt (кастомный промпт для ИИ)
    - и другие параметры.
    """
    svc = DataService(db)
    
    # Превращаем модель Pydantic в словарь, удаляя пустые поля (None)
    # Это позволяет обновлять только один параметр, не затрагивая остальные
    values = {k: v for k, v in payload.model_dump().items() if v is not None}
    
    if values:
        # Сохраняем изменения в базу данных
        await svc.update_settings(values)
        # Записываем в аудит, что именно и на что было изменено
        await svc.log_action(user_id, "settings.update", values)
    
    # ВОЗВРАЩАЕМ АКТУАЛЬНЫЕ ДАННЫЕ ВМЕСТО {"ok": True}
    return await svc.get_settings()