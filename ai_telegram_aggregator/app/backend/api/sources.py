from __future__ import annotations

import re
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.backend.api.deps import get_admin_user_id, get_session
from app.backend.models.schemas import SourceCreate, SourceUpdate
from app.backend.services.data_service import DataService

router = APIRouter(prefix="/api/sources", tags=["sources"])


@router.get("")
async def list_sources(db: AsyncSession = Depends(get_session), user_id: int = Depends(get_admin_user_id)) -> list[dict]:
    """
    Получение списка всех источников.
    Используется админкой для отображения таблицы каналов и их статистики.
    """
    svc = DataService(db)
    rows = await svc.list_sources()
    # Логируем просмотр списка (опционально для аудита)
    await svc.log_action(user_id, "sources.list", {})
    return rows


@router.post("")
async def add_source(payload: SourceCreate, db: AsyncSession = Depends(get_session), user_id: int = Depends(get_admin_user_id)) -> dict:
    """
    Добавление нового канала-источника.
    
    Включает мощную логику нормализации:
    - Превращает 'https://t.me/username' в '@username'
    - Превращает 'https://t.me/+hash' в '+hash' (приватные ссылки)
    - Превращает '123456789' в '-100123456789' (ID каналов)
    - Удаляет параметры запроса вроде ?boost=1
    """
    svc = DataService(db)
    
    # 1. Очистка входных данных от пробелов
    channel = payload.channel.strip()
    
    # 2. УМНАЯ НОРМАЛИЗАЦИЯ
    # Удаляем протоколы (http/https) и домены (t.me / telegram.me)
    channel = re.sub(r'^https?://(www\.)?(t\.me|telegram\.me)/', '', channel)
    
    # Удаляем параметры после знака вопроса (UTM-метки, параметры входа и т.д.)
    channel = channel.split('?')[0].strip('/')
    
    # Обработка старых форматов ссылок приглашения
    if channel.startswith('joinchat/'):
        channel = '+' + channel[len('joinchat/'):]
        
    # Приведение к итоговому стандарту для Telethon
    if channel.startswith('+') or channel.startswith('@'):
        # Уже корректный формат для приватных или публичных каналов
        pass
    elif channel.lstrip('-').isdigit():
        # Если это числовой ID (например, 2549200647)
        # Для каналов в Telegram ID всегда начинается с -100
        if not channel.startswith('-100') and len(channel.lstrip('-')) > 8:
            channel = f"-100{channel.lstrip('-')}"
    else:
        # Если передан просто текст (напр. 'durov'), добавляем @
        channel = f"@{channel}"
    
    # 3. СОХРАНЕНИЕ В БАЗУ
    # Передаем topic_id, чтобы новости сразу летели в нужную ветку (ветка может быть None)
    row = await svc.add_source(
        channel=channel, 
        priority=payload.priority, 
        category=payload.category, 
        language=payload.language,
        topic_id=payload.topic_id
        title=payload.title
    )
    
    # 4. АУДИТ
    # Сохраняем в лог как исходную ссылку, так и результат нормализации
    log_payload = payload.model_dump()
    log_payload["normalized_result"] = channel
    await svc.log_action(user_id, "sources.add", log_payload)
    
    return row


@router.patch("/{source_id}")
async def update_source(source_id: int, payload: SourceUpdate, db: AsyncSession = Depends(get_session), user_id: int = Depends(get_admin_user_id)) -> dict:
    """
    Частичное обновление настроек источника.
    Позволяет включить/выключить канал или сменить ему ID ветки (Topic).
    """
    svc = DataService(db)
    
    # Модели Pydantic позволяют получить только те поля, которые прислал клиент
    update_values = payload.model_dump(exclude_none=True)
    
    if update_values:
        await svc.update_source(source_id, update_values)
        # Записываем в аудит, что именно изменилось
        await svc.log_action(user_id, "sources.update", {"source_id": source_id, **update_values})
    
    return {"ok": True}


@router.delete("/{source_id}")
async def remove_source(source_id: int, db: AsyncSession = Depends(get_session), user_id: int = Depends(get_admin_user_id)) -> dict:
    """
    Удаление источника.
    В базе настроено каскадное удаление (ON DELETE CASCADE), 
    поэтому все сообщения этого канала тоже удалятся.
    """
    svc = DataService(db)
    await svc.remove_source(source_id)
    
    # Записываем в аудит
    await svc.log_action(user_id, "sources.remove", {"source_id": source_id})
    
    return {"ok": True}