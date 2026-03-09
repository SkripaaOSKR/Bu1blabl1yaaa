from __future__ import annotations

import logging
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.backend.api.deps import get_admin_user_id, get_session
from app.backend.services.data_service import DataService
from app.backend.services.processing_service import ProcessingService
from app.collector.telegram_client import TelegramCollector
from app.config import get_settings

router = APIRouter(prefix="/api/messages", tags=["messages"])
logger = logging.getLogger(__name__)

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
    """
    Получение списка сообщений для админки.
    Умная группировка: схлопывает медиа-альбомы в один пост.
    """
    svc = DataService(db)
    await svc.log_action(user_id, "messages.list", {"source_id": source_id, "tag": tag})
    
    raw_messages = await svc.list_messages(source_id, tag, date_from, date_to, limit)
    
    # --- СХЛОПЫВАЕМ АЛЬБОМЫ ---
    result = []
    album_map = {}
    
    for msg in raw_messages:
        mg_id = msg.get('media_group_id')
        if mg_id:
            if mg_id not in album_map:
                # Сохраняем первую найденную часть альбома
                album_map[mg_id] = msg
                result.append(msg)
            else:
                # Если у первой части не было текста, а у этой есть — забираем текст
                if not album_map[mg_id].get('text') and msg.get('text'):
                    album_map[mg_id]['text'] = msg.get('text')
                    album_map[mg_id]['fmt_text'] = msg.get('fmt_text')
        else:
            # Обычное одиночное сообщение
            result.append(msg)
            
    return result


@router.delete("/{message_id}")
async def delete_message(
    message_id: int, 
    db: AsyncSession = Depends(get_session), 
    user_id: int = Depends(get_admin_user_id)
) -> dict:
    """
    Пометка сообщения как СПАМ (Обучение системы).
    """
    svc = DataService(db)
    # Вместо физического удаления — помечаем как подтвержденный спам
    await svc.mark_confirmed_spam(message_id)
    await svc.log_action(user_id, "messages.mark_spam", {"message_id": message_id})
    return {"ok": True}


@router.post("/{message_id}/republish")
async def republish_message(
    message_id: int, 
    db: AsyncSession = Depends(get_session), 
    user_id: int = Depends(get_admin_user_id)
) -> dict:
    """
    Принудительная публикация сообщения (выход из Карантина).
    """
    # 1. ЗАЩИТА: Проверяем, не занят ли воркер
    # Если воркер работает, он держит файл сессии. Мы не можем мешать ему.
    state_res = await db.execute(text("SELECT last_run_status FROM processing_state WHERE id=1"))
    state_row = state_res.fetchone()
    
    if state_row and state_row.last_run_status == 'running':
        raise HTTPException(
            status_code=409, 
            detail="⚠️ Система сейчас собирает новости. Пожалуйста, подождите завершения цикла (10-30 сек) и попробуйте снова."
        )

    settings = get_settings()
    data_svc = DataService(db)
    
    # 2. Сначала получаем информацию о целевом сообщении
    base_query = text("""
        SELECT m.*, s.channel as source_channel, s.topic_id 
        FROM messages m 
        JOIN sources s ON s.id = m.source_id 
        WHERE m.id = :mid
    """)
    res = await db.execute(base_query, {"mid": message_id})
    target_msg = res.fetchone()
    
    if not target_msg:
        raise HTTPException(status_code=404, detail="Сообщение не найдено")

    target_dict = dict(target_msg._mapping)
    media_group_id = target_dict.get('media_group_id')

    # 3. Если это альбом, собираем все его части
    if media_group_id:
        album_query = text("""
            SELECT m.*, s.channel as source_channel, s.topic_id 
            FROM messages m 
            JOIN sources s ON s.id = m.source_id 
            WHERE m.media_group_id = :mgid
            ORDER BY m.id ASC
        """)
        album_res = await db.execute(album_query, {"mgid": media_group_id})
        messages_to_pub = [dict(r._mapping) for r in album_res.fetchall()]
    else:
        # Если не альбом, публикуем только одно это сообщение
        messages_to_pub = [target_dict]

    # 4. Публикация в Telegram
    try:
        async with TelegramCollector(
            settings.telegram_api_id, 
            settings.telegram_api_hash, 
            settings.telegram_session_name
        ) as collector:
            
            proc_svc = ProcessingService(db)
            
            # Вызываем метод публикации с флагом bypass_ai=True
            success = await proc_svc._publish_beautiful_post(
                collector=collector,
                channel_name=target_dict['source_channel'],
                messages=messages_to_pub,
                topic_id=target_dict['topic_id'],
                bypass_ai=True
            )
            
            if not success:
                raise HTTPException(status_code=500, detail="Ошибка отправки в Telegram (см. логи)")

    except Exception as e:
        logger.error(f"Republish error: {e}")
        await data_svc.log_action(user_id, "messages.republish_error", {"id": message_id, "error": str(e)})
        # Если это ошибка блокировки базы, даем понятное сообщение
        if "database is locked" in str(e):
             raise HTTPException(status_code=409, detail="База данных Telegram занята. Попробуйте через 5 секунд.")
        raise HTTPException(status_code=500, detail=f"Ошибка Telegram: {e}")

    # 5. Помечаем все опубликованные ID как is_published = TRUE
    pub_ids = [m['id'] for m in messages_to_pub]
    await db.execute(
        text("UPDATE messages SET is_published = TRUE WHERE id = ANY(:ids)"),
        {"ids": pub_ids}
    )
    await db.commit()
    
    await data_svc.log_action(user_id, "messages.republish_success", {"ids": pub_ids})
    
    return {"ok": True, "published_count": len(pub_ids)}