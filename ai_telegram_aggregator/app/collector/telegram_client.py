"""Telegram collection and publication adapters."""
from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator
from telethon.sessions import StringSession

from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError, 
    UserAlreadyParticipantError, 
    InviteHashExpiredError,
    ChannelPrivateError
)
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.extensions import html 

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class CollectedMessage:
    """Структура данных для собранного сообщения."""
    source: str              # Юзернейм или ID канала
    text: str                # Чистый текст
    fmt_text: str            # Текст с HTML-разметкой
    created_at: datetime     # Дата поста
    media_group_id: str | None # ID альбома
    message_id: int          # ID сообщения
    media_type: str | None   # Тип вложения


class TelegramCollector:
    """Класс для сбора сообщений из каналов и публикации результатов."""

    def __init__(self, api_id: int, api_hash: str, session_name: str, string_session: str = "") -> None:
        self.api_id = api_id
        self.api_hash = api_hash
        session = StringSession(string_session) if string_session else session_name
        self.client = TelegramClient(session, api_id, api_hash)

    async def __aenter__(self) -> "TelegramCollector":
        """Запуск клиента."""
        await self.client.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        """Отключение клиента."""
        await self.client.disconnect()

    async def iter_messages(
        self,
        channels: list[str],
        hours: int,
        since_timestamp: datetime | None = None,
    ) -> AsyncIterator[CollectedMessage]:
        """
        Асинхронный генератор сообщений.
        Умеет вступать в каналы и искать их через глобальный поиск.
        """
        since = since_timestamp if since_timestamp is not None else datetime.now(timezone.utc) - timedelta(hours=hours)
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)

        dialogs_fetched = False 

        for channel in channels:
            if isinstance(channel, str) and channel.lstrip('-').isdigit():
                channel = int(channel)
                
            retries = 0
            last_id = 0 
            
            while retries < 3:
                try:
                    entity = None
                    
                    # ✅ ИЗМЕНЕНИЕ 1: Добавляем твою умную проверку перед вступлением
                    # -----------------------------------------------------------------
                    try:
                        # Ищем канал через глобальный поиск Telegram, чтобы узнать его статус
                        target = channel
                        if isinstance(channel, str) and channel.startswith('+'):
                            target = f"https://t.me/{channel}"
                            
                        entity = await self.client.get_entity(target)
                        
                        # Если нашли, но мы НЕ состоим в канале (свойство left = True),
                        # то сбрасываем entity, чтобы следующий блок кода его подписал.
                        if hasattr(entity, 'left') and entity.left:
                            logger.info(f"Канал {channel} найден, но мы не участник. Готовимся к вступлению.")
                            entity = None # Сброс заставит сработать следующий блок
                            
                    except Exception:
                        # Если канал не найден (например, приватный по ID) — не страшно.
                        # entity останется None, и мы перейдем к блоку вступления.
                        pass

                    # -----------------------------------------------------------------
                    # ЭТАП 2: ВСТУПЛЕНИЕ, ЕСЛИ КАНАЛ НЕ НАЙДЕН ИЛИ МЫ НЕ В НЕМ
                    # -----------------------------------------------------------------
                    if not entity:
                        logger.info(f"Не удалось получить быстрый доступ к {channel}. Запускаю процедуру вступления...")
                        # А) Ссылка-приглашение (Приватный)
                        if isinstance(channel, str) and ("t.me/+" in channel or "joinchat/" in channel or channel.startswith('+')):
                            hash_match = re.search(r'(?:\+|joinchat/)([\w-]+)', channel)
                            if hash_match:
                                try:
                                    updates = await self.client(ImportChatInviteRequest(hash_match.group(1)))
                                    if updates and hasattr(updates, 'chats') and updates.chats:
                                        entity = updates.chats[0]
                                    await asyncio.sleep(2)
                                except UserAlreadyParticipantError: pass
                                except Exception as e: logger.error(f"Не удалось вступить по инвайту {channel}: {e}")

                        # Б) Юзернейм (Публичный)
                        elif isinstance(channel, str) and ("t.me/" in channel or channel.startswith('@')):
                            username = channel.split('/')[-1].replace('@', '')
                            try:
                                await self.client(JoinChannelRequest(username))
                                await asyncio.sleep(2)
                            except UserAlreadyParticipantError: pass
                            except Exception as e: logger.warning(f"Не удалось вступить в @{username}: {e}")
                        
                        # После попытки вступления, снова получаем entity. Теперь он должен найтись.
                        try:
                            if not entity:
                                target = channel
                                if isinstance(channel, str) and channel.startswith('+'):
                                    target = f"https://t.me/{channel}"
                                entity = await self.client.get_entity(target)
                        except Exception as e:
                            logger.error(f"Даже после вступления не удалось найти {channel}: {e}")
                    
                    # Если после всех попыток entity нет — сдаемся
                    if not entity:
                        logger.error(f"⛔ Пропускаем канал {channel}: не удалось получить доступ.")
                        break

                    # ---------------------------------------------------------
                    # ЭТАП 3: ПАРСИНГ СООБЩЕНИЙ
                    # ---------------------------------------------------------
                    # ✅ ИЗМЕНЕНИЕ 2: ЧИНИМ РАБОТУ КОМАНДЫ /run
                    if hours is not None:
                        # Если запуск ручной (через /run или кнопку) — игнорируем чекпоинты
                        offset_date = datetime.now(timezone.utc) - timedelta(hours=hours)
                    else:
                        # Если запуск автоматический — используем чекпоинты
                        offset_date = since

                    kwargs = {"limit": 100, "reverse": True, "offset_date": offset_date}
                    if last_id > 0:
                        kwargs["offset_id"] = last_id
                    
                    async for message in self.client.iter_messages(entity, **kwargs):
                        last_id = message.id
                        
                        if not message.date: continue
                        
                        msg_date = message.date
                        if msg_date.tzinfo is None:
                            msg_date = msg_date.replace(tzinfo=timezone.utc)

                        m_type = None
                        if message.photo: m_type = "photo"
                        elif message.video: m_type = "video"
                        elif message.voice: m_type = "voice"
                        elif message.audio: m_type = "audio"
                        elif message.document: m_type = "document"

                        if message.text or m_type:
                            source_id = getattr(entity, 'id', channel)
                            formatted_text = html.unparse(message.text, message.entities) if message.text else ""

                            yield CollectedMessage(
                                source=str(source_id),
                                text=message.text or "",
                                fmt_text=formatted_text,
                                created_at=msg_date,
                                media_group_id=str(message.grouped_id) if message.grouped_id else None,
                                message_id=message.id,
                                media_type=m_type
                            )
                    
                    break # Успех
                    
                except FloodWaitError as exc:
                    retries += 1
                    wait_time = exc.seconds + 5
                    if wait_time > 300:
                        logger.error(f"FloodWait слишком долгий ({wait_time}с), пропускаем {channel}")
                        break
                    logger.warning(f"FloodWait на {channel}, ждем {wait_time} сек...")
                    await asyncio.sleep(wait_time)
                    
                except Exception as e:
                    retries += 1
                    logger.exception(f"Ошибка при обработке {channel}, попытка {retries}/3: {e}")
                    await asyncio.sleep(2)

    async def publish(self, channel: str, text: str) -> None:
        """
        Базовый метод публикации (используется как запасной).
        Основная логика красивой публикации находится в ProcessingService.
        """
        for attempt in range(1, 4):
            try:
                target_entity = int(channel) if isinstance(channel, str) and channel.lstrip('-').isdigit() else channel
                await self.client.send_message(
                    entity=target_entity, 
                    message=text, 
                    parse_mode='html', 
                    link_preview=False
                )
                return
            except FloodWaitError as exc:
                if exc.seconds > 300:
                    logger.error("Publish FloodWait too long (%s sec), aborting", exc.seconds)
                    return
                await asyncio.sleep(exc.seconds)
            except Exception:
                logger.exception("Unexpected error during simple publish to %s", channel)
                await asyncio.sleep(2)