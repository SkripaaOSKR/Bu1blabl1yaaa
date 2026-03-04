"""Telegram collection and publication adapters."""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator

from telethon import TelegramClient
from telethon.errors import FloodWaitError

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class CollectedMessage:
    source: str
    text: str
    created_at: datetime
    media_group_id: str | None


class TelegramCollector:
    """Collects messages from source channels and publishes results."""

    def __init__(self, api_id: int, api_hash: str, session_name: str) -> None:
        self.client = TelegramClient(session_name, api_id, api_hash)

    async def __aenter__(self) -> "TelegramCollector":
        await self.client.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.client.disconnect()

    async def iter_messages(
        self,
        channels: list[str],
        hours: int,
        since_timestamp: datetime | None = None,
    ) -> AsyncIterator[CollectedMessage]:
        since = since_timestamp if since_timestamp is not None else datetime.now(timezone.utc) - timedelta(hours=hours)
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)

        for channel in channels:
            retries = 0
            while retries < 3:
                try:
                    async for message in self.client.iter_messages(channel, offset_date=None):
                        message_date = message.date
                        if not message_date:
                            continue
                        if message_date.tzinfo is None:
                            message_date = message_date.replace(tzinfo=timezone.utc)

                        if message_date <= since:
                            break

                        if message.text:
                            yield CollectedMessage(
                                source=channel,
                                text=message.text,
                                created_at=message_date,
                                media_group_id=str(message.grouped_id) if message.grouped_id else None,
                            )
                    break
                except FloodWaitError as exc:
                    logger.warning("FloodWait on %s, sleeping %s sec", channel, exc.seconds)
                    await asyncio.sleep(exc.seconds)
                except Exception:
                    retries += 1
                    logger.exception("Unexpected error while fetching channel=%s retry=%s", channel, retries)
                    await asyncio.sleep(1)
            if retries >= 3:
                logger.error("Failed channel batch: %s", channel)

    async def publish(self, channel: str, text: str) -> None:
        for attempt in range(1, 4):
            try:
                await self.client.send_message(entity=channel, message=text)
                return
            except FloodWaitError as exc:
                logger.warning(
                    "FloodWait on publish channel=%s attempt=%s/3, sleeping %s sec",
                    channel,
                    attempt,
                    exc.seconds,
                )
                await asyncio.sleep(exc.seconds)

        logger.error("Failed to publish message to %s after 3 FloodWait retries", channel)
