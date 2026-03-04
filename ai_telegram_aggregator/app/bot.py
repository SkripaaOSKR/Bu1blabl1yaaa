"""Telegram bot control commands (/run, /status, /stats)."""
from __future__ import annotations

import logging

from telethon import TelegramClient, events

logger = logging.getLogger(__name__)


def register_bot_handlers(client: TelegramClient, run_callback, status_callback, stats_callback) -> None:
    """Attach bot handlers to telegram client."""

    @client.on(events.NewMessage(pattern=r"/run (\d+)"))
    async def run_handler(event):
        hours = int(event.pattern_match.group(1))
        await run_callback(hours)
        await event.reply(f"Запуск обработки за {hours} часов завершен")

    @client.on(events.NewMessage(pattern=r"/status"))
    async def status_handler(event):
        await event.reply(await status_callback())

    @client.on(events.NewMessage(pattern=r"/stats"))
    async def stats_handler(event):
        await event.reply(await stats_callback())

    logger.info("Bot handlers registered")
